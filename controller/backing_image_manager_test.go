package controller

import (
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

type startingAtFixture struct {
	ds        *datastore.DataStore
	kube      *fake.Clientset
	lh        *lhfake.Clientset
	factories *util.InformerFactories
}

func newStartingAtFixture(t *testing.T) *startingAtFixture {
	t.Helper()
	previousSkipListerCheck := datastore.SkipListerCheck
	datastore.SkipListerCheck = true
	t.Cleanup(func() { datastore.SkipListerCheck = previousSkipListerCheck })

	kube := fake.NewClientset()
	lh := lhfake.NewSimpleClientset() // nolint: staticcheck
	factories := util.NewInformerFactories(TestNamespace, kube, lh, controller.NoResyncPeriodFunc())
	return &startingAtFixture{
		ds:        datastore.NewDataStoreForGlobal(TestNamespace, lh, kube, apiextensionsfake.NewClientset(), factories),
		kube:      kube,
		lh:        lh,
		factories: factories,
	}
}

func TestBackingImageManagerStartingAtPodRecreation(t *testing.T) {
	const startingAt = "2026-10-15T08:00:00Z"
	annotationKey := types.GetLonghornLabelKey(types.BackingImageManagerStartingAtAnnotationKeySuffix)
	require.Equal(t, "longhorn.io/backing-image-manager-starting-at", annotationKey)
	for _, tc := range []struct {
		name        string
		diskReadyAt string
		annotation  string
	}{
		{name: "missing annotation", diskReadyAt: startingAt},
		{name: "stale annotation", diskReadyAt: startingAt, annotation: "2026-10-14T08:00:00Z"},
		{name: "matching annotation", diskReadyAt: startingAt, annotation: startingAt},
		{name: "no Ready timestamp with stale annotation", annotation: "2026-10-14T08:00:00Z"},
		{name: "no Ready timestamp without annotation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStartingAtFixture(t)
			bim := &longhorn.BackingImageManager{
				Name: "bim-starting-at", Namespace: TestNamespace,
				Spec: longhorn.BackingImageManagerSpec{
					Image: "bim-image", NodeID: TestNode1, DiskUUID: TestDiskID1,
					DiskPath: TestDefaultDataPath,
				},
				Status: longhorn.BackingImageManagerStatus{
					CurrentState: longhorn.BackingImageManagerStateRunning,
					APIVersion:   1,
					BackingImageFileMap: map[string]longhorn.BackingImageFileInfo{
						"image": {State: longhorn.BackingImageStateReady},
					},
				},
			}
			pod := &corev1.Pod{
				Name: bim.Name, Namespace: TestNamespace,
				Spec: corev1.PodSpec{NodeName: TestNode1},
				Status: corev1.PodStatus{
					Phase:             corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
				},
			}
			if tc.annotation != "" {
				pod.Annotations = map[string]string{annotationKey: tc.annotation}
			}
			require.NoError(t, f.kube.Tracker().Add(pod))
			pods := f.factories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
			require.NoError(t, pods.Add(pod))
			stopCh := make(chan struct{})
			c := &BackingImageManagerController{
				baseController:     newBaseController("test-bim-starting-at", logrus.StandardLogger()),
				namespace:          TestNamespace,
				controllerID:       TestNode1,
				bimImageName:       bim.Spec.Image,
				eventRecorder:      record.NewFakeRecorder(100),
				ds:                 f.ds,
				lock:               new(sync.RWMutex),
				monitorMap:         map[string]chan struct{}{bim.Name: stopCh},
				podRecreateBackoff: flowcontrol.NewBackOff(time.Second, time.Minute),
			}
			backoff := flowcontrol.NewBackOff(time.Second, time.Minute)
			require.NoError(t, c.syncBackingImageManagerPod(bim, tc.diskReadyAt, backoff))

			if tc.diskReadyAt == "" || tc.annotation == tc.diskReadyAt {
				require.Equal(t, longhorn.BackingImageManagerStateRunning, bim.Status.CurrentState)
				require.Equal(t, longhorn.BackingImageStateReady, bim.Status.BackingImageFileMap["image"].State)
				for _, action := range f.kube.Actions() {
					require.NotEqual(t, "delete", action.GetVerb())
				}
				return
			}

			require.Equal(t, longhorn.BackingImageManagerStateError, bim.Status.CurrentState)
			require.Equal(t, longhorn.BackingImageStateUnknown, bim.Status.BackingImageFileMap["image"].State)
			require.False(t, c.isMonitoring(bim.Name))
			select {
			case <-stopCh:
			default:
				t.Fatal("the stale pod must no longer be monitored")
			}
			_, err := f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), bim.Name, metav1.GetOptions{})
			require.True(t, apierrors.IsNotFound(err))

			// Observe deletion, then let the existing state machine create the replacement.
			require.NoError(t, pods.Delete(pod))
			require.NoError(t, c.syncBackingImageManagerPod(bim, tc.diskReadyAt, backoff))
			replacement, err := f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), bim.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.diskReadyAt, replacement.Annotations[annotationKey])
			require.Equal(t, longhorn.BackingImageManagerStateStarting, bim.Status.CurrentState)
		})
	}
}

func TestBackingImageManagerDiskReadyTransitions(t *testing.T) {
	const oldReadyAt = "2026-10-14T08:00:00Z"
	const readyAt = "2026-10-15T08:00:00Z"
	const nextReadyAt = "2026-10-16T08:00:00Z"
	f := newStartingAtFixture(t)
	annotationKey := types.GetLonghornLabelKey(types.BackingImageManagerStartingAtAnnotationKeySuffix)
	node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node.Status.DiskStatus[TestDiskID1].Conditions = []longhorn.Condition{
		{Type: longhorn.DiskConditionTypeReady, Status: longhorn.ConditionStatusFalse, LastTransitionTime: oldReadyAt},
	}
	nodes := f.factories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	require.NoError(t, f.lh.Tracker().Add(node.DeepCopy()))
	require.NoError(t, nodes.Add(node.DeepCopy()))
	bim := &longhorn.BackingImageManager{
		Name: "bim-disk-ready", Namespace: TestNamespace,
		Labels: types.GetBackingImageManagerLabels(TestNode1, TestDiskID1),
		Spec: longhorn.BackingImageManagerSpec{
			Image: "bim-image", NodeID: TestNode1, DiskUUID: TestDiskID1, DiskPath: TestDefaultDataPath,
		},
		Status: longhorn.BackingImageManagerStatus{
			OwnerID: TestNode1, CurrentState: longhorn.BackingImageManagerStateRunning, APIVersion: 1,
			BackingImageFileMap: map[string]longhorn.BackingImageFileInfo{
				"image": {State: longhorn.BackingImageStateReady},
			},
		},
	}
	bims := f.factories.LhInformerFactory.Longhorn().V1beta2().BackingImageManagers().Informer().GetIndexer()
	require.NoError(t, f.lh.Tracker().Add(bim.DeepCopy()))
	require.NoError(t, bims.Add(bim.DeepCopy()))
	pod := &corev1.Pod{
		Name: bim.Name, Namespace: TestNamespace, UID: "original-pod",
		Annotations: map[string]string{annotationKey: oldReadyAt},
		Spec:        corev1.PodSpec{NodeName: TestNode1},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Ready: true}},
		},
	}
	pods := f.factories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	require.NoError(t, f.kube.Tracker().Add(pod.DeepCopy()))
	require.NoError(t, pods.Add(pod.DeepCopy()))
	stopCh := make(chan struct{})
	c := &BackingImageManagerController{
		baseController: newBaseController("test-bim-disk-ready", logrus.StandardLogger()),
		namespace:      TestNamespace,
		controllerID:   TestNode1,
		bimImageName:   bim.Spec.Image,
		eventRecorder:  record.NewFakeRecorder(100),
		ds:             f.ds,
		lock:           new(sync.RWMutex),
		monitorMap:     map[string]chan struct{}{bim.Name: stopCh},
		// Avoid file RPCs; this test covers the pod lifecycle, not BIM protocol compatibility.
		versionUpdater: func(bim *longhorn.BackingImageManager) error {
			bim.Status.APIVersion = 1
			return nil
		},
		podRecreateBackoff: flowcontrol.NewBackOff(time.Second, time.Minute),
	}
	t.Cleanup(c.queue.ShutDown)
	key := TestNamespace + "/" + bim.Name
	reloadBIM := func() *longhorn.BackingImageManager {
		t.Helper()
		persisted, err := f.lh.LonghornV1beta2().BackingImageManagers(TestNamespace).Get(t.Context(), bim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, bims.Update(persisted.DeepCopy()))
		require.Equal(t, bim.Spec, persisted.Spec, "disk transitions must not write BIM spec")
		return persisted
	}
	setDiskReady := func(status longhorn.ConditionStatus, transitionTime string) {
		t.Helper()
		node.Status.DiskStatus[TestDiskID1].Conditions[0] = longhorn.Condition{
			Type: longhorn.DiskConditionTypeReady, Status: status, LastTransitionTime: transitionTime,
		}
		persisted, err := f.lh.LonghornV1beta2().Nodes(TestNamespace).UpdateStatus(t.Context(), node, metav1.UpdateOptions{})
		require.NoError(t, err)
		require.NoError(t, nodes.Update(persisted.DeepCopy()))
	}

	require.NoError(t, c.syncBackingImageManager(key))
	require.Equal(t, longhorn.BackingImageManagerStateUnknown, reloadBIM().Status.CurrentState)
	_, err := f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err, "an unavailable disk must not cause pod recreation")

	setDiskReady(longhorn.ConditionStatusTrue, readyAt)
	c.enqueueForLonghornNode(node)
	require.Equal(t, 1, c.queue.Len(), "the existing Node watch must enqueue the BIM")
	queued, shutdown := c.queue.Get()
	require.False(t, shutdown)
	require.Equal(t, key, queued)
	c.queue.Done(queued)

	conflict := true
	f.lh.PrependReactor("update", "backingimagemanagers", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" && conflict {
			conflict = false
			return true, nil, apierrors.NewConflict(longhorn.Resource("backingimagemanagers"), bim.Name, nil)
		}
		return false, nil, nil
	})
	require.NoError(t, c.syncBackingImageManager(key))
	require.False(t, conflict)
	_, err = f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	require.Equal(t, longhorn.BackingImageManagerStateUnknown, reloadBIM().Status.CurrentState, "the injected status conflict leaves the persisted state unchanged")

	// Reload persisted state after deletion and the status conflict, then recreate the pod.
	require.NoError(t, pods.Delete(pod))
	require.NoError(t, c.syncBackingImageManager(key))
	require.Equal(t, longhorn.BackingImageManagerStateStarting, reloadBIM().Status.CurrentState)
	replacement, err := f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, readyAt, replacement.Annotations[annotationKey])
	replacement.UID = "replacement-pod"
	replacement.Status = pod.Status
	replacement, err = f.kube.CoreV1().Pods(TestNamespace).UpdateStatus(t.Context(), replacement, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, pods.Add(replacement.DeepCopy()))

	// Repeated full reconciliations must preserve the replacement for the same Ready timestamp.
	for range 3 {
		require.NoError(t, c.syncBackingImageManager(key))
		require.Equal(t, longhorn.BackingImageManagerStateRunning, reloadBIM().Status.CurrentState)
		current, err := f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, replacement.UID, current.UID)
		require.Equal(t, readyAt, current.Annotations[annotationKey])
	}

	setDiskReady(longhorn.ConditionStatusFalse, "2026-10-15T09:00:00Z")
	require.NoError(t, c.syncBackingImageManager(key))
	require.Equal(t, longhorn.BackingImageManagerStateUnknown, reloadBIM().Status.CurrentState)
	_, err = f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	setDiskReady(longhorn.ConditionStatusTrue, nextReadyAt)
	require.NoError(t, c.syncBackingImageManager(key))
	require.Equal(t, longhorn.BackingImageManagerStateError, reloadBIM().Status.CurrentState)
	_, err = f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "a later Ready transition must invalidate the previous replacement")
}
