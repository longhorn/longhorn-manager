package controller

import (
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fake "k8s.io/client-go/kubernetes/fake"

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
	for _, annotation := range []string{"", "2026-10-14T08:00:00Z", startingAt} {
		t.Run(annotation, func(t *testing.T) {
			f := newStartingAtFixture(t)
			bim := &longhorn.BackingImageManager{
				Name: "bim-starting-at", Namespace: TestNamespace,
				Spec: longhorn.BackingImageManagerSpec{
					Image: "bim-image", NodeID: TestNode1, DiskUUID: TestDiskID1,
					DiskPath: TestDefaultDataPath, StartingAt: startingAt,
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
			if annotation != "" {
				pod.Annotations = map[string]string{annotationKey: annotation}
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
			require.NoError(t, c.syncBackingImageManagerPod(bim, backoff))

			if annotation == startingAt {
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
			require.NoError(t, c.syncBackingImageManagerPod(bim, backoff))
			replacement, err := f.kube.CoreV1().Pods(TestNamespace).Get(t.Context(), bim.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, startingAt, replacement.Annotations[annotationKey])
			require.Equal(t, longhorn.BackingImageManagerStateStarting, bim.Status.CurrentState)
		})
	}
}
