package controller

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newTestKubernetesPodController(
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories) (*KubernetesPodController, error) {
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	kc, err := NewKubernetesPodController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient)
	if err != nil {
		return nil, err
	}

	kc.eventRecorder = record.NewFakeRecorder(100)
	for index := range kc.cacheSyncs {
		kc.cacheSyncs[index] = alwaysReady
	}

	return kc, nil
}

type fakeShareManagerHealthServer struct {
	healthpb.UnimplementedHealthServer
	status healthpb.HealthCheckResponse_ServingStatus
}

func (s *fakeShareManagerHealthServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: s.status}, nil
}

func startFakeShareManagerHealthServer(c *C, status healthpb.HealthCheckResponse_ServingStatus) func() {
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(engineapi.ShareManagerDefaultPort))
	lis, err := net.Listen("tcp", address)
	c.Assert(err, IsNil)

	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, &fakeShareManagerHealthServer{status: status})

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	return func() {
		grpcServer.Stop()
		_ = lis.Close()
	}
}

func (s *TestSuite) TestHandlePodDeletionForRWXVolumeRemountScenarios(c *C) {
	datastore.SkipListerCheck = true
	defer func() {
		datastore.SkipListerCheck = false
	}()

	testCases := map[string]struct {
		shareManagerPodExists      bool
		shareManagerPodNoStartTime bool
		shareManagerPodStartsAt    time.Duration
		shareManagerServing        bool
		shareManagerCheckErr       bool
		endpointNetworkRWX         bool
		remountRequestedAtOffset   time.Duration
		expectPodDeletion          bool
		description                string
	}{
		"auto-salvage: share-manager pod not restarted, skip deletion": {
			shareManagerPodExists:   true,
			shareManagerPodStartsAt: -20 * time.Second,
			shareManagerServing:     true,
			expectPodDeletion:       false,
			description:             "NFS server still running, client can recover",
		},
		"failover: share-manager pod restarted and serving after remount request, delete workload pod": {
			shareManagerPodExists:   true,
			shareManagerPodStartsAt: 5 * time.Second,
			shareManagerServing:     true,
			expectPodDeletion:       true,
			description:             "Replacement share-manager is ready to serve I/O",
		},
		"failover in progress: share-manager pod not found, skip deletion": {
			shareManagerPodExists: false,
			expectPodDeletion:     false,
			description:           "Wait for replacement share-manager pod before remounting workload",
		},
		"failover in progress: share-manager pod has no start time, skip deletion": {
			shareManagerPodExists:      true,
			shareManagerPodNoStartTime: true,
			expectPodDeletion:          false,
			description:                "Wait for replacement share-manager pod to start before remounting workload",
		},
		"failover in progress: replacement pod not serving yet, skip deletion": {
			shareManagerPodExists:   true,
			shareManagerPodStartsAt: 5 * time.Second,
			shareManagerServing:     false,
			expectPodDeletion:       false,
			description:             "Give replacement share-manager time to become ready",
		},
		"failover in progress: serving check error, skip deletion": {
			shareManagerPodExists:   true,
			shareManagerPodStartsAt: 5 * time.Second,
			shareManagerCheckErr:    true,
			expectPodDeletion:       false,
			description:             "Retry when serving state cannot be determined yet",
		},
		"endpoint-network RWX: use existing remount path without serving gate": {
			shareManagerPodExists:   true,
			shareManagerPodStartsAt: 5 * time.Second,
			endpointNetworkRWX:      true,
			expectPodDeletion:       true,
			description:             "Endpoint-network RWX should not wait for share-manager gRPC serving gate here",
		},
		"refreshed remount request after replacement pod start: skip deletion with current timestamp gate": {
			shareManagerPodExists:    true,
			shareManagerPodStartsAt:  5 * time.Second,
			shareManagerServing:      true,
			remountRequestedAtOffset: 10 * time.Second,
			expectPodDeletion:        false,
			description:              "Current behavior keeps the workload when RemountRequestedAt is refreshed after replacement start",
		},
	}

	for name, tc := range testCases {
		c.Logf("Running test case: %s", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset() //nolint:staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset()
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
		c.Assert(err, IsNil)

		referenceTime := time.Now().UTC()
		baseTime := referenceTime.Add(-2 * time.Minute)
		initialRemountRequestedAtTime := baseTime.Add(30 * time.Second)
		remountRequestedAtTime := initialRemountRequestedAtTime.Add(tc.remountRequestedAtOffset)
		remountRequestedAt := remountRequestedAtTime.UTC().Format(time.RFC3339)
		podStartTime := metav1.NewTime(baseTime)

		vol := newVolume(TestVolumeName, 1)
		vol.Namespace = TestNamespace
		vol.Spec.AccessMode = longhorn.AccessModeReadWriteMany
		vol.Spec.Migratable = false
		vol.Status.RemountRequestedAt = remountRequestedAt

		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: TestPVName,
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       types.LonghornDriverName,
						VolumeHandle: vol.Name,
					},
				},
			},
		}

		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      TestPVCName,
				Namespace: TestNamespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: pv.Name,
			},
		}

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      TestPod1,
				Namespace: TestNamespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: appsv1.SchemeGroupVersion.String(),
						Kind:       types.KubernetesKindDeployment,
						Name:       TestDeploymentName,
						Controller: ptrTo(true),
					},
				},
			},
			Spec: corev1.PodSpec{
				NodeName: TestNode1,
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvc.Name,
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{
				StartTime: &podStartTime,
			},
		}

		autoDeleteSetting := newSetting(string(types.SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly), "true")
		endpointNetworkSetting := newSetting(string(types.SettingNameEndpointNetworkForRWXVolume), "")
		if tc.endpointNetworkRWX {
			endpointNetworkSetting.Value = "10.0.0.1/24"
		}

		c.Assert(informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(autoDeleteSetting), IsNil)
		c.Assert(informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(endpointNetworkSetting), IsNil)
		c.Assert(informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer().Add(vol), IsNil)
		c.Assert(informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer().Add(pvc), IsNil)
		c.Assert(informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer().Add(pv), IsNil)
		c.Assert(informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod), IsNil)

		_, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), autoDeleteSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), endpointNetworkSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), vol, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		var stopFakeHealthServer func()
		if tc.shareManagerPodExists && !tc.shareManagerPodNoStartTime && !tc.endpointNetworkRWX && tc.shareManagerPodStartsAt > 0 && !tc.shareManagerCheckErr {
			status := healthpb.HealthCheckResponse_NOT_SERVING
			if tc.shareManagerServing {
				status = healthpb.HealthCheckResponse_SERVING
			}
			stopFakeHealthServer = startFakeShareManagerHealthServer(c, status)
		}

		if tc.shareManagerPodExists {
			smPodName := types.GetShareManagerPodNameFromShareManagerName(vol.Name)
			smPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      smPodName,
					Namespace: TestNamespace,
				},
				Spec: corev1.PodSpec{
					NodeName: TestNode1,
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					PodIP: "127.0.0.1",
				},
			}
			if !tc.shareManagerPodNoStartTime {
				smPodStartTime := metav1.NewTime(initialRemountRequestedAtTime.Add(tc.shareManagerPodStartsAt))
				smPod.Status.StartTime = &smPodStartTime
			}

			smPod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Name:  "share-manager",
					Ready: tc.shareManagerServing,
				},
			}
			smPod.Status.Conditions = []corev1.PodCondition{{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(referenceTime.Add(-time.Minute)),
			}}

			c.Assert(informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(smPod), IsNil)
			_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), smPod, metav1.CreateOptions{})
			c.Assert(err, IsNil)
		}

		kubeClient.ClearActions()

		err = kc.handlePodDeletionIfVolumeRequestRemount(pod)
		c.Assert(err, IsNil)

		actions := kubeClient.Actions()
		if tc.expectPodDeletion {
			c.Assert(actions, HasLen, 1, Commentf("Test case: %s - %s", name, tc.description))
			c.Assert(actions[0].GetVerb(), Equals, "delete", Commentf("Test case: %s", name))
			c.Assert(actions[0].GetResource().Resource, Equals, "pods", Commentf("Test case: %s", name))
		} else {
			c.Assert(actions, HasLen, 0, Commentf("Test case: %s - %s - should not delete pod", name, tc.description))
		}

		if stopFakeHealthServer != nil {
			stopFakeHealthServer()
		}
	}
}

func ptrTo[T any](v T) *T {
	return &v
}

func newCSIPluginPod(name, nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind: types.KubernetesKindDaemonSet,
					Name: types.CSIPluginName,
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

// TestEnqueuePodChangeEnqueuesCSIPluginPodOnAnyNode verifies that the global
// manager enqueues a CSI plugin pod regardless of which node it runs on. The
// pre-split DaemonSet only enqueued the CSI plugin pod on its own node
// (`pod.Spec.NodeName == controllerID`); the single-mode leader must observe
// CSI plugin pod failures cluster-wide, so that filter is removed.
func (s *TestSuite) TestEnqueuePodChangeEnqueuesCSIPluginPodOnAnyNode(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)

	// CSI plugin pod on an arbitrary node; the leader observes every node.
	csiPod := newCSIPluginPod("longhorn-csi-plugin-abcde", TestNode2)
	kc.enqueuePodChange(csiPod)

	c.Assert(kc.queue.Len(), Equals, 1)
}

// TestHandlePodDeletionForRemountOnAnyNode verifies that a workload pod is
// force-deleted for a volume remount regardless of which node it runs on. The
// pre-split DaemonSet only acted on pods on its own node
// (`pod.Spec.NodeName != controllerID` early-returned); the single-mode
// global-manager leader is the single executor for every node's pods.
func (s *TestSuite) TestHandlePodDeletionForRemountOnAnyNode(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	vIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)

	// Enable auto-delete-pod-when-volume-detached-unexpectedly.
	setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(),
		newSetting(string(types.SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly), "true"), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(sIndexer.Add(setting), IsNil)

	// RWO volume (no RWX endpoint gating) with a remount requested 10s ago.
	volume := newVolume(TestVolumeName, 2)
	volume.Status.RemountRequestedAt = time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339)
	v, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(vIndexer.Add(v), IsNil)

	pvObj, err := kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), newPV(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvIndexer.Add(pvObj), IsNil)
	pvcObj, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), newPVC(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvcIndexer.Add(pvcObj), IsNil)

	// Workload pod controlled by a StatefulSet, on an arbitrary node, started
	// before the remount request.
	isController := true
	pod := newPodWithPVC(TestPod1)
	pod.OwnerReferences[0].Controller = &isController
	pod.Spec.NodeName = TestNode2
	startTime := metav1.NewTime(time.Now().Add(-60 * time.Second))
	pod.Status.StartTime = &startTime
	podObj, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pIndexer.Add(podObj), IsNil)

	err = kc.handlePodDeletionIfVolumeRequestRemount(podObj)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), TestPod1, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

// TestCleanupForceDeletedPodResourcesOnAnyNode verifies that the stale
// VolumeAttachment of a force-deleted pod is cleaned up regardless of which
// node the pod was on. The pre-split DaemonSet gated this with
// `isControllerResponsibleFor(controllerID, ...)`; the single-mode leader runs
// the cleanup for every node's force-deleted pods.
func (s *TestSuite) TestCleanupForceDeletedPodResourcesOnAnyNode(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
	vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)

	// PV without a claimRef -> the attachment is safe to delete.
	pv := newPV()
	pv.Spec.ClaimRef = nil
	pvObj, err := kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvIndexer.Add(pvObj), IsNil)

	pvcObj, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), newPVC(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvcIndexer.Add(pvcObj), IsNil)

	// Force-deleted workload pod (grace period 0) on an arbitrary node.
	gracePeriod := int64(0)
	deletionTime := metav1.Now()
	pod := newPodWithPVC(TestPod1)
	pod.Spec.NodeName = TestNode2
	pod.DeletionTimestamp = &deletionTime
	pod.DeletionGracePeriodSeconds = &gracePeriod

	// Attached Longhorn VolumeAttachment for that pod's PV on TestNode2.
	pvName := TestPVName
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-abcde"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: types.LonghornDriverName,
			NodeName: TestNode2,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	vaObj, err := kubeClient.StorageV1().VolumeAttachments().Create(context.TODO(), va, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(vaIndexer.Add(vaObj), IsNil)

	_, err = kc.cleanupForceDeletedPodResources(pod)
	c.Assert(err, IsNil)

	_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

// TestCleanupForceDeletedPodResourcesMultiAttachEvent verifies that the stale
// VolumeAttachment of a force-deleted pod is cleaned up when the replacement pod
// reports the conflict with the event wording of older ("Multi-Attach error")
// and newer ("Waiting for detach", kubernetes/kubernetes#138837) Kubernetes versions.
func (s *TestSuite) TestCleanupForceDeletedPodResourcesMultiAttachEvent(c *C) {
	type testCase struct {
		eventReason  string
		eventMessage string
		expectDelete bool
	}
	testCases := map[string]testCase{
		"multi-attach error (older Kubernetes)": {
			eventMessage: `Multi-Attach error for volume "` + TestPVName + `" Volume is already used by pod(s) ` + TestPod1,
			expectDelete: true,
		},
		"waiting for detach (Kubernetes with kubernetes/kubernetes#138837)": {
			eventMessage: `Waiting for detach for volume "` + TestPVName + `" Volume is already used by pod(s) ` + TestPod1,
			expectDelete: true,
		},
		"multi-attach caused by another pod": {
			eventMessage: `Waiting for detach for volume "` + TestPVName + `" Volume is already used by pod(s) other-pod`,
			expectDelete: false,
		},
		"waiting for detach without pod names": {
			eventMessage: `Waiting for detach for volume "` + TestPVName + `" Volume is already exclusively attached to one node, waiting on detach before it can be attached to another node`,
			expectDelete: false,
		},
		"unrelated attach failure": {
			eventMessage: `AttachVolume.Attach failed for volume "` + TestPVName + `" : rpc error, pod ` + TestPod1,
			expectDelete: false,
		},
		"aggregated waiting for detach event": {
			eventMessage: `(combined from similar events): Waiting for detach for volume "` + TestPVName + `" Volume is already used by pod(s) ` + TestPod1,
			expectDelete: true,
		},
		"waiting for detach text in an event with another reason": {
			eventReason:  "FailedMount",
			eventMessage: `Waiting for detach for volume "` + TestPVName + `" Volume is already used by pod(s) ` + TestPod1,
			expectDelete: false,
		},
	}

	for name, tc := range testCases {
		c.Logf("testing %v", name)

		if tc.eventReason == "" {
			tc.eventReason = "FailedAttachVolume"
		}

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
		pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()

		kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
		c.Assert(err, IsNil)

		// PV bound to the claim, so the cleanup has to find a conflicting pod.
		pvObj, err := kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), newPV(), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(pvIndexer.Add(pvObj), IsNil)

		pvc := newPVC()
		pvc.Namespace = TestNamespace
		pvcObj, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(pvcIndexer.Add(pvcObj), IsNil)

		// Force-deleted workload pod (grace period 0) on TestNode1.
		gracePeriod := int64(0)
		deletionTime := metav1.Now()
		oldPod := newPodWithPVC(TestPod1)
		oldPod.Spec.NodeName = TestNode1
		oldPod.DeletionTimestamp = &deletionTime
		oldPod.DeletionGracePeriodSeconds = &gracePeriod

		// Replacement pod on TestNode2, still Pending because the volume is
		// attached to TestNode1.
		newPod := newPodWithPVC(TestPod2)
		newPod.Spec.NodeName = TestNode2
		newPod.Status.Phase = corev1.PodPending
		newPodObj, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), newPod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(pIndexer.Add(newPodObj), IsNil)

		_, err = kubeClient.CoreV1().Events(TestNamespace).Create(context.TODO(), &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: TestPod2 + ".failedattach", Namespace: TestNamespace},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Name:      TestPod2,
				Namespace: TestNamespace,
			},
			Reason:  tc.eventReason,
			Type:    corev1.EventTypeWarning,
			Message: tc.eventMessage,
		}, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		pvName := TestPVName
		va := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-abcde"},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: types.LonghornDriverName,
				NodeName: TestNode1,
				Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		}
		vaObj, err := kubeClient.StorageV1().VolumeAttachments().Create(context.TODO(), va, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(vaIndexer.Add(vaObj), IsNil)

		_, err = kc.cleanupForceDeletedPodResources(oldPod)
		c.Assert(err, IsNil)

		_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
		if tc.expectDelete {
			c.Assert(apierrors.IsNotFound(err), Equals, true, Commentf("test case: %v", name))
		} else {
			c.Assert(err, IsNil, Commentf("test case: %v", name))
		}
	}
}

// TestCleanupForceDeletedPodResourcesAfterPodDeletion verifies the cleanup when the
// worker processes the key only after the pod is gone from the informer cache.
func (s *TestSuite) TestCleanupForceDeletedPodResourcesAfterPodDeletion(c *C) {
	testCases := map[string]struct {
		gracePeriod     int64
		podPhase        corev1.PodPhase
		nodeReady       bool
		nodeDown        bool
		nodeGoesDown    bool
		tombstone       bool
		replacementPod  bool
		replacementNode string
		expectVADelete  bool
		expectStateKept bool
		expectRequeue   bool
	}{
		"force-deleted pod": {
			expectVADelete: true,
		},
		"force-deleted pod in DeletedFinalStateUnknown": {
			tombstone:      true,
			expectVADelete: true,
		},
		"force-deleted pod replaced by a pod with the same name on another node": {
			replacementPod:  true,
			replacementNode: TestNode2,
			expectVADelete:  true,
		},
		"force-deleted pod replaced by a pod with the same name on the same node": {
			replacementPod:  true,
			replacementNode: TestNode1,
			expectVADelete:  false,
		},
		"force-deleted pod replaced by a pod with the same name that is not scheduled yet": {
			replacementPod:  true,
			expectVADelete:  false,
			expectStateKept: true,
		},
		"gracefully deleted pod": {
			gracePeriod:    30,
			expectVADelete: false,
		},
		"zero grace-period deletion on a healthy node": {
			// The final delete of a graceful termination also has grace period 0, and the
			// kubelet leaves the pod in a terminal phase. The node is up, so the cleanup must
			// skip it, not requeue, and let Kubernetes detach the volume.
			nodeReady:      true,
			podPhase:       corev1.PodSucceeded,
			expectVADelete: false,
			expectRequeue:  false,
		},
		"force-deleted pod on a node reported down": {
			// Node object present with Ready=False, not just missing, so the cleanup runs
			// through IsNodeDownOrDeleted's condition path rather than its not-found path.
			nodeDown:       true,
			expectVADelete: true,
		},
		"force-deleted pod while its node is not yet reported down": {
			// The node-monitor grace period has not elapsed, so the node still looks up and
			// the VolumeAttachment stays. The pod never reached a terminal phase, so the
			// cleanup is requeued and the state kept. Once the node is reported down the
			// retry cleans up (nodeGoesDown follow-up below).
			nodeReady:       true,
			podPhase:        corev1.PodRunning,
			nodeGoesDown:    true,
			expectVADelete:  false,
			expectStateKept: true,
			expectRequeue:   true,
		},
		"force-deleted pod replaced on another node while the old node is not yet down": {
			// The replacement landed on another node but the old node is still up, so the old
			// VolumeAttachment stays and the cleanup is requeued. Once the old node is reported
			// down the retry removes it. This is the StatefulSet-style replacement path.
			nodeReady:       true,
			podPhase:        corev1.PodRunning,
			replacementPod:  true,
			replacementNode: TestNode2,
			nodeGoesDown:    true,
			expectVADelete:  false,
			expectStateKept: true,
			expectRequeue:   true,
		},
	}

	for name, tc := range testCases {
		c.Logf("Running test case: %s", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
		pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
		vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()
		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

		kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
		c.Assert(err, IsNil)

		// Record delayed requeues instead of scheduling them, so the retry can be asserted.
		queue := &delayRecordingQueue{TypedRateLimitingInterface: kc.queue, delayed: map[any]time.Duration{}}
		kc.queue = queue

		// Without a node object the cleanup treats the node as deleted. A Ready node means
		// the pod's volume is detached by Kubernetes, so the force cleanup has to be skipped.
		if tc.nodeReady {
			node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
			c.Assert(nodeIndexer.Add(node), IsNil)
		}
		if tc.nodeDown {
			node := newNode(TestNode1, TestNamespace, false, longhorn.ConditionStatusFalse, string(longhorn.NodeConditionReasonKubernetesNodeNotReady))
			c.Assert(nodeIndexer.Add(node), IsNil)
		}

		// PV without a claimRef -> the attachment is safe to delete.
		pv := newPV()
		pv.Spec.ClaimRef = nil
		pvObj, err := kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(pvIndexer.Add(pvObj), IsNil)

		pvcObj, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), newPVC(), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(pvcIndexer.Add(pvcObj), IsNil)

		pvName := TestPVName
		va := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-abcde"},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: types.LonghornDriverName,
				NodeName: TestNode1,
				Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		}
		vaObj, err := kubeClient.StorageV1().VolumeAttachments().Create(context.TODO(), va, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(vaIndexer.Add(vaObj), IsNil)

		// Update event: the pod is in the cache with the deletion grace period set.
		deletionTime := metav1.Now()
		pod := newPodWithPVC(TestPod1)
		pod.UID = "old-pod-uid"
		pod.Spec.NodeName = TestNode1
		pod.DeletionTimestamp = &deletionTime
		pod.DeletionGracePeriodSeconds = ptrTo(tc.gracePeriod)
		if tc.podPhase != "" {
			pod.Status.Phase = tc.podPhase
		}
		c.Assert(pIndexer.Add(pod), IsNil)
		kc.enqueuePodChange(pod)

		// Delete event before the worker picks up the key: the informer removes the pod from
		// the cache, then the delete handler stores its last state.
		c.Assert(pIndexer.Delete(pod), IsNil)
		var newPod *corev1.Pod
		if tc.replacementPod {
			newPod = newPodWithPVC(TestPod1)
			newPod.UID = "new-pod-uid"
			newPod.Spec.NodeName = tc.replacementNode
			c.Assert(pIndexer.Add(newPod), IsNil)
		}
		var deletedObj interface{} = pod
		if tc.tombstone {
			deletedObj = cache.DeletedFinalStateUnknown{Key: TestNamespace + "/" + TestPod1, Obj: pod}
		}
		kc.enqueuePodDeletion(deletedObj)

		c.Assert(kc.queue.Len(), Equals, 1)
		c.Assert(kc.processNextWorkItem(), Equals, true)
		c.Assert(kc.queue.Len(), Equals, 0)

		_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
		if tc.expectVADelete {
			c.Assert(apierrors.IsNotFound(err), Equals, true, Commentf("Test case: %s", name))
		} else {
			c.Assert(err, IsNil, Commentf("Test case: %s", name))
		}

		_, found := kc.zeroGraceDeletedPods.Load(TestNamespace + "/" + TestPod1)
		c.Assert(found, Equals, tc.expectStateKept, Commentf("Test case: %s", name))

		key := TestNamespace + "/" + TestPod1
		if tc.expectRequeue {
			c.Assert(queue.delayed, DeepEquals, map[any]time.Duration{key: forceDeletedPodCleanupRetryInterval}, Commentf("Test case: %s", name))
		} else {
			c.Assert(queue.delayed, HasLen, 0, Commentf("Test case: %s", name))
		}

		if tc.expectStateKept && tc.replacementPod && tc.replacementNode == "" {
			// The replacement pod gets scheduled to another node, its update re-enqueues the key
			// because of the stored state, also for a pod without a Longhorn volume.
			newPod.Spec.NodeName = TestNode2
			newPod.Spec.Volumes = nil
			c.Assert(pIndexer.Update(newPod), IsNil)
			kc.enqueuePodChange(newPod)

			c.Assert(kc.queue.Len(), Equals, 1, Commentf("Test case: %s", name))
			c.Assert(kc.processNextWorkItem(), Equals, true)

			_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
			c.Assert(apierrors.IsNotFound(err), Equals, true, Commentf("Test case: %s", name))
			_, found = kc.zeroGraceDeletedPods.Load(TestNamespace + "/" + TestPod1)
			c.Assert(found, Equals, false, Commentf("Test case: %s", name))
		}

		if tc.nodeGoesDown {
			// The node is reported down within the grace period and the key fires again. The
			// retry now finds the node down, cleans up the VolumeAttachment and drops the state.
			node := newNode(TestNode1, TestNamespace, false, longhorn.ConditionStatusFalse, string(longhorn.NodeConditionReasonKubernetesNodeNotReady))
			c.Assert(nodeIndexer.Update(node), IsNil)
			kc.queue.Add(key)
			c.Assert(kc.processNextWorkItem(), Equals, true)

			_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
			c.Assert(apierrors.IsNotFound(err), Equals, true, Commentf("Test case: %s", name))
			_, found = kc.zeroGraceDeletedPods.Load(TestNamespace + "/" + TestPod1)
			c.Assert(found, Equals, false, Commentf("Test case: %s", name))
		}
	}
}

func (s *TestSuite) TestCleanupForceDeletedPodResourcesWithFinalizer(c *C) {
	testCases := map[string]struct {
		phase        corev1.PodPhase
		expireWindow bool
	}{
		"node goes down before the finalizer is removed": {phase: corev1.PodRunning},
		"retry window expires while the node stays up":   {phase: corev1.PodRunning, expireWindow: true},
		"succeeded pod does not need retries":            {phase: corev1.PodSucceeded},
		"failed pod does not need retries":               {phase: corev1.PodFailed},
	}
	for name, tc := range testCases {
		c.Logf("Running test case: %s", name)
		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
		pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
		vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()
		nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

		kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
		c.Assert(err, IsNil)
		defer kc.queue.ShutDown()
		queue := &delayRecordingQueue{TypedRateLimitingInterface: kc.queue, delayed: map[any]time.Duration{}}
		kc.queue = queue

		c.Assert(sIndexer.Add(newSetting(string(types.SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly), "false")), IsNil)
		c.Assert(nodeIndexer.Add(newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")), IsNil)
		c.Assert(pvIndexer.Add(newPV()), IsNil)
		pvc := newPVC()
		pvc.Namespace = TestNamespace
		c.Assert(pvcIndexer.Add(pvc), IsNil)

		// The pod may already have been terminating for longer than the retry window
		// when its deletion is forced. Its finalizer prevents a delete event.
		deletionTime := metav1.NewTime(time.Now().Add(-time.Hour))
		pod := newPodWithPVC(TestPod1)
		pod.UID = "finalizing-pod-uid"
		pod.Finalizers = []string{"example.com/cleanup"}
		pod.Spec.NodeName = TestNode1
		pod.Status.Phase = tc.phase
		pod.DeletionTimestamp = &deletionTime
		pod.DeletionGracePeriodSeconds = ptrTo(int64(0))
		c.Assert(pIndexer.Add(pod), IsNil)

		pendingPod := newPodWithPVC(TestPod2)
		pendingPod.Spec.NodeName = TestNode2
		pendingPod.Status.Phase = corev1.PodPending
		c.Assert(pIndexer.Add(pendingPod), IsNil)
		_, err = kubeClient.CoreV1().Events(TestNamespace).Create(context.TODO(), &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: TestPod2 + ".failedattach", Namespace: TestNamespace},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Pod", Name: TestPod2, Namespace: TestNamespace,
			},
			Reason:  "FailedAttachVolume",
			Type:    corev1.EventTypeWarning,
			Message: `Waiting for detach for volume "` + TestPVName + `" Volume is already used by pod(s) ` + TestPod1,
		}, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		pvName := TestPVName
		va, err := kubeClient.StorageV1().VolumeAttachments().Create(context.TODO(), &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-finalizing"},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: types.LonghornDriverName,
				NodeName: TestNode1,
				Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		}, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(vaIndexer.Add(va), IsNil)

		key := TestNamespace + "/" + TestPod1
		kc.enqueuePodChange(pod)
		c.Assert(kc.queue.Len(), Equals, 1)
		c.Assert(kc.processNextWorkItem(), Equals, true)
		_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		obj, found := kc.zeroGraceDeletedPods.Load(key)
		if tc.phase != corev1.PodRunning {
			c.Assert(found, Equals, false)
			c.Assert(queue.delayed, HasLen, 0)
			continue
		}
		c.Assert(found, Equals, true)
		c.Assert(queue.delayed, DeepEquals, map[any]time.Duration{key: forceDeletedPodCleanupRetryInterval})

		// A retry while the node is still up must preserve the original deadline.
		delete(queue.delayed, key)
		kc.queue.Add(key)
		c.Assert(kc.processNextWorkItem(), Equals, true)
		state, found := kc.zeroGraceDeletedPods.Load(key)
		c.Assert(found, Equals, true)
		c.Assert(state, Equals, obj)
		c.Assert(queue.delayed, DeepEquals, map[any]time.Duration{key: forceDeletedPodCleanupRetryInterval})

		if tc.expireWindow {
			staleState := *obj.(*deletedPodState)
			staleState.observedAt = time.Now().Add(-forceDeletedPodCleanupRetryPeriod - time.Minute)
			kc.zeroGraceDeletedPods.Store(key, &staleState)
		} else {
			c.Assert(nodeIndexer.Update(newNode(TestNode1, TestNamespace, false, longhorn.ConditionStatusFalse,
				string(longhorn.NodeConditionReasonKubernetesNodeNotReady))), IsNil)
		}
		// Only the scheduled retry fires: there is no further pod event.
		delete(queue.delayed, key)
		kc.queue.Add(key)
		c.Assert(kc.processNextWorkItem(), Equals, true)
		_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
		if tc.expireWindow {
			c.Assert(err, IsNil)
		} else {
			c.Assert(apierrors.IsNotFound(err), Equals, true)
		}
		_, found = kc.zeroGraceDeletedPods.Load(key)
		c.Assert(found, Equals, false)
		c.Assert(queue.delayed, HasLen, 0)
	}
}

// TestHandleErrKeepsNewerForceDeletedPodState verifies that dropping a key after
// maxRetries only removes the force-deleted pod state the failed sync worked on,
// not a newer one the delete handler stored during that sync.
func (s *TestSuite) TestHandleErrKeepsNewerForceDeletedPodState(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
	vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)

	// PV without a claimRef -> the cleanup deletes the attachment.
	pv := newPV()
	pv.Spec.ClaimRef = nil
	pvObj, err := kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvIndexer.Add(pvObj), IsNil)

	pvcObj, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), newPVC(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvcIndexer.Add(pvcObj), IsNil)

	pvName := TestPVName
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-abcde"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: types.LonghornDriverName,
			NodeName: TestNode1,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	c.Assert(vaIndexer.Add(va), IsNil)

	newForceDeletedPod := func(uid string) *corev1.Pod {
		deletionTime := metav1.Now()
		pod := newPodWithPVC(TestPod1)
		pod.UID = k8stypes.UID(uid)
		pod.Spec.NodeName = TestNode1
		pod.DeletionTimestamp = &deletionTime
		pod.DeletionGracePeriodSeconds = ptrTo(int64(0))
		return pod
	}
	key := TestNamespace + "/" + TestPod1
	newerState := &deletedPodState{pod: newForceDeletedPod("newer-pod-uid"), observedAt: time.Now()}

	// The delete handler stores a newer state while the last retry deletes the attachment.
	kubeClient.PrependReactor("delete", "volumeattachments", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		kc.zeroGraceDeletedPods.Store(key, newerState)
		return true, nil, fmt.Errorf("delete failed")
	})

	kc.enqueuePodDeletion(newForceDeletedPod("failed-pod-uid"))
	for i := 0; i < maxRetries; i++ {
		kc.queue.AddRateLimited(key)
	}
	c.Assert(kc.processNextWorkItem(), Equals, true)

	obj, found := kc.zeroGraceDeletedPods.Load(key)
	c.Assert(found, Equals, true)
	c.Assert(obj, Equals, newerState)
}

// TestCleanupKeepsNewerForceDeletedPodState verifies that cleanupDeletedForceDeletedPod only
// drops the state it worked on. If the delete handler stored a newer state meanwhile, that
// newer state must survive so its own sync still runs. This is the CompareAndDelete contract
// in the cleanup path (handleErr covers the drop-after-maxRetries path separately).
func (s *TestSuite) TestCleanupKeepsNewerForceDeletedPodState(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
	vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)

	pv := newPV()
	pv.Spec.ClaimRef = nil
	pvObj, err := kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvIndexer.Add(pvObj), IsNil)
	pvcObj, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), newPVC(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvcIndexer.Add(pvcObj), IsNil)

	pvName := TestPVName
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-abcde"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: types.LonghornDriverName,
			NodeName: TestNode1,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	vaObj, err := kubeClient.StorageV1().VolumeAttachments().Create(context.TODO(), va, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(vaIndexer.Add(vaObj), IsNil)

	forcePod := func(uid string) *corev1.Pod {
		deletionTime := metav1.Now()
		pod := newPodWithPVC(TestPod1)
		pod.UID = k8stypes.UID(uid)
		pod.Spec.NodeName = TestNode1
		pod.DeletionTimestamp = &deletionTime
		pod.DeletionGracePeriodSeconds = ptrTo(int64(0))
		return pod
	}
	key := TestNamespace + "/" + TestPod1

	// No Longhorn node object means the node counts as down, so the cleanup runs and would
	// drop the state. The sync works on oldState while newerState is already stored.
	oldState := &deletedPodState{pod: forcePod("old-pod-uid"), observedAt: time.Now()}
	newerState := &deletedPodState{pod: forcePod("newer-pod-uid"), observedAt: time.Now()}
	kc.zeroGraceDeletedPods.Store(key, newerState)

	c.Assert(kc.cleanupDeletedForceDeletedPod(key, nil, oldState), IsNil)

	// The old pod's attachment is cleaned up (node down), but the newer state is kept.
	_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)
	obj, found := kc.zeroGraceDeletedPods.Load(key)
	c.Assert(found, Equals, true)
	c.Assert(obj, Equals, newerState)
}

// TestForceDeletedPodCleanupRetryWindowExpires verifies that the cleanup stops requeuing and
// drops the state once the retry window from the observed deletion has passed, even while the
// node is still up and the pod is not terminal.
func (s *TestSuite) TestForceDeletedPodCleanupRetryWindowExpires(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)
	queue := &delayRecordingQueue{TypedRateLimitingInterface: kc.queue, delayed: map[any]time.Duration{}}
	kc.queue = queue

	// Node is up, so without the window a force-deleted pod on it would be requeued.
	c.Assert(nodeIndexer.Add(newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")), IsNil)

	pvName := TestPVName
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-abcde"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: types.LonghornDriverName,
			NodeName: TestNode1,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	vaObj, err := kubeClient.StorageV1().VolumeAttachments().Create(context.TODO(), va, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(vaIndexer.Add(vaObj), IsNil)

	deletionTime := metav1.Now()
	pod := newPodWithPVC(TestPod1)
	pod.UID = "old-pod-uid"
	pod.Spec.NodeName = TestNode1
	pod.Status.Phase = corev1.PodRunning
	pod.DeletionTimestamp = &deletionTime
	pod.DeletionGracePeriodSeconds = ptrTo(int64(0))
	key := TestNamespace + "/" + TestPod1

	// Observed longer ago than the retry window.
	staleState := &deletedPodState{pod: pod, observedAt: time.Now().Add(-forceDeletedPodCleanupRetryPeriod - time.Minute)}
	kc.zeroGraceDeletedPods.Store(key, staleState)

	c.Assert(kc.cleanupDeletedForceDeletedPod(key, nil, staleState), IsNil)

	// Window expired: no requeue, state dropped, VolumeAttachment left to Kubernetes (node up).
	c.Assert(queue.delayed, HasLen, 0)
	_, found := kc.zeroGraceDeletedPods.Load(key)
	c.Assert(found, Equals, false)
	_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
}

// TestSyncSkipsReplacementPodWithoutLonghornVolume verifies that a replacement pod
// without a Longhorn volume, enqueued only because of a stored force-deleted pod
// state, does not reach the handlers for Longhorn workload pods.
func (s *TestSuite) TestSyncSkipsReplacementPodWithoutLonghornVolume(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)

	// The deleted pod's PVC and Longhorn PV, so enqueuePodDeletion recognises it as a
	// Longhorn-volume pod and snapshots its state.
	c.Assert(pvIndexer.Add(newPV()), IsNil)
	pvc := newPVC()
	pvc.Namespace = TestNamespace
	c.Assert(pvcIndexer.Add(pvc), IsNil)

	// The node-down policy would force-delete a terminating StatefulSet pod on TestNode2,
	// which has no Longhorn node and counts as down.
	setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(),
		newSetting(string(types.SettingNameNodeDownPodDeletionPolicy), string(types.NodeDownPodDeletionPolicyDeleteBothStatefulsetAndDeploymentPod)),
		metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(sIndexer.Add(setting), IsNil)

	isController := true
	deletionTime := metav1.NewTime(time.Now().Add(-60 * time.Second))
	deletedPod := newPodWithPVC(TestPod1)
	deletedPod.UID = "old-pod-uid"
	deletedPod.Spec.NodeName = TestNode1
	deletedPod.DeletionTimestamp = &deletionTime
	deletedPod.DeletionGracePeriodSeconds = ptrTo(int64(0))
	kc.enqueuePodDeletion(deletedPod)

	newPod := newPodWithPVC(TestPod1)
	newPod.UID = "new-pod-uid"
	newPod.OwnerReferences[0].Controller = &isController
	newPod.Spec.NodeName = TestNode2
	newPod.Spec.Volumes = nil
	newPod.DeletionTimestamp = &deletionTime
	newPod.DeletionGracePeriodSeconds = ptrTo(int64(30))
	podObj, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), newPod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pIndexer.Add(podObj), IsNil)
	kc.enqueuePodChange(podObj)

	c.Assert(kc.queue.Len(), Equals, 1)
	c.Assert(kc.processNextWorkItem(), Equals, true)

	_, found := kc.zeroGraceDeletedPods.Load(TestNamespace + "/" + TestPod1)
	c.Assert(found, Equals, false)
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), TestPod1, metav1.GetOptions{})
	c.Assert(err, IsNil)
}

// TestCleanupForceDeletedPodResourcesRetriesFailedEventLookup verifies that a failed
// event lookup keeps the force-deleted pod state and the next sync cleans up.
func (s *TestSuite) TestCleanupForceDeletedPodResourcesRetriesFailedEventLookup(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	pvIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	vaIndexer := informerFactories.KubeInformerFactory.Storage().V1().VolumeAttachments().Informer().GetIndexer()

	kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories)
	c.Assert(err, IsNil)

	// PV bound to the claim, so the cleanup has to find a conflicting pod.
	pvObj, err := kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), newPV(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvIndexer.Add(pvObj), IsNil)

	pvcObj, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), newPVC(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pvcIndexer.Add(pvcObj), IsNil)

	// Pending pod on TestNode2 that waits for the volume of the force-deleted pod.
	newPod := newPodWithPVC(TestPod2)
	newPod.Spec.NodeName = TestNode2
	newPod.Status.Phase = corev1.PodPending
	newPodObj, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), newPod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(pIndexer.Add(newPodObj), IsNil)

	_, err = kubeClient.CoreV1().Events(TestNamespace).Create(context.TODO(), &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: TestPod2 + ".failedattach", Namespace: TestNamespace},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      TestPod2,
			Namespace: TestNamespace,
		},
		Reason:  "FailedAttachVolume",
		Type:    corev1.EventTypeWarning,
		Message: `Waiting for detach for volume "` + TestPVName + `" Volume is already used by pod(s) ` + TestPod1,
	}, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	pvName := TestPVName
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "csi-attach-abcde"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: types.LonghornDriverName,
			NodeName: TestNode1,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	vaObj, err := kubeClient.StorageV1().VolumeAttachments().Create(context.TODO(), va, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(vaIndexer.Add(vaObj), IsNil)

	// The first event lookup fails.
	eventListCalls := 0
	kubeClient.PrependReactor("list", "events", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		eventListCalls++
		if eventListCalls == 1 {
			return true, nil, fmt.Errorf("list events failed")
		}
		return false, nil, nil
	})

	deletionTime := metav1.Now()
	oldPod := newPodWithPVC(TestPod1)
	oldPod.UID = "old-pod-uid"
	oldPod.Spec.NodeName = TestNode1
	oldPod.DeletionTimestamp = &deletionTime
	oldPod.DeletionGracePeriodSeconds = ptrTo(int64(0))
	kc.enqueuePodDeletion(oldPod)
	key := TestNamespace + "/" + TestPod1

	c.Assert(kc.processNextWorkItem(), Equals, true)
	_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	_, found := kc.zeroGraceDeletedPods.Load(key)
	c.Assert(found, Equals, true)

	// The key comes back rate-limited, the retry deletes the attachment.
	c.Assert(kc.processNextWorkItem(), Equals, true)
	_, err = kubeClient.StorageV1().VolumeAttachments().Get(context.TODO(), va.Name, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)
	_, found = kc.zeroGraceDeletedPods.Load(key)
	c.Assert(found, Equals, false)
	c.Assert(eventListCalls, Equals, 2)
}
