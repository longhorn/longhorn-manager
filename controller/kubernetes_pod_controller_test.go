package controller

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newTestKubernetesPodController(
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*KubernetesPodController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	kc, err := NewKubernetesPodController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, controllerID, false /* globalManagerEnabled */)
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

		kc, err := newTestKubernetesPodController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
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
