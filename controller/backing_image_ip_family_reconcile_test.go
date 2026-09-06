package controller

import (
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/util/flowcontrol"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8scontroller "k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestBackingImageManagerReconcilesPreferredIPFamily(t *testing.T) {
	for _, tc := range []struct {
		name       string
		podFamily  string
		expectGone bool
	}{
		{name: "matching pod is retained", podFamily: types.DataEngineIPFamilyIPv6},
		{name: "mismatched pod is deleted", podFamily: types.DataEngineIPFamilyIPv4, expectGone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient := kubefake.NewSimpleClientset()
			lhClient := lhfake.NewClientset()
			extensionsClient := apiextensionsfake.NewSimpleClientset()
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, k8scontroller.NoResyncPeriodFunc())
			ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
			require.NoError(t, ds.SettingInformer.GetStore().Add(newSetting(string(types.SettingNamePreferredDataEngineIPFamily), types.DataEngineIPFamilyIPv6)))

			bim := &longhorn.BackingImageManager{
				ObjectMeta: metav1.ObjectMeta{Name: "bim-family-test", Namespace: TestNamespace},
				Spec:       longhorn.BackingImageManagerSpec{NodeID: "node-1", DiskUUID: "disk-1"},
				Status:     longhorn.BackingImageManagerStatus{CurrentState: longhorn.BackingImageManagerStateStarting},
			}
			args := appendBackingImageIPFamilyArgs([]string{"backing-image-manager", "--debug", "daemon"}, tc.podFamily)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: bim.Name, Namespace: TestNamespace, Labels: map[string]string{types.GetLonghornLabelComponentKey(): types.LonghornLabelBackingImageManager}},
				Spec:       corev1.PodSpec{NodeName: bim.Spec.NodeID, Containers: []corev1.Container{{Name: BackingImageManagerPodContainerName, Args: args}}},
				Status:     corev1.PodStatus{Phase: corev1.PodPending},
			}
			_, err := kubeClient.CoreV1().Pods(TestNamespace).Create(t.Context(), pod, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, ds.PodInformer.GetStore().Add(pod))
			controller := &BackingImageManagerController{baseController: newBaseController("test", logrus.New()), namespace: TestNamespace, ds: ds, lock: &sync.RWMutex{}, monitorMap: map[string]chan struct{}{}}
			t.Cleanup(controller.queue.ShutDown)
			require.NoError(t, controller.syncBackingImageManagerPod(bim, flowcontrol.NewBackOff(time.Minute, time.Minute)))
			_, err = kubeClient.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
			if tc.expectGone {
				require.True(t, apierrors.IsNotFound(err), "expected pod deletion, got %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackingImageDataSourceReconcilesPreferredIPFamilyDuringTransfer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		podFamily  string
		state      longhorn.BackingImageState
		expectGone bool
	}{
		{name: "matching pod is retained", podFamily: types.DataEngineIPFamilyIPv6},
		{name: "mismatched active pod is deleted", podFamily: types.DataEngineIPFamilyIPv4, state: longhorn.BackingImageStateInProgress, expectGone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient := kubefake.NewSimpleClientset()
			lhClient := lhfake.NewClientset()
			extensionsClient := apiextensionsfake.NewSimpleClientset()
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, k8scontroller.NoResyncPeriodFunc())
			ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
			require.NoError(t, ds.SettingInformer.GetStore().Add(newSetting(string(types.SettingNamePreferredDataEngineIPFamily), types.DataEngineIPFamilyIPv6)))

			bids := &longhorn.BackingImageDataSource{
				ObjectMeta: metav1.ObjectMeta{Name: "bids-family-test", Namespace: TestNamespace},
				Spec:       longhorn.BackingImageDataSourceSpec{NodeID: "node-1", DiskUUID: "disk-1"},
				Status:     longhorn.BackingImageDataSourceStatus{CurrentState: tc.state},
			}
			podName := types.GetBackingImageDataSourcePodName(bids.Name)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: TestNamespace, Labels: map[string]string{
					types.GetLonghornLabelComponentKey():                   types.LonghornLabelBackingImageDataSource,
					types.GetLonghornLabelKey(types.LonghornLabelDiskUUID): bids.Spec.DiskUUID,
				}},
				Spec:   corev1.PodSpec{NodeName: bids.Spec.NodeID, Containers: []corev1.Container{{Name: BackingImageDataSourcePodContainerName, Args: appendBackingImageIPFamilyArgs([]string{"backing-image-manager", "--debug", "data-source"}, tc.podFamily)}}},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			}
			if tc.state == longhorn.BackingImageStateInProgress {
				pod.Status.Phase = corev1.PodRunning
				pod.Status.PodIP = "192.0.2.10"
				pod.Status.PodIPs = []corev1.PodIP{{IP: "192.0.2.10"}}
			}
			_, err := kubeClient.CoreV1().Pods(TestNamespace).Create(t.Context(), pod, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, ds.PodInformer.GetStore().Add(pod))
			controller := &BackingImageDataSourceController{baseController: newBaseController("test", logrus.New()), namespace: TestNamespace, ds: ds, lock: &sync.RWMutex{}, monitorMap: map[string]chan struct{}{}}
			t.Cleanup(controller.queue.ShutDown)
			if tc.state == longhorn.BackingImageStateInProgress {
				controller.monitorMap[bids.Name] = make(chan struct{})
				t.Cleanup(func() { controller.stopMonitoring(bids.Name) })
			}
			require.NoError(t, controller.syncBackingImageDataSourcePod(bids))
			_, err = kubeClient.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
			if tc.expectGone {
				require.True(t, apierrors.IsNotFound(err), "expected pod deletion, got %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
