package controller

import (
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8scontroller "k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestInstanceManagerIPFamilyOptionAfterDaemon(t *testing.T) {
	base := []string{"instance-manager", "--debug", "daemon"}
	require.Equal(t, base, appendInstanceManagerIPFamilyArgs(append([]string{}, base...), types.DataEngineIPFamilyDefault))
	for _, family := range []string{types.DataEngineIPFamilyIPv4, types.DataEngineIPFamilyIPv6} {
		args := appendInstanceManagerIPFamilyArgs(append([]string{}, base...), family)
		require.Equal(t, append(base, "--ip-family", family), args)
	}
}

func TestInstanceManagerIPFamilyOptionRejectsInvalidValues(t *testing.T) {
	for _, family := range []string{"IPv4", "IPv6", "default", "", "ipv4x"} {
		got, specified, valid := types.ParseDataEngineIPFamilyArgs([]string{"daemon", "--ip-family", family})
		require.True(t, specified)
		require.False(t, valid)
		require.Empty(t, got)
		require.Equal(t, []string{"daemon"}, appendInstanceManagerIPFamilyArgs([]string{"daemon"}, family))
	}
}

func newInstanceManagerIPFamilyOptionController(t *testing.T, family string) *InstanceManagerController {
	t.Helper()
	kubeClient := kubefake.NewSimpleClientset()
	lhClient := lhfake.NewClientset()
	extensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, k8scontroller.NoResyncPeriodFunc())
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	require.NoError(t, ds.SettingInformer.GetStore().Add(newSetting(string(types.SettingNamePreferredDataEngineIPFamily), family)))
	require.NoError(t, ds.SettingInformer.GetStore().Add(newSetting(string(types.SettingNameV1DataEngine), "true")))
	return &InstanceManagerController{
		baseController:              &baseController{logger: logrus.New().WithField("test", "ip-family")},
		namespace:                   TestNamespace,
		kubeClient:                  kubeClient,
		ds:                          ds,
		instanceManagerMonitorMutex: &sync.Mutex{},
		instanceManagerMonitorMap:   map[string]chan struct{}{},
	}
}

func newInstanceManagerIPFamilyOptionPod(name string, family string) *corev1.Pod {
	args := appendInstanceManagerIPFamilyArgs([]string{"instance-manager", "--debug", "daemon"}, family)
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: TestNamespace}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "instance-manager", Args: args}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "2001:db8::10", ContainerStatuses: []corev1.ContainerStatus{{Name: "instance-manager", Ready: true}}}}
}

func TestGetInstanceManagerIPFamilyFromPod(t *testing.T) {
	_, err := getInstanceManagerIPFamilyFromPod(nil)
	require.ErrorContains(t, err, "pod is nil")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar"}}},
	}
	_, err = getInstanceManagerIPFamilyFromPod(pod)
	require.ErrorContains(t, err, "no instance-manager container")

	pod.Spec.Containers[0] = corev1.Container{
		Name: "instance-manager",
		Args: []string{"daemon", "--ip-family"},
	}
	_, err = getInstanceManagerIPFamilyFromPod(pod)
	require.ErrorContains(t, err, "invalid IP family arguments")

	pod.Spec.Containers[0].Args = []string{"daemon"}
	family, err := getInstanceManagerIPFamilyFromPod(pod)
	require.NoError(t, err)
	require.Empty(t, family)

	pod.Spec.Containers[0].Args = []string{"daemon", "--ip-family", types.DataEngineIPFamilyIPv6}
	family, err = getInstanceManagerIPFamilyFromPod(pod)
	require.NoError(t, err)
	require.Equal(t, types.DataEngineIPFamilyIPv6, family)
}

func TestIsIPFamilyAppliedWithoutPod(t *testing.T) {
	imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyIPv6)
	im := &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace}}

	applied, err := imc.isIPFamilyApplied(im)
	require.NoError(t, err)
	require.True(t, applied)
}

func TestSyncStatusWithPodSelectsIPByFamily(t *testing.T) {
	imc, im, pod := newIPFamilyConditionFixture(t)
	pod.Status.PodIP = "192.0.2.20"
	pod.Status.PodIPs = []corev1.PodIP{{IP: pod.Status.PodIP}, {IP: "2001:db8::10"}}
	im.Status.IPFamily = types.DataEngineIPFamilyIPv4
	im.Status.IP = "192.0.2.10"
	require.NoError(t, imc.syncStatusWithPod(im))
	require.NoError(t, imc.handlePod(im))
	require.Equal(t, types.DataEngineIPFamilyIPv6, im.Status.IPFamily)
	require.Equal(t, "2001:db8::10", im.Status.IP)

	pod.Status.PodIPs = []corev1.PodIP{{IP: pod.Status.PodIP}}
	require.NoError(t, imc.syncStatusWithPod(im))
	require.Equal(t, pod.Status.PodIP, im.Status.IP)
}

func TestHandlePodCorrectsInvalidIPFamilyArguments(t *testing.T) {
	imc, im, pod := newIPFamilyConditionFixture(t)
	imc.controllerID = TestNode1
	imc.backoff = newBackoff(t.Context())

	pod.UID = "malformed-ip-family-pod"
	pod.Spec.Containers[0].Args = []string{"instance-manager", "daemon", "--ip-family"}
	require.NoError(t, imc.ds.PodInformer.GetStore().Update(pod))
	_, err := imc.kubeClient.CoreV1().Pods(TestNamespace).Update(t.Context(), pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.NoError(t, imc.syncStatusWithPod(im))
	require.NoError(t, imc.handlePod(im))

	correctedPod, err := imc.kubeClient.CoreV1().Pods(TestNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEqual(t, pod.UID, correctedPod.UID)
	family, err := getInstanceManagerIPFamilyFromPod(correctedPod)
	require.NoError(t, err)
	require.Equal(t, types.DataEngineIPFamilyIPv6, family)
}

func TestSyncStatusWithPodRefreshesOptionWhilePending(t *testing.T) {
	imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyIPv6)
	pod := newInstanceManagerIPFamilyOptionPod(TestInstanceManagerName, types.DataEngineIPFamilyIPv6)
	pod.Status.Phase = corev1.PodPending
	require.NoError(t, imc.ds.PodInformer.GetStore().Add(pod))
	oldFamily := types.DataEngineIPFamilyIPv4
	im := &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace}, Status: longhorn.InstanceManagerStatus{CurrentState: longhorn.InstanceManagerStateStarting, IPFamily: oldFamily, IP: "192.0.2.10"}}
	require.NoError(t, imc.syncStatusWithPod(im))
	require.Equal(t, longhorn.InstanceManagerStateStarting, im.Status.CurrentState)
	require.Equal(t, types.DataEngineIPFamilyIPv6, im.Status.IPFamily)
	require.Equal(t, "192.0.2.10", im.Status.IP)
}

func TestSyncStatusWithPodRefreshesDefaultAndInvalidOptions(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		expectFamily string
		expectState  longhorn.InstanceManagerState
	}{
		{name: "omitted option", args: []string{"instance-manager", "--debug", "daemon"}, expectFamily: "", expectState: longhorn.InstanceManagerStateRunning},
		{name: "malformed option", args: []string{"instance-manager", "--debug", "daemon", "--ip-family"}, expectFamily: "", expectState: longhorn.InstanceManagerStateRunning},
		{name: "duplicate option", args: []string{"instance-manager", "--debug", "daemon", "--ip-family", "ipv6", "--ip-family", "ipv4"}, expectFamily: "", expectState: longhorn.InstanceManagerStateRunning},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyIPv6)
			pod := newInstanceManagerIPFamilyOptionPod(TestInstanceManagerName, types.DataEngineIPFamilyIPv6)
			pod.Spec.Containers[0].Args = tc.args
			require.NoError(t, imc.ds.PodInformer.GetStore().Add(pod))
			oldFamily := types.DataEngineIPFamilyIPv4
			im := &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace}, Status: longhorn.InstanceManagerStatus{CurrentState: longhorn.InstanceManagerStateRunning, IPFamily: oldFamily}}
			require.NoError(t, imc.syncStatusWithPod(im))
			require.Equal(t, tc.expectFamily, im.Status.IPFamily)
			require.Equal(t, tc.expectState, im.Status.CurrentState)
		})
	}

	t.Run("missing pod clears stale option", func(t *testing.T) {
		imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyIPv6)
		oldFamily := types.DataEngineIPFamilyIPv4
		im := &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace}, Status: longhorn.InstanceManagerStatus{CurrentState: longhorn.InstanceManagerStateRunning, IPFamily: oldFamily}}
		require.NoError(t, imc.syncStatusWithPod(im))
		require.Empty(t, im.Status.IPFamily)
	})

	t.Run("malformed option retains the primary pod IP", func(t *testing.T) {
		imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyDefault)
		pod := newInstanceManagerIPFamilyOptionPod(TestInstanceManagerName, types.DataEngineIPFamilyIPv6)
		pod.Spec.Containers[0].Args = []string{"instance-manager", "--debug", "daemon", "--ip-family"}
		require.NoError(t, imc.ds.PodInformer.GetStore().Add(pod))
		_, err := imc.kubeClient.CoreV1().Pods(TestNamespace).Create(t.Context(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
		im := &longhorn.InstanceManager{
			ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace},
			Spec:       longhorn.InstanceManagerSpec{NodeID: TestNode1},
			Status: longhorn.InstanceManagerStatus{
				CurrentState: longhorn.InstanceManagerStateRunning,
				IP:           "2001:db8::10",
			},
		}
		require.NoError(t, imc.syncStatusWithPod(im))
		require.Equal(t, longhorn.InstanceManagerStateRunning, im.Status.CurrentState)
		require.Empty(t, im.Status.IPFamily)
		require.Equal(t, pod.Status.PodIP, im.Status.IP)
	})
}

func TestIsSettingDataEngineIPFamilySyncedUsesPodOption(t *testing.T) {
	setting := newSetting(string(types.SettingNamePreferredDataEngineIPFamily), types.DataEngineIPFamilyIPv6)
	instanceManagerIPFamily := types.DataEngineIPFamilyIPv6
	require.True(t, isSettingDataEngineIPFamilySynced(setting, instanceManagerIPFamily))
	setting.Value = types.DataEngineIPFamilyIPv4
	require.False(t, isSettingDataEngineIPFamilySynced(setting, instanceManagerIPFamily))
	instanceManagerIPFamily = ""
	require.False(t, isSettingDataEngineIPFamilySynced(setting, instanceManagerIPFamily))
	setting.Value = types.DataEngineIPFamilyDefault
	require.True(t, isSettingDataEngineIPFamilySynced(setting, instanceManagerIPFamily))
}
