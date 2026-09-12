package controller

import (
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
	return &InstanceManagerController{baseController: &baseController{logger: logrus.New().WithField("test", "ip-family")}, namespace: TestNamespace, ds: ds}
}

func newInstanceManagerIPFamilyOptionPod(name string, family string) *corev1.Pod {
	args := appendInstanceManagerIPFamilyArgs([]string{"instance-manager", "--debug", "daemon"}, family)
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: TestNamespace}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "instance-manager", Args: args}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "2001:db8::10", ContainerStatuses: []corev1.ContainerStatus{{Name: "instance-manager", Ready: true}}}}
}

func TestSyncInstanceManagerIPFamilyUsesRunningReadyPodOption(t *testing.T) {
	imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyIPv6)
	pod := newInstanceManagerIPFamilyOptionPod(TestInstanceManagerName, types.DataEngineIPFamilyIPv6)
	require.NoError(t, imc.ds.PodInformer.GetStore().Add(pod))
	oldFamily := types.DataEngineIPFamilyIPv4
	im := &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace}, Spec: longhorn.InstanceManagerSpec{DataEngine: longhorn.DataEngineTypeV1}, Status: longhorn.InstanceManagerStatus{CurrentState: longhorn.InstanceManagerStateRunning, IPFamily: &oldFamily}}
	require.NoError(t, imc.syncInstanceManagerIPFamily(im))
	require.Equal(t, types.DataEngineIPFamilyIPv6, *im.Status.IPFamily)
	require.Equal(t, "2001:db8::10", im.Status.IP)
}

func TestSyncStatusWithPodUsesCurrentOptionOverStaleStatus(t *testing.T) {
	imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyIPv6)
	pod := newInstanceManagerIPFamilyOptionPod(TestInstanceManagerName, types.DataEngineIPFamilyIPv6)
	require.NoError(t, imc.ds.PodInformer.GetStore().Add(pod))
	oldFamily := types.DataEngineIPFamilyIPv4
	im := &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace}, Status: longhorn.InstanceManagerStatus{CurrentState: longhorn.InstanceManagerStateStarting, IPFamily: &oldFamily}}
	require.NoError(t, imc.syncStatusWithPod(im))
	require.Equal(t, longhorn.InstanceManagerStateRunning, im.Status.CurrentState)
	require.Equal(t, types.DataEngineIPFamilyIPv4, *im.Status.IPFamily)
}
