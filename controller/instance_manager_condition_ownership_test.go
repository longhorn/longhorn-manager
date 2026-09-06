package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func newIPFamilyConditionFixture(t *testing.T) (*InstanceManagerController, *longhorn.InstanceManager, *corev1.Pod) {
	t.Helper()
	imc := newInstanceManagerIPFamilyOptionController(t, types.DataEngineIPFamilyIPv6)
	for name := range types.GetDangerZoneSettings() {
		definition, ok := types.GetSettingDefinition(name)
		require.True(t, ok)
		require.NoError(t, imc.ds.SettingInformer.GetStore().Update(newSetting(string(name), definition.Default)))
	}
	node := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}
	require.NoError(t, imc.ds.KubeNodeInformer.GetStore().Add(node))
	require.NoError(t, imc.ds.NodeInformer.GetStore().Add(newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")))
	im := &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: TestInstanceManagerName, Namespace: TestNamespace}, Spec: longhorn.InstanceManagerSpec{NodeID: TestNode1, Image: TestInstanceManagerImage, DataEngine: longhorn.DataEngineTypeV1}, Status: longhorn.InstanceManagerStatus{CurrentState: longhorn.InstanceManagerStateRunning}}
	require.NoError(t, imc.ds.InstanceManagerInformer.GetStore().Add(im))
	tolerations, err := imc.ds.GetSettingTaintToleration()
	require.NoError(t, err)
	pod, err := imc.createInstanceManagerPodSpec(im, tolerations, "", nil, longhorn.DataEngineTypeV1)
	require.NoError(t, err)
	pod.Status = newInstanceManagerIPFamilyOptionPod(im.Name, types.DataEngineIPFamilyIPv6).Status
	require.NoError(t, imc.ds.PodInformer.GetStore().Add(pod))
	synced, _, _, _, err := imc.areDangerZoneSettingsSyncedToIMPod(im)
	require.NoError(t, err)
	require.True(t, synced)
	return imc, im, pod
}

func TestIPFamilyConditionPreservesOtherDangerFailure(t *testing.T) {
	imc, im, _ := newIPFamilyConditionFixture(t)
	require.NoError(t, imc.ds.SettingInformer.GetStore().Update(newSetting(string(types.SettingNamePriorityClass), "different-priority")))
	synced, _, _, _, err := imc.areDangerZoneSettingsSyncedToIMPod(im)
	require.NoError(t, err)
	require.False(t, synced)
	before := types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced)
	require.Contains(t, before.Message, string(types.SettingNamePriorityClass))
	require.NoError(t, imc.syncInstanceManagerIPFamily(im))
	after := types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced)
	require.Equal(t, before.Status, after.Status)
	require.Equal(t, before.Reason, after.Reason)
	require.Equal(t, before.Message, after.Message)
}

func TestIPFamilyConditionPreservesAttachedMismatch(t *testing.T) {
	imc, im, _ := newIPFamilyConditionFixture(t)
	require.NoError(t, imc.ds.SettingInformer.GetStore().Update(newSetting(string(types.SettingNamePreferredDataEngineIPFamily), types.DataEngineIPFamilyIPv4)))
	require.NoError(t, imc.ds.VolumeInformer.GetStore().Add(&longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: "attached", Namespace: TestNamespace}, Status: longhorn.VolumeStatus{State: longhorn.VolumeStateAttached}}))
	_, _, _, _, err := imc.areDangerZoneSettingsSyncedToIMPod(im)
	require.NoError(t, err)
	before := types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced)
	require.Equal(t, longhorn.ConditionStatusTrue, before.Status)
	require.NoError(t, imc.syncInstanceManagerIPFamily(im))
	after := types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced)
	require.Equal(t, before.Status, after.Status)
}
