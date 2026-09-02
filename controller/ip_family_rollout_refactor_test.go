package controller

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestIPFamilyRolloutPreflightLeavesAllManagersUnchanged(t *testing.T) {
	oldFamily := types.DataEngineIPFamilyIPv4
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, &oldFamily, true)
	peer := newIPFamilyTestInstanceManager("v2-peer", longhorn.DataEngineTypeV2, &oldFamily, true)
	peer.Status.CurrentState = longhorn.InstanceManagerStateRunning
	peer.Status.APIVersion = engineapi.MinInstanceManagerAPIVersionForPerInstanceIPFamily - 1

	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, target, peer)
	v2Setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameV2DataEngine)
	require.NoError(t, err)
	v2Setting.Value = "true"

	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}
	blocked, err := imc.syncInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.True(t, blocked)
	require.NotNil(t, target.Status.IPFamily)
	require.Equal(t, oldFamily, *target.Status.IPFamily)
	require.NotNil(t, peer.Status.IPFamily)
	require.Equal(t, oldFamily, *peer.Status.IPFamily)
}

func TestIPFamilyRolloutPreflightBlocksActiveBIDSWithoutManagerMutation(t *testing.T) {
	oldFamily := types.DataEngineIPFamilyIPv4
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, &oldFamily, true)
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, target)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{Name: "bids-active", Namespace: TestNamespace},
		Status: longhorn.BackingImageDataSourceStatus{
			CurrentState: longhorn.BackingImageStateInProgress,
		},
	}
	require.NoError(t, sc.ds.BackingImageDataSourceInformer.GetStore().Add(bids))
	require.NoError(t, sc.ds.PodInformer.GetStore().Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.GetBackingImageDataSourcePodName(bids.Name),
			Namespace: TestNamespace,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: BackingImageDataSourcePodContainerName,
			Args: []string{"--ip-family", oldFamily},
		}}},
	}))

	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}
	blocked, err := imc.syncInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.True(t, blocked)
	require.NotNil(t, target.Status.IPFamily)
	require.Equal(t, oldFamily, *target.Status.IPFamily)
}

func TestBIMReplacementWaitsForAppliedManagerConsensus(t *testing.T) {
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, nil, true)
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, target)
	require.NoError(t, sc.ds.PodInformer.GetStore().Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bim-pending",
			Namespace: TestNamespace,
			Labels:    types.GetBackingImageManagerLabels("node-test", "disk-test"),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: BackingImageManagerPodContainerName,
			Args: []string{"daemon", "--ip-family", types.DataEngineIPFamilyIPv4},
		}}},
	}))

	err := sc.syncDataEngineIPFamily()
	require.Error(t, err)
	remaining, err := sc.ds.GetPod("bim-pending")
	require.NoError(t, err)
	require.NotNil(t, remaining)
	require.Nil(t, target.Status.IPFamily)
}
