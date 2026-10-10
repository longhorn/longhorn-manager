package v113xto1140

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestUpgradeResources(t *testing.T) {
	volumes := map[string]*longhorn.Volume{
		"legacy":   {},
		"ignored":  {Spec: longhorn.VolumeSpec{ReplicaSchedulingSkipUnhealthyDisk: longhorn.ReplicaSchedulingSkipUnhealthyDiskIgnored}},
		"enabled":  {Spec: longhorn.VolumeSpec{ReplicaSchedulingSkipUnhealthyDisk: longhorn.ReplicaSchedulingSkipUnhealthyDiskEnabled}},
		"disabled": {Spec: longhorn.VolumeSpec{ReplicaSchedulingSkipUnhealthyDisk: longhorn.ReplicaSchedulingSkipUnhealthyDiskDisabled}},
	}
	resourceMaps := map[string]interface{}{types.LonghornKindVolume: volumes}

	require.NoError(t, UpgradeResources("longhorn-system", nil, nil, resourceMaps))
	require.Equal(t, longhorn.ReplicaSchedulingSkipUnhealthyDiskIgnored, volumes["legacy"].Spec.ReplicaSchedulingSkipUnhealthyDisk)
	require.Equal(t, longhorn.ReplicaSchedulingSkipUnhealthyDiskIgnored, volumes["ignored"].Spec.ReplicaSchedulingSkipUnhealthyDisk)
	require.Equal(t, longhorn.ReplicaSchedulingSkipUnhealthyDiskEnabled, volumes["enabled"].Spec.ReplicaSchedulingSkipUnhealthyDisk)
	require.Equal(t, longhorn.ReplicaSchedulingSkipUnhealthyDiskDisabled, volumes["disabled"].Spec.ReplicaSchedulingSkipUnhealthyDisk)

	require.NoError(t, UpgradeResources("longhorn-system", nil, nil, resourceMaps))
	require.Equal(t, longhorn.ReplicaSchedulingSkipUnhealthyDiskIgnored, volumes["legacy"].Spec.ReplicaSchedulingSkipUnhealthyDisk)
	require.Error(t, UpgradeResources("longhorn-system", nil, nil, nil))
}
