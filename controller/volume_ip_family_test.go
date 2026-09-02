package controller

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestDetachedVolumeWaitsForAppliedIPFamilyBeforeAttaching(t *testing.T) {
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
	vc := &VolumeController{ds: sc.ds}
	volume := newVolume(TestVolumeName, 1)
	volume.Spec.NodeID = TestNode1
	volume.Status.State = longhorn.VolumeStateDetached
	volume.Status.CurrentNodeID = ""

	err := vc.reconcileAttachDetachStateMachine(
		volume, nil, nil, nil, false, logrus.NewEntry(logrus.New()))
	require.NoError(t, err)
	require.Equal(t, longhorn.VolumeStateDetached, volume.Status.State)
	require.Empty(t, volume.Status.CurrentNodeID)
}
