package controller

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

func hasReplicaEvictionRequested(rs map[string]*longhorn.Replica) bool {
	for _, r := range rs {
		if r.Status.EvictionRequested {
			return true
		}
	}

	return false
}

func isVolumeMigrating(v *longhorn.Volume) bool {
	return v.Spec.MigrationNodeID != "" || v.Status.CurrentMigrationNodeID != ""
}

func (vc *VolumeController) isVolumeUpgrading(v *longhorn.Volume) bool {
	return v.Status.CurrentImage != v.Spec.EngineImage
}

// isTargetVolumeOfCloning checks if the input volume is the target volume of an on-going cloning process
func isTargetVolumeOfCloning(v *longhorn.Volume) bool {
	isCloningDesired := types.IsDataFromVolume(v.Spec.DataSource)
	isCloningDone := v.Status.CloneStatus.State == longhorn.VolumeCloneStateCompleted ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateFailed
	return isCloningDesired && !isCloningDone
}

// isSourceVolumeOfCloning checks if the input volume is the source volume of an on-going cloning process
func (vc *VolumeController) isSourceVolumeOfCloning(v *longhorn.Volume) (bool, error) {
	vols, err := vc.ds.ListVolumes()
	if err != nil {
		return false, err
	}
	for _, vol := range vols {
		if isTargetVolumeOfCloning(vol) && types.GetVolumeName(vol.Spec.DataSource) == v.Name {
			return true, nil
		}
	}
	return false, nil
}
