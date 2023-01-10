package manager

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

func (m *VolumeManager) GetLHVolumeAttachment(volumeName string) (*longhorn.VolumeAttachment, error) {
	return m.ds.GetLHVolumeAttachment(types.GetLHVolumeAttachmentNameFromVolumeName(volumeName))
}
