package metricscollector

import (
	"github.com/prometheus/client_golang/prometheus"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	longhornName = "longhorn"

	subsystemVolume             = "volume"
	subsystemNode               = "node"
	subsystemDisk               = "disk"
	subsystemInstanceManager    = "instance_manager"
	subsystemManager            = "manager"
	subsystemBackup             = "backup"
	subsystemSnapshot           = "snapshot"
	subsystemBackingImage       = "backing_image"
	subsystemBackupBackingImage = "backup_backing_image"

	nodeLabel               = "node"
	diskLabel               = "disk"
	volumeLabel             = "volume"
	conditionLabel          = "condition"
	conditionReasonLabel    = "condition_reason"
	instanceManagerLabel    = "instance_manager"
	instanceManagerType     = "instance_manager_type"
	managerLabel            = "manager"
	backupLabel             = "backup"
	snapshotLabel           = "snapshot"
	pvcLabel                = "pvc"
	pvcNamespaceLabel       = "pvc_namespace"
	userCreatedLabel        = "user_created"
	backingImageLabel       = "backing_image"
	backupBackingImageLabel = "backup_backing_image"
	recurringJobLabel       = "recurring_job"
)

type metricInfo struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}

type diskInfo struct {
	longhorn.DiskSpec   `json:"diskSpec"`
	longhorn.DiskStatus `json:"diskStatus"`
}

func getDiskListFromNode(node *longhorn.Node) map[string]diskInfo {
	disks := make(map[string]diskInfo)
	if node.Status.DiskStatus == nil {
		return disks
	}

	for diskName, diskSpec := range node.Spec.Disks {
		di := diskInfo{
			DiskSpec: diskSpec,
		}
		if diskStatus, ok := node.Status.DiskStatus[diskName]; ok {
			di.DiskStatus = *diskStatus
		}
		disks[diskName] = di
	}

	return disks
}
