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
	subsystemEngine             = "engine"
	subsystemReplica            = "replica"
	subsystemBackup             = "backup"
	subsystemSnapshot           = "snapshot"
	subsystemBackingImage       = "backing_image"
	subsystemBackupBackingImage = "backup_backing_image"

	nodeLabel               = "node"
	diskLabel               = "disk"
	diskPathLabel           = "disk_path"
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
	engineLabel             = "engine"
	rebuildSrcLabel         = "rebuild_src"
	rebuildDstLabel         = "rebuild_dst"
	replicaLabel            = "replica"
	dataEngineLabel         = "data_engine"
	stateLabel              = "state"
	frontendLabel           = "frontend"
	imageLabel              = "image"
	modeLabel               = "mode"
)

type metricInfo struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}

type diskInfo struct {
	Spec   longhorn.DiskSpec   `json:"diskSpec"`
	Status longhorn.DiskStatus `json:"diskStatus"`
}

func getDiskListFromNode(node *longhorn.Node) map[string]diskInfo {
	disks := make(map[string]diskInfo)
	if node.Status.DiskStatus == nil {
		return disks
	}

	for diskName, diskSpec := range node.Spec.Disks {
		di := diskInfo{
			Spec: diskSpec,
		}
		if diskStatus, ok := node.Status.DiskStatus[diskName]; ok {
			di.Status = *diskStatus
		}
		disks[diskName] = di
	}

	return disks
}
