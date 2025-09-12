package types

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/longhorn/longhorn-manager/meta"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	DefaultSettingYAMLFileName  = "default-setting.yaml"
	DefaultResourceYAMLFileName = "default-resource.yaml"

	ValueEmpty   = "none"
	ValueUnknown = "unknown"

	// From `maximumChainLength` in longhorn-engine/pkg/replica/replica.go
	MaxSnapshotNum = 250

	DefaultMinNumberOfCopies = 3

	DefaultBackupstorePollInterval = 300 * time.Second

	BackupBlockSizeMi      int64 = 1 * 1024 * 1024
	BackupBlockSize2Mi           = 2 * BackupBlockSizeMi
	BackupBlockSize16Mi          = 16 * BackupBlockSizeMi
	BackupBlockSizeInvalid int64 = -1
)

type SettingType string

const (
	SettingTypeString     = SettingType("string")
	SettingTypeInt        = SettingType("int")
	SettingTypeFloat      = SettingType("float")
	SettingTypeBool       = SettingType("bool")
	SettingTypeDeprecated = SettingType("deprecated")

	ValueIntRangeMinimum = "minimum"
	ValueIntRangeMaximum = "maximum"

	ValueFloatRangeMinimum = "minimum"
	ValueFloatRangeMaximum = "maximum"
)

type SettingName string

const (
	SettingNameAllowRecurringJobWhileVolumeDetached                     = SettingName("allow-recurring-job-while-volume-detached")
	SettingNameCreateDefaultDiskLabeledNodes                            = SettingName("create-default-disk-labeled-nodes")
	SettingNameDefaultDataPath                                          = SettingName("default-data-path")
	SettingNameDefaultEngineImage                                       = SettingName("default-engine-image")
	SettingNameDefaultInstanceManagerImage                              = SettingName("default-instance-manager-image")
	SettingNameDefaultBackingImageManagerImage                          = SettingName("default-backing-image-manager-image")
	SettingNameSupportBundleManagerImage                                = SettingName("support-bundle-manager-image")
	SettingNameReplicaSoftAntiAffinity                                  = SettingName("replica-soft-anti-affinity")
	SettingNameReplicaAutoBalance                                       = SettingName("replica-auto-balance")
	SettingNameReplicaAutoBalanceDiskPressurePercentage                 = SettingName("replica-auto-balance-disk-pressure-percentage")
	SettingNameStorageOverProvisioningPercentage                        = SettingName("storage-over-provisioning-percentage")
	SettingNameStorageMinimalAvailablePercentage                        = SettingName("storage-minimal-available-percentage")
	SettingNameStorageReservedPercentageForDefaultDisk                  = SettingName("storage-reserved-percentage-for-default-disk")
	SettingNameUpgradeChecker                                           = SettingName("upgrade-checker")
	SettingNameUpgradeResponderURL                                      = SettingName("upgrade-responder-url")
	SettingNameAllowCollectingLonghornUsage                             = SettingName("allow-collecting-longhorn-usage-metrics")
	SettingNameCurrentLonghornVersion                                   = SettingName("current-longhorn-version")
	SettingNameLatestLonghornVersion                                    = SettingName("latest-longhorn-version")
	SettingNameStableLonghornVersions                                   = SettingName("stable-longhorn-versions")
	SettingNameDefaultReplicaCount                                      = SettingName("default-replica-count")
	SettingNameDefaultDataLocality                                      = SettingName("default-data-locality")
	SettingNameDefaultLonghornStaticStorageClass                        = SettingName("default-longhorn-static-storage-class")
	SettingNameTaintToleration                                          = SettingName("taint-toleration")
	SettingNameSystemManagedComponentsNodeSelector                      = SettingName("system-managed-components-node-selector")
	SettingNameCRDAPIVersion                                            = SettingName("crd-api-version")
	SettingNameAutoSalvage                                              = SettingName("auto-salvage")
	SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly              = SettingName("auto-delete-pod-when-volume-detached-unexpectedly")
	SettingNameRegistrySecret                                           = SettingName("registry-secret")
	SettingNameDisableSchedulingOnCordonedNode                          = SettingName("disable-scheduling-on-cordoned-node")
	SettingNameReplicaZoneSoftAntiAffinity                              = SettingName("replica-zone-soft-anti-affinity")
	SettingNameNodeDownPodDeletionPolicy                                = SettingName("node-down-pod-deletion-policy")
	SettingNameNodeDrainPolicy                                          = SettingName("node-drain-policy")
	SettingNameDetachManuallyAttachedVolumesWhenCordoned                = SettingName("detach-manually-attached-volumes-when-cordoned")
	SettingNamePriorityClass                                            = SettingName("priority-class")
	SettingNameDisableRevisionCounter                                   = SettingName("disable-revision-counter")
	SettingNameReplicaReplenishmentWaitInterval                         = SettingName("replica-replenishment-wait-interval")
	SettingNameConcurrentReplicaRebuildPerNodeLimit                     = SettingName("concurrent-replica-rebuild-per-node-limit")
	SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit          = SettingName("concurrent-backing-image-replenish-per-node-limit")
	SettingNameConcurrentBackupRestorePerNodeLimit                      = SettingName("concurrent-volume-backup-restore-per-node-limit")
	SettingNameSystemManagedPodsImagePullPolicy                         = SettingName("system-managed-pods-image-pull-policy")
	SettingNameAllowVolumeCreationWithDegradedAvailability              = SettingName("allow-volume-creation-with-degraded-availability")
	SettingNameAutoCleanupSystemGeneratedSnapshot                       = SettingName("auto-cleanup-system-generated-snapshot")
	SettingNameAutoCleanupRecurringJobBackupSnapshot                    = SettingName("auto-cleanup-recurring-job-backup-snapshot")
	SettingNameConcurrentAutomaticEngineUpgradePerNodeLimit             = SettingName("concurrent-automatic-engine-upgrade-per-node-limit")
	SettingNameBackingImageCleanupWaitInterval                          = SettingName("backing-image-cleanup-wait-interval")
	SettingNameBackingImageRecoveryWaitInterval                         = SettingName("backing-image-recovery-wait-interval")
	SettingNameGuaranteedInstanceManagerCPU                             = SettingName("guaranteed-instance-manager-cpu")
	SettingNameKubernetesClusterAutoscalerEnabled                       = SettingName("kubernetes-cluster-autoscaler-enabled")
	SettingNameOrphanAutoDeletion                                       = SettingName("orphan-auto-deletion") // replaced by SettingNameOrphanResourceAutoDeletion
	SettingNameOrphanResourceAutoDeletion                               = SettingName("orphan-resource-auto-deletion")
	SettingNameOrphanResourceAutoDeletionGracePeriod                    = SettingName("orphan-resource-auto-deletion-grace-period")
	SettingNameStorageNetwork                                           = SettingName("storage-network")
	SettingNameStorageNetworkForRWXVolumeEnabled                        = SettingName("storage-network-for-rwx-volume-enabled")
	SettingNameFailedBackupTTL                                          = SettingName("failed-backup-ttl")
	SettingNameRecurringSuccessfulJobsHistoryLimit                      = SettingName("recurring-successful-jobs-history-limit")
	SettingNameRecurringFailedJobsHistoryLimit                          = SettingName("recurring-failed-jobs-history-limit")
	SettingNameRecurringJobMaxRetention                                 = SettingName("recurring-job-max-retention")
	SettingNameSupportBundleFailedHistoryLimit                          = SettingName("support-bundle-failed-history-limit")
	SettingNameSupportBundleNodeCollectionTimeout                       = SettingName("support-bundle-node-collection-timeout")
	SettingNameDeletingConfirmationFlag                                 = SettingName("deleting-confirmation-flag")
	SettingNameEngineReplicaTimeout                                     = SettingName("engine-replica-timeout")
	SettingNameSnapshotDataIntegrity                                    = SettingName("snapshot-data-integrity")
	SettingNameSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation = SettingName("snapshot-data-integrity-immediate-check-after-snapshot-creation")
	SettingNameSnapshotDataIntegrityCronJob                             = SettingName("snapshot-data-integrity-cronjob")
	SettingNameSnapshotMaxCount                                         = SettingName("snapshot-max-count")
	SettingNameRestoreVolumeRecurringJobs                               = SettingName("restore-volume-recurring-jobs")
	SettingNameRemoveSnapshotsDuringFilesystemTrim                      = SettingName("remove-snapshots-during-filesystem-trim")
	SettingNameFastReplicaRebuildEnabled                                = SettingName("fast-replica-rebuild-enabled")
	SettingNameReplicaFileSyncHTTPClientTimeout                         = SettingName("replica-file-sync-http-client-timeout")
	SettingNameLongGPRCTimeOut                                          = SettingName("long-grpc-timeout")
	SettingNameBackupCompressionMethod                                  = SettingName("backup-compression-method")
	SettingNameBackupConcurrentLimit                                    = SettingName("backup-concurrent-limit")
	SettingNameRestoreConcurrentLimit                                   = SettingName("restore-concurrent-limit")
	SettingNameLogLevel                                                 = SettingName("log-level")
	SettingNameReplicaDiskSoftAntiAffinity                              = SettingName("replica-disk-soft-anti-affinity")
	SettingNameAllowEmptyNodeSelectorVolume                             = SettingName("allow-empty-node-selector-volume")
	SettingNameAllowEmptyDiskSelectorVolume                             = SettingName("allow-empty-disk-selector-volume")
	SettingNameDisableSnapshotPurge                                     = SettingName("disable-snapshot-purge")
	SettingNameV1DataEngine                                             = SettingName("v1-data-engine")
	SettingNameV2DataEngine                                             = SettingName("v2-data-engine")
	SettingNameDataEngineHugepageEnabled                                = SettingName("data-engine-hugepage-enabled")
	SettingNameDataEngineMemorySize                                     = SettingName("data-engine-memory-size")
	SettingNameDataEngineCPUMask                                        = SettingName("data-engine-cpu-mask")
	SettingNameDataEngineLogLevel                                       = SettingName("data-engine-log-level")
	SettingNameDataEngineLogFlags                                       = SettingName("data-engine-log-flags")
	SettingNameDataEngineInterruptModeEnabled                           = SettingName("data-engine-interrupt-mode-enabled")
	SettingNameFreezeFilesystemForSnapshot                              = SettingName("freeze-filesystem-for-snapshot")
	SettingNameAutoCleanupSnapshotWhenDeleteBackup                      = SettingName("auto-cleanup-when-delete-backup")
	SettingNameAutoCleanupSnapshotAfterOnDemandBackupCompleted          = SettingName("auto-cleanup-snapshot-after-on-demand-backup-completed")
	SettingNameDefaultMinNumberOfBackingImageCopies                     = SettingName("default-min-number-of-backing-image-copies")
	SettingNameBackupExecutionTimeout                                   = SettingName("backup-execution-timeout")
	SettingNameRWXVolumeFastFailover                                    = SettingName("rwx-volume-fast-failover")
	SettingNameOfflineReplicaRebuilding                                 = SettingName("offline-replica-rebuilding")
	SettingNameReplicaRebuildingBandwidthLimit                          = SettingName("replica-rebuilding-bandwidth-limit")
	SettingNameDefaultBackupBlockSize                                   = SettingName("default-backup-block-size")
	SettingNameInstanceManagerPodLivenessProbeTimeout                   = SettingName("instance-manager-pod-liveness-probe-timeout")
	SettingNameLogPath                                                  = SettingName("log-path")

	// These three backup target parameters are used in the "longhorn-default-resource" ConfigMap
	// to update the default BackupTarget resource.
	// Longhorn won't create the Setting resources for these three parameters.
	SettingNameBackupTarget                 = SettingName("backup-target")
	SettingNameBackupTargetCredentialSecret = SettingName("backup-target-credential-secret")
	SettingNameBackupstorePollInterval      = SettingName("backupstore-poll-interval")

	// The settings are deprecated and Longhorn won't create Setting Resources for these parameters.
	// TODO: Remove these settings in the future releases.
	SettingNameV2DataEngineHugepageLimit                = SettingName("v2-data-engine-hugepage-limit")
	SettingNameV2DataEngineGuaranteedInstanceManagerCPU = SettingName("v2-data-engine-guaranteed-instance-manager-cpu")
	SettingNameV2DataEngineCPUMask                      = SettingName("v2-data-engine-cpu-mask")
	SettingNameV2DataEngineLogLevel                     = SettingName("v2-data-engine-log-level")
	SettingNameV2DataEngineLogFlags                     = SettingName("v2-data-engine-log-flags")
	SettingNameV2DataEngineFastReplicaRebuilding        = SettingName("v2-data-engine-fast-replica-rebuilding")
	SettingNameV2DataEngineSnapshotDataIntegrity        = SettingName("v2-data-engine-snapshot-data-integrity")
)

var (
	SettingNameList = []SettingName{
		SettingNameAllowRecurringJobWhileVolumeDetached,
		SettingNameCreateDefaultDiskLabeledNodes,
		SettingNameDefaultDataPath,
		SettingNameDefaultEngineImage,
		SettingNameDefaultInstanceManagerImage,
		SettingNameDefaultBackingImageManagerImage,
		SettingNameSupportBundleManagerImage,
		SettingNameReplicaSoftAntiAffinity,
		SettingNameReplicaAutoBalance,
		SettingNameReplicaAutoBalanceDiskPressurePercentage,
		SettingNameStorageOverProvisioningPercentage,
		SettingNameStorageMinimalAvailablePercentage,
		SettingNameStorageReservedPercentageForDefaultDisk,
		SettingNameUpgradeChecker,
		SettingNameUpgradeResponderURL,
		SettingNameAllowCollectingLonghornUsage,
		SettingNameCurrentLonghornVersion,
		SettingNameLatestLonghornVersion,
		SettingNameStableLonghornVersions,
		SettingNameDefaultReplicaCount,
		SettingNameDefaultDataLocality,
		SettingNameDefaultLonghornStaticStorageClass,
		SettingNameTaintToleration,
		SettingNameSystemManagedComponentsNodeSelector,
		SettingNameCRDAPIVersion,
		SettingNameAutoSalvage,
		SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly,
		SettingNameRegistrySecret,
		SettingNameDisableSchedulingOnCordonedNode,
		SettingNameReplicaZoneSoftAntiAffinity,
		SettingNameNodeDownPodDeletionPolicy,
		SettingNameNodeDrainPolicy,
		SettingNameDetachManuallyAttachedVolumesWhenCordoned,
		SettingNamePriorityClass,
		SettingNameDisableRevisionCounter,
		SettingNameReplicaReplenishmentWaitInterval,
		SettingNameConcurrentReplicaRebuildPerNodeLimit,
		SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit,
		SettingNameConcurrentBackupRestorePerNodeLimit,
		SettingNameSystemManagedPodsImagePullPolicy,
		SettingNameAllowVolumeCreationWithDegradedAvailability,
		SettingNameAutoCleanupSystemGeneratedSnapshot,
		SettingNameAutoCleanupRecurringJobBackupSnapshot,
		SettingNameConcurrentAutomaticEngineUpgradePerNodeLimit,
		SettingNameBackingImageCleanupWaitInterval,
		SettingNameBackingImageRecoveryWaitInterval,
		SettingNameGuaranteedInstanceManagerCPU,
		SettingNameKubernetesClusterAutoscalerEnabled,
		SettingNameOrphanResourceAutoDeletion,
		SettingNameOrphanResourceAutoDeletionGracePeriod,
		SettingNameStorageNetwork,
		SettingNameStorageNetworkForRWXVolumeEnabled,
		SettingNameFailedBackupTTL,
		SettingNameRecurringSuccessfulJobsHistoryLimit,
		SettingNameRecurringFailedJobsHistoryLimit,
		SettingNameRecurringJobMaxRetention,
		SettingNameSupportBundleFailedHistoryLimit,
		SettingNameSupportBundleNodeCollectionTimeout,
		SettingNameDeletingConfirmationFlag,
		SettingNameEngineReplicaTimeout,
		SettingNameSnapshotDataIntegrity,
		SettingNameSnapshotDataIntegrityCronJob,
		SettingNameSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation,
		SettingNameSnapshotMaxCount,
		SettingNameRestoreVolumeRecurringJobs,
		SettingNameRemoveSnapshotsDuringFilesystemTrim,
		SettingNameFastReplicaRebuildEnabled,
		SettingNameReplicaFileSyncHTTPClientTimeout,
		SettingNameLongGPRCTimeOut,
		SettingNameBackupCompressionMethod,
		SettingNameBackupConcurrentLimit,
		SettingNameRestoreConcurrentLimit,
		SettingNameLogLevel,
		SettingNameV1DataEngine,
		SettingNameV2DataEngine,
		SettingNameDataEngineHugepageEnabled,
		SettingNameDataEngineMemorySize,
		SettingNameDataEngineCPUMask,
		SettingNameDataEngineLogLevel,
		SettingNameDataEngineLogFlags,
		SettingNameSnapshotDataIntegrity,
		SettingNameDataEngineInterruptModeEnabled,
		SettingNameReplicaDiskSoftAntiAffinity,
		SettingNameAllowEmptyNodeSelectorVolume,
		SettingNameAllowEmptyDiskSelectorVolume,
		SettingNameDisableSnapshotPurge,
		SettingNameFreezeFilesystemForSnapshot,
		SettingNameAutoCleanupSnapshotWhenDeleteBackup,
		SettingNameAutoCleanupSnapshotAfterOnDemandBackupCompleted,
		SettingNameDefaultMinNumberOfBackingImageCopies,
		SettingNameBackupExecutionTimeout,
		SettingNameRWXVolumeFastFailover,
		SettingNameOfflineReplicaRebuilding,
		SettingNameReplicaRebuildingBandwidthLimit,
		SettingNameDefaultBackupBlockSize,
		SettingNameInstanceManagerPodLivenessProbeTimeout,
		SettingNameLogPath,
	}
)

var replacedSettingNames = map[SettingName]bool{
	SettingNameOrphanAutoDeletion:                       true, // SettingNameOrphanResourceAutoDeletion
	SettingNameV2DataEngineHugepageLimit:                true, // SettingNameHugepageLimit
	SettingNameV2DataEngineGuaranteedInstanceManagerCPU: true, // SettingNameGuaranteedInstanceManagerCPU
	SettingNameV2DataEngineCPUMask:                      true, // SettingNameDataEngineCPUMask
	SettingNameV2DataEngineLogLevel:                     true, // SettingNameDataEngineLogLevel
	SettingNameV2DataEngineLogFlags:                     true, // SettingNameDataEngineLogFlags
	SettingNameV2DataEngineFastReplicaRebuilding:        true, // SettingNameFastReplicaRebuildEnabled
	SettingNameV2DataEngineSnapshotDataIntegrity:        true, // SettingNameSnapshotDataIntegrity
}

type SettingCategory string

const (
	SettingCategorySystemInfo = SettingCategory("system info")
	SettingCategoryGeneral    = SettingCategory("general")
	SettingCategoryBackup     = SettingCategory("backup")
	SettingCategoryOrphan     = SettingCategory("orphan")
	SettingCategoryScheduling = SettingCategory("scheduling")
	SettingCategoryDangerZone = SettingCategory("danger Zone")
	SettingCategorySnapshot   = SettingCategory("snapshot")
)

type SettingDefinition struct {
	DisplayName string          `json:"displayName"`
	Description string          `json:"description"`
	Category    SettingCategory `json:"category"`
	Type        SettingType     `json:"type"`
	Required    bool            `json:"required"`
	ReadOnly    bool            `json:"readOnly"`
	Default     string          `json:"default"`
	Choices     []any           `json:"options,omitempty"` // +optional

	// Use map to present minimum and maximum value instead of using int directly, so we can omitempy and distinguish 0 or nil at the same time.
	ValueIntRange   map[string]int     `json:"range,omitempty"`      // +optional
	ValueFloatRange map[string]float64 `json:"floatRange,omitempty"` // +optional

	// If DataEngineSpecific is false, the setting is applicable to both V1 and V2 Data Engine, and Default is a non-JSON string.
	// If DataEngineSpecific is true, the setting is only applicable to V1 or V2 Data Engine, and Default is a JSON string.
	DataEngineSpecific bool `json:"dataEngineSpecific,omitempty"` // +optional
}

var settingDefinitionsLock sync.RWMutex

var (
	settingDefinitions = map[SettingName]SettingDefinition{
		SettingNameAllowRecurringJobWhileVolumeDetached:                     SettingDefinitionAllowRecurringJobWhileVolumeDetached,
		SettingNameCreateDefaultDiskLabeledNodes:                            SettingDefinitionCreateDefaultDiskLabeledNodes,
		SettingNameDefaultDataPath:                                          SettingDefinitionDefaultDataPath,
		SettingNameDefaultEngineImage:                                       SettingDefinitionDefaultEngineImage,
		SettingNameDefaultInstanceManagerImage:                              SettingDefinitionDefaultInstanceManagerImage,
		SettingNameDefaultBackingImageManagerImage:                          SettingDefinitionDefaultBackingImageManagerImage,
		SettingNameSupportBundleManagerImage:                                SettingDefinitionSupportBundleManagerImage,
		SettingNameReplicaSoftAntiAffinity:                                  SettingDefinitionReplicaSoftAntiAffinity,
		SettingNameReplicaAutoBalance:                                       SettingDefinitionReplicaAutoBalance,
		SettingNameReplicaAutoBalanceDiskPressurePercentage:                 SettingDefinitionReplicaAutoBalanceDiskPressurePercentage,
		SettingNameStorageOverProvisioningPercentage:                        SettingDefinitionStorageOverProvisioningPercentage,
		SettingNameStorageMinimalAvailablePercentage:                        SettingDefinitionStorageMinimalAvailablePercentage,
		SettingNameStorageReservedPercentageForDefaultDisk:                  SettingDefinitionStorageReservedPercentageForDefaultDisk,
		SettingNameUpgradeChecker:                                           SettingDefinitionUpgradeChecker,
		SettingNameUpgradeResponderURL:                                      SettingDefinitionUpgradeResponderURL,
		SettingNameAllowCollectingLonghornUsage:                             SettingDefinitionAllowCollectingLonghornUsageMetrics,
		SettingNameCurrentLonghornVersion:                                   SettingDefinitionCurrentLonghornVersion,
		SettingNameLatestLonghornVersion:                                    SettingDefinitionLatestLonghornVersion,
		SettingNameStableLonghornVersions:                                   SettingDefinitionStableLonghornVersions,
		SettingNameDefaultReplicaCount:                                      SettingDefinitionDefaultReplicaCount,
		SettingNameDefaultDataLocality:                                      SettingDefinitionDefaultDataLocality,
		SettingNameDefaultLonghornStaticStorageClass:                        SettingDefinitionDefaultLonghornStaticStorageClass,
		SettingNameTaintToleration:                                          SettingDefinitionTaintToleration,
		SettingNameSystemManagedComponentsNodeSelector:                      SettingDefinitionSystemManagedComponentsNodeSelector,
		SettingNameCRDAPIVersion:                                            SettingDefinitionCRDAPIVersion,
		SettingNameAutoSalvage:                                              SettingDefinitionAutoSalvage,
		SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly:              SettingDefinitionAutoDeletePodWhenVolumeDetachedUnexpectedly,
		SettingNameRegistrySecret:                                           SettingDefinitionRegistrySecret,
		SettingNameDisableSchedulingOnCordonedNode:                          SettingDefinitionDisableSchedulingOnCordonedNode,
		SettingNameReplicaZoneSoftAntiAffinity:                              SettingDefinitionReplicaZoneSoftAntiAffinity,
		SettingNameNodeDownPodDeletionPolicy:                                SettingDefinitionNodeDownPodDeletionPolicy,
		SettingNameNodeDrainPolicy:                                          SettingDefinitionNodeDrainPolicy,
		SettingNameDetachManuallyAttachedVolumesWhenCordoned:                SettingDefinitionDetachManuallyAttachedVolumesWhenCordoned,
		SettingNamePriorityClass:                                            SettingDefinitionPriorityClass,
		SettingNameDisableRevisionCounter:                                   SettingDefinitionDisableRevisionCounter,
		SettingNameReplicaReplenishmentWaitInterval:                         SettingDefinitionReplicaReplenishmentWaitInterval,
		SettingNameConcurrentReplicaRebuildPerNodeLimit:                     SettingDefinitionConcurrentReplicaRebuildPerNodeLimit,
		SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit:          SettingDefinitionConcurrentBackingImageCopyReplenishPerNodeLimit,
		SettingNameConcurrentBackupRestorePerNodeLimit:                      SettingDefinitionConcurrentVolumeBackupRestorePerNodeLimit,
		SettingNameSystemManagedPodsImagePullPolicy:                         SettingDefinitionSystemManagedPodsImagePullPolicy,
		SettingNameAllowVolumeCreationWithDegradedAvailability:              SettingDefinitionAllowVolumeCreationWithDegradedAvailability,
		SettingNameAutoCleanupSystemGeneratedSnapshot:                       SettingDefinitionAutoCleanupSystemGeneratedSnapshot,
		SettingNameAutoCleanupRecurringJobBackupSnapshot:                    SettingDefinitionAutoCleanupRecurringJobBackupSnapshot,
		SettingNameConcurrentAutomaticEngineUpgradePerNodeLimit:             SettingDefinitionConcurrentAutomaticEngineUpgradePerNodeLimit,
		SettingNameBackingImageCleanupWaitInterval:                          SettingDefinitionBackingImageCleanupWaitInterval,
		SettingNameBackingImageRecoveryWaitInterval:                         SettingDefinitionBackingImageRecoveryWaitInterval,
		SettingNameGuaranteedInstanceManagerCPU:                             SettingDefinitionGuaranteedInstanceManagerCPU,
		SettingNameKubernetesClusterAutoscalerEnabled:                       SettingDefinitionKubernetesClusterAutoscalerEnabled,
		SettingNameOrphanResourceAutoDeletion:                               SettingDefinitionOrphanResourceAutoDeletion,
		SettingNameOrphanResourceAutoDeletionGracePeriod:                    SettingDefinitionOrphanResourceAutoDeletionGracePeriod,
		SettingNameStorageNetwork:                                           SettingDefinitionStorageNetwork,
		SettingNameStorageNetworkForRWXVolumeEnabled:                        SettingDefinitionStorageNetworkForRWXVolumeEnabled,
		SettingNameFailedBackupTTL:                                          SettingDefinitionFailedBackupTTL,
		SettingNameRecurringSuccessfulJobsHistoryLimit:                      SettingDefinitionRecurringSuccessfulJobsHistoryLimit,
		SettingNameRecurringFailedJobsHistoryLimit:                          SettingDefinitionRecurringFailedJobsHistoryLimit,
		SettingNameRecurringJobMaxRetention:                                 SettingDefinitionRecurringJobMaxRetention,
		SettingNameSupportBundleFailedHistoryLimit:                          SettingDefinitionSupportBundleFailedHistoryLimit,
		SettingNameSupportBundleNodeCollectionTimeout:                       SettingDefinitionSupportBundleNodeCollectionTimeout,
		SettingNameDeletingConfirmationFlag:                                 SettingDefinitionDeletingConfirmationFlag,
		SettingNameEngineReplicaTimeout:                                     SettingDefinitionEngineReplicaTimeout,
		SettingNameSnapshotDataIntegrity:                                    SettingDefinitionSnapshotDataIntegrity,
		SettingNameSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation: SettingDefinitionSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation,
		SettingNameSnapshotDataIntegrityCronJob:                             SettingDefinitionSnapshotDataIntegrityCronJob,
		SettingNameSnapshotMaxCount:                                         SettingDefinitionSnapshotMaxCount,
		SettingNameRestoreVolumeRecurringJobs:                               SettingDefinitionRestoreVolumeRecurringJobs,
		SettingNameRemoveSnapshotsDuringFilesystemTrim:                      SettingDefinitionRemoveSnapshotsDuringFilesystemTrim,
		SettingNameFastReplicaRebuildEnabled:                                SettingDefinitionFastReplicaRebuildEnabled,
		SettingNameReplicaFileSyncHTTPClientTimeout:                         SettingDefinitionReplicaFileSyncHTTPClientTimeout,
		SettingNameLongGPRCTimeOut:                                          SettingDefinitionLongGPRCTimeOut,
		SettingNameBackupCompressionMethod:                                  SettingDefinitionBackupCompressionMethod,
		SettingNameBackupConcurrentLimit:                                    SettingDefinitionBackupConcurrentLimit,
		SettingNameRestoreConcurrentLimit:                                   SettingDefinitionRestoreConcurrentLimit,
		SettingNameLogLevel:                                                 SettingDefinitionLogLevel,
		SettingNameV1DataEngine:                                             SettingDefinitionV1DataEngine,
		SettingNameV2DataEngine:                                             SettingDefinitionV2DataEngine,
		SettingNameDataEngineHugepageEnabled:                                SettingDefinitionDataEngineHugepageEnabled,
		SettingNameDataEngineMemorySize:                                     SettingDefinitionDataEngineMemorySize,
		SettingNameDataEngineCPUMask:                                        SettingDefinitionDataEngineCPUMask,
		SettingNameDataEngineLogLevel:                                       SettingDefinitionDataEngineLogLevel,
		SettingNameDataEngineLogFlags:                                       SettingDefinitionDataEngineLogFlags,
		SettingNameDataEngineInterruptModeEnabled:                           SettingDefinitionDataEngineInterruptModeEnabled,
		SettingNameReplicaDiskSoftAntiAffinity:                              SettingDefinitionReplicaDiskSoftAntiAffinity,
		SettingNameAllowEmptyNodeSelectorVolume:                             SettingDefinitionAllowEmptyNodeSelectorVolume,
		SettingNameAllowEmptyDiskSelectorVolume:                             SettingDefinitionAllowEmptyDiskSelectorVolume,
		SettingNameDisableSnapshotPurge:                                     SettingDefinitionDisableSnapshotPurge,
		SettingNameFreezeFilesystemForSnapshot:                              SettingDefinitionFreezeFilesystemForSnapshot,
		SettingNameAutoCleanupSnapshotWhenDeleteBackup:                      SettingDefinitionAutoCleanupSnapshotWhenDeleteBackup,
		SettingNameAutoCleanupSnapshotAfterOnDemandBackupCompleted:          SettingDefinitionAutoCleanupSnapshotAfterOnDemandBackupCompleted,
		SettingNameDefaultMinNumberOfBackingImageCopies:                     SettingDefinitionDefaultMinNumberOfBackingImageCopies,
		SettingNameBackupExecutionTimeout:                                   SettingDefinitionBackupExecutionTimeout,
		SettingNameRWXVolumeFastFailover:                                    SettingDefinitionRWXVolumeFastFailover,
		SettingNameOfflineReplicaRebuilding:                                 SettingDefinitionOfflineReplicaRebuilding,
		SettingNameReplicaRebuildingBandwidthLimit:                          SettingDefinitionReplicaRebuildingBandwidthLimit,
		SettingNameDefaultBackupBlockSize:                                   SettingDefinitionDefaultBackupBlockSize,
		SettingNameInstanceManagerPodLivenessProbeTimeout:                   SettingDefinitionInstanceManagerPodLivenessProbeTimeout,
		SettingNameLogPath:                                                  SettingDefinitionLogPath,
	}

	SettingDefinitionAllowRecurringJobWhileVolumeDetached = SettingDefinition{
		DisplayName:        "Allow Recurring Job While Volume Is Detached",
		Description:        "If this setting is enabled, Longhorn will automatically attach the volume and take a snapshot/backup when it is time for a recurring job. The volume will not be available for workloads during this time and the workload will be paused until the job completes.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionFailedBackupTTL = SettingDefinition{
		DisplayName:        "Failed Backup Time To Live",
		Description:        "In minutes. This setting determines how long Longhorn will keep a failed backup resource. Set to 0 to disable auto-deletion. Failed backups are checked and cleaned up during the backupstore polling interval (controlled by **Backupstore Poll Interval**). Therefore, the actual cleanup interval is a multiple of the polling interval. Disabling the **Backupstore Poll Interval** also disables this auto-deletion.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "1440",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionBackupExecutionTimeout = SettingDefinition{
		DisplayName:        "Backup Execution Timeout",
		Description:        "In minutes. This setting determines the number of minutes Longhorn allows for a backup operation to complete. If the backup does not finish within this time, it will be marked as a failure.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "1",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
		},
	}

	SettingDefinitionRestoreVolumeRecurringJobs = SettingDefinition{
		DisplayName: "Restore Volume Recurring Jobs",
		Description: "This setting allows restoring recurring jobs from the backup volume on the backup target and create recurring jobs if it does not already exist during a backup restoration." +
			"\nLonghorn also supports individual volume setting. The setting can be specified on the Backup page when making a backup restoration, this overrules the global setting." +
			"\n\n**The available options are**:\n" +
			"\n- **ignored**: This is the default option that instructs Longhorn to inherit from the global setting." +
			"\n- **enabled**: This option instructs Longhorn to restore recurring jobs/groups from the backup target forcibly." +
			"\n- **disabled**: This option instructs Longhorn no restoring recurring jobs/groups should be done.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionCreateDefaultDiskLabeledNodes = SettingDefinition{
		DisplayName:        "Create Default Disk On Labeled Nodes",
		Description:        "When enabled, a default disk is automatically created only on nodes with the label \"node.longhorn.io/create-default-disk=true\", and only if no other disks already exist on that node. If disabled, a default disk will be created on all new nodes when they are first added to the cluster. This setting is useful for scaling your cluster without using storage on new nodes, or when you want to customize disks for specific Longhorn nodes.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionDefaultDataPath = SettingDefinition{
		DisplayName:        "Default Data Path",
		Description:        "The default path on a host for storing Longhorn volume data. An absolute directory path, like \"/var/lib/longhorn/\", indicates a filesystem-type disk for the V1 Data Engine, while a path to a block device indicates a block-type disk for the V2 Data Engine. This setting can be used with \"Create Default Disk on Labeled Nodes\" to ensure Longhorn only utilizes specific storage when scaling the cluster.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "/var/lib/longhorn/",
	}

	SettingDefinitionDefaultEngineImage = SettingDefinition{
		DisplayName: "Default Engine Image",
		Description: "The default engine image used by the Longhorn manager. This image is required only for the V1 Data Engine and not for the V2 Data Engine. This setting can only be changed on the manager's command line at startup." +
			"\n\n**NOTE**: An arrow in the Longhorn UI will indicate if existing volumes need to be upgraded to this default engine image.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           true,
		DataEngineSpecific: false,
	}

	SettingDefinitionDefaultInstanceManagerImage = SettingDefinition{
		DisplayName:        "Default Instance Manager Image",
		Description:        "The default instance manager image used by the Longhorn manager. This setting can only be changed on the manager's command line at startup.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeDeprecated,
		Required:           true,
		ReadOnly:           true,
		DataEngineSpecific: false,
	}

	SettingDefinitionDefaultBackingImageManagerImage = SettingDefinition{
		DisplayName:        "Default Backing Image Manager Image",
		Description:        "The default backing image manager image used by the Longhorn manager. This setting can only be changed on the manager's command line at startup.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeDeprecated,
		Required:           true,
		ReadOnly:           true,
		DataEngineSpecific: false,
	}

	SettingDefinitionSupportBundleManagerImage = SettingDefinition{
		DisplayName:        "Support Bundle Manager Image",
		Description:        "The support bundle manager image used by Longhorn to generate the support bundle.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
	}

	SettingDefinitionReplicaSoftAntiAffinity = SettingDefinition{
		DisplayName: "Replica Node Level Soft Anti-Affinity",
		Description: "When enabled, this setting allows Longhorn to schedule new replicas of a volume on nodes that already have a healthy replica for the same volume. If disabled, Longhorn will forbid scheduling new replicas on nodes with existing healthy replicas." +
			"\n\n**NOTE**: This setting is superseded if replicas are forbidden from sharing a zone by the **Replica Zone Level Anti-Affinity** setting.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionFreezeFilesystemForSnapshot = SettingDefinition{
		DisplayName: "Freeze Filesystem For Snapshot",
		Description: "This setting applies only to volumes with the Kubernetes volume mode `Filesystem`. When enabled, Longhorn freezes the volume's filesystem immediately before creating a user-initiated snapshot. This results in more consistent snapshots but may pause workload activity under heavy I/O. If disabled or for volumes in `Block` mode, Longhorn instead performs a system sync before the snapshot, which is less likely to affect workloads. The default option is `false` due to potential kernel issues in versions older than `v5.17`. It is only recommended to enable this setting with ext4 or XFS filesystems on kernels `v5.17` or later." +
			"\n\nThis setting can be overridden for specific volumes via the Longhorn UI, a StorageClass, or by directly modifying an existing volume. The override options are `ignored` (uses the global setting), `enabled` (forces freezing before snapshot), and `disabled` (prevents freezing before snapshot).",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"false\"}", longhorn.DataEngineTypeV1),
	}

	SettingDefinitionReplicaAutoBalance = SettingDefinition{
		DisplayName: "Replica Auto Balance",
		Description: "This setting automatically rebalances replicas when new, available nodes are discovered. This ensures optimal redundancy and resource utilization." +
			"\n\n**Global Options**:" +
			"\n- **disabled**: (Default) No replica auto-balance will be done." +
			"\n- **least-effort**: Instructs Longhorn to balance replicas for minimal redundancy." +
			"\n- **best-effort**: Instructs Longhorn to try to balance replicas for even redundancy. This does not force a balance at the zone level if there are not enough nodes; instead, Longhorn will rebalance at the node level." +
			"\n\nLonghorn also supports customizing for individual volume. The setting can be specified in UI or with Kubernetes manifest `volume.spec.replicaAutoBalance`, this overrules the global setting." +
			"\n\n**Individual Volume Setting**:" +
			"\n- **ignored**: (Default) The volume will inherit the global setting." +
			"\n- **disabled**: No replica auto-balance will be done for this volume." +
			"\n- **least-effort**: Balance replicas for minimal redundancy for this volume." +
			"\n- **best-effort**: Attempt to balance replicas for even redundancy at the node level for this volume." +
			"\n\n**NOTE**: Longhorn does not forcefully re-schedule the replicas to a zone that does not have enough nodes to support even balance. Instead, Longhorn will re-schedule to balance at the node level.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            string(longhorn.ReplicaAutoBalanceDisabled),
		Choices: []any{
			string(longhorn.ReplicaAutoBalanceDisabled),
			string(longhorn.ReplicaAutoBalanceLeastEffort),
			string(longhorn.ReplicaAutoBalanceBestEffort),
		},
	}

	SettingDefinitionReplicaAutoBalanceDiskPressurePercentage = SettingDefinition{
		DisplayName: "Replica Auto Balance Disk Pressure Threshold (%)",
		Description: "The percentage of used storage that, when reached, triggers automatic replica rebalancing. When the threshold is met, Longhorn rebuilds replicas that are under disk pressure on a different disk within the same node. Set the value to `0` to disable this feature." +
			"\n\nThis setting only takes effect when **Replica Auto Balance** is set to **best-effort** and at least one other disk on the node has sufficient available space." +
			"\n\n**NOTE**: This setting is not affected by **Replica Node Level Soft Anti-Affinity**, which prevents Longhorn from scheduling replicas on the same node. This feature will still attempt to rebuild a replica on a different disk on the same node for migration purposes, regardless of that setting's value.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "90",
	}

	SettingDefinitionStorageOverProvisioningPercentage = SettingDefinition{
		DisplayName: "Storage Over Provisioning Percentage",
		Description: "This setting defines the ratio of allocated storage to the hard drive’s actual physical capacity. Adjusting this allows Longhorn Manager to schedule new replicas on a disk as long as their combined size (of all replicas within the permitted over-provisioning percentage of the usable disk space) doesn't exceed the over-provisioning limit. This limit is based on the disk's usable space, which is calculated as the `Storage Maximum` minus the `Storage Reserved` amount." +
			"\n\n**NOTE**: Replicas may consume more space than a volume's nominal size due to snapshot data. You can reclaim disk space by deleting snapshots that are no longer needed.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "100",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionStorageMinimalAvailablePercentage = SettingDefinition{
		DisplayName:        "Storage Minimal Available Percentage",
		Description:        "This setting defines the minimum amount of disk space that must remain available on a disk before Longhorn can schedule a new replica. If adding a new replica would cause the available space to drop below this percentage, the disk is marked as unschedulable until more space is freed up. This helps protect your disks from becoming too full, preventing potential performance issues and storage failures.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "25",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
			ValueIntRangeMaximum: 100,
		},
	}

	SettingDefinitionStorageReservedPercentageForDefaultDisk = SettingDefinition{
		DisplayName: "Storage Reserved Percentage For Default Disk",
		Description: "This setting reserves a percentage of disk space that will not be allocated to the default disk on each new Longhorn node." +
			"\n\nThis setting only affects the default disk of a new adding node or nodes when installing Longhorn.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "30",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
			ValueIntRangeMaximum: 100,
		},
	}

	SettingDefinitionUpgradeChecker = SettingDefinition{
		DisplayName:        "Enable Upgrade Checker",
		Description:        "Upgrade Checker will check for new Longhorn version periodically. When there is a new version available, a notification will appear in the Longhorn UI.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionUpgradeResponderURL = SettingDefinition{
		DisplayName:        "Upgrade Responder URL",
		Description:        "The Upgrade Responder sends a notification whenever a new version of Longhorn is available to upgrade.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "https://longhorn-upgrade-responder.rancher.io/v1/checkupgrade",
	}

	SettingDefinitionAllowCollectingLonghornUsageMetrics = SettingDefinition{
		DisplayName: "Allow Collecting Longhorn Usage Metrics",
		Description: "Enabling this setting allows Longhorn to provide valuable usage metrics to `https://metrics.longhorn.io/`. This information helps us understand how Longhorn is being used, which will ultimately contribute to future improvements. The collected data is anonymized and includes details about the cluster, nodes, and volume configuration, but does not include any personal or sensitive information. It is important to note that the `Upgrade Checker` setting must be enabled for this data to be periodically sent." +
			"\n\n---" +
			"\n\n**Collected Metrics**:" +
			"\n\nNode Information collected from all cluster nodes includes:" +
			"\n- Number of disks of each device type (HDD, SSD, NVMe, unknown). This value may not be accurate for virtual machines." +
			"\n- Number of disks for each Longhorn disk type (block, filesystem)." +
			"\n- Host system architecture." +
			"\n- Host kernel release." +
			"\n- Host operating system (OS) distribution." +
			"\n- Kubernetes node provider." +
			"\n\nCluster Information collected from one of the cluster nodes includes:" +
			"\n- Longhorn namespace UID." +
			"\n- Number of Longhorn nodes." +
			"\n- Number of volumes of each access mode (RWO, RWX, unknown)." +
			"\n- Number of volumes of each data engine (V1, V2)." +
			"\n- Number of volumes of each data locality type (disabled, best_effort, strict_local, unknown)." +
			"\n- Number of volumes that are encrypted or unencrypted." +
			"\n- Number of volumes of each frontend type (blockdev, iscsi)." +
			"\n- Number of replicas." +
			"\n- Number of snapshots." +
			"\n- Number of backing images." +
			"\n- Number of orphans." +
			"\n- Average volume size in bytes." +
			"\n- Average volume actual size in bytes." +
			"\n- Average number of snapshots per volume." +
			"\n- Average number of replicas per volume." +
			"\n- Average Longhorn component CPU usage (instance manager, manager) in millicores." +
			"\n- Average Longhorn component memory usage (instance manager, manager) in bytes." +
			"\n\nLonghorn settings:" +
			"\n- Partially included: Backup Target Type/Protocol (azblob, cifs, nfs, s3, none, unknown). This is from the Backup Target setting." +
			"\n- Included as true or false to indicate if this setting is configured:" +
			"\n  - Priority Class" +
			"\n  - Registry Secret" +
			"\n  - Snapshot Data Integrity CronJob" +
			"\n  - Storage Network" +
			"\n  - System Managed Components Node Selector" +
			"\n  - Taint Toleration" +
			"\n- Included as it is:" +
			"\n  - Allow Recurring Job While Volume Is Detached" +
			"\n  - Allow Volume Creation With Degraded Availability" +
			"\n  - Automatically Clean up System Generated Snapshot" +
			"\n  - Automatically Clean up Outdated Snapshots of Recurring Backup Jobs" +
			"\n  - Automatically Delete Workload Pod when The Volume Is Detached Unexpectedly" +
			"\n  - Automatic Salvage" +
			"\n  - Backing Image Cleanup Wait Interval" +
			"\n  - Backing Image Recovery Wait Interval" +
			"\n  - Backup Compression Method" +
			"\n  - Backupstore Poll Interval" +
			"\n  - Backup Concurrent Limit" +
			"\n  - Concurrent Automatic Engine Upgrade Per Node Limit" +
			"\n  - Concurrent Backup Restore Per Node Limit" +
			"\n  - Concurrent Replica Rebuild Per Node Limit" +
			"\n  - CRD API Version" +
			"\n  - Create Default Disk Labeled Nodes" +
			"\n  - Default Data Locality" +
			"\n  - Default Replica Count" +
			"\n  - Disable Revision Counter" +
			"\n  - Disable Scheduling On Cordoned Node" +
			"\n  - Engine Replica Timeout" +
			"\n  - Failed Backup TTL" +
			"\n  - Fast Replica Rebuild Enabled" +
			"\n  - Guaranteed Instance Manager CPU" +
			"\n  - Kubernetes Cluster Autoscaler Enabled" +
			"\n  - Node Down Pod Deletion Policy" +
			"\n  - Node Drain Policy" +
			"\n  - Orphan Auto Deletion" +
			"\n  - Recurring Failed Jobs History Limit" +
			"\n  - Recurring Successful Jobs History Limit" +
			"\n  - Remove Snapshots During Filesystem Trim" +
			"\n  - Replica Auto Balance" +
			"\n  - Replica File Sync HTTP Client Timeout" +
			"\n  - Replica Replenishment Wait Interval" +
			"\n  - Replica Soft Anti Affinity" +
			"\n  - Replica Zone Soft Anti Affinity" +
			"\n  - Replica Disk Soft Anti Affinity" +
			"\n  - Restore Concurrent Limit" +
			"\n  - Restore Volume Recurring Jobs" +
			"\n  - Snapshot Data Integrity" +
			"\n  - Snapshot DataIntegrity Immediate Check After Snapshot Creation" +
			"\n  - Storage Minimal Available Percentage" +
			"\n  - Storage Network For RWX Volume Enabled" +
			"\n  - Storage Over Provisioning Percentage" +
			"\n  - Storage Reserved Percentage For Default Disk" +
			"\n  - Support Bundle Failed History Limit" +
			"\n  - Support Bundle Node Collection Timeout" +
			"\n  - System Managed Pods Image Pull Policy",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionCurrentLonghornVersion = SettingDefinition{
		DisplayName:        "Current Longhorn Version",
		Description:        "The current version of Longhorn.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           true,
		DataEngineSpecific: false,
		Default:            meta.Version,
	}

	SettingDefinitionLatestLonghornVersion = SettingDefinition{
		DisplayName:        "Latest Longhorn Version",
		Description:        "The latest version of Longhorn available. This is updated automatically by the Upgrade Checker.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           true,
		DataEngineSpecific: false,
	}

	SettingDefinitionStableLonghornVersions = SettingDefinition{
		DisplayName:        "Stable Longhorn Versions",
		Description:        "The latest stable version for each minor release line of Longhorn. This is updated automatically by the Upgrade Checker.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           true,
		DataEngineSpecific: false,
	}

	SettingDefinitionDefaultReplicaCount = SettingDefinition{
		DisplayName: "Default Replica Count",
		Description: "The default number of replicas created when a volume is provisioned from the Longhorn UI. For Kubernetes, you should set this value in the `numberOfReplicas` field of the StorageClass." +
			"\nThe recommended replica count is 3 for clusters with three or more storage nodes, and 2 for smaller clusters. While a single replica is possible on a single-node cluster, it will not provide high availability, though you can still use snapshots and backups.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"3\",%q:\"3\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
			ValueIntRangeMaximum: 20,
		},
	}

	SettingDefinitionDefaultDataLocality = SettingDefinition{
		DisplayName: "Default Data Locality",
		Description: "A Longhorn volume has data locality if a replica of the volume is on the same node as the pod using it. This setting specifies the default data locality when a volume is created from the Longhorn UI. For Kubernetes, this is configured via the `dataLocality` in the StorageClass." +
			"\n\n**The available modes are**:\n" +
			"\n- **disabled**: (Default) There may or may not be a replica on the same node as the workload." +
			"\n- **best-effort**: Longhorn will try to keep a replica on the same node as the workload. The volume will not be stopped if this cannot be achieved due to limitations like insufficient disk space." +
			"\n- **strict-local**: Longhorn enforces having only one replica on the same node as the workload, which provides higher IOPS and lower latency.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            string(longhorn.DataLocalityDisabled),
		Choices: []any{
			string(longhorn.DataLocalityDisabled),
			string(longhorn.DataLocalityBestEffort),
			string(longhorn.DataLocalityStrictLocal),
		},
	}

	SettingDefinitionDefaultLonghornStaticStorageClass = SettingDefinition{
		DisplayName:        "Default Longhorn Static StorageClass Name",
		Description:        "This setting specifies the StorageClass name used to bind an existing Longhorn volume to a PersistentVolume (PV) and PersistentVolumeClaim (PVC). The StorageClass is only used as a matching label for PVC binding purposes, so users don't need to manually create the object in Kubernetes. By default, only a StorageClass named `longhorn-static` will be automatically created by Longhorn if it does not already exist.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "longhorn-static",
	}

	SettingDefinitionTaintToleration = SettingDefinition{
		DisplayName: "Kubernetes Taint Toleration",
		Description: "To dedicate nodes for Longhorn replicas and prevent other workloads from running on them, you can set tolerations for all Longhorn components and add taints to the dedicated nodes. This setting only applies to system-managed components (for example, instance manager, engine image, CSI driver). You must set tolerations for user-deployed components (for example, Longhorn manager, driver, UI) separately via the Helm chart or deployment YAML." +
			"\n\nAll Longhorn volumes must be detached before modifying these settings. It is highly recommended to configure tolerations during initial deployment, as the system cannot be operated during the update. When volumes are in use, Longhorn components are not restarted, and you need to reconfigure the settings after detaching the remaining volumes; otherwise, you can wait for the setting change to be reconciled in an hour. We recommend setting tolerations during Longhorn deployment because the Longhorn system cannot be operated during the update." +
			"\n\nMultiple tolerations can be set, separated by a semicolon. For example:" +
			"\n- `key1=value1:NoSchedule; key2:NoExecute`" +
			"\n- `:` (tolerates everything because an empty key with an `Exists` operator matches all keys, values, and effects)" +
			"\n- `key1=value1:` (has an empty effect and matches all effects with key `key1`)" +
			"\n\n**WARNING**: Do not use keys with the `kubernetes.io` prefix, as they are reserved for Kubernetes default tolerations.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
	}

	SettingDefinitionSystemManagedComponentsNodeSelector = SettingDefinition{
		DisplayName: "System Managed Components Node Selector",
		Description: "This setting restricts Longhorn's system-managed components to run only on a specific set of nodes. Longhorn's system includes user-deployed components (for example, Longhorn manager, driver, UI) and system-managed components (for example, instance manager, engine image, CSI driver, etc.). You must set node selectors for both." +
			"\n\n**You must follow this order when setting the node selector**:" +
			"\n\n1. Set the node selector for user-deployed components in the Helm chart or deployment YAML file." +
			"\n2. Set the node selector for system-managed components here." +
			"\n\nAll Longhorn volumes must be detached before modifying this setting. It is recommended to set node selectors during initial Longhorn deployment, as the system will be unavailable while components restart. Multiple label key-value pairs are separated by a semicolon. For example: `label-key1=label-value1; label-key2=label-value2`." +
			"\n\nPlease see the documentation at https://longhorn.io for more detailed instructions about changing node selector.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
	}

	SettingDefinitionCRDAPIVersion = SettingDefinition{
		DisplayName:        "Custom Resource API Version",
		Description:        "The current customer resource's API version, for example, longhorn.io/v1beta2. It is set by the Longhorn manager automatically.",
		Category:           SettingCategorySystemInfo,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           true,
		DataEngineSpecific: false,
	}

	SettingDefinitionAutoSalvage = SettingDefinition{
		DisplayName:        "Automatic Salvage",
		Description:        "If enabled, Longhorn will automatically attempt to recover a volume when all its replicas appear faulty. Longhorn will identify and use any salvageable replicas to restore the volume's functionality.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionAutoDeletePodWhenVolumeDetachedUnexpectedly = SettingDefinition{
		DisplayName: "Automatically Delete Workload Pod When The Volume Is Detached Unexpectedly",
		Description: "If enabled, Longhorn automatically deletes a workload pod managed by a controller (for example, deployment, statefulset) when its Longhorn volume is unexpectedly detached (for example, during a Kubernetes upgrade, Docker reboot, or network disconnect). Deleting the pod allows its controller to restart it, and Kubernetes then handles volume reattachment and remount." +
			"\n\nIf disabled, Longhorn will not delete the pod. You will have to manually restart it to reattach the volume." +
			"\n\n**NOTE**: This setting does not apply in the following cases:" +
			"\n- Workload pods without a controller; Longhorn never deletes them." +
			"\n- Workload pods with **cluster network** RWX volumes. This is because the Longhorn Share Manager, which provides the RWX NFS service, has its own resilience mechanism. This setting does, however, apply to workload pods with **storage network** RWX volumes.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionRegistrySecret = SettingDefinition{
		DisplayName:        "Registry Secret",
		Description:        "The name of the Kubernetes Secret.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "",
	}

	SettingDefinitionDisableSchedulingOnCordonedNode = SettingDefinition{
		DisplayName: "Disable Scheduling On Cordoned Node",
		Description: "When this setting is enabled, the Longhorn manager will not schedule new replicas on a Kubernetes cordoned node. When this setting is disabled, the Longhorn Manager will schedule replicas on Kubernetes cordoned nodes." +
			"\nCordoning a node prevents new pods from being scheduled on it, which is often done in preparation for maintenance or decommissioning.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionReplicaZoneSoftAntiAffinity = SettingDefinition{
		DisplayName:        "Replica Zone Level Soft Anti-Affinity",
		Description:        "When enabled, this setting allows Longhorn to schedule new replicas of a volume on nodes within the same Kubernetes zone as existing healthy replicas. If disabled, Longhorn will prevent replicas from being scheduled in the same zone. Nodes that don't belong to a zone are considered to be in the same zone. Longhorn identifies zones based on the `topology.kubernetes.io/zone` label on the Kubernetes node object.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionNodeDownPodDeletionPolicy = SettingDefinition{
		DisplayName: "Pod Deletion Policy When Node Is Down",
		Description: "This setting defines the Longhorn action when a Volume is stuck with a StatefulSet/Deployment Pod on a node that is down." +
			"\n\n**The various available options are**:" +
			"\n- **do-nothing** is the **default** Kubernetes behavior of never force deleting StatefulSet/Deployment terminating pods. Since the pod on the node that is down isn't removed, Longhorn volumes are stuck on nodes that are down." +
			"\n- **delete-statefulset-pod** Longhorn will force delete StatefulSet terminating pods on nodes that are down to release Longhorn volumes so that Kubernetes can spin up replacement pods." +
			"\n- **delete-deployment-pod** Longhorn will force delete Deployment terminating pods on nodes that are down to release Longhorn volumes so that Kubernetes can spin up replacement pods." +
			"\n- **delete-both-statefulset-and-deployment-pod** Longhorn will force delete StatefulSet/Deployment terminating pods on nodes that are down to release Longhorn volumes so that Kubernetes can spin up replacement pods.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            string(NodeDownPodDeletionPolicyDoNothing),
		Choices: []any{
			string(NodeDownPodDeletionPolicyDoNothing),
			string(NodeDownPodDeletionPolicyDeleteStatefulSetPod),
			string(NodeDownPodDeletionPolicyDeleteDeploymentPod),
			string(NodeDownPodDeletionPolicyDeleteBothStatefulsetAndDeploymentPod),
		},
	}

	SettingDefinitionNodeDrainPolicy = SettingDefinition{
		DisplayName: "Node Drain Policy",
		Description: "This setting defines the policy to use when a node with the last healthy replica of a volume is drained." +
			"\n\n**The various available options are**:" +
			"\n- **block-for-eviction**: Longhorn will automatically evict all replicas and block the drain until eviction is complete." +
			"\n- **block-for-eviction-if-contains-last-replica**: Longhorn will automatically evict any replicas that don't have a healthy counterpart and block the drain until eviction is complete." +
			"\n- **block-if-contains-last-replica**: Longhorn will block the drain when the node contains the last healthy replica of a volume." +
			"\n- **allow-if-replica-is-stopped**: Longhorn will allow the drain when the node contains the last healthy replica of a volume but the replica is stopped. **Warning*: There is a possibility of data loss if the node is removed after draining. Select this option if you want to drain the node and do in-place upgrade/maintenance." +
			"\n- **always-allow**: Longhorn will allow the drain even though the node contains the last healthy replica of a volume. **WARNING**: There is a possibility of data loss if the node is removed after draining. Data corruption is also possible if the last replica was running during the draining.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            string(NodeDrainPolicyBlockIfContainsLastReplica),
		Choices: []any{
			string(NodeDrainPolicyBlockForEviction),
			string(NodeDrainPolicyBlockForEvictionIfContainsLastReplica),
			string(NodeDrainPolicyBlockIfContainsLastReplica),
			string(NodeDrainPolicyAllowIfReplicaIsStopped),
			string(NodeDrainPolicyAlwaysAllow),
		},
	}

	SettingDefinitionDetachManuallyAttachedVolumesWhenCordoned = SettingDefinition{
		DisplayName:        "Detach Manually Attached Volumes When Cordoned",
		Description:        "When enabled, Longhorn will automatically detach volumes that were manually attached to a node once that node is cordoned. This prevent the draining process stuck by the PDB of instance-manager which still has running engine on the node.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionPriorityClass = SettingDefinition{
		DisplayName: "Priority Class",
		Description: "This setting specifies the name of the Kubernetes PriorityClass to apply to Longhorn's system-managed components. This can help prevent Longhorn pods from being evicted under node pressure (for example, low memory)." +
			"\n\nBy default, Longhorn workloads run with the same priority as other pods in the cluster, making them equally susceptible to eviction. This setting allows you to assign a higher priority to Longhorn workloads so they are not the first to be evicted." +
			"\n\nThe Longhorn system includes both user-deployed components (for example, manager, driver, UI) and system-managed components (for example, instance manager, engine image, CSI driver). Note that this setting only applies to system-managed components. You must set the PriorityClass for user-deployed components separately in your Helm chart or deployment YAML." +
			"\n\n**WARNING**: This setting should only be changed after all Longhorn volumes are detached, as system components will be restarted. The system will be unavailable during the update. It is recommended to set the PriorityClass during initial Longhorn deployment.",
		Category:           SettingCategoryDangerZone,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
	}

	SettingDefinitionDisableRevisionCounter = SettingDefinition{
		DisplayName:        "Disable Revision Counter",
		Description:        "This setting applies only to volumes created via the Longhorn UI. When this setting is enabled (the default), Longhorn does not use a revision counter to track volume writes. Instead, during salvage recovery, it uses file modification time and size to select the best replica. When this setting is disabled, Longhorn maintains a revision counter, which allows it to pick the replica with the highest counter for more accurate recovery during salvage.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"true\"}", longhorn.DataEngineTypeV1),
	}

	SettingDefinitionReplicaReplenishmentWaitInterval = SettingDefinition{
		DisplayName: "Replica Replenishment Wait Interval",
		Description: "In seconds. This setting specifies how long Longhorn will wait before creating a new replica for a degraded volume. During this time, Longhorn attempts to reuse data from an existing failed replica to save rebuilding time and network bandwidth." +
			"\n\n**WARNING**: This setting only takes effect when there is at least one failed replica in the volume, and it may temporarily delay the volume's recovery.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "600",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionConcurrentReplicaRebuildPerNodeLimit = SettingDefinition{
		DisplayName: "Concurrent Replica Rebuild Per Node Limit",
		Description: "This setting controls how many replicas on a node can be rebuilt simultaneously." +
			"\n\nTypically, Longhorn can block the replica starting once the current rebuilding count on a node exceeds the limit. But when the value is 0, it means disabling the replica rebuilding." +
			"\n\n**WARNING**:\n" +
			"\n  - The old setting \"Disable Replica Rebuild\" is replaced by this setting." +
			"\n  - Different from relying on replica starting delay to limit the concurrent rebuilding, if the rebuilding is disabled, replica object replenishment will be directly skipped." +
			"\n  - When the value is 0, the eviction and data locality feature won't work. But this shouldn't have any impact to any current replica rebuild and backup restore.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "5",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionConcurrentBackingImageCopyReplenishPerNodeLimit = SettingDefinition{
		DisplayName: "Concurrent Backing Image Replenish Per Node Limit",
		Description: "This setting controls how many backing images copy on a node can be replenished simultaneously." +
			"\n\nTypically, Longhorn can block the backing image copy starting once the current replenishing count on a node exceeds the limit. But when the value is **0**, it means disabling the backing image replenish.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "5",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionConcurrentVolumeBackupRestorePerNodeLimit = SettingDefinition{
		DisplayName:        "Concurrent Volume Backup Restore Per Node Limit",
		Description:        "This setting controls the maximum number of volumes that can be restored from a backup concurrently on a single node. Longhorn will block new backup restore operations once the limit is exceeded. Set the value to **0** to disable backup restore.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "5",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionSystemManagedPodsImagePullPolicy = SettingDefinition{
		DisplayName: "System Managed Pod Image Pull Policy",
		Description: "This setting defines the Image Pull Policy for Longhorn's system-managed pods, such as the instance manager, engine image, and CSI driver. The new policy will only apply after these pods restart." +
			"\n\nThis setting is exactly the same as the one used in Kubernetes, and the available options are:" +
			"\n\n- **always**: Every time a pod is launched, the kubelet will pull the image from the registry to ensure it has the latest version." +
			"\n\n- **if-not-present**: (Default) The image is only pulled if it does not already exist locally on the node." +
			"\n\n- **never**: The image is assumed to exist locally. No attempt is made to pull the image from the registry.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            string(SystemManagedPodsImagePullPolicyIfNotPresent),
		Choices: []any{
			string(SystemManagedPodsImagePullPolicyIfNotPresent),
			string(SystemManagedPodsImagePullPolicyNever),
			string(SystemManagedPodsImagePullPolicyAlways),
		},
	}

	SettingDefinitionAllowVolumeCreationWithDegradedAvailability = SettingDefinition{
		DisplayName: "Allow Volume Creation With Degraded Availability",
		Description: "This setting allows user to create and attach a volume that does not have all the replicas scheduled at the time of creation." +
			"\n\n**NOTE**: It is recommended to disable this setting when using Longhorn in the production environment.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionAutoCleanupSystemGeneratedSnapshot = SettingDefinition{
		DisplayName:        "Automatically Cleanup System Generated Snapshot",
		Description:        "This setting enables Longhorn to automatically clean up system-generated snapshots, which are created during replica rebuilding. If not cleaned up, these snapshots can accumulate and take up disk space, requiring manual deletion if a recurring snapshot schedule is not in place.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionAutoCleanupRecurringJobBackupSnapshot = SettingDefinition{
		DisplayName:        "Automatically Cleanup Recurring Job Backup Snapshot",
		Description:        "When this setting is enabled, it allows Longhorn to automatically cleanup the snapshot generated by a recurring backup job.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionConcurrentAutomaticEngineUpgradePerNodeLimit = SettingDefinition{
		DisplayName: "Concurrent Automatic Engine Upgrade Per Node Limit",
		Description: "This setting controls how Longhorn automatically upgrades volumes' engines after upgrading Longhorn manager." +
			"The value of this setting specifies the maximum number of engines per node that are allowed to upgrade to the default engine image at the same time." +
			"If the value is **0**, Longhorn will not automatically upgrade volumes' engines to default version.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "0",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionBackingImageCleanupWaitInterval = SettingDefinition{
		DisplayName:        "Backing Image Cleanup Wait Interval",
		Description:        "In minutes. The interval determines how long Longhorn will wait before cleaning up the backing image file when there is no replica in the disk using it.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "60",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionBackingImageRecoveryWaitInterval = SettingDefinition{
		DisplayName: "Backing Image Recovery Wait Interval",
		Description: "In seconds. The interval determines how long Longhorn will wait before re-downloading the backing image file when all disk files of this backing image become failed or unknown." +
			"\n\n**NOTE**:\n" +
			"\n  - This recovery only works for the backing image of which the creation type is \"download\"." +
			"\n  - File state \"unknown\" means the related manager pods on the pod is not running or the node itself is down/disconnected.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "300",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionGuaranteedInstanceManagerCPU = SettingDefinition{
		DisplayName: "Guaranteed Instance Manager CPU",
		Description: "This setting reserves a percentage of a node's total allocatable CPU resources for each instance manager pod. For example, a value of **10** means 10 percent of the total CPU on a node is reserved for each instance manager. This helps maintain engine and replica stability during high node workload." +
			"\n\n**V1 and V2 Data Engines**:\n" +
			"\nThis setting applies to both data engines. For the V2 Data Engine, the Storage Performance Development Kit (SPDK) target daemon within each instance manager pod uses dedicated CPU cores, making it critical to reserve CPU for stability during high node load." +
			"\n\n**Calculation and Tuning**:\n" +
			"\nTo prevent unexpected volume instance crashes and ensure acceptable I/O performance, you can use the following formula to calculate a recommended value:" +
			"\n`Guaranteed Instance Manager CPU = (Estimated max Longhorn volume engine and replica count on a node * 0.1) / (Total allocatable CPUs on the node) * 100`" +
			"\nIf you cannot estimate the usage, you can use the default value of **12%** and tune it later when no workloads are running." +
			"\n\n**Warnings and Notes**:\n" +
			"\n- A value of **0** unsets CPU requests for instance manager pods." +
			"\n- The value must be between 0 and 40." +
			"\n- This global setting is ignored for any node where the **InstanceManagerCPURequest** field is manually set." +
			"\n- Changing this setting will restart instance manager pods. To avoid disruption, ensure all volumes on the instance manager are detached before the update." +
			"\n- During a system upgrade, new instance manager pods may be deployed. If the node's available CPU is insufficient, you must detach volumes using the oldest pods to release CPU resources so that the new pods can be launched.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeFloat,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"12\",%q:\"12\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
		ValueFloatRange: map[string]float64{
			ValueFloatRangeMinimum: 0,
			ValueFloatRangeMaximum: 40,
		},
	}

	SettingDefinitionKubernetesClusterAutoscalerEnabled = SettingDefinition{
		DisplayName: "Kubernetes Cluster Autoscaler Enabled (Experimental)",
		Description: "This (Experimental) setting notifies Longhorn that the cluster is using the Kubernetes Cluster Autoscaler." +
			"\n\nLonghorn prevents data loss by only allowing the Cluster Autoscaler to scale down a node that met all conditions:" +
			"  \n- No volume attached to the node." +
			"  \n- Is not the last node containing the replica of any volume." +
			"  \n- Is not running backing image components pod." +
			"  \n- Is not running share manager components pod.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionOrphanResourceAutoDeletion = SettingDefinition{
		DisplayName: "Orphan Resource Automatic Deletion",
		Description: "This setting allows Longhorn to automatically delete orphan resources and their corresponding orphaned resources. Orphan resources located on nodes that are in down or unknown state will not be cleaned up automatically." +
			"\nList the enabled resource types in a semicolon-separated list." +
			"\n\n**The available options are**:\n" +
			"\n- **replica-data**: replica data store." +
			"\n- **instance**: engine and replica runtime instance.",
		Category:           SettingCategoryOrphan,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "",
	}

	SettingDefinitionOrphanResourceAutoDeletionGracePeriod = SettingDefinition{
		DisplayName: "Orphan Resource Automatic Deletion Grace Period",
		Description: "In seconds. This setting specifies the wait time, before Longhorn automatically deletes an orphaned Custom Resource (CR) and its associated resources." +
			"\n\n**NOTE**: If a user manually deletes an orphaned CR, the deletion occurs immediately and does not respect this grace period.",
		Category:           SettingCategoryOrphan,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "300",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionStorageNetwork = SettingDefinition{
		DisplayName: "Storage Network",
		Description: "Longhorn uses this setting to segregate in-cluster data traffic from the default Kubernetes cluster network. Leave this blank to use the Kubernetes cluster network. To use a dedicated storage network, input the pre-existing **Multus NetworkAttachmentDefinition** in **<namespace>/<name>** format." +
			"\n\nBy default, this setting applies only to RWO (Read-Write-Once) volumes. For RWX (Read-Write-Many) volumes, you must also enable the **Storage Network for RWX Volume Enabled** setting." +
			"\n\n**WARNING**:\n" +
			"\n- The cluster must have **Multus** installed, and the IPs in the NetworkAttachmentDefinition must be reachable between nodes." +
			"\n- This setting should only be changed after all Longhorn volumes are detached, as some system component pods will be recreated to apply the change. If volumes are in use, components will not restart immediately, and you will either need to re-apply the setting after detaching volumes or wait for the change to be reconciled in an hour.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            CniNetworkNone,
	}

	SettingDefinitionStorageNetworkForRWXVolumeEnabled = SettingDefinition{
		DisplayName: "Storage Network for RWX Volume Enabled",
		Description: "This setting allows Longhorn to use the dedicated storage network for RWX (Read-Write-Many) volumes. You must first enable the **Storage Network** setting before enabling this one." +
			"\n\n**WARNING:**\n" +
			"\n- This setting should only be changed after all Longhorn RWX volumes are detached, as some system component pods (like the CSI plugin pod) will be recreated to apply the change. If volumes are in use, these pods will not restart immediately, and you will either need to reapply the setting after detaching volumes or wait for the change to be reconciled on the hour." +
			"\n- When this setting is enabled, RWX volumes are mounted with the storage network within the CSI plugin pod's network namespace. As a result, restarting the CSI plugin pod while volumes are attached may lead to unresponsive volume mounts. If this occurs, you must restart the workload pod to re-establish the mount connection. You can also enable the **Automatically Delete Workload Pod when The Volume Is Detached Unexpectedly** setting to have Longhorn perform this restart automatically.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeBool,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionRecurringSuccessfulJobsHistoryLimit = SettingDefinition{
		DisplayName:        "Cronjob Successful Jobs History Limit",
		Description:        "This setting specifies how many successful recurring backup or snapshot job histories should be retained. History will not be retained if the value is **0**.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "1",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionRecurringFailedJobsHistoryLimit = SettingDefinition{
		DisplayName:        "Cronjob Failed Jobs History Limit",
		Description:        "This setting specifies how many failed recurring backup or snapshot job histories should be retained. History will not be retained if the value is **0**.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "1",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionRecurringJobMaxRetention = SettingDefinition{
		DisplayName:        "Maximum Retention Number for Recurring Job",
		Description:        "This setting specifies the maximum number of backups or snapshots to be retained for each recurring job.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "100",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
			ValueIntRangeMaximum: MaxSnapshotNum,
		},
	}

	SettingDefinitionSupportBundleFailedHistoryLimit = SettingDefinition{
		DisplayName:        "Support Bundle Failed History Limit",
		Description:        "This setting specifies the maximum number of failed support bundles that can exist in the cluster. Retained failed bundles are for analysis and must be cleaned up manually. Longhorn will block new support bundle creation when this limit is reached. You can set this value to **0** to have Longhorn automatically purge all failed support bundles.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "1",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionSupportBundleNodeCollectionTimeout = SettingDefinition{
		DisplayName:        "Timeout For Support Bundle Node Collection",
		Description:        "In minutes. The timeout for collecting node bundles for support bundle generation. When the timeout is reached, the support bundle generation will proceed without requiring the collection of node bundles.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "30",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionDeletingConfirmationFlag = SettingDefinition{
		DisplayName: "Deleting Confirmation Flag",
		Description: "This flag is designed to prevent Longhorn from being accidentally uninstalled which will lead to data lost" +
			"\n\nSet this flag to **true** to allow Longhorn uninstallation." +
			"\nIf this flag **false**, Longhorn uninstallation job will fail.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionEngineReplicaTimeout = SettingDefinition{
		DisplayName: "Engine Replica Timeout",
		Description: "The time in seconds will wait for a response from a replica before marking it as failed. Values between 8 and 30 are allowed. The engine replica timeout is only in effect while there are I/O requests outstanding." +
			"\n\nThis setting only applies to additional replicas. A V1 engine marks the last active replica as failed only after twice the configured number of seconds (timeout value x 2) have passed. This behavior is intended to balance volume responsiveness with volume availability.\n" +
			"\n- The engine can quickly (after the configured timeout) ignore individual replicas that become unresponsive in favor of other available ones. This ensures future I/O will not be held up." +
			"\n- The engine waits on the last replica (until twice the configured timeout) to prevent unnecessarily crashing as a result of having no available backends.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"8\",%q:\"8\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 8,
			ValueIntRangeMaximum: 30,
		},
	}

	SettingDefinitionSnapshotDataIntegrity = SettingDefinition{
		DisplayName: "Snapshot Data Integrity",
		Description: "This setting allows users to enable or disable snapshot hashing and data integrity checking." +
			"\n\n** The available options are**:\n" +
			"\n- **disabled**: Disable snapshot disk file hashing and data integrity checking." +
			"\n- **enabled**: Enables periodic snapshot disk file hashing and data integrity checking. To detect the filesystem-unaware corruption caused by bit rot or other issues in snapshot disk files, Longhorn system periodically hashes files and finds corrupted ones. Hence, the system performance will be impacted during the periodical checking." +
			"\n- **fast-check**: Enable snapshot disk file hashing and fast data integrity checking. Longhorn system only hashes snapshot disk files if their are not hashed or the modification time are changed. In this mode, filesystem-unaware corruption cannot be detected, but the impact on system performance can be minimized.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default: fmt.Sprintf("{%q:%q,%q:%q}", longhorn.DataEngineTypeV1, string(longhorn.SnapshotDataIntegrityFastCheck),
			longhorn.DataEngineTypeV2, string(longhorn.SnapshotDataIntegrityFastCheck)),
		Choices: []any{
			string(longhorn.SnapshotDataIntegrityDisabled),
			string(longhorn.SnapshotDataIntegrityEnabled),
			string(longhorn.SnapshotDataIntegrityFastCheck),
		},
	}

	SettingDefinitionSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation = SettingDefinition{
		DisplayName:        "Immediate Snapshot Data Integrity Check After Creating a Snapshot",
		Description:        "Hashing snapshot disk files impacts the performance of the system. The immediate snapshot hashing and checking can be disabled to minimize the impact after creating a snapshot.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"false\",%q:\"false\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
	}

	SettingDefinitionSnapshotDataIntegrityCronJob = SettingDefinition{
		DisplayName: "Snapshot Data Integrity Check CronJob",
		Description: "Unix-cron string format. The setting specifies when Longhorn checks the data integrity of snapshot disk files." +
			"\n\n**WARNING**: Hashing snapshot disk files impacts the performance of the system. It is recommended to run data integrity checks during off-peak times and to reduce the frequency of checks.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:%q,%q:%q}", longhorn.DataEngineTypeV1, string("0 0 */7 * *"), longhorn.DataEngineTypeV2, string("0 0 */7 * *")),
	}

	SettingDefinitionSnapshotMaxCount = SettingDefinition{
		DisplayName:        "Snapshot Maximum Count",
		Description:        "This setting specifies the maximum number of snapshots that can be retained for a volume. The value must be between **2** and **250**.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            strconv.Itoa(MaxSnapshotNum),
	}

	SettingDefinitionRemoveSnapshotsDuringFilesystemTrim = SettingDefinition{
		DisplayName:        "Remove Snapshots During Filesystem Trim",
		Description:        "This setting allows the Longhorn filesystem trim feature to automatically mark the latest snapshot and its ancestors as removed, stopping at any snapshot with multiple children. The filesystem trim feature only applies to the volume head and continuous removed or system snapshots. If you try to trim a removed file from a valid snapshot, the filesystem will do nothing and discard the in-memory trim information. To retry the trim after marking a snapshot as removed, you may need to unmount and remount the filesystem so it can recollect the trimmable file information.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionFastReplicaRebuildEnabled = SettingDefinition{
		DisplayName:        "Fast Replica Rebuild Enabled",
		Description:        "This setting enables the fast replica rebuilding feature. It relies on the checksums of snapshot disk files, so setting the snapshot-data-integrity to **enable** or **fast-check** is a prerequisite.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"true\",%q:\"true\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
	}

	SettingDefinitionReplicaFileSyncHTTPClientTimeout = SettingDefinition{
		DisplayName:        "Timeout Of HTTP Client To Replica File Sync Server",
		Description:        "In seconds. This setting specifies the timeout for the HTTP client used to synchronize replica files.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "30",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 5,
			ValueIntRangeMaximum: 120,
		},
	}

	SettingDefinitionLongGPRCTimeOut = SettingDefinition{
		DisplayName:        "Long gRPC Timeout",
		Description:        "In seconds. This setting specifies the maximum number of seconds that Longhorn allows replica rebuilding and snapshot cloning, to complete.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "86400",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
			ValueIntRangeMaximum: 604800,
		},
	}

	SettingDefinitionBackupCompressionMethod = SettingDefinition{
		DisplayName: "Backup Compression Method",
		Description: "This setting allows users to specify backup compression method." +
			"\n\n**The available options are**:\n" +
			"\n- **none**: Disable the compression method. Suitable for multimedia data such as encoded images and videos." +
			"\n- **lz4**: Fast compression method. Suitable for flat files." +
			"\n- **gzip**: A bit of higher compression ratio but relatively slow.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            string(longhorn.BackupCompressionMethodLz4),
		Choices: []any{
			string(longhorn.BackupCompressionMethodNone),
			string(longhorn.BackupCompressionMethodLz4),
			string(longhorn.BackupCompressionMethodGzip),
		},
	}

	SettingDefinitionBackupConcurrentLimit = SettingDefinition{
		DisplayName:        "Backup Concurrent Limit Per Backup",
		Description:        "This setting controls the number of worker threads used concurrently for each backup operation.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "2",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
		},
	}

	SettingDefinitionRestoreConcurrentLimit = SettingDefinition{
		DisplayName:        "Restore Concurrent Limit Per Backup",
		Description:        "This setting controls the number of worker threads used concurrently for each restore operation.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "2",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
		},
	}

	SettingDefinitionDefaultBackupBlockSize = SettingDefinition{
		DisplayName:        "Default Backup Block Size",
		Description:        "This setting specifies the default backup block size, in MiB, used when creating a new volume. Supported values are **2** or **16**.",
		Category:           SettingCategoryBackup,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Choices:            []any{int64(2), int64(16)},
		Default:            "2",
	}

	SettingDefinitionInstanceManagerPodLivenessProbeTimeout = SettingDefinition{
		DisplayName: "Instance Manager Pod Liveness Probe Timeout",
		Description: "In seconds. The setting specifies the timeout for the instance manager pod liveness probe. The default value is 10 seconds." +
			"\n\n**WARNING**: When applying the setting, Longhorn will try to restart all instance-manager pods if all volumes are detached and eventually restart the instance manager pod without instances running on the instance manager.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "10",
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
			ValueIntRangeMaximum: 60,
		},
	}

	SettingDefinitionLogLevel = SettingDefinition{
		DisplayName:        "Log Level",
		Description:        "The log level `Panic`, `Fatal`, `Error`, `Warn`, `Info` (default), `Debug`, `Trace` used in the Longhorn manager.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "Info",
		Choices:            []any{"Panic", "Fatal", "Error", "Warn", "Info", "Debug", "Trace"},
	}

	SettingDefinitionV1DataEngine = SettingDefinition{
		DisplayName: "V1 Data Engine",
		Description: "Setting that allows you to enable the V1 Data Engine." +
			"\n\n**WARNING**:\n" +
			"\n - DO NOT CHANGE THIS SETTING WITH ATTACHED VOLUMES. Longhorn will block this setting update when there are attached V1 volumes.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionV2DataEngine = SettingDefinition{
		DisplayName: "V2 Data Engine",
		Description: "This setting allows you to enable the experimental V2 Data Engine, which is based on the Storage Performance Development Kit (SPDK). It should not be used in production environments." +
			"\n\n**WARNING**:\n" +
			"\n  - Do NOT change this setting with attached volumes. Longhorn will block the update when there are attached V2 volumes." +
			"\n  - When enabled, each instance-manager pod utilizes 100 percent of a dedicated CPU core due to the intensive polling required by the SPDK target daemon. This ensures optimal performance and responsiveness for storage operations.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionDataEngineHugepageEnabled = SettingDefinition{
		DisplayName:        "Data Engine Hugepage Enabled",
		Description:        "This setting applies only to the V2 Data Engine. It enables hugepages for the Storage Performance Development Kit (SPDK) target daemon. If disabled, legacy memory is used. Allocation size is set via the Data Engine Memory Size setting.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"true\"}", longhorn.DataEngineTypeV2),
	}

	SettingDefinitionDataEngineMemorySize = SettingDefinition{
		DisplayName:        "Data Engine Memory Size",
		Description:        "This setting applies only to the V2 Data Engine. It specifies the memory size, in MiB, allocated to the Storage Performance Development Kit (SPDK) target daemon. When hugepage is enabled, this defines the hugepage size; when legacy memory is used, hugepage is disabled.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"2048\"}", longhorn.DataEngineTypeV2),
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 0,
		},
	}

	SettingDefinitionDataEngineCPUMask = SettingDefinition{
		DisplayName:        "Data Engine CPU Mask",
		Description:        "This setting applies only to the V2 Data Engine. It specifies the CPU cores on which the Storage Performance Development Kit (SPDK) target daemon runs. The daemon is deployed in each Instance Manager pod. Ensure that the number of assigned cores does not exceed the guaranteed Instance Manager CPUs for the V2 Data Engine.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"0x1\"}", longhorn.DataEngineTypeV2),
	}

	SettingDefinitionDataEngineInterruptModeEnabled = SettingDefinition{
		DisplayName: "Enable Interrupt Mode For Data Engine",
		Description: "This setting applies only to the V2 Data Engine. It specifies whether the Storage Performance Development Kit (SPDK) target daemon should run in interrupt mode or polling mode (default)." +
			"\n\n**The available options are**:\n" +
			"\n  - DO NOT CHANGE THIS SETTING WITH ATTACHED VOLUMES. Longhorn will block this setting update when there are attached V2 volumes." +
			"\n  - **true**: It enables interrupt mode, which may reduce CPU usage." +
			"\n**WARNING**:\n" +
			"\n  - DO NOT CHANGE THIS SETTING WITH ATTACHED VOLUMES. Longhorn will block this setting update when there are attached V2 volumes." +
			"\n  - **true**: It enables interrupt mode, which may reduce CPU usage." +
			"\n  - **false**: It uses polling mode for maximum performance.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"false\"}", longhorn.DataEngineTypeV2),
	}

	SettingDefinitionReplicaDiskSoftAntiAffinity = SettingDefinition{
		DisplayName: "Replica Disk Level Soft Anti-Affinity",
		Description: "When enabled, this setting allows Longhorn to schedule new replicas of a volume on the same disk as existing healthy replicas. If disabled, Longhorn will forbid scheduling new replicas to the same disks" +
			"\n\n**NOTE**: Even when this setting is enabled, Longhorn will still attempt to use a different disk if possible, even if it's on the same node. This setting is superseded if replicas are forbidden from sharing a zone or a node by either of the other Soft Anti-Affinity settings.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionAllowEmptyNodeSelectorVolume = SettingDefinition{
		DisplayName:        "Allow Scheduling Empty Node Selector Volumes To Any Node",
		Description:        "When enabled, this setting allows a volume with no node selector to be scheduled on any node, including those with tags.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionAllowEmptyDiskSelectorVolume = SettingDefinition{
		DisplayName:        "Allow Scheduling Empty Disk Selector Volumes To Any Disk",
		Description:        "When enabled, this setting allows a volume with no disk selector to be scheduled on any disk, including those with tags.",
		Category:           SettingCategoryScheduling,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "true",
	}

	SettingDefinitionDisableSnapshotPurge = SettingDefinition{
		DisplayName:        "Disable Snapshot Purge",
		Description:        "When enabled, this setting temporarily prevents all attempts to purge volume snapshots. Longhorn typically purges snapshots during replica rebuilding and user-initiated snapshot deletion to free up space. This process consumes temporary disk space, and if a disk has insufficient space, you may need to temporarily disable purging while moving data to another disk.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionDataEngineLogLevel = SettingDefinition{
		DisplayName:        "Data Engine Log Level",
		Description:        "This setting applies only to the V2 Data Engine. It specifies the log level for the Storage Performance Development Kit (SPDK) target daemon. Supported values are: `Error`, `Warning`, `Notice` (default), `Info`, and `Debug`.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Choices:            []any{"Error", "Warning", "Notice", "Info", "Debug"},
		Default:            fmt.Sprintf("{%q:\"Notice\"}", longhorn.DataEngineTypeV2),
	}

	SettingDefinitionDataEngineLogFlags = SettingDefinition{
		DisplayName:        "Data Engine Log Flags",
		Description:        "This setting applies only to the V2 Data Engine. It specifies the log flags for the Storage Performance Development Kit (SPDK) target daemon.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeString,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"\"}", longhorn.DataEngineTypeV2),
	}

	SettingDefinitionReplicaRebuildingBandwidthLimit = SettingDefinition{
		DisplayName: "Replica Rebuilding Bandwidth Limit",
		Description: "This setting applies only to the V2 Data Engine. It specifies the default write bandwidth limit, in megabytes per second (MB/s), for volume replica rebuilding." +
			"\n\nIf this value is set to 0, there will be no write bandwidth limitation. Individual volumes can override this setting by specifying their own rebuilding bandwidth limit.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           false,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"0\"}", longhorn.DataEngineTypeV2),
	}

	SettingDefinitionAutoCleanupSnapshotWhenDeleteBackup = SettingDefinition{
		DisplayName:        "Automatically Cleanup Snapshot When Deleting Backup",
		Description:        "When enabled, this setting allows Longhorn to automatically delete the source snapshot of a backup after the backup itself has been deleted.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionAutoCleanupSnapshotAfterOnDemandBackupCompleted = SettingDefinition{
		DisplayName:        "Automatically Cleanup Snapshot After On-Demand Backup Completed",
		Description:        "When enabled, this setting allows Longhorn to automatically delete the source snapshot of an on-demand backup after the backup operation is successfully completed.",
		Category:           SettingCategorySnapshot,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionDefaultMinNumberOfBackingImageCopies = SettingDefinition{
		DisplayName:        "Default Minimum Number Of Backing Image Copies",
		Description:        "This setting specifies the minimum number of backing image copies that Longhorn will maintain in the cluster.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeInt,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            strconv.Itoa(DefaultMinNumberOfCopies),
		ValueIntRange: map[string]int{
			ValueIntRangeMinimum: 1,
		},
	}

	SettingDefinitionRWXVolumeFastFailover = SettingDefinition{
		DisplayName:        "RWX Volume Fast Failover (Experimental)",
		Description:        "This (Experimental) setting enables logic to quickly detect unresponsive RWX (Read-Write-Many) volumes and initiate a failover.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            "false",
	}

	SettingDefinitionOfflineReplicaRebuilding = SettingDefinition{
		DisplayName: "Offline Replica Rebuilding",
		Description: "This setting controls whether Longhorn automatically rebuilds degraded replicas while the volume is detached. This setting only takes effect if the individual volume setting is set to `ignored` or `enabled`." +
			"\n\n**The available options are**:\n" +
			"\n- **true**: Enables offline replica rebuilding for all detached volumes, unless overridden at the volume level." +
			"\n- **false**: Disables offline replica rebuilding globally, unless overridden at the volume level." +
			"\n\n**NOTE**: Offline rebuilding applies only when a volume is detached. Volumes in a faulted state will not trigger offline rebuilding.",
		Category:           SettingCategoryGeneral,
		Type:               SettingTypeBool,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"false\",%q:\"false\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
	}

	SettingDefinitionLogPath = SettingDefinition{
		DisplayName:        "Log Path",
		Description:        "This setting specifies the directory on the host where Longhorn stores log files for the instance manager pod. Currently, it is only used for instance manager pods in the V2 data engine.",
		Category:           SettingCategoryDangerZone,
		Type:               SettingTypeString,
		Required:           true,
		ReadOnly:           false,
		DataEngineSpecific: false,
		Default:            DefaultLogDirectoryOnHost,
	}
)

type NodeDownPodDeletionPolicy string

const (
	NodeDownPodDeletionPolicyDoNothing                             = NodeDownPodDeletionPolicy("do-nothing") // Kubernetes default behavior
	NodeDownPodDeletionPolicyDeleteStatefulSetPod                  = NodeDownPodDeletionPolicy("delete-statefulset-pod")
	NodeDownPodDeletionPolicyDeleteDeploymentPod                   = NodeDownPodDeletionPolicy("delete-deployment-pod")
	NodeDownPodDeletionPolicyDeleteBothStatefulsetAndDeploymentPod = NodeDownPodDeletionPolicy("delete-both-statefulset-and-deployment-pod")
)

type NodeDrainPolicy string

const (
	NodeDrainPolicyBlockForEviction                      = NodeDrainPolicy("block-for-eviction")
	NodeDrainPolicyBlockForEvictionIfContainsLastReplica = NodeDrainPolicy("block-for-eviction-if-contains-last-replica")
	NodeDrainPolicyBlockIfContainsLastReplica            = NodeDrainPolicy("block-if-contains-last-replica")
	NodeDrainPolicyAllowIfReplicaIsStopped               = NodeDrainPolicy("allow-if-replica-is-stopped")
	NodeDrainPolicyAlwaysAllow                           = NodeDrainPolicy("always-allow")
)

type SystemManagedPodsImagePullPolicy string

const (
	SystemManagedPodsImagePullPolicyNever        = SystemManagedPodsImagePullPolicy("never")
	SystemManagedPodsImagePullPolicyIfNotPresent = SystemManagedPodsImagePullPolicy("if-not-present")
	SystemManagedPodsImagePullPolicyAlways       = SystemManagedPodsImagePullPolicy("always")
)

type CNIAnnotation string

const (
	CNIAnnotationNetworks      = CNIAnnotation("k8s.v1.cni.cncf.io/networks")
	CNIAnnotationNetworkStatus = CNIAnnotation("k8s.v1.cni.cncf.io/network-status")

	// CNIAnnotationNetworksStatus is deprecated since Multus v3.6 and completely removed in v4.0.0.
	// This exists to support older Multus versions.
	// Ref: https://github.com/longhorn/longhorn/issues/6953
	CNIAnnotationNetworksStatus = CNIAnnotation("k8s.v1.cni.cncf.io/networks-status")
)

type OrphanResourceType string

const (
	OrphanResourceTypeReplicaData = OrphanResourceType("replica-data")
	OrphanResourceTypeInstance    = OrphanResourceType("instance")
)

// ValidateSetting checks if the given value is valid for the given setting name.
func ValidateSetting(name, value string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "value %v of settings %v is invalid", value, name)
	}()

	definition, ok := GetSettingDefinition(SettingName(name))
	if !ok {
		return fmt.Errorf("setting %v is not supported", name)
	}

	if definition.Required && value == "" {
		return fmt.Errorf("required setting %v shouldn't be empty", name)
	}

	switch definition.Type {
	case SettingTypeBool:
		err = validateSettingBool(definition, value)
	case SettingTypeInt:
		err = validateSettingInt(definition, value)
	case SettingTypeFloat:
		err = validateSettingFloat(definition, value)
	case SettingTypeString:
		err = validateSettingString(SettingName(name), definition, value)
	}

	return err
}

// isValidChoice checks if the passed value is part of the choices array,
// an empty choices array allows for all values
func isValidChoice(choices []any, value any) bool {
	if slices.Contains(choices, value) {
		return true
	}
	return len(choices) == 0
}

// GetCustomizedDefaultSettings retrieves the customized default settings from the provided ConfigMap.
func GetCustomizedDefaultSettings(defaultSettingCM *corev1.ConfigMap) (defaultSettings map[string]string, err error) {
	defaultSettingYAMLData := []byte(defaultSettingCM.Data[DefaultSettingYAMLFileName])

	defaultSettings, err = util.GetDataContentFromYAML(defaultSettingYAMLData)
	if err != nil {
		return nil, err
	}

	// won't accept partially valid result
	for name, value := range defaultSettings {
		value = strings.Trim(value, " ")
		definition, exist := GetSettingDefinition(SettingName(name))
		if !exist {
			logrus.Errorf("Customized settings are invalid, will give up using them: undefined setting %v", name)
			defaultSettings = map[string]string{}
			break
		}
		if value == "" {
			continue
		}
		// Make sure the value of boolean setting is always "true" or "false" in Longhorn.
		// Otherwise the Longhorn UI cannot display the boolean setting correctly.
		if definition.Type == SettingTypeBool {
			result, err := strconv.ParseBool(value)
			if err != nil {
				logrus.WithError(err).Errorf("Invalid value %v for the boolean setting %v", value, name)
				defaultSettings = map[string]string{}
				break
			}
			value = strconv.FormatBool(result)
		}
		if err := ValidateSetting(name, value); err != nil {
			logrus.WithError(err).Errorf("Customized settings are invalid, will give up using them: the value of customized setting %v is invalid", name)
			defaultSettings = map[string]string{}
			break
		}
		defaultSettings[name] = value
	}

	return defaultSettings, nil
}

// UnmarshalTolerations unmarshals the given toleration setting string into a slice of Toleration.
func UnmarshalTolerations(tolerationSetting string) ([]corev1.Toleration, error) {
	tolerations := []corev1.Toleration{}

	tolerationSetting = strings.ReplaceAll(tolerationSetting, " ", "")
	if tolerationSetting == "" {
		return tolerations, nil
	}

	tolerationList := strings.Split(tolerationSetting, ";")
	for _, toleration := range tolerationList {
		toleration, err := parseToleration(toleration)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse toleration: %s", toleration)
		}
		tolerations = append(tolerations, *toleration)
	}
	return tolerations, nil
}

func parseToleration(taintToleration string) (*corev1.Toleration, error) {
	// The schema should be `key=value:effect` or `key:effect`
	parts := strings.Split(taintToleration, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("missing key/value and effect pair")
	}

	// parse `key=value` or `key`
	key, value, operator := "", "", corev1.TolerationOperator("")
	pair := strings.Split(parts[0], "=")
	switch len(pair) {
	case 1:
		key, value, operator = parts[0], "", corev1.TolerationOpExists
	case 2:
		key, value, operator = pair[0], pair[1], corev1.TolerationOpEqual
	}

	effect := corev1.TaintEffect(parts[1])
	switch effect {
	case "", corev1.TaintEffectNoExecute, corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule:
	default:
		return nil, fmt.Errorf("invalid effect: %v", parts[1])
	}

	return &corev1.Toleration{
		Key:      key,
		Value:    value,
		Operator: operator,
		Effect:   effect,
	}, nil
}

func validateAndUnmarshalLabel(label string) (key, value string, err error) {
	label = strings.Trim(label, " ")
	parts := strings.Split(label, ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid label %v: should contain the separator ':'", label)
	}
	return strings.Trim(parts[0], " "), strings.Trim(parts[1], " "), nil
}

func UnmarshalNodeSelector(nodeSelectorSetting string) (map[string]string, error) {
	nodeSelector := map[string]string{}

	nodeSelectorSetting = strings.Trim(nodeSelectorSetting, " ")
	if nodeSelectorSetting != "" {
		labelList := strings.Split(nodeSelectorSetting, ";")
		for _, label := range labelList {
			key, value, err := validateAndUnmarshalLabel(label)
			if err != nil {
				return nil, errors.Wrap(err, "Error while unmarshal node selector")
			}
			nodeSelector[key] = value
		}
	}
	return nodeSelector, nil
}

func UnmarshalOrphanResourceTypes(resourceTypesSetting string) (map[OrphanResourceType]bool, error) {
	resourceTypes := map[OrphanResourceType]bool{
		OrphanResourceTypeReplicaData: false,
		OrphanResourceTypeInstance:    false,
	}

	resourceTypesSetting = strings.Trim(resourceTypesSetting, " ")
	invalidItems := make([]string, 0, len(resourceTypesSetting))
	if resourceTypesSetting != "" {
		resourceTypeList := strings.Split(resourceTypesSetting, ";")
		for _, item := range resourceTypeList {
			resourceType := OrphanResourceType(strings.Trim(item, " "))
			if _, ok := resourceTypes[resourceType]; ok {
				resourceTypes[resourceType] = true
			} else {
				invalidItems = append(invalidItems, item)
			}
		}
	}
	if len(invalidItems) > 0 {
		return nil, fmt.Errorf("invalid orphan resource types: %s", strings.Join(invalidItems, ", "))
	}
	return resourceTypes, nil
}

func IsSettingReplaced(name SettingName) bool {
	return replacedSettingNames[name]
}

// GetSettingDefinition gets the setting definition in `settingDefinitions` by the parameter `name`
func GetSettingDefinition(name SettingName) (SettingDefinition, bool) {
	settingDefinitionsLock.RLock()
	defer settingDefinitionsLock.RUnlock()
	setting, ok := settingDefinitions[name]
	return setting, ok
}

// SetSettingDefinition sets the setting definition in `settingDefinitions` by the parameter `name` and `definition`
func SetSettingDefinition(name SettingName, definition SettingDefinition) {
	settingDefinitionsLock.Lock()
	defer settingDefinitionsLock.Unlock()
	settingDefinitions[name] = definition
}

func GetDangerZoneSettings() sets.Set[SettingName] {
	settingList := sets.New[SettingName]()
	for settingName, setting := range settingDefinitions {
		if setting.Category == SettingCategoryDangerZone {
			settingList = settingList.Insert(settingName)
		}
	}

	return settingList
}

// IsJSONFormat checks if the input string starts with '{' indicating JSON format.
func IsJSONFormat(value string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(value), &js) == nil
}

// validateSettingBool validates a boolean setting value based on the provided definition.
// It supports both single boolean values and JSON-formatted data-engine-specific values.
func validateSettingBool(definition SettingDefinition, value string) (err error) {
	var values map[longhorn.DataEngineType]any
	var defaultValues map[longhorn.DataEngineType]any

	if IsJSONFormat(strings.TrimSpace(value)) {
		values, err = ParseDataEngineSpecificSetting(definition, value)
	} else {
		values, err = parseSettingSingleBool(definition, value)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse value %s for setting %s", value, definition.DisplayName)
	}

	if len(values) == 0 {
		return fmt.Errorf("failed to parse value %s for setting %s, value cannot be empty", value, definition.DisplayName)
	}

	if IsJSONFormat(strings.TrimSpace(definition.Default)) {
		defaultValues, err = ParseDataEngineSpecificSetting(definition, definition.Default)
	} else {
		defaultValues, err = parseSettingSingleBool(definition, definition.Default)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse default value %s for setting %s", definition.Default, definition.DisplayName)
	}

	for dataEngine := range values {
		if _, ok := defaultValues[dataEngine]; !ok {
			return fmt.Errorf("data engine %s is not supported for setting %s", dataEngine, definition.DisplayName)
		}
	}

	return err
}

func ParseDataEngineSpecificSetting(definition SettingDefinition, value string) (map[longhorn.DataEngineType]any, error) {
	if !definition.DataEngineSpecific {
		return map[longhorn.DataEngineType]any{}, fmt.Errorf("JSON-formatted value is not supported for non-data-engine-specific setting")
	}

	var jsonValues map[longhorn.DataEngineType]string
	if err := json.Unmarshal([]byte(value), &jsonValues); err != nil {
		return map[longhorn.DataEngineType]any{}, errors.Wrapf(err, "failed to parse JSON-formatted value %v", value)
	}

	switch definition.Type {
	case SettingTypeBool:
		return parseDataEngineSpecificSettingBools(jsonValues)
	case SettingTypeInt:
		return parseDataEngineSpecificSettingInts(jsonValues)
	case SettingTypeFloat:
		return parseDataEngineSpecificSettingFloats(jsonValues)
	case SettingTypeString:
		return parseDataEngineSpecificSettingStrings(jsonValues)
	default:
		return nil, fmt.Errorf("unsupported setting type %s for data-engine-specific settings", definition.Type)
	}
}

func parseDataEngineSpecificSettingBools(jsonValues map[longhorn.DataEngineType]string) (map[longhorn.DataEngineType]any, error) {
	values := make(map[longhorn.DataEngineType]any)

	var errs []error
	for dataEngine, valueStr := range jsonValues {
		boolValue, err := strconv.ParseBool(valueStr)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to parse bool value %s for data engine %s", valueStr, dataEngine))
			continue
		}
		values[dataEngine] = boolValue
	}

	return values, errors.Join(errs...)
}

// ParseSettingSingleValue parses a single value for a setting based on its definition.
func ParseSettingSingleValue(definition SettingDefinition, value string) (map[longhorn.DataEngineType]any, error) {
	switch definition.Type {
	case SettingTypeBool:
		return parseSettingSingleBool(definition, value)
	case SettingTypeInt:
		return parseSettingSingleInt(definition, value)
	case SettingTypeFloat:
		return parseSettingSingleFloat(definition, value)
	case SettingTypeString:
		return parseSettingSingleString(definition, value)
	default:
		return nil, fmt.Errorf("unsupported setting type %s for single value parsing", definition.Type)
	}
}

// parseSettingSingleBool processes a single boolean value for both general and data-engine-specific settings.
func parseSettingSingleBool(definition SettingDefinition, value string) (map[longhorn.DataEngineType]any, error) {
	trimmedValue := strings.TrimSpace(value)

	values := make(map[longhorn.DataEngineType]any)

	boolValue, err := strconv.ParseBool(trimmedValue)
	if err != nil {
		return values, errors.Wrapf(err, "failed to parse single bool value %s", value)
	}

	if definition.DataEngineSpecific {
		var jsonValues map[longhorn.DataEngineType]string
		if err := json.Unmarshal([]byte(definition.Default), &jsonValues); err != nil {
			return values, errors.Wrapf(err, "failed to parse JSON-formatted default value %v for setting %s", definition.Default, definition.DisplayName)
		}
		for dataEngine := range jsonValues {
			values[dataEngine] = boolValue
		}
	} else {
		values[longhorn.DataEngineTypeAll] = boolValue
	}

	return values, nil
}

// validateSettingInt validates an integer setting value based on the provided definition.
// It supports both single integer values and JSON-formatted data-engine-specific values.
func validateSettingInt(definition SettingDefinition, value string) (err error) {
	var values map[longhorn.DataEngineType]any
	var defaultValues map[longhorn.DataEngineType]any

	if IsJSONFormat(strings.TrimSpace(value)) {
		values, err = ParseDataEngineSpecificSetting(definition, value)
	} else {
		values, err = ParseSettingSingleValue(definition, value)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse value %s for setting %s", value, definition.DisplayName)
	}

	if len(values) == 0 {
		return fmt.Errorf("failed to parse value %s for setting %s, value cannot be empty", value, definition.DisplayName)
	}

	if IsJSONFormat(strings.TrimSpace(definition.Default)) {
		defaultValues, err = ParseDataEngineSpecificSetting(definition, definition.Default)
	} else {
		defaultValues, err = ParseSettingSingleValue(definition, definition.Default)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse default value %s for setting %s", definition.Default, definition.DisplayName)
	}

	for dataEngine := range values {
		if _, ok := defaultValues[dataEngine]; !ok {
			return fmt.Errorf("data engine %s is not supported for setting %s", dataEngine, definition.DisplayName)
		}
	}

	return validateSettingIntValues(definition, values)
}

func parseDataEngineSpecificSettingInts(jsonValues map[longhorn.DataEngineType]string) (map[longhorn.DataEngineType]any, error) {
	values := make(map[longhorn.DataEngineType]any)

	var errs []error
	for dataEngine, valueStr := range jsonValues {
		intValue, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to parse int value %s for data engine %s", valueStr, dataEngine))
			continue
		}
		values[dataEngine] = int64(intValue)
	}

	return values, errors.Join(errs...)
}

// parseSettingSingleInt processes a single integer value for both general and data-engine-specific settings.
func parseSettingSingleInt(definition SettingDefinition, value string) (map[longhorn.DataEngineType]any, error) {
	trimmedValue := strings.TrimSpace(value)

	values := make(map[longhorn.DataEngineType]any)

	intValue, err := strconv.ParseInt(trimmedValue, 10, 64)
	if err != nil {
		return values, errors.Wrapf(err, "failed to parse single int value %s", value)
	}

	if definition.DataEngineSpecific {
		var jsonValues map[longhorn.DataEngineType]string
		if err := json.Unmarshal([]byte(definition.Default), &jsonValues); err != nil {
			return values, errors.Wrapf(err, "failed to parse JSON-formatted default value %q for setting %s", definition.Default, definition.DisplayName)
		}
		for dataEngine := range jsonValues {
			values[dataEngine] = int64(intValue)
		}
	} else {
		values[longhorn.DataEngineTypeAll] = int64(intValue)
	}

	return values, nil
}

// validateSettingIntValues ensures all integer values are within the defined range.
func validateSettingIntValues(definition SettingDefinition, values map[longhorn.DataEngineType]any) error {
	var errs []error
	for dataEngine, value := range values {
		intValue, ok := value.(int64)
		if !ok {
			errs = append(errs, fmt.Errorf("value for data engine %v is not an int64: %v", dataEngine, value))
			continue
		}

		valueIntRange := definition.ValueIntRange
		if minValue, exists := valueIntRange[ValueIntRangeMinimum]; exists {
			if int(intValue) < minValue {
				errs = append(errs, fmt.Errorf("value %v for data engine %v should be larger than or equal to %v", intValue, dataEngine, minValue))
				continue
			}
		}

		if maxValue, exists := valueIntRange[ValueIntRangeMaximum]; exists {
			if int(intValue) > maxValue {
				errs = append(errs, fmt.Errorf("value %v for data engine %v should be less than or equal to %v", intValue, dataEngine, maxValue))
				continue
			}
		}

		if len(definition.Choices) > 0 {
			if !isValidChoice(definition.Choices, value) {
				return fmt.Errorf("value %v is not a valid choice, available choices %v", value, definition.Choices)
			}
			return nil
		}
	}

	return errors.Join(errs...)
}

// validateSettingFloat validates a float64 setting value based on the provided definition.
// It supports both single float64 values and JSON-formatted data-engine-specific values.
func validateSettingFloat(definition SettingDefinition, value string) (err error) {
	var values map[longhorn.DataEngineType]any
	var defaultValues map[longhorn.DataEngineType]any

	if IsJSONFormat(strings.TrimSpace(value)) {
		values, err = ParseDataEngineSpecificSetting(definition, value)
	} else {
		values, err = ParseSettingSingleValue(definition, value)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse value %s for setting %s", value, definition.DisplayName)
	}

	if len(values) == 0 {
		return fmt.Errorf("failed to parse value %s for setting %s, value cannot be empty", value, definition.DisplayName)
	}

	if IsJSONFormat(strings.TrimSpace(definition.Default)) {
		defaultValues, err = ParseDataEngineSpecificSetting(definition, definition.Default)
	} else {
		defaultValues, err = parseSettingSingleFloat(definition, definition.Default)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse default value %s for setting %s", definition.Default, definition.DisplayName)
	}

	for dataEngine := range values {
		if _, ok := defaultValues[dataEngine]; !ok {
			return fmt.Errorf("data engine %s is not supported for setting %s", dataEngine, definition.DisplayName)
		}
	}

	return validateSettingFloatValues(definition, values)
}

func parseDataEngineSpecificSettingFloats(jsonValues map[longhorn.DataEngineType]string) (map[longhorn.DataEngineType]any, error) {
	values := make(map[longhorn.DataEngineType]any)

	var errs []error
	for dataEngine, valueStr := range jsonValues {
		floatValue, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to parse float value %s for data engine %s", valueStr, dataEngine))
			continue
		}
		values[dataEngine] = floatValue
	}

	return values, errors.Join(errs...)
}

// parseSettingSingleFloat processes a single float64 value for both general and data-engine-specific settings.
func parseSettingSingleFloat(definition SettingDefinition, value string) (map[longhorn.DataEngineType]any, error) {
	trimmedValue := strings.TrimSpace(value)

	values := make(map[longhorn.DataEngineType]any)

	floatValue, err := strconv.ParseFloat(trimmedValue, 64)
	if err != nil {
		return values, errors.Wrapf(err, "failed to parse single float value %s", value)
	}

	if definition.DataEngineSpecific {
		var jsonValues map[longhorn.DataEngineType]string
		if err := json.Unmarshal([]byte(definition.Default), &jsonValues); err != nil {
			return values, errors.Wrapf(err, "failed to parse JSON-formatted default value %q for setting %s", definition.Default, definition.DisplayName)
		}
		for dataEngine := range jsonValues {
			values[dataEngine] = floatValue
		}
	} else {
		values[longhorn.DataEngineTypeAll] = floatValue
	}

	return values, nil
}

// validateSettingFloatValues ensures all float64 values are within the defined range.
func validateSettingFloatValues(definition SettingDefinition, values map[longhorn.DataEngineType]any) error {
	var errs []error

	for dataEngine, value := range values {
		floatValue, ok := value.(float64)
		if !ok {
			errs = append(errs, fmt.Errorf("value for data engine %v is not a float64: %v", dataEngine, value))
			continue
		}

		valueFloatRange := definition.ValueFloatRange
		if minValue, exists := valueFloatRange[ValueFloatRangeMinimum]; exists {
			if floatValue < minValue {
				errs = append(errs, fmt.Errorf("value %v for data engine %v should be larger than or equal to %v", floatValue, dataEngine, minValue))
				continue
			}
		}

		if maxValue, exists := valueFloatRange[ValueFloatRangeMaximum]; exists {
			if floatValue > maxValue {
				errs = append(errs, fmt.Errorf("value %v for data engine %v should be less than or equal to %v", floatValue, dataEngine, maxValue))
				continue
			}
		}
	}

	return errors.Join(errs...)
}

// validateSettingString validates a string setting value based on the provided definition.
// It supports both single string values and JSON-formatted data-engine-specific values.
func validateSettingString(name SettingName, definition SettingDefinition, value string) (err error) {
	var values map[longhorn.DataEngineType]any
	var defaultValues map[longhorn.DataEngineType]any

	if IsJSONFormat(strings.TrimSpace(value)) {
		values, err = ParseDataEngineSpecificSetting(definition, value)
	} else {
		values, err = ParseSettingSingleValue(definition, value)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse value %s for setting %s", value, definition.DisplayName)
	}

	if len(values) == 0 {
		return fmt.Errorf("failed to parse value %s for setting %s, value cannot be empty", value, definition.DisplayName)
	}

	if IsJSONFormat(strings.TrimSpace(definition.Default)) {
		defaultValues, err = ParseDataEngineSpecificSetting(definition, definition.Default)
	} else {
		defaultValues, err = ParseSettingSingleValue(definition, definition.Default)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to parse default value %s for setting %s", definition.Default, definition.DisplayName)
	}

	for dataEngine := range values {
		if _, ok := defaultValues[dataEngine]; !ok {
			return fmt.Errorf("data engine %s is not supported for setting %s", dataEngine, definition.DisplayName)
		}
	}

	for _, raw := range values {
		strValue, ok := raw.(string)
		if !ok {
			return fmt.Errorf("expected string value but got %T", raw)
		}

		// multi-choices
		if len(definition.Choices) > 0 {
			if !isValidChoice(definition.Choices, strValue) {
				return fmt.Errorf("value %v is not a valid choice, available choices %v", strValue, definition.Choices)
			}
			return nil
		}

		switch name {
		case SettingNameSnapshotDataIntegrityCronJob:
			schedule, err := cron.ParseStandard(strValue)
			if err != nil {
				return errors.Wrapf(err, "invalid cron job format: %v", strValue)
			}

			runAt := schedule.Next(time.Unix(0, 0))
			nextRunAt := schedule.Next(runAt)

			logrus.Infof("The interval between two data integrity checks is %v seconds", nextRunAt.Sub(runAt).Seconds())

		case SettingNameTaintToleration:
			if _, err := UnmarshalTolerations(strValue); err != nil {
				return errors.Wrapf(err, "the value of %v is invalid", name)
			}
		case SettingNameSystemManagedComponentsNodeSelector:
			if _, err := UnmarshalNodeSelector(strValue); err != nil {
				return errors.Wrapf(err, "the value of %v is invalid", name)
			}

		case SettingNameStorageNetwork:
			if err := ValidateStorageNetwork(strValue); err != nil {
				return errors.Wrapf(err, "the value of %v is invalid", name)
			}

		case SettingNameDataEngineLogFlags:
			if err := ValidateDataEngineLogFlags(strValue); err != nil {
				return errors.Wrapf(err, "failed to validate data engine log flags %v", strValue)
			}

		case SettingNameOrphanResourceAutoDeletion:
			if _, err := UnmarshalOrphanResourceTypes(strValue); err != nil {
				return errors.Wrapf(err, "the value of %v is invalid", name)
			}
		}
	}

	return nil
}

func parseDataEngineSpecificSettingStrings(jsonValues map[longhorn.DataEngineType]string) (map[longhorn.DataEngineType]any, error) {
	values := make(map[longhorn.DataEngineType]any)

	for dataEngine, valueStr := range jsonValues {
		values[dataEngine] = valueStr
	}

	return values, nil
}

// parseSettingSingleString processes a single string value for both general and data-engine-specific settings.
func parseSettingSingleString(definition SettingDefinition, value string) (map[longhorn.DataEngineType]any, error) {
	trimmedValue := strings.TrimSpace(value)

	values := make(map[longhorn.DataEngineType]any)

	if definition.DataEngineSpecific {
		var jsonValues map[longhorn.DataEngineType]string
		if err := json.Unmarshal([]byte(definition.Default), &jsonValues); err != nil {
			return values, errors.Wrapf(err, "failed to parse JSON-formatted default value %q for setting %s", definition.Default, definition.DisplayName)
		}
		for dataEngine := range jsonValues {
			values[dataEngine] = trimmedValue
		}
	} else {
		values[longhorn.DataEngineTypeAll] = trimmedValue
	}

	return values, nil
}
