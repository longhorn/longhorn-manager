package steve

import (
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/sirupsen/logrus"

	steveserver "github.com/rancher/steve/pkg/server"
)

// registerSchemaTemplates registers schema templates for Longhorn resources
// to add custom actions, links, and formatters.
// This is called internally by New() after the Steve server is created.
func registerSchemaTemplates(steve *steveserver.Server) {
	logrus.Info("Registering Longhorn Steve schema templates")
	registerVolumeSchema(steve)
	registerNodeSchema(steve)
	registerBackingImageSchema(steve)
	registerBackupVolumeSchema(steve)
	registerBackupTargetSchema(steve)
	registerBackupBackingImageSchema(steve)
	registerSupportBundleSchema(steve)
	logrus.Info("Finished registering Longhorn Steve schema templates")
}

// volumeFormatter adds action URLs to volume resources
func volumeFormatter(request *types.APIRequest, resource *types.RawResource) {
	logrus.Debugf("volumeFormatter called for resource: %s", resource.ID)
	// Initialize the Actions map - this is required before calling AddAction
	resource.Actions = make(map[string]string, 20)
	resource.AddAction(request, "attach")
	resource.AddAction(request, "detach")
	resource.AddAction(request, "salvage")
	resource.AddAction(request, "activate")
	resource.AddAction(request, "expand")
	resource.AddAction(request, "cancelExpansion")
	resource.AddAction(request, "snapshotCreate")
	resource.AddAction(request, "snapshotDelete")
	resource.AddAction(request, "snapshotRevert")
	resource.AddAction(request, "snapshotBackup")
	resource.AddAction(request, "snapshotList")
	resource.AddAction(request, "snapshotPurge")
	resource.AddAction(request, "recurringJobAdd")
	resource.AddAction(request, "recurringJobDelete")
	resource.AddAction(request, "recurringJobList")
	resource.AddAction(request, "replicaRemove")
	resource.AddAction(request, "engineUpgrade")
	resource.AddAction(request, "pvCreate")
	resource.AddAction(request, "pvcCreate")
	resource.AddAction(request, "trimFilesystem")
}

// nodeFormatter adds action URLs to node resources
func nodeFormatter(request *types.APIRequest, resource *types.RawResource) {
	logrus.Debugf("nodeFormatter called for resource: %s", resource.ID)
	// Initialize the Actions map - this is required before calling AddAction
	resource.Actions = make(map[string]string, 1)
	resource.AddAction(request, "diskUpdate")
}

// backingImageFormatter adds action URLs to backing image resources
func backingImageFormatter(request *types.APIRequest, resource *types.RawResource) {
	// Initialize the Actions map - this is required before calling AddAction
	resource.Actions = make(map[string]string, 4)
	resource.AddAction(request, "backingImageCleanup")
	resource.AddAction(request, "backingImageUpload")
	resource.AddAction(request, "backupBackingImageCreate")
	resource.AddAction(request, "updateMinNumberOfCopies")
}

// backupVolumeFormatter adds action URLs to backup volume resources
func backupVolumeFormatter(request *types.APIRequest, resource *types.RawResource) {
	// Initialize the Actions map - this is required before calling AddAction
	resource.Actions = make(map[string]string, 5)
	resource.AddAction(request, "backupList")
	resource.AddAction(request, "backupListByVolume")
	resource.AddAction(request, "backupGet")
	resource.AddAction(request, "backupDelete")
	resource.AddAction(request, "backupVolumeSync")
}

// backupTargetFormatter adds action URLs to backup target resources
func backupTargetFormatter(request *types.APIRequest, resource *types.RawResource) {
	// Initialize the Actions map - this is required before calling AddAction
	resource.Actions = make(map[string]string, 2)
	resource.AddAction(request, "backupTargetSync")
	resource.AddAction(request, "backupTargetUpdate")
}

// backupBackingImageFormatter adds action URLs to backup backing image resources
func backupBackingImageFormatter(request *types.APIRequest, resource *types.RawResource) {
	// Initialize the Actions map - this is required before calling AddAction
	resource.Actions = make(map[string]string, 1)
	resource.AddAction(request, "backupBackingImageRestore")
}

// supportBundleFormatter adds action URLs to support bundle resources
func supportBundleFormatter(request *types.APIRequest, resource *types.RawResource) {
	// Initialize the Actions map - this is required before calling AddAction
	resource.Actions = make(map[string]string, 1)
	resource.AddAction(request, "download")
}

var volumeActionNames = []string{
	"attach", "detach", "salvage", "activate", "expand", "cancelExpansion",
	"offlineReplicaRebuilding",
	"snapshotCreate", "snapshotDelete", "snapshotRevert", "snapshotBackup",
	"snapshotGet", "snapshotList", "snapshotPurge",
	"snapshotCRCreate", "snapshotCRList", "snapshotCRGet", "snapshotCRDelete",
	"recurringJobAdd", "recurringJobDelete", "recurringJobList",
	"replicaRemove", "engineUpgrade",
	"updateReplicaCount", "updateDataLocality", "updateAccessMode",
	"updateUnmapMarkSnapChainRemoved",
	"updateSnapshotMaxCount", "updateSnapshotMaxSize",
	"updateReplicaRebuildingBandwidthLimit",
	"updateUblkQueueDepth", "updateUblkNumberOfQueue",
	"updateReplicaSoftAntiAffinity", "updateReplicaZoneSoftAntiAffinity",
	"updateReplicaDiskSoftAntiAffinity",
	"updateReplicaAutoBalance", "updateRebuildConcurrentSyncLimit",
	"updateSnapshotDataIntegrity", "updateBackupCompressionMethod",
	"updateFreezeFilesystemForSnapshot", "updateBackupTargetName",
	"pvCreate", "pvcCreate", "trimFilesystem",
}

func registerVolumeSchema(steve *steveserver.Server) {
	steve.SchemaFactory.AddTemplate(schema.Template{
		ID:        "longhorn.io.volume",
		Formatter: volumeFormatter,
		Customize: func(apiSchema *types.APISchema) {
			apiSchema.ActionHandlers = bridgeMap("volumes", "name", volumeActionNames...)
			apiSchema.ResourceActions = map[string]schemas.Action{
				"attach": {
					Input: "attachInput",
				},
				"detach": {
					Input: "detachInput",
				},
				"salvage": {
					Input: "salvageInput",
				},
				"activate": {
					Input: "activateInput",
				},
				"expand": {
					Input: "expandInput",
				},
				"cancelExpansion": {},
				"offlineReplicaRebuilding": {
					Input: "offlineReplicaRebuildingInput",
				},
				"snapshotCreate": {
					Input:  "snapshotInput",
					Output: "snapshot",
				},
				"snapshotDelete": {
					Input: "snapshotInput",
				},
				"snapshotRevert": {
					Input: "snapshotInput",
				},
				"snapshotBackup": {
					Input: "snapshotInput",
				},
				"snapshotGet": {
					Input: "snapshotInput",
				},
				"snapshotList":  {},
				"snapshotPurge": {},
				"snapshotCRCreate": {
					Input: "snapshotCRInput",
				},
				"snapshotCRList":   {},
				"snapshotCRGet":    {},
				"snapshotCRDelete": {},
				"recurringJobAdd": {
					Input: "volumeRecurringJobInput",
				},
				"recurringJobDelete": {
					Input: "volumeRecurringJobInput",
				},
				"recurringJobList": {},
				"replicaRemove": {
					Input: "replicaRemoveInput",
				},
				"engineUpgrade": {
					Input: "engineUpgradeInput",
				},
				"updateReplicaCount": {
					Input: "updateReplicaCountInput",
				},
				"updateDataLocality": {
					Input: "updateDataLocalityInput",
				},
				"updateAccessMode": {
					Input: "updateAccessModeInput",
				},
				"updateUnmapMarkSnapChainRemoved": {
					Input: "updateUnmapMarkSnapChainRemovedInput",
				},
				"updateSnapshotMaxCount": {
					Input: "updateSnapshotMaxCountInput",
				},
				"updateSnapshotMaxSize": {
					Input: "updateSnapshotMaxSizeInput",
				},
				"updateReplicaRebuildingBandwidthLimit": {
					Input: "updateReplicaRebuildingBandwidthLimitInput",
				},
				"updateUblkQueueDepth": {
					Input: "updateUblkQueueDepthInput",
				},
				"updateUblkNumberOfQueue": {
					Input: "updateUblkNumberOfQueueInput",
				},
				"updateReplicaSoftAntiAffinity": {
					Input: "updateReplicaSoftAntiAffinityInput",
				},
				"updateReplicaZoneSoftAntiAffinity": {
					Input: "updateReplicaZoneSoftAntiAffinityInput",
				},
				"updateReplicaDiskSoftAntiAffinity": {
					Input: "updateReplicaDiskSoftAntiAffinityInput",
				},
				"updateReplicaAutoBalance": {
					Input: "updateReplicaAutoBalanceInput",
				},
				"updateRebuildConcurrentSyncLimit": {
					Input: "updateRebuildConcurrentSyncLimitInput",
				},
				"updateSnapshotDataIntegrity": {
					Input: "updateSnapshotDataIntegrityInput",
				},
				"updateBackupCompressionMethod": {
					Input: "updateBackupCompressionInput",
				},
				"updateFreezeFilesystemForSnapshot": {
					Input: "updateFreezeFilesystemForSnapshotInput",
				},
				"updateBackupTargetName": {
					Input: "updateBackupTargetNameInput",
				},
				"pvCreate": {
					Input: "pvCreateInput",
				},
				"pvcCreate": {
					Input: "pvcCreateInput",
				},
				"trimFilesystem": {},
			}
		},
	})
}

func registerNodeSchema(steve *steveserver.Server) {
	steve.SchemaFactory.AddTemplate(schema.Template{
		Group:     "longhorn.io",
		Kind:      "Node",
		Formatter: nodeFormatter,
		Customize: func(apiSchema *types.APISchema) {
			apiSchema.ActionHandlers = bridgeMap("nodes", "name", "diskUpdate")
			apiSchema.ResourceActions = map[string]schemas.Action{
				"diskUpdate": {
					Input: "diskUpdateInput",
				},
			}
		},
	})
}

func registerBackingImageSchema(steve *steveserver.Server) {
	steve.SchemaFactory.AddTemplate(schema.Template{
		ID:        "longhorn.io.backingimage",
		Formatter: backingImageFormatter,
		Customize: func(apiSchema *types.APISchema) {
			apiSchema.ActionHandlers = bridgeMap("backingimages", "name",
				"backingImageCleanup", "backingImageUpload",
				"backupBackingImageCreate", "updateMinNumberOfCopies")
			apiSchema.ResourceActions = map[string]schemas.Action{
				"backingImageCleanup": {
					Input: "backingImageCleanupInput",
				},
				"backingImageUpload": {},
				"backupBackingImageCreate": {
					Input: "backupBackingImageCreateInput",
				},
				"updateMinNumberOfCopies": {
					Input: "updateMinNumberOfCopiesInput",
				},
			}
		},
	})
}

func registerBackupVolumeSchema(steve *steveserver.Server) {
	steve.SchemaFactory.AddTemplate(schema.Template{
		ID:        "longhorn.io.backupvolume",
		Formatter: backupVolumeFormatter,
		Customize: func(apiSchema *types.APISchema) {
			apiSchema.ActionHandlers = bridgeMap("backupvolumes", "backupVolumeName",
				"backupList", "backupListByVolume", "backupGet",
				"backupDelete", "backupVolumeSync")
			apiSchema.ResourceActions = map[string]schemas.Action{
				"backupList":         {},
				"backupListByVolume": {},
				"backupGet": {
					Input: "backupInput",
				},
				"backupDelete": {
					Input: "backupInput",
				},
				"backupVolumeSync": {},
			}
		},
	})
}

func registerBackupTargetSchema(steve *steveserver.Server) {
	steve.SchemaFactory.AddTemplate(schema.Template{
		ID:        "longhorn.io.backuptarget",
		Formatter: backupTargetFormatter,
		Customize: func(apiSchema *types.APISchema) {
			apiSchema.ActionHandlers = bridgeMap("backuptargets", "backupTargetName",
				"backupTargetSync", "backupTargetUpdate")
			apiSchema.ResourceActions = map[string]schemas.Action{
				"backupTargetSync":   {},
				"backupTargetUpdate": {},
			}
		},
	})
}

func registerBackupBackingImageSchema(steve *steveserver.Server) {
	steve.SchemaFactory.AddTemplate(schema.Template{
		ID:        "longhorn.io.backupbackingimage",
		Formatter: backupBackingImageFormatter,
		Customize: func(apiSchema *types.APISchema) {
			apiSchema.ActionHandlers = bridgeMap("backupbackingimages", "name",
				"backupBackingImageRestore")
			apiSchema.ResourceActions = map[string]schemas.Action{
				"backupBackingImageRestore": {
					Input: "backupBackingImageRestoreInput",
				},
			}
		},
	})
}

func registerSupportBundleSchema(steve *steveserver.Server) {
	steve.SchemaFactory.AddTemplate(schema.Template{
		ID:        "longhorn.io.supportbundle",
		Formatter: supportBundleFormatter,
		Customize: func(apiSchema *types.APISchema) {
			apiSchema.ResourceActions = map[string]schemas.Action{
				"download": {},
			}
		},
	})
}
