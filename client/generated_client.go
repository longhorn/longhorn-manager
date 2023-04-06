package client

type RancherClient struct {
	RancherBaseClient

	ApiVersion                         ApiVersionOperations
	Error                              ErrorOperations
	AttachInput                        AttachInputOperations
	DetachInput                        DetachInputOperations
	SnapshotInput                      SnapshotInputOperations
	BackupTarget                       BackupTargetOperations
	Backup                             BackupOperations
	BackupInput                        BackupInputOperations
	BackupStatus                       BackupStatusOperations
	Orphan                             OrphanOperations
	RestoreStatus                      RestoreStatusOperations
	PurgeStatus                        PurgeStatusOperations
	RebuildStatus                      RebuildStatusOperations
	ReplicaRemoveInput                 ReplicaRemoveInputOperations
	SalvageInput                       SalvageInputOperations
	ActivateInput                      ActivateInputOperations
	ExpandInput                        ExpandInputOperations
	EngineUpgradeInput                 EngineUpgradeInputOperations
	Replica                            ReplicaOperations
	Controller                         ControllerOperations
	DiskUpdate                         DiskUpdateOperations
	UpdateReplicaCountInput            UpdateReplicaCountInputOperations
	UpdateReplicaAutoBalanceInput      UpdateReplicaAutoBalanceInputOperations
	UpdateDataLocalityInput            UpdateDataLocalityInputOperations
	UpdateAccessModeInput              UpdateAccessModeInputOperations
	UpdateSnapshotDataIntegrityInput   UpdateSnapshotDataIntegrityInputOperations
	UpdateBackupCompressionMethodInput UpdateBackupCompressionMethodInputOperations
	WorkloadStatus                     WorkloadStatusOperations
	CloneStatus                        CloneStatusOperations
	VolumeRecurringJob                 VolumeRecurringJobOperations
	VolumeRecurringJobInput            VolumeRecurringJobInputOperations
	PVCreateInput                      PVCreateInputOperations
	PVCCreateInput                     PVCCreateInputOperations
	SettingDefinition                  SettingDefinitionOperations
	VolumeCondition                    VolumeConditionOperations
	NodeCondition                      NodeConditionOperations
	DiskCondition                      DiskConditionOperations
	SupportBundle                      SupportBundleOperations
	SupportBundleInitateInput          SupportBundleInitateInputOperations
	Tag                                TagOperations
	InstanceManager                    InstanceManagerOperations
	BackingImageDiskFileStatus         BackingImageDiskFileStatusOperations
	BackingImageCleanupInput           BackingImageCleanupInputOperations
	Volume                             VolumeOperations
	Snapshot                           SnapshotOperations
	BackupVolume                       BackupVolumeOperations
	Setting                            SettingOperations
	RecurringJob                       RecurringJobOperations
	EngineImage                        EngineImageOperations
	BackingImage                       BackingImageOperations
	ShareManager                       ShareManagerOperations
	Node                               NodeOperations
	DiskUpdateInput                    DiskUpdateInputOperations
	DiskInfo                           DiskInfoOperations
	KubernetesStatus                   KubernetesStatusOperations
	BackupListOutput                   BackupListOutputOperations
	SnapshotListOutput                 SnapshotListOutputOperations
}

func constructClient(rancherBaseClient *RancherBaseClientImpl) *RancherClient {
	client := &RancherClient{
		RancherBaseClient: rancherBaseClient,
	}

	client.ApiVersion = newApiVersionClient(client)
	client.Error = newErrorClient(client)
	client.AttachInput = newAttachInputClient(client)
	client.DetachInput = newDetachInputClient(client)
	client.SnapshotInput = newSnapshotInputClient(client)
	client.BackupTarget = newBackupTargetClient(client)
	client.Backup = newBackupClient(client)
	client.BackupInput = newBackupInputClient(client)
	client.BackupStatus = newBackupStatusClient(client)
	client.Orphan = newOrphanClient(client)
	client.ShareManager = newShareManagerClient(client)
	client.RestoreStatus = newRestoreStatusClient(client)
	client.PurgeStatus = newPurgeStatusClient(client)
	client.RebuildStatus = newRebuildStatusClient(client)
	client.ReplicaRemoveInput = newReplicaRemoveInputClient(client)
	client.SalvageInput = newSalvageInputClient(client)
	client.ActivateInput = newActivateInputClient(client)
	client.ExpandInput = newExpandInputClient(client)
	client.EngineUpgradeInput = newEngineUpgradeInputClient(client)
	client.Replica = newReplicaClient(client)
	client.Controller = newControllerClient(client)
	client.DiskUpdate = newDiskUpdateClient(client)
	client.UpdateReplicaCountInput = newUpdateReplicaCountInputClient(client)
	client.UpdateReplicaAutoBalanceInput = newUpdateReplicaAutoBalanceInputClient(client)
	client.UpdateDataLocalityInput = newUpdateDataLocalityInputClient(client)
	client.UpdateAccessModeInput = newUpdateAccessModeInputClient(client)
	client.UpdateSnapshotDataIntegrityInput = newUpdateSnapshotDataIntegrityInputClient(client)
	client.UpdateBackupCompressionMethodInput = newUpdateBackupCompressionMethodInputClient(client)
	client.WorkloadStatus = newWorkloadStatusClient(client)
	client.CloneStatus = newCloneStatusClient(client)
	client.VolumeRecurringJob = newVolumeRecurringJobClient(client)
	client.VolumeRecurringJobInput = newVolumeRecurringJobInputClient(client)
	client.PVCreateInput = newPVCreateInputClient(client)
	client.PVCCreateInput = newPVCCreateInputClient(client)
	client.SettingDefinition = newSettingDefinitionClient(client)
	client.VolumeCondition = newVolumeConditionClient(client)
	client.NodeCondition = newNodeConditionClient(client)
	client.DiskCondition = newDiskConditionClient(client)
	client.SupportBundle = newSupportBundleClient(client)
	client.SupportBundleInitateInput = newSupportBundleInitateInputClient(client)
	client.Tag = newTagClient(client)
	client.InstanceManager = newInstanceManagerClient(client)
	client.BackingImageDiskFileStatus = newBackingImageDiskFileStatusClient(client)
	client.BackingImageCleanupInput = newBackingImageCleanupInputClient(client)
	client.Volume = newVolumeClient(client)
	client.Snapshot = newSnapshotClient(client)
	client.BackupVolume = newBackupVolumeClient(client)
	client.Setting = newSettingClient(client)
	client.RecurringJob = newRecurringJobClient(client)
	client.EngineImage = newEngineImageClient(client)
	client.BackingImage = newBackingImageClient(client)
	client.Node = newNodeClient(client)
	client.DiskUpdateInput = newDiskUpdateInputClient(client)
	client.DiskInfo = newDiskInfoClient(client)
	client.KubernetesStatus = newKubernetesStatusClient(client)
	client.BackupListOutput = newBackupListOutputClient(client)
	client.SnapshotListOutput = newSnapshotListOutputClient(client)

	return client
}

func NewRancherClient(opts *ClientOpts) (*RancherClient, error) {
	rancherBaseClient := &RancherBaseClientImpl{
		Types: map[string]Schema{},
	}
	client := constructClient(rancherBaseClient)

	err := setupRancherBaseClient(rancherBaseClient, opts)
	if err != nil {
		return nil, err
	}

	return client, nil
}
