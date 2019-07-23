package client

type RancherClient struct {
	RancherBaseClient

	ApiVersion                ApiVersionOperations
	Error                     ErrorOperations
	Snapshot                  SnapshotOperations
	AttachInput               AttachInputOperations
	SnapshotInput             SnapshotInputOperations
	Backup                    BackupOperations
	BackupInput               BackupInputOperations
	BackupStatus              BackupStatusOperations
	RecurringJob              RecurringJobOperations
	ReplicaRemoveInput        ReplicaRemoveInputOperations
	SalvageInput              SalvageInputOperations
	ActivateInput             ActivateInputOperations
	EngineUpgradeInput        EngineUpgradeInputOperations
	Replica                   ReplicaOperations
	Controller                ControllerOperations
	DiskUpdate                DiskUpdateOperations
	NodeInput                 NodeInputOperations
	UpdateReplicaCountInput   UpdateReplicaCountInputOperations
	WorkloadStatus            WorkloadStatusOperations
	PVCreateInput             PVCreateInputOperations
	PVCCreateInput            PVCCreateInputOperations
	SettingDefinition         SettingDefinitionOperations
	VolumeCondition           VolumeConditionOperations
	NodeCondition             NodeConditionOperations
	DiskCondition             DiskConditionOperations
	SupportBundle             SupportBundleOperations
	SupportBundleInitateInput SupportBundleInitateInputOperations
	Tag                       TagOperations
	Volume                    VolumeOperations
	BackupVolume              BackupVolumeOperations
	Setting                   SettingOperations
	RecurringInput            RecurringInputOperations
	EngineImage               EngineImageOperations
	Node                      NodeOperations
	DiskUpdateInput           DiskUpdateInputOperations
	DiskInfo                  DiskInfoOperations
	KubernetesStatus          KubernetesStatusOperations
}

func constructClient(rancherBaseClient *RancherBaseClientImpl) *RancherClient {
	client := &RancherClient{
		RancherBaseClient: rancherBaseClient,
	}

	client.ApiVersion = newApiVersionClient(client)
	client.Error = newErrorClient(client)
	client.Snapshot = newSnapshotClient(client)
	client.AttachInput = newAttachInputClient(client)
	client.SnapshotInput = newSnapshotInputClient(client)
	client.Backup = newBackupClient(client)
	client.BackupInput = newBackupInputClient(client)
	client.BackupStatus = newBackupStatusClient(client)
	client.RecurringJob = newRecurringJobClient(client)
	client.ReplicaRemoveInput = newReplicaRemoveInputClient(client)
	client.SalvageInput = newSalvageInputClient(client)
	client.ActivateInput = newActivateInputClient(client)
	client.EngineUpgradeInput = newEngineUpgradeInputClient(client)
	client.Replica = newReplicaClient(client)
	client.Controller = newControllerClient(client)
	client.DiskUpdate = newDiskUpdateClient(client)
	client.NodeInput = newNodeInputClient(client)
	client.UpdateReplicaCountInput = newUpdateReplicaCountInputClient(client)
	client.WorkloadStatus = newWorkloadStatusClient(client)
	client.PVCreateInput = newPVCreateInputClient(client)
	client.PVCCreateInput = newPVCCreateInputClient(client)
	client.SettingDefinition = newSettingDefinitionClient(client)
	client.VolumeCondition = newVolumeConditionClient(client)
	client.NodeCondition = newNodeConditionClient(client)
	client.DiskCondition = newDiskConditionClient(client)
	client.SupportBundle = newSupportBundleClient(client)
	client.SupportBundleInitateInput = newSupportBundleInitateInputClient(client)
	client.Tag = newTagClient(client)
	client.Volume = newVolumeClient(client)
	client.BackupVolume = newBackupVolumeClient(client)
	client.Setting = newSettingClient(client)
	client.RecurringInput = newRecurringInputClient(client)
	client.EngineImage = newEngineImageClient(client)
	client.Node = newNodeClient(client)
	client.DiskUpdateInput = newDiskUpdateInputClient(client)
	client.DiskInfo = newDiskInfoClient(client)
	client.KubernetesStatus = newKubernetesStatusClient(client)

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
