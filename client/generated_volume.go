package client

const (
	VOLUME_TYPE = "volume"
)

type Volume struct {
	Resource `yaml:"-"`

	BackupStatus []BackupStatus `json:"backupStatus,omitempty" yaml:"backup_status,omitempty"`

	BaseImage string `json:"baseImage,omitempty" yaml:"base_image,omitempty"`

	Conditions map[string]interface{} `json:"conditions,omitempty" yaml:"conditions,omitempty"`

	Controllers []Controller `json:"controllers,omitempty" yaml:"controllers,omitempty"`

	Created string `json:"created,omitempty" yaml:"created,omitempty"`

	CurrentImage string `json:"currentImage,omitempty" yaml:"current_image,omitempty"`

	DataLocality string `json:"dataLocality,omitempty" yaml:"data_locality,omitempty"`

	DisableFrontend bool `json:"disableFrontend,omitempty" yaml:"disable_frontend,omitempty"`

	DiskSelector []string `json:"diskSelector,omitempty" yaml:"disk_selector,omitempty"`

	EngineImage string `json:"engineImage,omitempty" yaml:"engine_image,omitempty"`

	FromBackup string `json:"fromBackup,omitempty" yaml:"from_backup,omitempty"`

	Frontend string `json:"frontend,omitempty" yaml:"frontend,omitempty"`

	KubernetesStatus KubernetesStatus `json:"kubernetesStatus,omitempty" yaml:"kubernetes_status,omitempty"`

	LastBackup string `json:"lastBackup,omitempty" yaml:"last_backup,omitempty"`

	LastBackupAt string `json:"lastBackupAt,omitempty" yaml:"last_backup_at,omitempty"`

	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	NodeSelector []string `json:"nodeSelector,omitempty" yaml:"node_selector,omitempty"`

	NumberOfReplicas int64 `json:"numberOfReplicas,omitempty" yaml:"number_of_replicas,omitempty"`

	PurgeStatus []PurgeStatus `json:"purgeStatus,omitempty" yaml:"purge_status,omitempty"`

	Ready bool `json:"ready,omitempty" yaml:"ready,omitempty"`

	RebuildStatus []RebuildStatus `json:"rebuildStatus,omitempty" yaml:"rebuild_status,omitempty"`

	RecurringJobs []RecurringJob `json:"recurringJobs,omitempty" yaml:"recurring_jobs,omitempty"`

	Replicas []Replica `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	RestoreRequired bool `json:"restoreRequired,omitempty" yaml:"restore_required,omitempty"`

	RestoreStatus []RestoreStatus `json:"restoreStatus,omitempty" yaml:"restore_status,omitempty"`

	Robustness string `json:"robustness,omitempty" yaml:"robustness,omitempty"`

	Size string `json:"size,omitempty" yaml:"size,omitempty"`

	StaleReplicaTimeout int64 `json:"staleReplicaTimeout,omitempty" yaml:"stale_replica_timeout,omitempty"`

	Standby bool `json:"standby,omitempty" yaml:"standby,omitempty"`

	State string `json:"state,omitempty" yaml:"state,omitempty"`

	Timestamp string `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
}

type VolumeCollection struct {
	Collection
	Data   []Volume `json:"data,omitempty"`
	client *VolumeClient
}

type VolumeClient struct {
	rancherClient *RancherClient
}

type VolumeOperations interface {
	List(opts *ListOpts) (*VolumeCollection, error)
	Create(opts *Volume) (*Volume, error)
	Update(existing *Volume, updates interface{}) (*Volume, error)
	ById(id string) (*Volume, error)
	Delete(container *Volume) error

	ActionActivate(*Volume, *ActivateInput) (*Volume, error)

	ActionAttach(*Volume, *AttachInput) (*Volume, error)

	ActionCancelExpansion(*Volume) (*Volume, error)

	ActionDetach(*Volume) (*Volume, error)

	ActionExpand(*Volume, *ExpandInput) (*Volume, error)

	ActionPvCreate(*Volume, *PVCreateInput) (*Volume, error)

	ActionPvcCreate(*Volume, *PVCCreateInput) (*Volume, error)

	ActionReplicaRemove(*Volume, *ReplicaRemoveInput) (*Volume, error)

	ActionSalvage(*Volume, *SalvageInput) (*Volume, error)

	ActionSnapshotBackup(*Volume, *SnapshotInput) (*Volume, error)

	ActionSnapshotCreate(*Volume, *SnapshotInput) (*Snapshot, error)

	ActionSnapshotDelete(*Volume, *SnapshotInput) (*Volume, error)

	ActionSnapshotGet(*Volume, *SnapshotInput) (*Snapshot, error)

	ActionSnapshotList(*Volume) (*SnapshotListOutput, error)

	ActionSnapshotPurge(*Volume) (*Volume, error)

	ActionSnapshotRevert(*Volume, *SnapshotInput) (*Snapshot, error)
}

func newVolumeClient(rancherClient *RancherClient) *VolumeClient {
	return &VolumeClient{
		rancherClient: rancherClient,
	}
}

func (c *VolumeClient) Create(container *Volume) (*Volume, error) {
	resp := &Volume{}
	err := c.rancherClient.doCreate(VOLUME_TYPE, container, resp)
	return resp, err
}

func (c *VolumeClient) Update(existing *Volume, updates interface{}) (*Volume, error) {
	resp := &Volume{}
	err := c.rancherClient.doUpdate(VOLUME_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *VolumeClient) List(opts *ListOpts) (*VolumeCollection, error) {
	resp := &VolumeCollection{}
	err := c.rancherClient.doList(VOLUME_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *VolumeCollection) Next() (*VolumeCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &VolumeCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *VolumeClient) ById(id string) (*Volume, error) {
	resp := &Volume{}
	err := c.rancherClient.doById(VOLUME_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *VolumeClient) Delete(container *Volume) error {
	return c.rancherClient.doResourceDelete(VOLUME_TYPE, &container.Resource)
}

func (c *VolumeClient) ActionActivate(resource *Volume, input *ActivateInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "activate", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionAttach(resource *Volume, input *AttachInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "attach", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionCancelExpansion(resource *Volume) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "cancelExpansion", &resource.Resource, nil, resp)

	return resp, err
}

func (c *VolumeClient) ActionDetach(resource *Volume) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "detach", &resource.Resource, nil, resp)

	return resp, err
}

func (c *VolumeClient) ActionExpand(resource *Volume, input *ExpandInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "expand", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionPvCreate(resource *Volume, input *PVCreateInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "pvCreate", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionPvcCreate(resource *Volume, input *PVCCreateInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "pvcCreate", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionReplicaRemove(resource *Volume, input *ReplicaRemoveInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "replicaRemove", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionSalvage(resource *Volume, input *SalvageInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "salvage", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionSnapshotBackup(resource *Volume, input *SnapshotInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "snapshotBackup", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionSnapshotCreate(resource *Volume, input *SnapshotInput) (*Snapshot, error) {

	resp := &Snapshot{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "snapshotCreate", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionSnapshotDelete(resource *Volume, input *SnapshotInput) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "snapshotDelete", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionSnapshotGet(resource *Volume, input *SnapshotInput) (*Snapshot, error) {

	resp := &Snapshot{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "snapshotGet", &resource.Resource, input, resp)

	return resp, err
}

func (c *VolumeClient) ActionSnapshotList(resource *Volume) (*SnapshotListOutput, error) {

	resp := &SnapshotListOutput{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "snapshotList", &resource.Resource, nil, resp)

	return resp, err
}

func (c *VolumeClient) ActionSnapshotPurge(resource *Volume) (*Volume, error) {

	resp := &Volume{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "snapshotPurge", &resource.Resource, nil, resp)

	return resp, err
}

func (c *VolumeClient) ActionSnapshotRevert(resource *Volume, input *SnapshotInput) (*Snapshot, error) {

	resp := &Snapshot{}

	err := c.rancherClient.doAction(VOLUME_TYPE, "snapshotRevert", &resource.Resource, input, resp)

	return resp, err
}
