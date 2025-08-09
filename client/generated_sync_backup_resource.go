package client

const (
	SYNC_BACKUP_RESOURCE_TYPE = "syncBackupResource"
)

type SyncBackupResource struct {
	Resource `yaml:"-"`

	SyncAllBackupTargets bool `json:"syncAllBackupTargets,omitempty" yaml:"sync_all_backup_targets,omitempty"`

	SyncAllBackupVolumes bool `json:"syncAllBackupVolumes,omitempty" yaml:"sync_all_backup_volumes,omitempty"`

	SyncBackupTarget bool `json:"syncBackupTarget,omitempty" yaml:"sync_backup_target,omitempty"`

	SyncBackupVolume bool `json:"syncBackupVolume,omitempty" yaml:"sync_backup_volume,omitempty"`
}

type SyncBackupResourceCollection struct {
	Collection
	Data   []SyncBackupResource `json:"data,omitempty"`
	client *SyncBackupResourceClient
}

type SyncBackupResourceClient struct {
	rancherClient *RancherClient
}

type SyncBackupResourceOperations interface {
	List(opts *ListOpts) (*SyncBackupResourceCollection, error)
	Create(opts *SyncBackupResource) (*SyncBackupResource, error)
	Update(existing *SyncBackupResource, updates interface{}) (*SyncBackupResource, error)
	ById(id string) (*SyncBackupResource, error)
	Delete(container *SyncBackupResource) error
}

func newSyncBackupResourceClient(rancherClient *RancherClient) *SyncBackupResourceClient {
	return &SyncBackupResourceClient{
		rancherClient: rancherClient,
	}
}

func (c *SyncBackupResourceClient) Create(container *SyncBackupResource) (*SyncBackupResource, error) {
	resp := &SyncBackupResource{}
	err := c.rancherClient.doCreate(SYNC_BACKUP_RESOURCE_TYPE, container, resp)
	return resp, err
}

func (c *SyncBackupResourceClient) Update(existing *SyncBackupResource, updates interface{}) (*SyncBackupResource, error) {
	resp := &SyncBackupResource{}
	err := c.rancherClient.doUpdate(SYNC_BACKUP_RESOURCE_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *SyncBackupResourceClient) List(opts *ListOpts) (*SyncBackupResourceCollection, error) {
	resp := &SyncBackupResourceCollection{}
	err := c.rancherClient.doList(SYNC_BACKUP_RESOURCE_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *SyncBackupResourceCollection) Next() (*SyncBackupResourceCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &SyncBackupResourceCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *SyncBackupResourceClient) ById(id string) (*SyncBackupResource, error) {
	resp := &SyncBackupResource{}
	err := c.rancherClient.doById(SYNC_BACKUP_RESOURCE_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *SyncBackupResourceClient) Delete(container *SyncBackupResource) error {
	return c.rancherClient.doResourceDelete(SYNC_BACKUP_RESOURCE_TYPE, &container.Resource)
}
