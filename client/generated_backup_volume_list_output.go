package client

const (
	BACKUP_VOLUME_LIST_OUTPUT_TYPE = "backupVolumeListOutput"
)

type BackupVolumeListOutput struct {
	Resource `yaml:"-"`

	Data []BackupVolume `json:"data,omitempty" yaml:"data,omitempty"`
}

type BackupVolumeListOutputCollection struct {
	Collection
	Data   []BackupVolumeListOutput `json:"data,omitempty"`
	client *BackupVolumeListOutputClient
}

type BackupVolumeListOutputClient struct {
	rancherClient *RancherClient
}

type BackupVolumeListOutputOperations interface {
	List(opts *ListOpts) (*BackupVolumeListOutputCollection, error)
	Create(opts *BackupVolumeListOutput) (*BackupVolumeListOutput, error)
	Update(existing *BackupVolumeListOutput, updates interface{}) (*BackupVolumeListOutput, error)
	ById(id string) (*BackupVolumeListOutput, error)
	Delete(container *BackupVolumeListOutput) error
}

func newBackupVolumeListOutputClient(rancherClient *RancherClient) *BackupVolumeListOutputClient {
	return &BackupVolumeListOutputClient{
		rancherClient: rancherClient,
	}
}

func (c *BackupVolumeListOutputClient) Create(container *BackupVolumeListOutput) (*BackupVolumeListOutput, error) {
	resp := &BackupVolumeListOutput{}
	err := c.rancherClient.doCreate(BACKUP_VOLUME_LIST_OUTPUT_TYPE, container, resp)
	return resp, err
}

func (c *BackupVolumeListOutputClient) Update(existing *BackupVolumeListOutput, updates interface{}) (*BackupVolumeListOutput, error) {
	resp := &BackupVolumeListOutput{}
	err := c.rancherClient.doUpdate(BACKUP_VOLUME_LIST_OUTPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *BackupVolumeListOutputClient) List(opts *ListOpts) (*BackupVolumeListOutputCollection, error) {
	resp := &BackupVolumeListOutputCollection{}
	err := c.rancherClient.doList(BACKUP_VOLUME_LIST_OUTPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *BackupVolumeListOutputCollection) Next() (*BackupVolumeListOutputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &BackupVolumeListOutputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *BackupVolumeListOutputClient) ById(id string) (*BackupVolumeListOutput, error) {
	resp := &BackupVolumeListOutput{}
	err := c.rancherClient.doById(BACKUP_VOLUME_LIST_OUTPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *BackupVolumeListOutputClient) Delete(container *BackupVolumeListOutput) error {
	return c.rancherClient.doResourceDelete(BACKUP_VOLUME_LIST_OUTPUT_TYPE, &container.Resource)
}
