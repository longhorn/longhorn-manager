package client

const (
	BACKUP_TARGET_LIST_OUTPUT_TYPE = "backupTargetListOutput"
)

type BackupTargetListOutput struct {
	Resource `yaml:"-"`

	Data []BackupTarget `json:"data,omitempty" yaml:"data,omitempty"`
}

type BackupTargetListOutputCollection struct {
	Collection
	Data   []BackupTargetListOutput `json:"data,omitempty"`
	client *BackupTargetListOutputClient
}

type BackupTargetListOutputClient struct {
	rancherClient *RancherClient
}

type BackupTargetListOutputOperations interface {
	List(opts *ListOpts) (*BackupTargetListOutputCollection, error)
	Create(opts *BackupTargetListOutput) (*BackupTargetListOutput, error)
	Update(existing *BackupTargetListOutput, updates interface{}) (*BackupTargetListOutput, error)
	ById(id string) (*BackupTargetListOutput, error)
	Delete(container *BackupTargetListOutput) error
}

func newBackupTargetListOutputClient(rancherClient *RancherClient) *BackupTargetListOutputClient {
	return &BackupTargetListOutputClient{
		rancherClient: rancherClient,
	}
}

func (c *BackupTargetListOutputClient) Create(container *BackupTargetListOutput) (*BackupTargetListOutput, error) {
	resp := &BackupTargetListOutput{}
	err := c.rancherClient.doCreate(BACKUP_TARGET_LIST_OUTPUT_TYPE, container, resp)
	return resp, err
}

func (c *BackupTargetListOutputClient) Update(existing *BackupTargetListOutput, updates interface{}) (*BackupTargetListOutput, error) {
	resp := &BackupTargetListOutput{}
	err := c.rancherClient.doUpdate(BACKUP_TARGET_LIST_OUTPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *BackupTargetListOutputClient) List(opts *ListOpts) (*BackupTargetListOutputCollection, error) {
	resp := &BackupTargetListOutputCollection{}
	err := c.rancherClient.doList(BACKUP_TARGET_LIST_OUTPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *BackupTargetListOutputCollection) Next() (*BackupTargetListOutputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &BackupTargetListOutputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *BackupTargetListOutputClient) ById(id string) (*BackupTargetListOutput, error) {
	resp := &BackupTargetListOutput{}
	err := c.rancherClient.doById(BACKUP_TARGET_LIST_OUTPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *BackupTargetListOutputClient) Delete(container *BackupTargetListOutput) error {
	return c.rancherClient.doResourceDelete(BACKUP_TARGET_LIST_OUTPUT_TYPE, &container.Resource)
}
