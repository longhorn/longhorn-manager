package client

const (
	UPDATE_BACKUP_TARGET_INPUT_TYPE = "UpdateBackupTargetInput"
)

type UpdateBackupTargetInput struct {
	Resource `yaml:"-"`

	BackupTargetName string `json:"backupTargetName,omitempty" yaml:"backup_target_name,omitempty"`
}

type UpdateBackupTargetInputCollection struct {
	Collection
	Data   []UpdateBackupTargetInput `json:"data,omitempty"`
	client *UpdateBackupTargetInputClient
}

type UpdateBackupTargetInputClient struct {
	rancherClient *RancherClient
}

type UpdateBackupTargetInputOperations interface {
	List(opts *ListOpts) (*UpdateBackupTargetInputCollection, error)
	Create(opts *UpdateBackupTargetInput) (*UpdateBackupTargetInput, error)
	Update(existing *UpdateBackupTargetInput, updates interface{}) (*UpdateBackupTargetInput, error)
	ById(id string) (*UpdateBackupTargetInput, error)
	Delete(container *UpdateBackupTargetInput) error
}

func newUpdateBackupTargetInputClient(rancherClient *RancherClient) *UpdateBackupTargetInputClient {
	return &UpdateBackupTargetInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateBackupTargetInputClient) Create(container *UpdateBackupTargetInput) (*UpdateBackupTargetInput, error) {
	resp := &UpdateBackupTargetInput{}
	err := c.rancherClient.doCreate(UPDATE_BACKUP_TARGET_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateBackupTargetInputClient) Update(existing *UpdateBackupTargetInput, updates interface{}) (*UpdateBackupTargetInput, error) {
	resp := &UpdateBackupTargetInput{}
	err := c.rancherClient.doUpdate(UPDATE_BACKUP_TARGET_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateBackupTargetInputClient) List(opts *ListOpts) (*UpdateBackupTargetInputCollection, error) {
	resp := &UpdateBackupTargetInputCollection{}
	err := c.rancherClient.doList(UPDATE_BACKUP_TARGET_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateBackupTargetInputCollection) Next() (*UpdateBackupTargetInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateBackupTargetInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateBackupTargetInputClient) ById(id string) (*UpdateBackupTargetInput, error) {
	resp := &UpdateBackupTargetInput{}
	err := c.rancherClient.doById(UPDATE_BACKUP_TARGET_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateBackupTargetInputClient) Delete(container *UpdateBackupTargetInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_BACKUP_TARGET_INPUT_TYPE, &container.Resource)
}
