package client

const (
	UPDATE_BACKUP_COMPRESSION_METHOD_INPUT_TYPE = "UpdateBackupCompressionMethodInput"
)

type UpdateBackupCompressionMethodInput struct {
	Resource `yaml:"-"`

	BackupCompressionMethod string `json:"backupCompressionMethod,omitempty" yaml:"backup_compression_method,omitempty"`
}

type UpdateBackupCompressionMethodInputCollection struct {
	Collection
	Data   []UpdateBackupCompressionMethodInput `json:"data,omitempty"`
	client *UpdateBackupCompressionMethodInputClient
}

type UpdateBackupCompressionMethodInputClient struct {
	rancherClient *RancherClient
}

type UpdateBackupCompressionMethodInputOperations interface {
	List(opts *ListOpts) (*UpdateBackupCompressionMethodInputCollection, error)
	Create(opts *UpdateBackupCompressionMethodInput) (*UpdateBackupCompressionMethodInput, error)
	Update(existing *UpdateBackupCompressionMethodInput, updates interface{}) (*UpdateBackupCompressionMethodInput, error)
	ById(id string) (*UpdateBackupCompressionMethodInput, error)
	Delete(container *UpdateBackupCompressionMethodInput) error
}

func newUpdateBackupCompressionMethodInputClient(rancherClient *RancherClient) *UpdateBackupCompressionMethodInputClient {
	return &UpdateBackupCompressionMethodInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateBackupCompressionMethodInputClient) Create(container *UpdateBackupCompressionMethodInput) (*UpdateBackupCompressionMethodInput, error) {
	resp := &UpdateBackupCompressionMethodInput{}
	err := c.rancherClient.doCreate(UPDATE_BACKUP_COMPRESSION_METHOD_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateBackupCompressionMethodInputClient) Update(existing *UpdateBackupCompressionMethodInput, updates interface{}) (*UpdateBackupCompressionMethodInput, error) {
	resp := &UpdateBackupCompressionMethodInput{}
	err := c.rancherClient.doUpdate(UPDATE_BACKUP_COMPRESSION_METHOD_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateBackupCompressionMethodInputClient) List(opts *ListOpts) (*UpdateBackupCompressionMethodInputCollection, error) {
	resp := &UpdateBackupCompressionMethodInputCollection{}
	err := c.rancherClient.doList(UPDATE_BACKUP_COMPRESSION_METHOD_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateBackupCompressionMethodInputCollection) Next() (*UpdateBackupCompressionMethodInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateBackupCompressionMethodInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateBackupCompressionMethodInputClient) ById(id string) (*UpdateBackupCompressionMethodInput, error) {
	resp := &UpdateBackupCompressionMethodInput{}
	err := c.rancherClient.doById(UPDATE_BACKUP_COMPRESSION_METHOD_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateBackupCompressionMethodInputClient) Delete(container *UpdateBackupCompressionMethodInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_BACKUP_COMPRESSION_METHOD_INPUT_TYPE, &container.Resource)
}
