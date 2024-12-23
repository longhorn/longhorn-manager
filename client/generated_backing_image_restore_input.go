package client

const (
	BACKING_IMAGE_RESTORE_INPUT_TYPE = "backingImageRestoreInput"
)

type BackingImageRestoreInput struct {
	Resource `yaml:"-"`

	DataEngine string `json:"dataEngine,omitempty" yaml:"data_engine,omitempty"`

	Secret string `json:"secret,omitempty" yaml:"secret,omitempty"`

	SecretNamespace string `json:"secretNamespace,omitempty" yaml:"secret_namespace,omitempty"`
}

type BackingImageRestoreInputCollection struct {
	Collection
	Data   []BackingImageRestoreInput `json:"data,omitempty"`
	client *BackingImageRestoreInputClient
}

type BackingImageRestoreInputClient struct {
	rancherClient *RancherClient
}

type BackingImageRestoreInputOperations interface {
	List(opts *ListOpts) (*BackingImageRestoreInputCollection, error)
	Create(opts *BackingImageRestoreInput) (*BackingImageRestoreInput, error)
	Update(existing *BackingImageRestoreInput, updates interface{}) (*BackingImageRestoreInput, error)
	ById(id string) (*BackingImageRestoreInput, error)
	Delete(container *BackingImageRestoreInput) error
}

func newBackingImageRestoreInputClient(rancherClient *RancherClient) *BackingImageRestoreInputClient {
	return &BackingImageRestoreInputClient{
		rancherClient: rancherClient,
	}
}

func (c *BackingImageRestoreInputClient) Create(container *BackingImageRestoreInput) (*BackingImageRestoreInput, error) {
	resp := &BackingImageRestoreInput{}
	err := c.rancherClient.doCreate(BACKING_IMAGE_RESTORE_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *BackingImageRestoreInputClient) Update(existing *BackingImageRestoreInput, updates interface{}) (*BackingImageRestoreInput, error) {
	resp := &BackingImageRestoreInput{}
	err := c.rancherClient.doUpdate(BACKING_IMAGE_RESTORE_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *BackingImageRestoreInputClient) List(opts *ListOpts) (*BackingImageRestoreInputCollection, error) {
	resp := &BackingImageRestoreInputCollection{}
	err := c.rancherClient.doList(BACKING_IMAGE_RESTORE_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *BackingImageRestoreInputCollection) Next() (*BackingImageRestoreInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &BackingImageRestoreInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *BackingImageRestoreInputClient) ById(id string) (*BackingImageRestoreInput, error) {
	resp := &BackingImageRestoreInput{}
	err := c.rancherClient.doById(BACKING_IMAGE_RESTORE_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *BackingImageRestoreInputClient) Delete(container *BackingImageRestoreInput) error {
	return c.rancherClient.doResourceDelete(BACKING_IMAGE_RESTORE_INPUT_TYPE, &container.Resource)
}
