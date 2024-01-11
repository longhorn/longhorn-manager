package client

const (
	UPDATE_SNAPSHOT_MAX_SIZE_INPUT_TYPE = "UpdateSnapshotMaxSizeInput"
)

type UpdateSnapshotMaxSizeInput struct {
	Resource `yaml:"-"`

	SnapshotMaxSize string `json:"snapshotMaxSize,omitempty" yaml:"snapshot_max_size,omitempty"`
}

type UpdateSnapshotMaxSizeInputCollection struct {
	Collection
	Data   []UpdateSnapshotMaxSizeInput `json:"data,omitempty"`
	client *UpdateSnapshotMaxSizeInputClient
}

type UpdateSnapshotMaxSizeInputClient struct {
	rancherClient *RancherClient
}

type UpdateSnapshotMaxSizeInputOperations interface {
	List(opts *ListOpts) (*UpdateSnapshotMaxSizeInputCollection, error)
	Create(opts *UpdateSnapshotMaxSizeInput) (*UpdateSnapshotMaxSizeInput, error)
	Update(existing *UpdateSnapshotMaxSizeInput, updates interface{}) (*UpdateSnapshotMaxSizeInput, error)
	ById(id string) (*UpdateSnapshotMaxSizeInput, error)
	Delete(container *UpdateSnapshotMaxSizeInput) error
}

func newUpdateSnapshotMaxSizeInputClient(rancherClient *RancherClient) *UpdateSnapshotMaxSizeInputClient {
	return &UpdateSnapshotMaxSizeInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateSnapshotMaxSizeInputClient) Create(container *UpdateSnapshotMaxSizeInput) (*UpdateSnapshotMaxSizeInput, error) {
	resp := &UpdateSnapshotMaxSizeInput{}
	err := c.rancherClient.doCreate(UPDATE_SNAPSHOT_MAX_SIZE_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateSnapshotMaxSizeInputClient) Update(existing *UpdateSnapshotMaxSizeInput, updates interface{}) (*UpdateSnapshotMaxSizeInput, error) {
	resp := &UpdateSnapshotMaxSizeInput{}
	err := c.rancherClient.doUpdate(UPDATE_SNAPSHOT_MAX_SIZE_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateSnapshotMaxSizeInputClient) List(opts *ListOpts) (*UpdateSnapshotMaxSizeInputCollection, error) {
	resp := &UpdateSnapshotMaxSizeInputCollection{}
	err := c.rancherClient.doList(UPDATE_SNAPSHOT_MAX_SIZE_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateSnapshotMaxSizeInputCollection) Next() (*UpdateSnapshotMaxSizeInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateSnapshotMaxSizeInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateSnapshotMaxSizeInputClient) ById(id string) (*UpdateSnapshotMaxSizeInput, error) {
	resp := &UpdateSnapshotMaxSizeInput{}
	err := c.rancherClient.doById(UPDATE_SNAPSHOT_MAX_SIZE_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateSnapshotMaxSizeInputClient) Delete(container *UpdateSnapshotMaxSizeInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_SNAPSHOT_MAX_SIZE_INPUT_TYPE, &container.Resource)
}
