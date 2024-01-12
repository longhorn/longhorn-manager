package client

const (
	UPDATE_SNAPSHOT_MAX_COUNT_INPUT_TYPE = "UpdateSnapshotMaxCountInput"
)

type UpdateSnapshotMaxCountInput struct {
	Resource `yaml:"-"`

	SnapshotMaxCount int64 `json:"snapshotMaxCount,omitempty" yaml:"snapshot_max_count,omitempty"`
}

type UpdateSnapshotMaxCountInputCollection struct {
	Collection
	Data   []UpdateSnapshotMaxCountInput `json:"data,omitempty"`
	client *UpdateSnapshotMaxCountInputClient
}

type UpdateSnapshotMaxCountInputClient struct {
	rancherClient *RancherClient
}

type UpdateSnapshotMaxCountInputOperations interface {
	List(opts *ListOpts) (*UpdateSnapshotMaxCountInputCollection, error)
	Create(opts *UpdateSnapshotMaxCountInput) (*UpdateSnapshotMaxCountInput, error)
	Update(existing *UpdateSnapshotMaxCountInput, updates interface{}) (*UpdateSnapshotMaxCountInput, error)
	ById(id string) (*UpdateSnapshotMaxCountInput, error)
	Delete(container *UpdateSnapshotMaxCountInput) error
}

func newUpdateSnapshotMaxCountInputClient(rancherClient *RancherClient) *UpdateSnapshotMaxCountInputClient {
	return &UpdateSnapshotMaxCountInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateSnapshotMaxCountInputClient) Create(container *UpdateSnapshotMaxCountInput) (*UpdateSnapshotMaxCountInput, error) {
	resp := &UpdateSnapshotMaxCountInput{}
	err := c.rancherClient.doCreate(UPDATE_SNAPSHOT_MAX_COUNT_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateSnapshotMaxCountInputClient) Update(existing *UpdateSnapshotMaxCountInput, updates interface{}) (*UpdateSnapshotMaxCountInput, error) {
	resp := &UpdateSnapshotMaxCountInput{}
	err := c.rancherClient.doUpdate(UPDATE_SNAPSHOT_MAX_COUNT_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateSnapshotMaxCountInputClient) List(opts *ListOpts) (*UpdateSnapshotMaxCountInputCollection, error) {
	resp := &UpdateSnapshotMaxCountInputCollection{}
	err := c.rancherClient.doList(UPDATE_SNAPSHOT_MAX_COUNT_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateSnapshotMaxCountInputCollection) Next() (*UpdateSnapshotMaxCountInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateSnapshotMaxCountInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateSnapshotMaxCountInputClient) ById(id string) (*UpdateSnapshotMaxCountInput, error) {
	resp := &UpdateSnapshotMaxCountInput{}
	err := c.rancherClient.doById(UPDATE_SNAPSHOT_MAX_COUNT_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateSnapshotMaxCountInputClient) Delete(container *UpdateSnapshotMaxCountInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_SNAPSHOT_MAX_COUNT_INPUT_TYPE, &container.Resource)
}
