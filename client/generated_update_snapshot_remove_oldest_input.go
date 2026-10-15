package client

const (
	UPDATE_SNAPSHOT_REMOVE_OLDEST_INPUT_TYPE = "UpdateSnapshotRemoveOldestInput"
)

type UpdateSnapshotRemoveOldestInput struct {
	Resource `yaml:"-"`

	SnapshotRemoveOldest string `json:"snapshotRemoveOldest,omitempty" yaml:"snapshot_max_size,omitempty"`
}

type UpdateSnapshotRemoveOldestInputCollection struct {
	Collection
	Data   []UpdateSnapshotRemoveOldestInput `json:"data,omitempty"`
	client *UpdateSnapshotRemoveOldestInputClient
}

type UpdateSnapshotRemoveOldestInputClient struct {
	rancherClient *RancherClient
}

type UpdateSnapshotRemoveOldestInputOperations interface {
	List(opts *ListOpts) (*UpdateSnapshotRemoveOldestInputCollection, error)
	Create(opts *UpdateSnapshotRemoveOldestInput) (*UpdateSnapshotRemoveOldestInput, error)
	Update(existing *UpdateSnapshotRemoveOldestInput, updates interface{}) (*UpdateSnapshotRemoveOldestInput, error)
	ById(id string) (*UpdateSnapshotRemoveOldestInput, error)
	Delete(container *UpdateSnapshotRemoveOldestInput) error
}

func newUpdateSnapshotRemoveOldestInputClient(rancherClient *RancherClient) *UpdateSnapshotRemoveOldestInputClient {
	return &UpdateSnapshotRemoveOldestInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateSnapshotRemoveOldestInputClient) Create(container *UpdateSnapshotRemoveOldestInput) (*UpdateSnapshotRemoveOldestInput, error) {
	resp := &UpdateSnapshotRemoveOldestInput{}
	err := c.rancherClient.doCreate(UPDATE_SNAPSHOT_REMOVE_OLDEST_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateSnapshotRemoveOldestInputClient) Update(existing *UpdateSnapshotRemoveOldestInput, updates interface{}) (*UpdateSnapshotRemoveOldestInput, error) {
	resp := &UpdateSnapshotRemoveOldestInput{}
	err := c.rancherClient.doUpdate(UPDATE_SNAPSHOT_REMOVE_OLDEST_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateSnapshotRemoveOldestInputClient) List(opts *ListOpts) (*UpdateSnapshotRemoveOldestInputCollection, error) {
	resp := &UpdateSnapshotRemoveOldestInputCollection{}
	err := c.rancherClient.doList(UPDATE_SNAPSHOT_REMOVE_OLDEST_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateSnapshotRemoveOldestInputCollection) Next() (*UpdateSnapshotRemoveOldestInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateSnapshotRemoveOldestInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateSnapshotRemoveOldestInputClient) ById(id string) (*UpdateSnapshotRemoveOldestInput, error) {
	resp := &UpdateSnapshotRemoveOldestInput{}
	err := c.rancherClient.doById(UPDATE_SNAPSHOT_REMOVE_OLDEST_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateSnapshotRemoveOldestInputClient) Delete(container *UpdateSnapshotRemoveOldestInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_SNAPSHOT_REMOVE_OLDEST_INPUT_TYPE, &container.Resource)
}
