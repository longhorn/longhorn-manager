package client

const (
	UPDATE_REBUILD_CONCURRENT_SYNC_LIMIT_INPUT_TYPE = "UpdateRebuildConcurrentSyncLimitInput"
)

type UpdateRebuildConcurrentSyncLimitInput struct {
	Resource `yaml:"-"`

	RebuildConcurrentSyncLimit int64 `json:"rebuildConcurrentSyncLimit,omitempty" yaml:"rebuild_concurrent_sync_limit,omitempty"`
}

type UpdateRebuildConcurrentSyncLimitInputCollection struct {
	Collection
	Data   []UpdateRebuildConcurrentSyncLimitInput `json:"data,omitempty"`
	client *UpdateRebuildConcurrentSyncLimitInputClient
}

type UpdateRebuildConcurrentSyncLimitInputClient struct {
	rancherClient *RancherClient
}

type UpdateRebuildConcurrentSyncLimitInputOperations interface {
	List(opts *ListOpts) (*UpdateRebuildConcurrentSyncLimitInputCollection, error)
	Create(opts *UpdateRebuildConcurrentSyncLimitInput) (*UpdateRebuildConcurrentSyncLimitInput, error)
	Update(existing *UpdateRebuildConcurrentSyncLimitInput, updates interface{}) (*UpdateRebuildConcurrentSyncLimitInput, error)
	ById(id string) (*UpdateRebuildConcurrentSyncLimitInput, error)
	Delete(container *UpdateRebuildConcurrentSyncLimitInput) error
}

func newUpdateRebuildConcurrentSyncLimitInputClient(rancherClient *RancherClient) *UpdateRebuildConcurrentSyncLimitInputClient {
	return &UpdateRebuildConcurrentSyncLimitInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateRebuildConcurrentSyncLimitInputClient) Create(container *UpdateRebuildConcurrentSyncLimitInput) (*UpdateRebuildConcurrentSyncLimitInput, error) {
	resp := &UpdateRebuildConcurrentSyncLimitInput{}
	err := c.rancherClient.doCreate(UPDATE_REBUILD_CONCURRENT_SYNC_LIMIT_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateRebuildConcurrentSyncLimitInputClient) Update(existing *UpdateRebuildConcurrentSyncLimitInput, updates interface{}) (*UpdateRebuildConcurrentSyncLimitInput, error) {
	resp := &UpdateRebuildConcurrentSyncLimitInput{}
	err := c.rancherClient.doUpdate(UPDATE_REBUILD_CONCURRENT_SYNC_LIMIT_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateRebuildConcurrentSyncLimitInputClient) List(opts *ListOpts) (*UpdateRebuildConcurrentSyncLimitInputCollection, error) {
	resp := &UpdateRebuildConcurrentSyncLimitInputCollection{}
	err := c.rancherClient.doList(UPDATE_REBUILD_CONCURRENT_SYNC_LIMIT_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateRebuildConcurrentSyncLimitInputCollection) Next() (*UpdateRebuildConcurrentSyncLimitInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateRebuildConcurrentSyncLimitInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateRebuildConcurrentSyncLimitInputClient) ById(id string) (*UpdateRebuildConcurrentSyncLimitInput, error) {
	resp := &UpdateRebuildConcurrentSyncLimitInput{}
	err := c.rancherClient.doById(UPDATE_REBUILD_CONCURRENT_SYNC_LIMIT_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateRebuildConcurrentSyncLimitInputClient) Delete(container *UpdateRebuildConcurrentSyncLimitInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_REBUILD_CONCURRENT_SYNC_LIMIT_INPUT_TYPE, &container.Resource)
}
