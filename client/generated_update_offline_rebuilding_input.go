package client

const (
	UPDATE_OFFLINE_REBUILDING_INPUT_TYPE = "UpdateOfflineRebuildingInput"
)

type UpdateOfflineRebuildingInput struct {
	Resource `yaml:"-"`

	OfflineRebuilding string `json:"offlineRebuilding,omitempty" yaml:"offline_rebuilding,omitempty"`
}

type UpdateOfflineRebuildingInputCollection struct {
	Collection
	Data   []UpdateOfflineRebuildingInput `json:"data,omitempty"`
	client *UpdateOfflineRebuildingInputClient
}

type UpdateOfflineRebuildingInputClient struct {
	rancherClient *RancherClient
}

type UpdateOfflineRebuildingInputOperations interface {
	List(opts *ListOpts) (*UpdateOfflineRebuildingInputCollection, error)
	Create(opts *UpdateOfflineRebuildingInput) (*UpdateOfflineRebuildingInput, error)
	Update(existing *UpdateOfflineRebuildingInput, updates interface{}) (*UpdateOfflineRebuildingInput, error)
	ById(id string) (*UpdateOfflineRebuildingInput, error)
	Delete(container *UpdateOfflineRebuildingInput) error
}

func newUpdateOfflineRebuildingInputClient(rancherClient *RancherClient) *UpdateOfflineRebuildingInputClient {
	return &UpdateOfflineRebuildingInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateOfflineRebuildingInputClient) Create(container *UpdateOfflineRebuildingInput) (*UpdateOfflineRebuildingInput, error) {
	resp := &UpdateOfflineRebuildingInput{}
	err := c.rancherClient.doCreate(UPDATE_OFFLINE_REBUILDING_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateOfflineRebuildingInputClient) Update(existing *UpdateOfflineRebuildingInput, updates interface{}) (*UpdateOfflineRebuildingInput, error) {
	resp := &UpdateOfflineRebuildingInput{}
	err := c.rancherClient.doUpdate(UPDATE_OFFLINE_REBUILDING_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateOfflineRebuildingInputClient) List(opts *ListOpts) (*UpdateOfflineRebuildingInputCollection, error) {
	resp := &UpdateOfflineRebuildingInputCollection{}
	err := c.rancherClient.doList(UPDATE_OFFLINE_REBUILDING_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateOfflineRebuildingInputCollection) Next() (*UpdateOfflineRebuildingInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateOfflineRebuildingInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateOfflineRebuildingInputClient) ById(id string) (*UpdateOfflineRebuildingInput, error) {
	resp := &UpdateOfflineRebuildingInput{}
	err := c.rancherClient.doById(UPDATE_OFFLINE_REBUILDING_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateOfflineRebuildingInputClient) Delete(container *UpdateOfflineRebuildingInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_OFFLINE_REBUILDING_INPUT_TYPE, &container.Resource)
}
