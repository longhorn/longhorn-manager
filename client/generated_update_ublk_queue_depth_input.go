package client

const (
	UPDATE_UBLK_QUEUE_DEPTH_INPUT_TYPE = "UpdateUblkQueueDepthInput"
)

type UpdateUblkQueueDepthInput struct {
	Resource `yaml:"-"`

	UblkQueueDepth int64 `json:"ublkQueueDepth,omitempty" yaml:"ublk_queue_depth,omitempty"`
}

type UpdateUblkQueueDepthInputCollection struct {
	Collection
	Data   []UpdateUblkQueueDepthInput `json:"data,omitempty"`
	client *UpdateUblkQueueDepthInputClient
}

type UpdateUblkQueueDepthInputClient struct {
	rancherClient *RancherClient
}

type UpdateUblkQueueDepthInputOperations interface {
	List(opts *ListOpts) (*UpdateUblkQueueDepthInputCollection, error)
	Create(opts *UpdateUblkQueueDepthInput) (*UpdateUblkQueueDepthInput, error)
	Update(existing *UpdateUblkQueueDepthInput, updates interface{}) (*UpdateUblkQueueDepthInput, error)
	ById(id string) (*UpdateUblkQueueDepthInput, error)
	Delete(container *UpdateUblkQueueDepthInput) error
}

func newUpdateUblkQueueDepthInputClient(rancherClient *RancherClient) *UpdateUblkQueueDepthInputClient {
	return &UpdateUblkQueueDepthInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateUblkQueueDepthInputClient) Create(container *UpdateUblkQueueDepthInput) (*UpdateUblkQueueDepthInput, error) {
	resp := &UpdateUblkQueueDepthInput{}
	err := c.rancherClient.doCreate(UPDATE_UBLK_QUEUE_DEPTH_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateUblkQueueDepthInputClient) Update(existing *UpdateUblkQueueDepthInput, updates interface{}) (*UpdateUblkQueueDepthInput, error) {
	resp := &UpdateUblkQueueDepthInput{}
	err := c.rancherClient.doUpdate(UPDATE_UBLK_QUEUE_DEPTH_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateUblkQueueDepthInputClient) List(opts *ListOpts) (*UpdateUblkQueueDepthInputCollection, error) {
	resp := &UpdateUblkQueueDepthInputCollection{}
	err := c.rancherClient.doList(UPDATE_UBLK_QUEUE_DEPTH_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateUblkQueueDepthInputCollection) Next() (*UpdateUblkQueueDepthInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateUblkQueueDepthInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateUblkQueueDepthInputClient) ById(id string) (*UpdateUblkQueueDepthInput, error) {
	resp := &UpdateUblkQueueDepthInput{}
	err := c.rancherClient.doById(UPDATE_UBLK_QUEUE_DEPTH_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateUblkQueueDepthInputClient) Delete(container *UpdateUblkQueueDepthInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_UBLK_QUEUE_DEPTH_INPUT_TYPE, &container.Resource)
}
