package client

const (
	UPDATE_UBLK_NUMBER_OF_QUEUE_INPUT_TYPE = "UpdateUblkNumberOfQueueInput"
)

type UpdateUblkNumberOfQueueInput struct {
	Resource `yaml:"-"`

	UblkNumberOfQueue int64 `json:"ublkNumberOfQueue,omitempty" yaml:"ublk_number_of_queue,omitempty"`
}

type UpdateUblkNumberOfQueueInputCollection struct {
	Collection
	Data   []UpdateUblkNumberOfQueueInput `json:"data,omitempty"`
	client *UpdateUblkNumberOfQueueInputClient
}

type UpdateUblkNumberOfQueueInputClient struct {
	rancherClient *RancherClient
}

type UpdateUblkNumberOfQueueInputOperations interface {
	List(opts *ListOpts) (*UpdateUblkNumberOfQueueInputCollection, error)
	Create(opts *UpdateUblkNumberOfQueueInput) (*UpdateUblkNumberOfQueueInput, error)
	Update(existing *UpdateUblkNumberOfQueueInput, updates interface{}) (*UpdateUblkNumberOfQueueInput, error)
	ById(id string) (*UpdateUblkNumberOfQueueInput, error)
	Delete(container *UpdateUblkNumberOfQueueInput) error
}

func newUpdateUblkNumberOfQueueInputClient(rancherClient *RancherClient) *UpdateUblkNumberOfQueueInputClient {
	return &UpdateUblkNumberOfQueueInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateUblkNumberOfQueueInputClient) Create(container *UpdateUblkNumberOfQueueInput) (*UpdateUblkNumberOfQueueInput, error) {
	resp := &UpdateUblkNumberOfQueueInput{}
	err := c.rancherClient.doCreate(UPDATE_UBLK_NUMBER_OF_QUEUE_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateUblkNumberOfQueueInputClient) Update(existing *UpdateUblkNumberOfQueueInput, updates interface{}) (*UpdateUblkNumberOfQueueInput, error) {
	resp := &UpdateUblkNumberOfQueueInput{}
	err := c.rancherClient.doUpdate(UPDATE_UBLK_NUMBER_OF_QUEUE_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateUblkNumberOfQueueInputClient) List(opts *ListOpts) (*UpdateUblkNumberOfQueueInputCollection, error) {
	resp := &UpdateUblkNumberOfQueueInputCollection{}
	err := c.rancherClient.doList(UPDATE_UBLK_NUMBER_OF_QUEUE_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateUblkNumberOfQueueInputCollection) Next() (*UpdateUblkNumberOfQueueInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateUblkNumberOfQueueInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateUblkNumberOfQueueInputClient) ById(id string) (*UpdateUblkNumberOfQueueInput, error) {
	resp := &UpdateUblkNumberOfQueueInput{}
	err := c.rancherClient.doById(UPDATE_UBLK_NUMBER_OF_QUEUE_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateUblkNumberOfQueueInputClient) Delete(container *UpdateUblkNumberOfQueueInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_UBLK_NUMBER_OF_QUEUE_INPUT_TYPE, &container.Resource)
}
