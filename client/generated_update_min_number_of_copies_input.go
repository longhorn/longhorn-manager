package client

const (
	UPDATE_MIN_NUMBER_OF_COPIES_INPUT_TYPE = "updateMinNumberOfCopiesInput"
)

type UpdateMinNumberOfCopiesInput struct {
	Resource `yaml:"-"`

	MinNumberOfCopies int64 `json:"minNumberOfCopies,omitempty" yaml:"min_number_of_copies,omitempty"`
}

type UpdateMinNumberOfCopiesInputCollection struct {
	Collection
	Data   []UpdateMinNumberOfCopiesInput `json:"data,omitempty"`
	client *UpdateMinNumberOfCopiesInputClient
}

type UpdateMinNumberOfCopiesInputClient struct {
	rancherClient *RancherClient
}

type UpdateMinNumberOfCopiesInputOperations interface {
	List(opts *ListOpts) (*UpdateMinNumberOfCopiesInputCollection, error)
	Create(opts *UpdateMinNumberOfCopiesInput) (*UpdateMinNumberOfCopiesInput, error)
	Update(existing *UpdateMinNumberOfCopiesInput, updates interface{}) (*UpdateMinNumberOfCopiesInput, error)
	ById(id string) (*UpdateMinNumberOfCopiesInput, error)
	Delete(container *UpdateMinNumberOfCopiesInput) error
}

func newUpdateMinNumberOfCopiesInputClient(rancherClient *RancherClient) *UpdateMinNumberOfCopiesInputClient {
	return &UpdateMinNumberOfCopiesInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateMinNumberOfCopiesInputClient) Create(container *UpdateMinNumberOfCopiesInput) (*UpdateMinNumberOfCopiesInput, error) {
	resp := &UpdateMinNumberOfCopiesInput{}
	err := c.rancherClient.doCreate(UPDATE_MIN_NUMBER_OF_COPIES_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateMinNumberOfCopiesInputClient) Update(existing *UpdateMinNumberOfCopiesInput, updates interface{}) (*UpdateMinNumberOfCopiesInput, error) {
	resp := &UpdateMinNumberOfCopiesInput{}
	err := c.rancherClient.doUpdate(UPDATE_MIN_NUMBER_OF_COPIES_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateMinNumberOfCopiesInputClient) List(opts *ListOpts) (*UpdateMinNumberOfCopiesInputCollection, error) {
	resp := &UpdateMinNumberOfCopiesInputCollection{}
	err := c.rancherClient.doList(UPDATE_MIN_NUMBER_OF_COPIES_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateMinNumberOfCopiesInputCollection) Next() (*UpdateMinNumberOfCopiesInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateMinNumberOfCopiesInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateMinNumberOfCopiesInputClient) ById(id string) (*UpdateMinNumberOfCopiesInput, error) {
	resp := &UpdateMinNumberOfCopiesInput{}
	err := c.rancherClient.doById(UPDATE_MIN_NUMBER_OF_COPIES_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateMinNumberOfCopiesInputClient) Delete(container *UpdateMinNumberOfCopiesInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_MIN_NUMBER_OF_COPIES_INPUT_TYPE, &container.Resource)
}
