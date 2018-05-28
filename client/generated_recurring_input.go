package client

const (
	RECURRING_INPUT_TYPE = "recurringInput"
)

type RecurringInput struct {
	Resource `yaml:"-"`

	Jobs []RecurringJob `json:"jobs,omitempty" yaml:"jobs,omitempty"`
}

type RecurringInputCollection struct {
	Collection
	Data   []RecurringInput `json:"data,omitempty"`
	client *RecurringInputClient
}

type RecurringInputClient struct {
	rancherClient *RancherClient
}

type RecurringInputOperations interface {
	List(opts *ListOpts) (*RecurringInputCollection, error)
	Create(opts *RecurringInput) (*RecurringInput, error)
	Update(existing *RecurringInput, updates interface{}) (*RecurringInput, error)
	ById(id string) (*RecurringInput, error)
	Delete(container *RecurringInput) error
}

func newRecurringInputClient(rancherClient *RancherClient) *RecurringInputClient {
	return &RecurringInputClient{
		rancherClient: rancherClient,
	}
}

func (c *RecurringInputClient) Create(container *RecurringInput) (*RecurringInput, error) {
	resp := &RecurringInput{}
	err := c.rancherClient.doCreate(RECURRING_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *RecurringInputClient) Update(existing *RecurringInput, updates interface{}) (*RecurringInput, error) {
	resp := &RecurringInput{}
	err := c.rancherClient.doUpdate(RECURRING_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *RecurringInputClient) List(opts *ListOpts) (*RecurringInputCollection, error) {
	resp := &RecurringInputCollection{}
	err := c.rancherClient.doList(RECURRING_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *RecurringInputCollection) Next() (*RecurringInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &RecurringInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *RecurringInputClient) ById(id string) (*RecurringInput, error) {
	resp := &RecurringInput{}
	err := c.rancherClient.doById(RECURRING_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *RecurringInputClient) Delete(container *RecurringInput) error {
	return c.rancherClient.doResourceDelete(RECURRING_INPUT_TYPE, &container.Resource)
}
