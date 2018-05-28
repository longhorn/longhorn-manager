package client

const (
	HOST_TYPE = "host"
)

type Host struct {
	Resource `yaml:"-"`

	Address string `json:"address,omitempty" yaml:"address,omitempty"`

	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	Uuid string `json:"uuid,omitempty" yaml:"uuid,omitempty"`
}

type HostCollection struct {
	Collection
	Data   []Host `json:"data,omitempty"`
	client *HostClient
}

type HostClient struct {
	rancherClient *RancherClient
}

type HostOperations interface {
	List(opts *ListOpts) (*HostCollection, error)
	Create(opts *Host) (*Host, error)
	Update(existing *Host, updates interface{}) (*Host, error)
	ById(id string) (*Host, error)
	Delete(container *Host) error
}

func newHostClient(rancherClient *RancherClient) *HostClient {
	return &HostClient{
		rancherClient: rancherClient,
	}
}

func (c *HostClient) Create(container *Host) (*Host, error) {
	resp := &Host{}
	err := c.rancherClient.doCreate(HOST_TYPE, container, resp)
	return resp, err
}

func (c *HostClient) Update(existing *Host, updates interface{}) (*Host, error) {
	resp := &Host{}
	err := c.rancherClient.doUpdate(HOST_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *HostClient) List(opts *ListOpts) (*HostCollection, error) {
	resp := &HostCollection{}
	err := c.rancherClient.doList(HOST_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *HostCollection) Next() (*HostCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &HostCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *HostClient) ById(id string) (*Host, error) {
	resp := &Host{}
	err := c.rancherClient.doById(HOST_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *HostClient) Delete(container *Host) error {
	return c.rancherClient.doResourceDelete(HOST_TYPE, &container.Resource)
}
