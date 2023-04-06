package client

const (
	SHARE_MANAGER_TYPE = "shareManager"
)

type ShareManager struct {
	Resource `yaml:"-"`

	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`

	Image string `json:"image,omitempty" yaml:"image,omitempty"`

	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	OwnerID string `json:"ownerID,omitempty" yaml:"owner_id,omitempty"`

	State string `json:"state,omitempty" yaml:"state,omitempty"`
}

type ShareManagerCollection struct {
	Collection
	Data   []ShareManager `json:"data,omitempty"`
	client *ShareManagerClient
}

type ShareManagerClient struct {
	rancherClient *RancherClient
}

type ShareManagerOperations interface {
	List(opts *ListOpts) (*ShareManagerCollection, error)
	Create(opts *ShareManager) (*ShareManager, error)
	Update(existing *ShareManager, updates interface{}) (*ShareManager, error)
	ById(id string) (*ShareManager, error)
	Delete(container *ShareManager) error
}

func newShareManagerClient(rancherClient *RancherClient) *ShareManagerClient {
	return &ShareManagerClient{
		rancherClient: rancherClient,
	}
}

func (c *ShareManagerClient) Create(container *ShareManager) (*ShareManager, error) {
	resp := &ShareManager{}
	err := c.rancherClient.doCreate(SHARE_MANAGER_TYPE, container, resp)
	return resp, err
}

func (c *ShareManagerClient) Update(existing *ShareManager, updates interface{}) (*ShareManager, error) {
	resp := &ShareManager{}
	err := c.rancherClient.doUpdate(SHARE_MANAGER_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *ShareManagerClient) List(opts *ListOpts) (*ShareManagerCollection, error) {
	resp := &ShareManagerCollection{}
	err := c.rancherClient.doList(SHARE_MANAGER_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *ShareManagerCollection) Next() (*ShareManagerCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &ShareManagerCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *ShareManagerClient) ById(id string) (*ShareManager, error) {
	resp := &ShareManager{}
	err := c.rancherClient.doById(SHARE_MANAGER_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *ShareManagerClient) Delete(container *ShareManager) error {
	return c.rancherClient.doResourceDelete(SHARE_MANAGER_TYPE, &container.Resource)
}
