package client

const (
	VOLUME_TOPOLOGY_TERM_TYPE = "volumeTopologyTerm"
)

type VolumeTopologyTerm struct {
	Resource `yaml:"-"`

	Region string `json:"region,omitempty" yaml:"region,omitempty"`

	Zone string `json:"zone,omitempty" yaml:"zone,omitempty"`
}

type VolumeTopologyTermCollection struct {
	Collection
	Data   []VolumeTopologyTerm `json:"data,omitempty"`
	client *VolumeTopologyTermClient
}

type VolumeTopologyTermClient struct {
	rancherClient *RancherClient
}

type VolumeTopologyTermOperations interface {
	List(opts *ListOpts) (*VolumeTopologyTermCollection, error)
	Create(opts *VolumeTopologyTerm) (*VolumeTopologyTerm, error)
	Update(existing *VolumeTopologyTerm, updates interface{}) (*VolumeTopologyTerm, error)
	ById(id string) (*VolumeTopologyTerm, error)
	Delete(container *VolumeTopologyTerm) error
}

func newVolumeTopologyTermClient(rancherClient *RancherClient) *VolumeTopologyTermClient {
	return &VolumeTopologyTermClient{
		rancherClient: rancherClient,
	}
}

func (c *VolumeTopologyTermClient) Create(container *VolumeTopologyTerm) (*VolumeTopologyTerm, error) {
	resp := &VolumeTopologyTerm{}
	err := c.rancherClient.doCreate(VOLUME_TOPOLOGY_TERM_TYPE, container, resp)
	return resp, err
}

func (c *VolumeTopologyTermClient) Update(existing *VolumeTopologyTerm, updates interface{}) (*VolumeTopologyTerm, error) {
	resp := &VolumeTopologyTerm{}
	err := c.rancherClient.doUpdate(VOLUME_TOPOLOGY_TERM_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *VolumeTopologyTermClient) List(opts *ListOpts) (*VolumeTopologyTermCollection, error) {
	resp := &VolumeTopologyTermCollection{}
	err := c.rancherClient.doList(VOLUME_TOPOLOGY_TERM_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *VolumeTopologyTermCollection) Next() (*VolumeTopologyTermCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &VolumeTopologyTermCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *VolumeTopologyTermClient) ById(id string) (*VolumeTopologyTerm, error) {
	resp := &VolumeTopologyTerm{}
	err := c.rancherClient.doById(VOLUME_TOPOLOGY_TERM_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *VolumeTopologyTermClient) Delete(container *VolumeTopologyTerm) error {
	return c.rancherClient.doResourceDelete(VOLUME_TOPOLOGY_TERM_TYPE, &container.Resource)
}
