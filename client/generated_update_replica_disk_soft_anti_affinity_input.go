package client

const (
	UPDATE_REPLICA_DISK_SOFT_ANTI_AFFINITY_INPUT_TYPE = "UpdateReplicaDiskSoftAntiAffinityInput"
)

type UpdateReplicaDiskSoftAntiAffinityInput struct {
	Resource `yaml:"-"`

	ReplicaDiskSoftAntiAffinity string `json:"replicaDiskSoftAntiAffinity,omitempty" yaml:"replica_disk_soft_anti_affinity,omitempty"`
}

type UpdateReplicaDiskSoftAntiAffinityInputCollection struct {
	Collection
	Data   []UpdateReplicaDiskSoftAntiAffinityInput `json:"data,omitempty"`
	client *UpdateReplicaDiskSoftAntiAffinityInputClient
}

type UpdateReplicaDiskSoftAntiAffinityInputClient struct {
	rancherClient *RancherClient
}

type UpdateReplicaDiskSoftAntiAffinityInputOperations interface {
	List(opts *ListOpts) (*UpdateReplicaDiskSoftAntiAffinityInputCollection, error)
	Create(opts *UpdateReplicaDiskSoftAntiAffinityInput) (*UpdateReplicaDiskSoftAntiAffinityInput, error)
	Update(existing *UpdateReplicaDiskSoftAntiAffinityInput, updates interface{}) (*UpdateReplicaDiskSoftAntiAffinityInput, error)
	ById(id string) (*UpdateReplicaDiskSoftAntiAffinityInput, error)
	Delete(container *UpdateReplicaDiskSoftAntiAffinityInput) error
}

func newUpdateReplicaDiskSoftAntiAffinityInputClient(rancherClient *RancherClient) *UpdateReplicaDiskSoftAntiAffinityInputClient {
	return &UpdateReplicaDiskSoftAntiAffinityInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateReplicaDiskSoftAntiAffinityInputClient) Create(container *UpdateReplicaDiskSoftAntiAffinityInput) (*UpdateReplicaDiskSoftAntiAffinityInput, error) {
	resp := &UpdateReplicaDiskSoftAntiAffinityInput{}
	err := c.rancherClient.doCreate(UPDATE_REPLICA_DISK_SOFT_ANTI_AFFINITY_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateReplicaDiskSoftAntiAffinityInputClient) Update(existing *UpdateReplicaDiskSoftAntiAffinityInput, updates interface{}) (*UpdateReplicaDiskSoftAntiAffinityInput, error) {
	resp := &UpdateReplicaDiskSoftAntiAffinityInput{}
	err := c.rancherClient.doUpdate(UPDATE_REPLICA_DISK_SOFT_ANTI_AFFINITY_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateReplicaDiskSoftAntiAffinityInputClient) List(opts *ListOpts) (*UpdateReplicaDiskSoftAntiAffinityInputCollection, error) {
	resp := &UpdateReplicaDiskSoftAntiAffinityInputCollection{}
	err := c.rancherClient.doList(UPDATE_REPLICA_DISK_SOFT_ANTI_AFFINITY_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateReplicaDiskSoftAntiAffinityInputCollection) Next() (*UpdateReplicaDiskSoftAntiAffinityInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateReplicaDiskSoftAntiAffinityInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateReplicaDiskSoftAntiAffinityInputClient) ById(id string) (*UpdateReplicaDiskSoftAntiAffinityInput, error) {
	resp := &UpdateReplicaDiskSoftAntiAffinityInput{}
	err := c.rancherClient.doById(UPDATE_REPLICA_DISK_SOFT_ANTI_AFFINITY_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateReplicaDiskSoftAntiAffinityInputClient) Delete(container *UpdateReplicaDiskSoftAntiAffinityInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_REPLICA_DISK_SOFT_ANTI_AFFINITY_INPUT_TYPE, &container.Resource)
}
