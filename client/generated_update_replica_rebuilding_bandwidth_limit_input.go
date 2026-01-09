package client

const (
	UPDATE_REPLICA_REBUILDING_BANDWIDTH_LIMIT_INPUT_TYPE = "UpdateReplicaRebuildingBandwidthLimitInput"
)

type UpdateReplicaRebuildingBandwidthLimitInput struct {
	Resource `yaml:"-"`

	ReplicaRebuildingBandwidthLimit string `json:"replicaRebuildingBandwidthLimit,omitempty" yaml:"replica_rebuilding_bandwidth_limit,omitempty"`
}

type UpdateReplicaRebuildingBandwidthLimitInputCollection struct {
	Collection
	Data   []UpdateReplicaRebuildingBandwidthLimitInput `json:"data,omitempty"`
	client *UpdateReplicaRebuildingBandwidthLimitInputClient
}

type UpdateReplicaRebuildingBandwidthLimitInputClient struct {
	rancherClient *RancherClient
}

type UpdateReplicaRebuildingBandwidthLimitInputOperations interface {
	List(opts *ListOpts) (*UpdateReplicaRebuildingBandwidthLimitInputCollection, error)
	Create(opts *UpdateReplicaRebuildingBandwidthLimitInput) (*UpdateReplicaRebuildingBandwidthLimitInput, error)
	Update(existing *UpdateReplicaRebuildingBandwidthLimitInput, updates interface{}) (*UpdateReplicaRebuildingBandwidthLimitInput, error)
	ById(id string) (*UpdateReplicaRebuildingBandwidthLimitInput, error)
	Delete(container *UpdateReplicaRebuildingBandwidthLimitInput) error
}

func newUpdateReplicaRebuildingBandwidthLimitInputClient(rancherClient *RancherClient) *UpdateReplicaRebuildingBandwidthLimitInputClient {
	return &UpdateReplicaRebuildingBandwidthLimitInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateReplicaRebuildingBandwidthLimitInputClient) Create(container *UpdateReplicaRebuildingBandwidthLimitInput) (*UpdateReplicaRebuildingBandwidthLimitInput, error) {
	resp := &UpdateReplicaRebuildingBandwidthLimitInput{}
	err := c.rancherClient.doCreate(UPDATE_REPLICA_REBUILDING_BANDWIDTH_LIMIT_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateReplicaRebuildingBandwidthLimitInputClient) Update(existing *UpdateReplicaRebuildingBandwidthLimitInput, updates interface{}) (*UpdateReplicaRebuildingBandwidthLimitInput, error) {
	resp := &UpdateReplicaRebuildingBandwidthLimitInput{}
	err := c.rancherClient.doUpdate(UPDATE_REPLICA_REBUILDING_BANDWIDTH_LIMIT_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateReplicaRebuildingBandwidthLimitInputClient) List(opts *ListOpts) (*UpdateReplicaRebuildingBandwidthLimitInputCollection, error) {
	resp := &UpdateReplicaRebuildingBandwidthLimitInputCollection{}
	err := c.rancherClient.doList(UPDATE_REPLICA_REBUILDING_BANDWIDTH_LIMIT_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateReplicaRebuildingBandwidthLimitInputCollection) Next() (*UpdateReplicaRebuildingBandwidthLimitInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateReplicaRebuildingBandwidthLimitInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateReplicaRebuildingBandwidthLimitInputClient) ById(id string) (*UpdateReplicaRebuildingBandwidthLimitInput, error) {
	resp := &UpdateReplicaRebuildingBandwidthLimitInput{}
	err := c.rancherClient.doById(UPDATE_REPLICA_REBUILDING_BANDWIDTH_LIMIT_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateReplicaRebuildingBandwidthLimitInputClient) Delete(container *UpdateReplicaRebuildingBandwidthLimitInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_REPLICA_REBUILDING_BANDWIDTH_LIMIT_INPUT_TYPE, &container.Resource)
}
