package client

const (
	UPDATE_REPLICA_SCHEDULING_SKIP_UNHEALTHY_DISK_INPUT_TYPE = "UpdateReplicaSchedulingSkipUnhealthyDiskInput"
)

type UpdateReplicaSchedulingSkipUnhealthyDiskInput struct {
	Resource `yaml:"-"`

	ReplicaSchedulingSkipUnhealthyDisk string `json:"replicaSchedulingSkipUnhealthyDisk,omitempty" yaml:"replica_scheduling_skip_unhealthy_disk,omitempty"`
}

type UpdateReplicaSchedulingSkipUnhealthyDiskInputCollection struct {
	Collection
	Data   []UpdateReplicaSchedulingSkipUnhealthyDiskInput `json:"data,omitempty"`
	client *UpdateReplicaSchedulingSkipUnhealthyDiskInputClient
}

type UpdateReplicaSchedulingSkipUnhealthyDiskInputClient struct {
	rancherClient *RancherClient
}

type UpdateReplicaSchedulingSkipUnhealthyDiskInputOperations interface {
	List(opts *ListOpts) (*UpdateReplicaSchedulingSkipUnhealthyDiskInputCollection, error)
	Create(opts *UpdateReplicaSchedulingSkipUnhealthyDiskInput) (*UpdateReplicaSchedulingSkipUnhealthyDiskInput, error)
	Update(existing *UpdateReplicaSchedulingSkipUnhealthyDiskInput, updates interface{}) (*UpdateReplicaSchedulingSkipUnhealthyDiskInput, error)
	ById(id string) (*UpdateReplicaSchedulingSkipUnhealthyDiskInput, error)
	Delete(container *UpdateReplicaSchedulingSkipUnhealthyDiskInput) error
}

func newUpdateReplicaSchedulingSkipUnhealthyDiskInputClient(rancherClient *RancherClient) *UpdateReplicaSchedulingSkipUnhealthyDiskInputClient {
	return &UpdateReplicaSchedulingSkipUnhealthyDiskInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateReplicaSchedulingSkipUnhealthyDiskInputClient) Create(container *UpdateReplicaSchedulingSkipUnhealthyDiskInput) (*UpdateReplicaSchedulingSkipUnhealthyDiskInput, error) {
	resp := &UpdateReplicaSchedulingSkipUnhealthyDiskInput{}
	err := c.rancherClient.doCreate(UPDATE_REPLICA_SCHEDULING_SKIP_UNHEALTHY_DISK_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateReplicaSchedulingSkipUnhealthyDiskInputClient) Update(existing *UpdateReplicaSchedulingSkipUnhealthyDiskInput, updates interface{}) (*UpdateReplicaSchedulingSkipUnhealthyDiskInput, error) {
	resp := &UpdateReplicaSchedulingSkipUnhealthyDiskInput{}
	err := c.rancherClient.doUpdate(UPDATE_REPLICA_SCHEDULING_SKIP_UNHEALTHY_DISK_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateReplicaSchedulingSkipUnhealthyDiskInputClient) List(opts *ListOpts) (*UpdateReplicaSchedulingSkipUnhealthyDiskInputCollection, error) {
	resp := &UpdateReplicaSchedulingSkipUnhealthyDiskInputCollection{}
	err := c.rancherClient.doList(UPDATE_REPLICA_SCHEDULING_SKIP_UNHEALTHY_DISK_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateReplicaSchedulingSkipUnhealthyDiskInputCollection) Next() (*UpdateReplicaSchedulingSkipUnhealthyDiskInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateReplicaSchedulingSkipUnhealthyDiskInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateReplicaSchedulingSkipUnhealthyDiskInputClient) ById(id string) (*UpdateReplicaSchedulingSkipUnhealthyDiskInput, error) {
	resp := &UpdateReplicaSchedulingSkipUnhealthyDiskInput{}
	err := c.rancherClient.doById(UPDATE_REPLICA_SCHEDULING_SKIP_UNHEALTHY_DISK_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateReplicaSchedulingSkipUnhealthyDiskInputClient) Delete(container *UpdateReplicaSchedulingSkipUnhealthyDiskInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_REPLICA_SCHEDULING_SKIP_UNHEALTHY_DISK_INPUT_TYPE, &container.Resource)
}
