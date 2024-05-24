package client

const (
	UPDATE_FREEZE_FSFOR_SNAPSHOT_INPUT_TYPE = "UpdateFreezeFSForSnapshotInput"
)

type UpdateFreezeFSForSnapshotInput struct {
	Resource `yaml:"-"`

	FreezeFSForSnapshot string `json:"freezeFSForSnapshot,omitempty" yaml:"freeze_fsfor_snapshot,omitempty"`
}

type UpdateFreezeFSForSnapshotInputCollection struct {
	Collection
	Data   []UpdateFreezeFSForSnapshotInput `json:"data,omitempty"`
	client *UpdateFreezeFSForSnapshotInputClient
}

type UpdateFreezeFSForSnapshotInputClient struct {
	rancherClient *RancherClient
}

type UpdateFreezeFSForSnapshotInputOperations interface {
	List(opts *ListOpts) (*UpdateFreezeFSForSnapshotInputCollection, error)
	Create(opts *UpdateFreezeFSForSnapshotInput) (*UpdateFreezeFSForSnapshotInput, error)
	Update(existing *UpdateFreezeFSForSnapshotInput, updates interface{}) (*UpdateFreezeFSForSnapshotInput, error)
	ById(id string) (*UpdateFreezeFSForSnapshotInput, error)
	Delete(container *UpdateFreezeFSForSnapshotInput) error
}

func newUpdateFreezeFSForSnapshotInputClient(rancherClient *RancherClient) *UpdateFreezeFSForSnapshotInputClient {
	return &UpdateFreezeFSForSnapshotInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateFreezeFSForSnapshotInputClient) Create(container *UpdateFreezeFSForSnapshotInput) (*UpdateFreezeFSForSnapshotInput, error) {
	resp := &UpdateFreezeFSForSnapshotInput{}
	err := c.rancherClient.doCreate(UPDATE_FREEZE_FSFOR_SNAPSHOT_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateFreezeFSForSnapshotInputClient) Update(existing *UpdateFreezeFSForSnapshotInput, updates interface{}) (*UpdateFreezeFSForSnapshotInput, error) {
	resp := &UpdateFreezeFSForSnapshotInput{}
	err := c.rancherClient.doUpdate(UPDATE_FREEZE_FSFOR_SNAPSHOT_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateFreezeFSForSnapshotInputClient) List(opts *ListOpts) (*UpdateFreezeFSForSnapshotInputCollection, error) {
	resp := &UpdateFreezeFSForSnapshotInputCollection{}
	err := c.rancherClient.doList(UPDATE_FREEZE_FSFOR_SNAPSHOT_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateFreezeFSForSnapshotInputCollection) Next() (*UpdateFreezeFSForSnapshotInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateFreezeFSForSnapshotInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateFreezeFSForSnapshotInputClient) ById(id string) (*UpdateFreezeFSForSnapshotInput, error) {
	resp := &UpdateFreezeFSForSnapshotInput{}
	err := c.rancherClient.doById(UPDATE_FREEZE_FSFOR_SNAPSHOT_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateFreezeFSForSnapshotInputClient) Delete(container *UpdateFreezeFSForSnapshotInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_FREEZE_FSFOR_SNAPSHOT_INPUT_TYPE, &container.Resource)
}
