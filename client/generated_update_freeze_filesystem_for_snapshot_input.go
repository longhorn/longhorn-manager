package client

const (
	UPDATE_FREEZE_FILESYSTEM_FOR_SNAPSHOT_INPUT_TYPE = "UpdateFreezeFilesystemForSnapshotInput"
)

type UpdateFreezeFilesystemForSnapshotInput struct {
	Resource `yaml:"-"`

	FreezeFilesystemForSnapshot string `json:"freezeFilesystemForSnapshot,omitempty" yaml:"freeze_filesystem_for_snapshot,omitempty"`
}

type UpdateFreezeFilesystemForSnapshotInputCollection struct {
	Collection
	Data   []UpdateFreezeFilesystemForSnapshotInput `json:"data,omitempty"`
	client *UpdateFreezeFilesystemForSnapshotInputClient
}

type UpdateFreezeFilesystemForSnapshotInputClient struct {
	rancherClient *RancherClient
}

type UpdateFreezeFilesystemForSnapshotInputOperations interface {
	List(opts *ListOpts) (*UpdateFreezeFilesystemForSnapshotInputCollection, error)
	Create(opts *UpdateFreezeFilesystemForSnapshotInput) (*UpdateFreezeFilesystemForSnapshotInput, error)
	Update(existing *UpdateFreezeFilesystemForSnapshotInput, updates interface{}) (*UpdateFreezeFilesystemForSnapshotInput, error)
	ById(id string) (*UpdateFreezeFilesystemForSnapshotInput, error)
	Delete(container *UpdateFreezeFilesystemForSnapshotInput) error
}

func newUpdateFreezeFilesystemForSnapshotInputClient(rancherClient *RancherClient) *UpdateFreezeFilesystemForSnapshotInputClient {
	return &UpdateFreezeFilesystemForSnapshotInputClient{
		rancherClient: rancherClient,
	}
}

func (c *UpdateFreezeFilesystemForSnapshotInputClient) Create(container *UpdateFreezeFilesystemForSnapshotInput) (*UpdateFreezeFilesystemForSnapshotInput, error) {
	resp := &UpdateFreezeFilesystemForSnapshotInput{}
	err := c.rancherClient.doCreate(UPDATE_FREEZE_FILESYSTEM_FOR_SNAPSHOT_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *UpdateFreezeFilesystemForSnapshotInputClient) Update(existing *UpdateFreezeFilesystemForSnapshotInput, updates interface{}) (*UpdateFreezeFilesystemForSnapshotInput, error) {
	resp := &UpdateFreezeFilesystemForSnapshotInput{}
	err := c.rancherClient.doUpdate(UPDATE_FREEZE_FILESYSTEM_FOR_SNAPSHOT_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *UpdateFreezeFilesystemForSnapshotInputClient) List(opts *ListOpts) (*UpdateFreezeFilesystemForSnapshotInputCollection, error) {
	resp := &UpdateFreezeFilesystemForSnapshotInputCollection{}
	err := c.rancherClient.doList(UPDATE_FREEZE_FILESYSTEM_FOR_SNAPSHOT_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *UpdateFreezeFilesystemForSnapshotInputCollection) Next() (*UpdateFreezeFilesystemForSnapshotInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &UpdateFreezeFilesystemForSnapshotInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *UpdateFreezeFilesystemForSnapshotInputClient) ById(id string) (*UpdateFreezeFilesystemForSnapshotInput, error) {
	resp := &UpdateFreezeFilesystemForSnapshotInput{}
	err := c.rancherClient.doById(UPDATE_FREEZE_FILESYSTEM_FOR_SNAPSHOT_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *UpdateFreezeFilesystemForSnapshotInputClient) Delete(container *UpdateFreezeFilesystemForSnapshotInput) error {
	return c.rancherClient.doResourceDelete(UPDATE_FREEZE_FILESYSTEM_FOR_SNAPSHOT_INPUT_TYPE, &container.Resource)
}
