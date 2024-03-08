package client

const (
	BACKUP_BACKING_IMAGE_TYPE = "backupBackingImage"
)

type BackupBackingImage struct {
	Resource `yaml:"-"`

	CompressionMethod string `json:"compressionMethod,omitempty" yaml:"compression_method,omitempty"`

	Created string `json:"created,omitempty" yaml:"created,omitempty"`

	Error string `json:"error,omitempty" yaml:"error,omitempty"`

	Labels map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`

	Messages map[string]string `json:"messages,omitempty" yaml:"messages,omitempty"`

	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	Progress int64 `json:"progress,omitempty" yaml:"progress,omitempty"`

	Size int64 `json:"size,omitempty" yaml:"size,omitempty"`

	State string `json:"state,omitempty" yaml:"state,omitempty"`

	Url string `json:"url,omitempty" yaml:"url,omitempty"`
}

type BackupBackingImageCollection struct {
	Collection
	Data   []BackupBackingImage `json:"data,omitempty"`
	client *BackupBackingImageClient
}

type BackupBackingImageClient struct {
	rancherClient *RancherClient
}

type BackupBackingImageOperations interface {
	List(opts *ListOpts) (*BackupBackingImageCollection, error)
	Create(opts *BackupBackingImage) (*BackupBackingImage, error)
	Update(existing *BackupBackingImage, updates interface{}) (*BackupBackingImage, error)
	ById(id string) (*BackupBackingImage, error)
	Delete(container *BackupBackingImage) error
}

func newBackupBackingImageClient(rancherClient *RancherClient) *BackupBackingImageClient {
	return &BackupBackingImageClient{
		rancherClient: rancherClient,
	}
}

func (c *BackupBackingImageClient) Create(container *BackupBackingImage) (*BackupBackingImage, error) {
	resp := &BackupBackingImage{}
	err := c.rancherClient.doCreate(BACKUP_BACKING_IMAGE_TYPE, container, resp)
	return resp, err
}

func (c *BackupBackingImageClient) Update(existing *BackupBackingImage, updates interface{}) (*BackupBackingImage, error) {
	resp := &BackupBackingImage{}
	err := c.rancherClient.doUpdate(BACKUP_BACKING_IMAGE_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *BackupBackingImageClient) List(opts *ListOpts) (*BackupBackingImageCollection, error) {
	resp := &BackupBackingImageCollection{}
	err := c.rancherClient.doList(BACKUP_BACKING_IMAGE_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *BackupBackingImageCollection) Next() (*BackupBackingImageCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &BackupBackingImageCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *BackupBackingImageClient) ById(id string) (*BackupBackingImage, error) {
	resp := &BackupBackingImage{}
	err := c.rancherClient.doById(BACKUP_BACKING_IMAGE_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *BackupBackingImageClient) Delete(container *BackupBackingImage) error {
	return c.rancherClient.doResourceDelete(BACKUP_BACKING_IMAGE_TYPE, &container.Resource)
}
