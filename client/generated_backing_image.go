package client

const (
	BACKING_IMAGE_TYPE = "backingImage"
)

type BackingImage struct {
	Resource `yaml:"-"`

	DeletionTimestamp string `json:"deletionTimestamp,omitempty" yaml:"deletion_timestamp,omitempty"`

	DiskStateMap map[string]string `json:"diskStateMap,omitempty" yaml:"disk_state_map,omitempty"`

	ImageURL string `json:"imageURL,omitempty" yaml:"image_url,omitempty"`

	Name string `json:"name,omitempty" yaml:"name,omitempty"`
}

type BackingImageCollection struct {
	Collection
	Data   []BackingImage `json:"data,omitempty"`
	client *BackingImageClient
}

type BackingImageClient struct {
	rancherClient *RancherClient
}

type BackingImageOperations interface {
	List(opts *ListOpts) (*BackingImageCollection, error)
	Create(opts *BackingImage) (*BackingImage, error)
	Update(existing *BackingImage, updates interface{}) (*BackingImage, error)
	ById(id string) (*BackingImage, error)
	Delete(container *BackingImage) error

	ActionBackingImageCleanup(*BackingImage, *BackingImageCleanupInput) (*BackingImage, error)
}

func newBackingImageClient(rancherClient *RancherClient) *BackingImageClient {
	return &BackingImageClient{
		rancherClient: rancherClient,
	}
}

func (c *BackingImageClient) Create(container *BackingImage) (*BackingImage, error) {
	resp := &BackingImage{}
	err := c.rancherClient.doCreate(BACKING_IMAGE_TYPE, container, resp)
	return resp, err
}

func (c *BackingImageClient) Update(existing *BackingImage, updates interface{}) (*BackingImage, error) {
	resp := &BackingImage{}
	err := c.rancherClient.doUpdate(BACKING_IMAGE_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *BackingImageClient) List(opts *ListOpts) (*BackingImageCollection, error) {
	resp := &BackingImageCollection{}
	err := c.rancherClient.doList(BACKING_IMAGE_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *BackingImageCollection) Next() (*BackingImageCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &BackingImageCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *BackingImageClient) ById(id string) (*BackingImage, error) {
	resp := &BackingImage{}
	err := c.rancherClient.doById(BACKING_IMAGE_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *BackingImageClient) Delete(container *BackingImage) error {
	return c.rancherClient.doResourceDelete(BACKING_IMAGE_TYPE, &container.Resource)
}

func (c *BackingImageClient) ActionBackingImageCleanup(resource *BackingImage, input *BackingImageCleanupInput) (*BackingImage, error) {

	resp := &BackingImage{}

	err := c.rancherClient.doAction(BACKING_IMAGE_TYPE, "backingImageCleanup", &resource.Resource, input, resp)

	return resp, err
}
