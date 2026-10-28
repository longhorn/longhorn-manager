package client

const (
	VOLUME_ANTI_AFFINITY_FROM_POD_TYPE = "volumeAntiAffinityFromPod"
)

type VolumeAntiAffinityFromPod struct {
	Resource `yaml:"-"`

	PendingInheritance bool `json:"pendingInheritance,omitempty" yaml:"pending_inheritance,omitempty"`

	Pod string `json:"pod,omitempty" yaml:"pod,omitempty"`

	PreferredTerms []VolumeAntiAffinityTerm `json:"preferredTerms,omitempty" yaml:"preferred_terms,omitempty"`

	RequiredTerms []VolumeAntiAffinityTerm `json:"requiredTerms,omitempty" yaml:"required_terms,omitempty"`
}

type VolumeAntiAffinityFromPodCollection struct {
	Collection
	Data   []VolumeAntiAffinityFromPod `json:"data,omitempty"`
	client *VolumeAntiAffinityFromPodClient
}

type VolumeAntiAffinityFromPodClient struct {
	rancherClient *RancherClient
}

type VolumeAntiAffinityFromPodOperations interface {
	List(opts *ListOpts) (*VolumeAntiAffinityFromPodCollection, error)
	Create(opts *VolumeAntiAffinityFromPod) (*VolumeAntiAffinityFromPod, error)
	Update(existing *VolumeAntiAffinityFromPod, updates interface{}) (*VolumeAntiAffinityFromPod, error)
	ById(id string) (*VolumeAntiAffinityFromPod, error)
	Delete(container *VolumeAntiAffinityFromPod) error
}

func newVolumeAntiAffinityFromPodClient(rancherClient *RancherClient) *VolumeAntiAffinityFromPodClient {
	return &VolumeAntiAffinityFromPodClient{
		rancherClient: rancherClient,
	}
}

func (c *VolumeAntiAffinityFromPodClient) Create(container *VolumeAntiAffinityFromPod) (*VolumeAntiAffinityFromPod, error) {
	resp := &VolumeAntiAffinityFromPod{}
	err := c.rancherClient.doCreate(VOLUME_ANTI_AFFINITY_FROM_POD_TYPE, container, resp)
	return resp, err
}

func (c *VolumeAntiAffinityFromPodClient) Update(existing *VolumeAntiAffinityFromPod, updates interface{}) (*VolumeAntiAffinityFromPod, error) {
	resp := &VolumeAntiAffinityFromPod{}
	err := c.rancherClient.doUpdate(VOLUME_ANTI_AFFINITY_FROM_POD_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *VolumeAntiAffinityFromPodClient) List(opts *ListOpts) (*VolumeAntiAffinityFromPodCollection, error) {
	resp := &VolumeAntiAffinityFromPodCollection{}
	err := c.rancherClient.doList(VOLUME_ANTI_AFFINITY_FROM_POD_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *VolumeAntiAffinityFromPodCollection) Next() (*VolumeAntiAffinityFromPodCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &VolumeAntiAffinityFromPodCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *VolumeAntiAffinityFromPodClient) ById(id string) (*VolumeAntiAffinityFromPod, error) {
	resp := &VolumeAntiAffinityFromPod{}
	err := c.rancherClient.doById(VOLUME_ANTI_AFFINITY_FROM_POD_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *VolumeAntiAffinityFromPodClient) Delete(container *VolumeAntiAffinityFromPod) error {
	return c.rancherClient.doResourceDelete(VOLUME_ANTI_AFFINITY_FROM_POD_TYPE, &container.Resource)
}
