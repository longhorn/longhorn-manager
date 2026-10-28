package client

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	VOLUME_ANTI_AFFINITY_TERM_TYPE = "volumeAntiAffinityTerm"
)

type VolumeAntiAffinityTerm struct {
	Resource `yaml:"-"`

	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty" yaml:"label_selector,omitempty"`

	Namespaces []string `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
}

type VolumeAntiAffinityTermCollection struct {
	Collection
	Data   []VolumeAntiAffinityTerm `json:"data,omitempty"`
	client *VolumeAntiAffinityTermClient
}

type VolumeAntiAffinityTermClient struct {
	rancherClient *RancherClient
}

type VolumeAntiAffinityTermOperations interface {
	List(opts *ListOpts) (*VolumeAntiAffinityTermCollection, error)
	Create(opts *VolumeAntiAffinityTerm) (*VolumeAntiAffinityTerm, error)
	Update(existing *VolumeAntiAffinityTerm, updates interface{}) (*VolumeAntiAffinityTerm, error)
	ById(id string) (*VolumeAntiAffinityTerm, error)
	Delete(container *VolumeAntiAffinityTerm) error
}

func newVolumeAntiAffinityTermClient(rancherClient *RancherClient) *VolumeAntiAffinityTermClient {
	return &VolumeAntiAffinityTermClient{
		rancherClient: rancherClient,
	}
}

func (c *VolumeAntiAffinityTermClient) Create(container *VolumeAntiAffinityTerm) (*VolumeAntiAffinityTerm, error) {
	resp := &VolumeAntiAffinityTerm{}
	err := c.rancherClient.doCreate(VOLUME_ANTI_AFFINITY_TERM_TYPE, container, resp)
	return resp, err
}

func (c *VolumeAntiAffinityTermClient) Update(existing *VolumeAntiAffinityTerm, updates interface{}) (*VolumeAntiAffinityTerm, error) {
	resp := &VolumeAntiAffinityTerm{}
	err := c.rancherClient.doUpdate(VOLUME_ANTI_AFFINITY_TERM_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *VolumeAntiAffinityTermClient) List(opts *ListOpts) (*VolumeAntiAffinityTermCollection, error) {
	resp := &VolumeAntiAffinityTermCollection{}
	err := c.rancherClient.doList(VOLUME_ANTI_AFFINITY_TERM_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *VolumeAntiAffinityTermCollection) Next() (*VolumeAntiAffinityTermCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &VolumeAntiAffinityTermCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *VolumeAntiAffinityTermClient) ById(id string) (*VolumeAntiAffinityTerm, error) {
	resp := &VolumeAntiAffinityTerm{}
	err := c.rancherClient.doById(VOLUME_ANTI_AFFINITY_TERM_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *VolumeAntiAffinityTermClient) Delete(container *VolumeAntiAffinityTerm) error {
	return c.rancherClient.doResourceDelete(VOLUME_ANTI_AFFINITY_TERM_TYPE, &container.Resource)
}
