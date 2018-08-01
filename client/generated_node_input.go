package client

const (
	NODE_INPUT_TYPE = "nodeInput"
)

type NodeInput struct {
	Resource `yaml:"-"`

	NodeId string `json:"nodeId,omitempty" yaml:"node_id,omitempty"`
}

type NodeInputCollection struct {
	Collection
	Data   []NodeInput `json:"data,omitempty"`
	client *NodeInputClient
}

type NodeInputClient struct {
	rancherClient *RancherClient
}

type NodeInputOperations interface {
	List(opts *ListOpts) (*NodeInputCollection, error)
	Create(opts *NodeInput) (*NodeInput, error)
	Update(existing *NodeInput, updates interface{}) (*NodeInput, error)
	ById(id string) (*NodeInput, error)
	Delete(container *NodeInput) error
}

func newNodeInputClient(rancherClient *RancherClient) *NodeInputClient {
	return &NodeInputClient{
		rancherClient: rancherClient,
	}
}

func (c *NodeInputClient) Create(container *NodeInput) (*NodeInput, error) {
	resp := &NodeInput{}
	err := c.rancherClient.doCreate(NODE_INPUT_TYPE, container, resp)
	return resp, err
}

func (c *NodeInputClient) Update(existing *NodeInput, updates interface{}) (*NodeInput, error) {
	resp := &NodeInput{}
	err := c.rancherClient.doUpdate(NODE_INPUT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *NodeInputClient) List(opts *ListOpts) (*NodeInputCollection, error) {
	resp := &NodeInputCollection{}
	err := c.rancherClient.doList(NODE_INPUT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *NodeInputCollection) Next() (*NodeInputCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &NodeInputCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *NodeInputClient) ById(id string) (*NodeInput, error) {
	resp := &NodeInput{}
	err := c.rancherClient.doById(NODE_INPUT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *NodeInputClient) Delete(container *NodeInput) error {
	return c.rancherClient.doResourceDelete(NODE_INPUT_TYPE, &container.Resource)
}
