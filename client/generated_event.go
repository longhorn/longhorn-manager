package client

const (
	EVENT_TYPE = "event"
)

type Event struct {
	Resource `yaml:"-"`

	Action string `json:"action,omitempty" yaml:"action,omitempty"`

	Annotations map[string]string `json:"annotations,omitempty" yaml:"annotations,omitempty"`

	ApiVersion string `json:"apiVersion,omitempty" yaml:"api_version,omitempty"`

	ClusterName string `json:"clusterName,omitempty" yaml:"cluster_name,omitempty"`

	Count int64 `json:"count,omitempty" yaml:"count,omitempty"`

	CreationTimestamp V1.Time `json:"creationTimestamp,omitempty" yaml:"creation_timestamp,omitempty"`

	EventTime V1.MicroTime `json:"eventTime,omitempty" yaml:"event_time,omitempty"`

	EventType string `json:"eventType,omitempty" yaml:"event_type,omitempty"`

	Finalizers []string `json:"finalizers,omitempty" yaml:"finalizers,omitempty"`

	FirstTimestamp V1.Time `json:"firstTimestamp,omitempty" yaml:"first_timestamp,omitempty"`

	GenerateName string `json:"generateName,omitempty" yaml:"generate_name,omitempty"`

	Generation int64 `json:"generation,omitempty" yaml:"generation,omitempty"`

	InvolvedObject V1.ObjectReference `json:"involvedObject,omitempty" yaml:"involved_object,omitempty"`

	Kind string `json:"kind,omitempty" yaml:"kind,omitempty"`

	Labels map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`

	LastTimestamp V1.Time `json:"lastTimestamp,omitempty" yaml:"last_timestamp,omitempty"`

	Message string `json:"message,omitempty" yaml:"message,omitempty"`

	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`

	OwnerReferences []string `json:"ownerReferences,omitempty" yaml:"owner_references,omitempty"`

	Reason string `json:"reason,omitempty" yaml:"reason,omitempty"`

	ReportingComponent string `json:"reportingComponent,omitempty" yaml:"reporting_component,omitempty"`

	ReportingInstance string `json:"reportingInstance,omitempty" yaml:"reporting_instance,omitempty"`

	ResourceVersion string `json:"resourceVersion,omitempty" yaml:"resource_version,omitempty"`

	SelfLink string `json:"selfLink,omitempty" yaml:"self_link,omitempty"`

	Source V1.EventSource `json:"source,omitempty" yaml:"source,omitempty"`

	Uid string `json:"uid,omitempty" yaml:"uid,omitempty"`
}

type EventCollection struct {
	Collection
	Data   []Event `json:"data,omitempty"`
	client *EventClient
}

type EventClient struct {
	rancherClient *RancherClient
}

type EventOperations interface {
	List(opts *ListOpts) (*EventCollection, error)
	Create(opts *Event) (*Event, error)
	Update(existing *Event, updates interface{}) (*Event, error)
	ById(id string) (*Event, error)
	Delete(container *Event) error
}

func newEventClient(rancherClient *RancherClient) *EventClient {
	return &EventClient{
		rancherClient: rancherClient,
	}
}

func (c *EventClient) Create(container *Event) (*Event, error) {
	resp := &Event{}
	err := c.rancherClient.doCreate(EVENT_TYPE, container, resp)
	return resp, err
}

func (c *EventClient) Update(existing *Event, updates interface{}) (*Event, error) {
	resp := &Event{}
	err := c.rancherClient.doUpdate(EVENT_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *EventClient) List(opts *ListOpts) (*EventCollection, error) {
	resp := &EventCollection{}
	err := c.rancherClient.doList(EVENT_TYPE, opts, resp)
	resp.client = c
	return resp, err
}

func (cc *EventCollection) Next() (*EventCollection, error) {
	if cc != nil && cc.Pagination != nil && cc.Pagination.Next != "" {
		resp := &EventCollection{}
		err := cc.client.rancherClient.doNext(cc.Pagination.Next, resp)
		resp.client = cc.client
		return resp, err
	}
	return nil, nil
}

func (c *EventClient) ById(id string) (*Event, error) {
	resp := &Event{}
	err := c.rancherClient.doById(EVENT_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *EventClient) Delete(container *Event) error {
	return c.rancherClient.doResourceDelete(EVENT_TYPE, &container.Resource)
}
