package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ObjectEndpointState string

const (
	ObjectEndpointStateUnknown  = ObjectEndpointState("unknown")
	ObjectEndpointStateStarting = ObjectEndpointState("starting")
	ObjectEndpointStateRunning  = ObjectEndpointState("running")
	ObjectEndpointStateStopping = ObjectEndpointState("stopping")
	ObjectEndpointStateStopped  = ObjectEndpointState("stopped")
	ObjectEndpointStateError    = ObjectEndpointState("error")
)

type ObjectEndpointStatus struct {
	// +optional
	CurrentState ObjectEndpointState `json:"state"`
	// +optional
	Endpoint string `json:"endpoint"`
}

type ObjectEndpointCredentials struct {
	// +optional
	AccessKey string `json:"accessKey"`
	// +optional
	SecretKey string `json:"secretKey"`
}

type ObjectEndpointSpec struct {
	// +optional
	Image string `json:"image"`
	// +optional
	Credentials ObjectEndpointCredentials `json:"credentials"`
	// +optional
	Volume string `json:"volume"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhoe
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.currentState`,description="The state of object endpoint"
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`,description=""
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Representation of an object storage endpoint in the K8s API.
type ObjectEndpoint struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObjectEndpointSpec   `json:"spec,omitempty"`
	Status ObjectEndpointStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ObjectEndpointList is a list of ObjectEndpoints.
type ObjectEndpointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ObjectEndpoint `json:"items"`
}
