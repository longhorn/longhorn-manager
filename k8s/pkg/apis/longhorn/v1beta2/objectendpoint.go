package v1beta2

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ObjectEndpointState string

const (
	ObjectEndpointStateUnknown  = ObjectEndpointState("Unknown")
	ObjectEndpointStateStarting = ObjectEndpointState("Starting")
	ObjectEndpointStateRunning  = ObjectEndpointState("Running")
	ObjectEndpointStateStopping = ObjectEndpointState("Stopping")
	ObjectEndpointStateError    = ObjectEndpointState("Error")
)

type ObjectEndpointStatus struct {
	// The state of the object endpoint as observed by the object endpoint
	// controller.
	//
	// The object endpoint implemets a state machine with this, beginning at
	// "unknown". Once the object endpoint controller detects the new object
	// endpoint and begins resource creation the state is transformed to
	// "starting". The object endpoint controller observes the resources and once
	// all resources are up and ready the state transitions to "running".
	// The state remains in "running" until a the object endpoint is deleted, at
	// which point the controller will clean up the associated resources.
	//
	//  │ object endpoint created
	//  │
	//  └──►┌───────────┐
	//      │ unknown   │
	//  ┌── └───────────┘
	//  │
	//  │ controller creates resources
	//  │
	//  └──►┌───────────┐
	//      │ starting  │
	//  ┌── └───────────┘
	//  │
	//  │ controller detects all resources ready
	//  │
	//  └──►┌───────────┐
	//      │ running   │
	//  ┌── └───────────┘
	//  │
	//  │ object endpoint is marked for deletion
	//  │
	//  └──►┌───────────┐
	//      │ stopping  │
	//  ┌── └───────────┘
	//  │
	//  │ controller waits for dependent resources to disappear before removing
	//  │ the finalizer, thereby letting the object endpoint be deleted
	//
	// +optional
	// +default="Unknown"
	State ObjectEndpointState `json:"state"`

	// An address where the S3 endpoint is exposed.
	//
	// +optional
	Endpoint string `json:"endpoint"`
}

type ObjectEndpointCredentials struct {
	// An access key. This will be the initial access key for the administrative
	// account.
	//
	// +optional
	AccessKey string `json:"accessKey"`

	// A secret key. This is the secret key corresponding to the access key above.
	//
	// +optional
	SecretKey string `json:"secretKey"`
}

type ObjectEndpointSpec struct {
	// Credentials contain a pair of access and secret keys that are used to seed
	// the object endpoint.
	//
	// +optional
	Credentials ObjectEndpointCredentials `json:"credentials"`

	// The name of a storage class. The backing storage is going to be a new
	// volume from that storage class.
	//
	// +optional
	StorageClass string `json:"storageClassName"`

	// The initial size of the volume to provision. This size together with the
	// storage class will be used to automatically create a suitable volume where
	// the objects will be stored.
	//
	// +optional
	Size resource.Quantity `json:"size"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope="Cluster",shortName={lhoe}
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Name",type=string,JSONPath=`.metadata.name`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The state of object endpoint"
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`,description="The endpoint address"
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
