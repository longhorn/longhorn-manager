package v1beta2

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ObjectStoreState string

const (
	ObjectStoreStateUnknown     = ObjectStoreState("unknown")
	ObjectStoreStateStarting    = ObjectStoreState("starting")
	ObjectStoreStateRunning     = ObjectStoreState("running")
	ObjectStoreStateStopping    = ObjectStoreState("stopping")
	ObjectStoreStateStopped     = ObjectStoreState("stopped")
	ObjectStoreStateTerminating = ObjectStoreState("terminating")
	ObjectStoreStateError       = ObjectStoreState("error")
)

type ObjectStoreStatus struct {
	// The state of the object store as observed by the object store controller.
	//
	// The object store implements a state machine with this, beginning at
	// "unknown". Once the object store controller detects the new object store
	// and begins resource creation the state is transformed to "starting". The
	// object store controller observes the resources and once all resources are
	// up and ready the state transitions to "running".
	// The state remains in "running" until the object store is deleted, at
	// which point the controller will clean up the associated resources.
	//
	//  │ object store created
	//  │
	//  └──►┌─────────────┐
	//      │ unknown     │
	//  ┌── └─────────────┘
	//  │
	//  │ controller creates resources
	//  │
	//  └──►┌─────────────┐
	//      │ starting    │◄─────────────────────────────┐
	//  ┌── └─────────────┘                              │
	//  │                                                │
	//  │ controller detects all resources ready         │ target state
	//  │                                                │ is set to `running`
	//  └──►┌─────────────┐                              │
	//      │ running     │                              │
	//  ┌── └─────────────┘                              │
	//  │     │                                          │
	//  │     │ target state is set to `stopped`         │
	//  │     │                                          │
	//  │     └──►┌─────────────┐                        │
	//  │         │ stopping    │                        │
	//  │     ┌── └─────────────┘                        │
	//  │     │                                          │
	//  │     │ deployment is scaled down and volume     │
	//  │     │ is detached                              │
	//  │     │                                          │
	//  │     └──►┌─────────────┐                        │
	//  │         │ stopped     │                        │
	//  │         └─────────────┘────────────────────────┘
	//  │
	//  │ object endpoint is marked for deletion
	//  │
	//  └──►┌─────────────┐
	//      │ terminating │
	//  ┌── └─────────────┘
	//  │
	//  │ controller waits for dependent resources to disappear before removing
	//  │ the finalizer, thereby letting the object store be deleted
	//
	// The "stopped" state is used in cases where a user may need to stop the s3gw
	// workload on the volume but wants to restart it with the same configuration
	// and data afterwards. E.g. maintenance work, moving the longhorn volume to a
	// new instance manager etc.
	//
	// Any state can transition to the "error" state. At which point probably user
	// intervention is necessary to recover.
	//
	// +kubebuilder:validation:Enum:=unknown;starting;running;stopping;stopped;terminating;error
	// +optional
	// +default="Unknown"
	State ObjectStoreState `json:"state,omitempty"`

	// A list of addresses where the S3 store is exposed.
	//
	// +optional
	Endpoints []string `json:"endpoints,omitempty"`
}

type ObjectStoreVolumeParameterSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=3
	// +optional
	NumberOfReplicas int `json:"numberOfReplicas,omitempty"`

	// Replica soft anti affinity of the volume. Set enabled to allow replicas to
	// be scheduled on the same node
	//
	// +optional
	ReplicaSoftAntiAffinity ReplicaSoftAntiAffinity `json:"replicaSoftAntiAffinity,omitempty"`

	// Replica zone soft anti affinity of the volume. Set enabled to allow
	// replicas to be scheduled in the same zone
	//
	// +optional
	ReplicaZoneSoftAntiAffinity ReplicaZoneSoftAntiAffinity `json:"replicaZoneSoftAntiAffinity,omitempty"`

	// Replica disk soft anti affinity of the volume. Set enabled to allow
	// replicas to be scheduled in the same disk.
	//
	// +optional
	ReplicaDiskSoftAntiAffinity ReplicaDiskSoftAntiAffinity `json:"replicaDiskSoftAntiAffinity,omitempty"`

	// +optional
	DiskSelector []string `json:"diskSelector,omitempty"`
	// +optional
	NodeSelector []string `json:"nodeSelector,omitempty"`

	// +optional
	DataLocality DataLocality `json:"dataLocality,omitempty"`

	// +optional
	FromBackup string `json:"fromBackup,omitempty"`

	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=2880
	// +optional
	StaleReplicaTimeout int `json:"staleReplicaTimeout,omitempty"`

	// +optional
	RecurringJobSelector []VolumeRecurringJob `json:"recurringJobSelector,omitempty"`

	// +optional
	ReplicaAutoBalance ReplicaAutoBalance `json:"replicaAutoBalance,omitempty"`

	// +optional
	RevisionCounterDisabled bool `json:"revisionCounterDisabled,omitempty"`

	// +optional
	UnmapMarkSnapChainRemoved UnmapMarkSnapChainRemoved `json:"unmapMarkSnapChainRemoved,omitempty"`

	// +kubebuilder:validation:Enum=v1;v2
	// +optional
	BackendStoreDriver BackendStoreDriverType `json:"backendStoreDriver,omitempty"`
}

type ObjectStoreEndpointSpec struct {
	// +kubebuilder:validation:Required
	// +required
	Name string `json:"name"`

	// +optional
	DomainName string `json:"domainName,omitempty"`

	// Reference to a secret containing TLS certificate and key for DomainName
	// +optional
	TLS corev1.SecretReference `json:"tls,omitempty"`
}

type ObjectStoreSpec struct {
	// Credentials contain a pair of access and secret keys that are used to seed
	// the object store.
	//
	// This expects an opaque secret with the two keys
	// `RGW_DEFAULT_USER_ACCESS_KEY` and `RGW_DEFAULT_USER_SECRET_KEY` to be
	// provided by the user in the longhorn-system namespace.
	//
	// +optional
	Credentials corev1.SecretReference `json:"credentials"`

	// The initial size of the volume to provision. The volume will be created
	// such that it can be expanded if resources are available
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:example:=100Gi
	// +optional
	Size resource.Quantity `json:"size,omitempty"`

	// The configuration of the longhorn volume providing storage for the object
	// store.
	//
	// +optional
	VolumeParameters ObjectStoreVolumeParameterSpec `json:"volumeParameters"`

	// A list of endpoint configurations to make the object store available at.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	Endpoints []ObjectStoreEndpointSpec `json:"endpoints"`

	// A target state. Can be used to volutarily stop the gateway for offline
	// volume operations
	//
	// +kubebuilder:validation:Enum:=running;stopped
	// +kubebuilder:default:=running
	// +optional
	TargetState ObjectStoreState `json:"targetState,omitempty"`

	// Desired images for the gateway and UI containers.
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	UIImage string `json:"uiImage,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName={lhos}
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The state of object store"
// +kubebuilder:printcolumn:name="Endpoints",type=string,JSONPath=`.status.endpoints[*]`,description="Endpoint addresses"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Representation of an object storage endpoint in the K8s API.
type ObjectStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObjectStoreSpec   `json:"spec,omitempty"`
	Status ObjectStoreStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ObjectStoreList is a list of ObjectStores.
type ObjectStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ObjectStore `json:"items"`
}

func (os *ObjectStore) Hub() {}
