package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EngineFrontendSpec defines the desired state of the Longhorn engine frontend (v2 initiator)
type EngineFrontendSpec struct {
	InstanceSpec `json:""`
	// +optional
	Frontend VolumeFrontend `json:"frontend"`
	// ublkQueueDepth controls the depth of each queue for ublk frontend.
	// +optional
	UblkQueueDepth int `json:"ublkQueueDepth,omitempty"`
	// ublkNumberOfQueue controls the number of queues for ublk frontend.
	// +optional
	UblkNumberOfQueue int `json:"ublkNumberOfQueue,omitempty"`
	// TargetIP is the IP address of the v2 engine target
	// +optional
	TargetIP string `json:"targetIP"`
	// TargetPort is the port of the v2 engine target
	// +optional
	TargetPort int `json:"targetPort"`
	// EngineName is the name of the v2 engine target (required for EngineFrontend instance creation)
	// +optional
	EngineName string `json:"engineName"`
	// +optional
	DisableFrontend bool `json:"disableFrontend"`
	// +optional
	Active bool `json:"active"`
}

// EngineFrontendStatus defines the observed state of the Longhorn engine frontend
type EngineFrontendStatus struct {
	InstanceStatus `json:""`
	// Endpoint is the initiator endpoint (e.g., /dev/longhorn/vol-name)
	// +optional
	Endpoint string `json:"endpoint"`
	// TargetIP is the currently connected IP address of the v2 engine target
	// +optional
	TargetIP string `json:"targetIP"`
	// TargetPort is the currently connected port of the v2 engine target
	// +optional
	TargetPort int `json:"targetPort"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhef
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Data Engine",type=string,JSONPath=`.spec.dataEngine`,description="The data engine of the engine frontend"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.currentState`,description="The current state of the engine frontend"
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeID`,description="The node that the engine frontend is on"
// +kubebuilder:printcolumn:name="InstanceManager",type=string,JSONPath=`.status.instanceManagerName`,description="The instance manager of the engine frontend"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EngineFrontend is where Longhorn stores engine frontend object for v2 data engine initiator.
type EngineFrontend struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngineFrontendSpec   `json:"spec,omitempty"`
	Status EngineFrontendStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EngineFrontendList is a list of EngineFrontends.
type EngineFrontendList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EngineFrontend `json:"items"`
}
