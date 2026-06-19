package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EngineFrontendSwitchoverPhase tracks the current phase of a control-plane
// driven NVMe/TCP multipath target switchover.
type EngineFrontendSwitchoverPhase string

const (
	// EngineFrontendSwitchoverPhaseNone indicates no switchover in progress.
	EngineFrontendSwitchoverPhaseNone EngineFrontendSwitchoverPhase = ""
	// EngineFrontendSwitchoverPhasePreparing connects the new multipath path
	// and sets the new target ANA state to non-optimized.
	EngineFrontendSwitchoverPhasePreparing EngineFrontendSwitchoverPhase = "preparing"
	// EngineFrontendSwitchoverPhaseSwitching sets the old target ANA state
	// to inaccessible.
	EngineFrontendSwitchoverPhaseSwitching EngineFrontendSwitchoverPhase = "switching"
	// EngineFrontendSwitchoverPhasePromoting sets the new target ANA state
	// to optimized and reloads the initiator device info.
	EngineFrontendSwitchoverPhasePromoting EngineFrontendSwitchoverPhase = "promoting"
)

// EngineFrontendSpec defines the desired state of the Longhorn engine frontend (v2 initiator)
type EngineFrontendSpec struct {
	InstanceSpec `json:""`
	// Size is the desired size of the frontend device in bytes, as requested
	// by the volume owner. The EngineFrontend controller drives the frontend
	// device toward this size independently of the engine's target size.
	// +kubebuilder:validation:Type=string
	// +optional
	Size int64 `json:"size,string"`
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

type EngineFrontendNvmeTCPPath struct {
	// +optional
	TargetIP string `json:"targetIP,omitempty"`
	// +optional
	TargetPort int `json:"targetPort,omitempty"`
	// +optional
	EngineName string `json:"engineName,omitempty"`
	// +optional
	NQN string `json:"nqn,omitempty"`
	// +optional
	NGUID string `json:"nguid,omitempty"`
	// +optional
	ANAState string `json:"anaState,omitempty"`
}

// EngineFrontendStatus defines the observed state of the Longhorn engine frontend
type EngineFrontendStatus struct {
	InstanceStatus `json:""`
	// CurrentSize is the current size of the frontend device in bytes, as
	// observed from the data plane. It is 0 while the engine frontend is not
	// running.
	// +kubebuilder:validation:Type=string
	// +optional
	CurrentSize int64 `json:"currentSize,string"`
	// Endpoint is the initiator endpoint (e.g., /dev/longhorn/vol-name)
	// +optional
	Endpoint string `json:"endpoint"`
	// TargetIP is the currently connected IP address of the v2 engine target
	// +optional
	TargetIP string `json:"targetIP"`
	// TargetPort is the currently connected port of the v2 engine target
	// +optional
	TargetPort int `json:"targetPort"`
	// ActivePath is the currently active frontend path address.
	// +optional
	ActivePath string `json:"activePath,omitempty"`
	// PreferredPath is the preferred frontend path address.
	// +optional
	PreferredPath string `json:"preferredPath,omitempty"`
	// Paths describes the currently known frontend multipath state.
	// +optional
	Paths []EngineFrontendNvmeTCPPath `json:"paths,omitempty"`
	// SwitchoverPhase is the last completed switchover phase reported by the data plane.
	// +optional
	SwitchoverPhase EngineFrontendSwitchoverPhase `json:"switchoverPhase,omitempty"`
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
