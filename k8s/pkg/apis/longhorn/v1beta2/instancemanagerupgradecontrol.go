package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NodeUpgradeState is the upgrade state of a single node within the control CR.
type NodeUpgradeState string

const (
	NodeUpgradeStatePending    = NodeUpgradeState("pending")
	NodeUpgradeStateInProgress = NodeUpgradeState("in-progress")
	NodeUpgradeStateCompleted  = NodeUpgradeState("completed")
	NodeUpgradeStateFailed     = NodeUpgradeState("failed")
)

// InstanceManagerUpgradeControlSpec defines the desired state of the
// InstanceManagerUpgradeControl singleton.
type InstanceManagerUpgradeControlSpec struct {
	// TargetImage is the desired instance manager image for all nodes.
	// +optional
	TargetImage string `json:"targetImage,omitempty"`

	// StartAt is the RFC3339 timestamp at which the upgrade cycle should begin.
	// +optional
	StartAt string `json:"startAt,omitempty"`
}

// InstanceManagerUpgradeControlStatus defines the observed state of the
// InstanceManagerUpgradeControl.
type InstanceManagerUpgradeControlStatus struct {
	// OwnerID is the ID of the Longhorn manager pod that currently owns this CR.
	// +optional
	OwnerID string `json:"ownerID,omitempty"`

	// CurrentNode is the name of the node that is actively being upgraded.
	// +optional
	CurrentNode string `json:"currentNode,omitempty"`

	// Nodes holds the upgrade status for nodes participating in the current or
	// most recent upgrade cycle.
	// +optional
	// +nullable
	Nodes map[string]NodeUpgradeInfo `json:"nodes,omitempty"`
}

// NodeUpgradeInfo records the upgrade status for a single node.
type NodeUpgradeInfo struct {
	// State is the current upgrade state for this node.
	State NodeUpgradeState `json:"state"`

	// IMUName is the name of the InstanceManagerUpgrade CR created for this node.
	// +optional
	IMUName string `json:"imuName,omitempty"`

	// RetryCount tracks how many times the upgrade has been attempted for this node.
	// +optional
	RetryCount int `json:"retryCount,omitempty"`

	// StartedAt is the RFC3339 timestamp when the upgrade began on this node.
	// +optional
	StartedAt string `json:"startedAt,omitempty"`

	// CompletedAt is the RFC3339 timestamp when the upgrade finished on this node.
	// +optional
	CompletedAt string `json:"completedAt,omitempty"`

	// ErrorMsg records the last error encountered while upgrading this node.
	// +optional
	ErrorMsg string `json:"errorMsg,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhimuc
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Target Image",type=string,JSONPath=`.spec.targetImage`,description="Desired instance manager image"
// +kubebuilder:printcolumn:name="Current Node",type=string,JSONPath=`.status.currentNode`,description="Node currently being upgraded"
// +kubebuilder:printcolumn:name="Start At",type=string,JSONPath=`.spec.startAt`,description="Scheduled start time"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// InstanceManagerUpgradeControl is the singleton Longhorn CR that orchestrates
// rolling live upgrades of v2 instance managers across all cluster nodes.
type InstanceManagerUpgradeControl struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec InstanceManagerUpgradeControlSpec `json:"spec,omitempty"`

	// +optional
	Status InstanceManagerUpgradeControlStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InstanceManagerUpgradeControlList contains a list of InstanceManagerUpgradeControl objects.
type InstanceManagerUpgradeControlList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []InstanceManagerUpgradeControl `json:"items"`
}
