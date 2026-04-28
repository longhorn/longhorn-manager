package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type InstanceManagerUpgradeState string

const (
	InstanceManagerUpgradeStatePending                  = InstanceManagerUpgradeState("pending")
	InstanceManagerUpgradeStateRelocatingEngines        = InstanceManagerUpgradeState("relocating-engines")
	InstanceManagerUpgradeStateWaitingForSourceIM       = InstanceManagerUpgradeState("waiting-for-source-im")
	InstanceManagerUpgradeStateRestoringEngines         = InstanceManagerUpgradeState("restoring-engines")
	InstanceManagerUpgradeStateWaitingForHealthyVolumes = InstanceManagerUpgradeState("waiting-for-healthy-volumes")
	InstanceManagerUpgradeStateCompleted                = InstanceManagerUpgradeState("completed")
	InstanceManagerUpgradeStateFailed                   = InstanceManagerUpgradeState("failed")
)

// InstanceManagerUpgradeSpec defines the desired state of the InstanceManagerUpgrade.
type InstanceManagerUpgradeSpec struct {
	// NodeID is the node where the source instance manager is running.
	// Engines managed by the source instance manager are temporarily relocated
	// away from this node during the upgrade and restored to it afterward.
	// +optional
	NodeID string `json:"nodeID"`

	// TargetImage is the desired instance manager image after upgrade.
	// +optional
	TargetImage string `json:"targetImage"`
}

// InstanceManagerUpgradeStatus defines the observed state of the InstanceManagerUpgrade.
type InstanceManagerUpgradeStatus struct {
	// OwnerID is the owner node ID of this InstanceManagerUpgrade.
	// +optional
	OwnerID string `json:"ownerID"`

	// State indicates the overall progress of the instance manager upgrade.
	// +optional
	State InstanceManagerUpgradeState `json:"state,omitempty"`

	// Engines records the relocation plan for each engine managed by the source
	// instance manager. The map key is the engine name.
	// +optional
	// +nullable
	Engines map[string]EngineRelocation `json:"engines,omitempty"`

	// StartedAt records when the upgrade transitioned out of Pending and began
	// active work. It is used to enforce the upgrade timeout. This timestamp is
	// set once and never reset, providing a single global timeline for the entire upgrade.
	// +optional
	StartedAt string `json:"startedAt,omitempty"`

	// AbortRequested is set by the controller when an abort condition is detected
	// (e.g., timeout, target image change). This is controller-owned state.
	// +optional
	AbortRequested bool `json:"abortRequested,omitempty"`

	// AbortReason explains why AbortRequested was set (e.g., "timeout", "target-image-changed").
	// This provides observability and helps the upgrade-control controller distinguish
	// between different abort scenarios.
	// +optional
	AbortReason string `json:"abortReason,omitempty"`

	// ErrorMsg records the terminal error encountered during the upgrade, if any.
	// +optional
	ErrorMsg string `json:"errorMsg,omitempty"`

	// Conditions records the current conditions of the upgrade.
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions,omitempty"`
}

// EngineRelocation records the relocation metadata of an engine during an
// instance manager upgrade.
type EngineRelocation struct {
	// OriginalNodeID is the node where the engine was originally running.
	OriginalNodeID string `json:"originalNodeID"`

	// TemporaryNodeID is the node the engine is temporarily relocated to
	// while the source instance manager is being upgraded.
	TemporaryNodeID string `json:"temporaryNodeID,omitempty"`

	// SnapshotName is the name of the pre-upgrade snapshot created for this
	// volume before relocation, used to support fast replica rebuild. It is
	// only set when the TakeSnapshotBeforeV2DataEngineUpgrade setting is
	// enabled and snapshot data integrity checking is enabled for the volume.
	// +optional
	SnapshotName string `json:"snapshotName,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhimu
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The state of the instance manager upgrade"
// +kubebuilder:printcolumn:name="Target Image",type=string,JSONPath=`.spec.targetImage`,description="The desired image after upgrade"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// InstanceManagerUpgrade is the Longhorn CR that tracks the live upgrade of a
// v2 instance manager and the temporary relocation of its managed engines.
type InstanceManagerUpgrade struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec InstanceManagerUpgradeSpec `json:"spec,omitempty"`

	// +optional
	Status InstanceManagerUpgradeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InstanceManagerUpgradeList contains a list of InstanceManagerUpgrade objects.
type InstanceManagerUpgradeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []InstanceManagerUpgrade `json:"items"`
}
