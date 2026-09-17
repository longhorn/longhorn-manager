package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type InstanceManagerUpgradeState string

const (
	InstanceManagerUpgradeStatePending                  = InstanceManagerUpgradeState("pending")
	InstanceManagerUpgradeStateRelocatingEngines        = InstanceManagerUpgradeState("relocating-engines")
	InstanceManagerUpgradeStateWaitingForSourceIM       = InstanceManagerUpgradeState("waiting-for-source-im")
	InstanceManagerUpgradeStateWaitingForHealthyVolumes = InstanceManagerUpgradeState("waiting-for-healthy-volumes")
	InstanceManagerUpgradeStateRestoringEngines         = InstanceManagerUpgradeState("restoring-engines")
	InstanceManagerUpgradeStateCompleted                = InstanceManagerUpgradeState("completed")
	InstanceManagerUpgradeStateFailed                   = InstanceManagerUpgradeState("failed")
)

// InstanceManagerUpgradeSpec defines the desired state of the InstanceManagerUpgrade.
type InstanceManagerUpgradeSpec struct {
	// NodeID is the node where the source instance manager is running.
	NodeID string `json:"nodeID"`

	// TargetImage is the desired instance manager image after upgrade.
	TargetImage string `json:"targetImage"`
}

// InstanceManagerUpgradeStatus defines the observed state of the InstanceManagerUpgrade.
type InstanceManagerUpgradeStatus struct {
	// OwnerID is the owner node ID of this InstanceManagerUpgrade.
	// +optional
	OwnerID string `json:"ownerID,omitempty"`

	// State indicates the overall progress of the instance manager upgrade.
	// +optional
	State InstanceManagerUpgradeState `json:"state,omitempty"`

	// Engines records the relocation plan for each engine managed by the source
	// instance manager. The map key is the volume name.
	// +optional
	// +nullable
	Engines map[string]EngineRelocation `json:"engines,omitempty"`

	// PlannedDetachedReplicas records the replicas that should be temporarily
	// detached from engines during the instance manager upgrade. The map key is
	// the volume name.
	// +optional
	// +nullable
	PlannedDetachedReplicas map[string][]PlannedDetachedReplica `json:"plannedDetachedReplicas,omitempty"`

	// StartedAt records when the timed or active portion of the upgrade began.
	// +optional
	StartedAt string `json:"startedAt,omitempty"`

	// AbortRequested is set by the controller when an abort condition is detected.
	// +optional
	AbortRequested bool `json:"abortRequested,omitempty"`

	// AbortReason explains why AbortRequested was set.
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

	// TemporaryNodeID is the node the engine is temporarily relocated to while
	// the source instance manager is being upgraded.
	TemporaryNodeID string `json:"temporaryNodeID,omitempty"`
}

// PlannedDetachedReplica records a backend that should be temporarily detached
// from an engine during an instance manager upgrade.
type PlannedDetachedReplica struct {
	// Name is the replica name.
	Name string `json:"name"`

	// Address is the raw replica backend address recorded when the detach plan
	// is created.
	Address string `json:"address"`
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
// v2 instance manager.
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
