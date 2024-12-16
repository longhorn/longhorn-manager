package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type UpgradeState string

const (
	UpgradeStateUndefined                = UpgradeState("")
	UpgradeStatePending                  = UpgradeState("pending")
	UpgradeStateInitializing             = UpgradeState("initializing")
	UpgradeStateSwitchingOver            = UpgradeState("switching-over")
	UpgradeStateFailingReplicas          = UpgradeState("failing-replicas")
	UpgradeStateSwitchedOver             = UpgradeState("switched-over")
	UpgradeStateUpgrading                = UpgradeState("upgrading")
	UpgradeStateUpgradingInstanceManager = UpgradeState("upgrading-instance-manager")
	UpgradeStateSwitchingBack            = UpgradeState("switching-back")
	UpgradeStateSwitchedBack             = UpgradeState("switched-back")
	UpgradeStateRebuildingReplica        = UpgradeState("rebuilding-replica")
	UpgradeStateFinalizing               = UpgradeState("finalizing")
	UpgradeStateCompleted                = UpgradeState("completed")
	UpgradeStateError                    = UpgradeState("error")
)

// NodeDataEngineUpgradeSpec defines the desired state of the node data engine upgrade resource
type NodeDataEngineUpgradeSpec struct {
	// NodeID specifies the ID of the node where the data engine upgrade will take place.
	// +optional
	NodeID string `json:"nodeID"`
	// DataEngine specifies the data engine type to upgrade to.
	// +optional
	// +kubebuilder:validation:Enum=v2
	DataEngine DataEngineType `json:"dataEngine"`
	// InstanceManagerImage specifies the image to use for the instance manager upgrade.
	// +optional
	InstanceManagerImage string `json:"instanceManagerImage"`
	// DataEngineUpgradeManager specifies the name of the upgrade manager resource that is managing the upgrade process.
	// +optional
	DataEngineUpgradeManager string `json:"dataEngineUpgradeManager"`
}

type VolumeUpgradeStatus struct {
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	Message string `json:"message"`
}

// NodeDataEngineUpgradeStatus defines the observed state of the node upgrade resource
type NodeDataEngineUpgradeStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	Volumes map[string]*VolumeUpgradeStatus `json:"volumes"`
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	Message string `json:"message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhnu
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.ownerID`,description="The node that the upgrade is being performed on"
// +kubebuilder:printcolumn:name="Data Engine",type=string,JSONPath=`.spec.dataEngine`,description="The data engine targeted for node upgrade"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The current state of the node upgrade process"
// NodeDataEngineUpgrade is where Longhorn stores node upgrade object.
type NodeDataEngineUpgrade struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodeDataEngineUpgradeSpec   `json:"spec,omitempty"`
	Status NodeDataEngineUpgradeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeDataEngineUpgradeList is a list of NodeDataEngineUpgrades.
type NodeDataEngineUpgradeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeDataEngineUpgrade `json:"items"`
}
