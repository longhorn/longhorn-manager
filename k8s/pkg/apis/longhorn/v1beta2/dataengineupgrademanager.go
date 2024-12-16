package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// DataEngineUpgradeManagerSpec defines the desired state of the upgrade manager resource
type DataEngineUpgradeManagerSpec struct {
	// DataEngine specifies the data engine type to upgrade to.
	// +optional
	// +kubebuilder:validation:Enum=v2
	DataEngine DataEngineType `json:"dataEngine"`
	// Nodes specifies the list of nodes to perform the data engine upgrade on.
	// If empty, the upgrade will be performed on all available nodes.
	// +optional
	Nodes []string `json:"nodes"`
}

// UpgradeState defines the state of the node upgrade process
type UpgradeNodeStatus struct {
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	Message string `json:"message"`
}

// DataEngineUpgradeManagerStatus defines the observed state of the upgrade manager resource
type DataEngineUpgradeManagerStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	InstanceManagerImage string `json:"instanceManagerImage"`
	// +optional
	State UpgradeState `json:"state"`
	// +optional
	Message string `json:"message"`
	// +optional
	UpgradingNode string `json:"upgradingNode"`
	// +optional
	UpgradeNodes map[string]*UpgradeNodeStatus `json:"upgradeNodes"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhum
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.ownerID`,description="The node that the upgrade manager is being performed on"
// +kubebuilder:printcolumn:name="Data Engine",type=string,JSONPath=`.spec.dataEngine`,description="The data engine targeted for upgrade"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The current state of the upgrade process"
// +kubebuilder:printcolumn:name="Upgrading Node",type=string,JSONPath=`.status.upgradingNode`,description="The node that is currently being upgraded"
// DataEngineUpgradeManager is where Longhorn stores upgrade manager object.
type DataEngineUpgradeManager struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataEngineUpgradeManagerSpec   `json:"spec,omitempty"`
	Status DataEngineUpgradeManagerStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DataEngineUpgradeManagerList is a list of DataEngineUpgradeManagers.
type DataEngineUpgradeManagerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataEngineUpgradeManager `json:"items"`
}
