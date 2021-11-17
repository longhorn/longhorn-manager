package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ShareManagerState string

const (
	ShareManagerStateUnknown  = ShareManagerState("unknown")
	ShareManagerStateStarting = ShareManagerState("starting")
	ShareManagerStateRunning  = ShareManagerState("running")
	ShareManagerStateStopping = ShareManagerState("stopping")
	ShareManagerStateStopped  = ShareManagerState("stopped")
	ShareManagerStateError    = ShareManagerState("error")
)

type ShareManagerSpec struct {
	// +optional
	Image string `json:"image"`
}

type ShareManagerStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	State ShareManagerState `json:"state"`
	// +optional
	Endpoint string `json:"endpoint"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ShareManager struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShareManagerSpec   `json:"spec,omitempty"`
	Status ShareManagerStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ShareManagerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShareManager `json:"items"`
}
