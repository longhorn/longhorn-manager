package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ReplicaSpec struct {
	InstanceSpec `json:""`

	// +optional
	EngineName string `json:"engineName"`
	// +optional
	HealthyAt string `json:"healthyAt"`
	// +optional
	FailedAt string `json:"failedAt"`
	// +optional
	DiskID string `json:"diskID"`
	// +optional
	DiskPath string `json:"diskPath"`
	// +optional
	DataDirectoryName string `json:"dataDirectoryName"`
	// +optional
	BackingImage string `json:"backingImage"`
	// +optional
	Active bool `json:"active"`
	// +optional
	HardNodeAffinity string `json:"hardNodeAffinity"`
	// +optional
	RevisionCounterDisabled bool `json:"revisionCounterDisabled"`
	// +optional
	RebuildRetryCount int `json:"rebuildRetryCount"`
	// Deprecated
	// +optional
	DataPath string `json:"dataPath"`
	// Deprecated. Rename to BackingImage
	// +optional
	BaseImage string `json:"baseImage"`
}

type ReplicaStatus struct {
	InstanceStatus `json:""`

	// +optional
	EvictionRequested bool `json:"evictionRequested"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type Replica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ReplicaSpec   `json:"spec,omitempty"`
	Status ReplicaStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Replica `json:"items"`
}
