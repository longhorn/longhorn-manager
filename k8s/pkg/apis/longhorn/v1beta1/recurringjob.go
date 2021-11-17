package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type RecurringJobType string

const (
	RecurringJobTypeSnapshot = RecurringJobType("snapshot")
	RecurringJobTypeBackup   = RecurringJobType("backup")

	RecurringJobGroupDefault = "default"
)

type VolumeRecurringJob struct {
	Name    string `json:"name"`
	IsGroup bool   `json:"isGroup"`
}

type RecurringJobSpec struct {
	// +optional
	Name string `json:"name"`
	// +optional
	Groups []string `json:"groups,omitempty"`
	// +optional
	Task RecurringJobType `json:"task"`
	// +optional
	Cron string `json:"cron"`
	// +optional
	Retain int `json:"retain"`
	// +optional
	Concurrency int `json:"concurrency"`
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

type RecurringJobStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type RecurringJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RecurringJobSpec   `json:"spec,omitempty"`
	Status RecurringJobStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type RecurringJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RecurringJob `json:"items"`
}
