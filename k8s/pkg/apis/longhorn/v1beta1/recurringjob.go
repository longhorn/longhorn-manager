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
// +kubebuilder:resource:shortName=lhrj
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Groups",type=string,JSONPath=`.spec.groups`,description="Sets groupings to the jobs. When set to \"default\" group will be added to the volume label when no other job label exist in volume"
// +kubebuilder:printcolumn:name="Task",type=string,JSONPath=`.spec.task`,description="Should be one of \"backup\" or \"snapshot\""
// +kubebuilder:printcolumn:name="Cron",type=string,JSONPath=`.spec.cron`,description="The cron expression represents recurring job scheduling"
// +kubebuilder:printcolumn:name="Retain",type=integer,JSONPath=`.spec.retain`,description="The number of snapshots/backups to keep for the volume"
// +kubebuilder:printcolumn:name="Concurrency",type=integer,JSONPath=`.spec.concurrency`,description="The concurrent job to run by each cron job"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Labels",type=string,JSONPath=`.spec.labels`,description="Specify the labels"
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
