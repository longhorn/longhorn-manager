package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +kubebuilder:validation:Enum=snapshot;snapshot-force-create;snapshot-cleanup;snapshot-delete;backup;backup-force-create;filesystem-trim;system-backup
type RecurringJobType string

const (
	RecurringJobTypeSnapshot            = RecurringJobType("snapshot")              // periodically create snapshots except for old snapshots cleanup failed before creating new snapshots
	RecurringJobTypeSnapshotForceCreate = RecurringJobType("snapshot-force-create") // periodically create snapshots even if old snapshots cleanup failed
	RecurringJobTypeSnapshotCleanup     = RecurringJobType("snapshot-cleanup")      // periodically purge removable snapshots and system snapshots
	RecurringJobTypeSnapshotDelete      = RecurringJobType("snapshot-delete")       // periodically remove and purge all kinds of snapshots that exceed the retention count
	RecurringJobTypeBackup              = RecurringJobType("backup")                // periodically create snapshots then do backups
	RecurringJobTypeBackupForceCreate   = RecurringJobType("backup-force-create")   // periodically create snapshots then do backups even if old snapshots cleanup failed
	RecurringJobTypeFilesystemTrim      = RecurringJobType("filesystem-trim")       // periodically trim filesystem to reclaim disk space
	RecurringJobTypeSystemBackup        = RecurringJobType("system-backup")         // periodically create system backups

	RecurringJobGroupDefault = "default"
)

// +kubebuilder:validation:Enum=count-base;age-base
type RecurringJobRetentionPolicy string

const (
	// RecurringJobRetentionPolicyCountBase cleans up the snapshots/backups beyond
	// the newest Retain ones. RetainAge is not consulted.
	RecurringJobRetentionPolicyCountBase = RecurringJobRetentionPolicy("count-base")
	// RecurringJobRetentionPolicyAgeBase cleans up the snapshots/backups that have
	// existed for longer than RetainAge. Retain is not consulted.
	RecurringJobRetentionPolicyAgeBase = RecurringJobRetentionPolicy("age-base")
)

type VolumeRecurringJob struct {
	Name    string `json:"name"`
	IsGroup bool   `json:"isGroup"`
}

// VolumeRecurringJobInfo defines the Longhorn recurring job information stored in the backup volume configuration
type VolumeRecurringJobInfo struct {
	JobSpec   RecurringJobSpec `json:"jobSpec"`
	FromGroup []string         `json:"fromGroup,omitempty"`
	FromJob   bool             `json:"fromJob"`
}

// RecurringJobSpec defines the desired state of the Longhorn recurring job
type RecurringJobSpec struct {
	// The recurring job name.
	// +optional
	Name string `json:"name"`
	// The recurring job group.
	// +optional
	Groups []string `json:"groups,omitempty"`
	// The recurring job task.
	// Can be "snapshot", "snapshot-force-create", "snapshot-cleanup", "snapshot-delete", "backup", "backup-force-create", "filesystem-trim" or "system-backup".
	// +optional
	Task RecurringJobType `json:"task"`
	// The cron setting.
	// +optional
	Cron string `json:"cron"`
	// The retain count of the snapshot/backup.
	// Only takes effect when the retention policy is "count-base".
	// +optional
	Retain int `json:"retain"`
	// The retain age of the snapshot/backup, as a Go duration string such as
	// "10m", "24h" or "8760h". Note that Go durations have no day or year unit,
	// so a day is "24h". Snapshots/backups that have existed for longer than
	// this are cleaned up by the recurring job.
	// Only takes effect when the retention policy is "age-base".
	// +optional
	RetainAge metav1.Duration `json:"retainAge,omitempty"`
	// The retention policy that decides which of the retain count and the retain
	// age the recurring job cleans up by. Can be "count-base" or "age-base".
	// The two work independently: "count-base" (the default) keeps the newest
	// Retain snapshots/backups and ignores RetainAge, while "age-base" keeps the
	// ones that have existed for no longer than RetainAge and ignores Retain.
	// +kubebuilder:default:=count-base
	// +optional
	RetentionPolicy RecurringJobRetentionPolicy `json:"retentionPolicy,omitempty"`
	// The concurrency of taking the snapshot/backup.
	// +optional
	Concurrency int `json:"concurrency"`
	// The label of the snapshot/backup.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// The parameters of the snapshot/backup.
	// Support parameters: "full-backup-interval", "volume-backup-policy".
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// RecurringJobStatus defines the observed state of the Longhorn recurring job
type RecurringJobStatus struct {
	// The owner ID which is responsible to reconcile this recurring job CR.
	// +optional
	OwnerID string `json:"ownerID"`
	// The number of jobs that have been triggered.
	// +optional
	ExecutionCount int `json:"executionCount"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhrj
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Groups",type=string,JSONPath=`.spec.groups`,description="Sets groupings to the jobs. When set to \"default\" group will be added to the volume label when no other job label exist in volume"
// +kubebuilder:printcolumn:name="Task",type=string,JSONPath=`.spec.task`,description="Should be one of \"snapshot\", \"snapshot-force-create\", \"snapshot-cleanup\", \"snapshot-delete\", \"backup\", \"backup-force-create\", \"filesystem-trim\" or \"system-backup\""
// +kubebuilder:printcolumn:name="Cron",type=string,JSONPath=`.spec.cron`,description="The cron expression represents recurring job scheduling"
// +kubebuilder:printcolumn:name="Retain",type=integer,JSONPath=`.spec.retain`,description="The number of snapshots/backups to keep for the volume"
// +kubebuilder:printcolumn:name="RetainAge",type=string,JSONPath=`.spec.retainAge`,description="Snapshots/backups older than this duration are cleaned up when the retention policy is \"age-base\""
// +kubebuilder:printcolumn:name="RetentionPolicy",type=string,JSONPath=`.spec.retentionPolicy`,description="Whether the job cleans up by the retain count (\"count-base\") or by the retain age (\"age-base\")"
// +kubebuilder:printcolumn:name="Concurrency",type=integer,JSONPath=`.spec.concurrency`,description="The concurrent job to run by each cron job"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Labels",type=string,JSONPath=`.spec.labels`,description="Specify the labels"

// RecurringJob is where Longhorn stores recurring job object.
type RecurringJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RecurringJobSpec   `json:"spec,omitempty"`
	Status RecurringJobStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RecurringJobList is a list of RecurringJobs.
type RecurringJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RecurringJob `json:"items"`
}
