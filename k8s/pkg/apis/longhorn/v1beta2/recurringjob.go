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

// +kubebuilder:validation:Enum=count-based;age-based
type RecurringJobRetentionPolicy string

const (
	// RecurringJobRetentionPolicyCountBased retains snapshots/backups based on the configured count,
	// keeping the newest ones and deleting older ones. RetainAge is not consulted.
	RecurringJobRetentionPolicyCountBased = RecurringJobRetentionPolicy("count-based")
	// RecurringJobRetentionPolicyAgeBased retains snapshots/backups based on their age,
	// deleting those older than the configured RetainAge. Retain is not consulted.
	RecurringJobRetentionPolicyAgeBased = RecurringJobRetentionPolicy("age-based")
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
	// Retain represents the number of snapshots/backups to retain and only when the retention policy is "count-based".
	// +optional
	Retain int `json:"retain"`
	// The retention age of the snapshot/backup, specified as a Go duration string,
	// such as "10m", "24h", or "8760h". Note that Go durations have no day or year unit,
	// so a day is "24h". Snapshots/backups older than this are cleaned up by the recurring job.
	// Only takes effect when the retention policy is "age-based".
	// If the retention policy is "age-based", this value is 0s, the recurring job will not start.
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('-')",message="retainAge must be a positive duration"
	RetainAge metav1.Duration `json:"retainAge,omitempty"`
	// The retention policy that determines whether the recurring job cleans up
	// snapshots/backups based on their count or age. Can be "count-based" or
	// "age-based". The two policies work independently: "count-based" (the default)
	// retains the configured number of newest snapshots/backups and ignores
	// RetainAge, while "age-based" retains snapshots/backups no older than RetainAge
	// and ignores Retain.
	// +kubebuilder:default:=count-based
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
// +kubebuilder:printcolumn:name="RetentionPolicy",type=string,JSONPath=`.spec.retentionPolicy`,description="Whether snapshots/backups are retained based on count (\"count-based\") or age (\"age-based\")"
// +kubebuilder:printcolumn:name="RetainCount",type=integer,JSONPath=`.spec.retain`,description="The number of snapshots/backups to keep for the volume"
// +kubebuilder:printcolumn:name="RetainAge",type=string,JSONPath=`.spec.retainAge`,description="Snapshots/backups older than this duration are cleaned up when the retention policy is \"age-based\""
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
