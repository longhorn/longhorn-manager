package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type BackupState string

const (
	BackupStateInProgress = BackupState("InProgress")
	BackupStateCompleted  = BackupState("Completed")
	BackupStateError      = BackupState("Error")
	BackupStateUnknown    = BackupState("Unknown")
)

type SnapshotBackupSpec struct {
	// +optional
	SyncRequestedAt metav1.Time `json:"syncRequestedAt"`
	// +optional
	SnapshotName string `json:"snapshotName"`
	// +optional
	Labels map[string]string `json:"labels"`
}

type SnapshotBackupStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	State BackupState `json:"state"`
	// +optional
	URL string `json:"url"`
	// +optional
	SnapshotName string `json:"snapshotName"`
	// +optional
	SnapshotCreatedAt string `json:"snapshotCreatedAt"`
	// +optional
	BackupCreatedAt string `json:"backupCreatedAt"`
	// +optional
	Size string `json:"size"`
	// +optional
	Labels map[string]string `json:"labels"`
	// +optional
	Messages map[string]string `json:"messages"`
	// +optional
	VolumeName string `json:"volumeName"`
	// +optional
	VolumeSize string `json:"volumeSize"`
	// +optional
	VolumeCreated string `json:"volumeCreated"`
	// +optional
	VolumeBackingImageName string `json:"volumeBackingImageName"`
	// +optional
	LastSyncedAt metav1.Time `json:"lastSyncedAt"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type Backup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotBackupSpec   `json:"spec,omitempty"`
	Status SnapshotBackupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}
