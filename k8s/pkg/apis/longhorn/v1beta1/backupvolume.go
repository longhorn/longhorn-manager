package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type BackupVolumeSpec struct {
	// +optional
	SyncRequestedAt metav1.Time `json:"syncRequestedAt"`
}

type BackupVolumeStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	LastModificationTime metav1.Time `json:"lastModificationTime"`
	// +optional
	Size string `json:"size"`
	// +optional
	Labels map[string]string `json:"labels"`
	// +optional
	CreatedAt string `json:"createdAt"`
	// +optional
	LastBackupName string `json:"lastBackupName"`
	// +optional
	LastBackupAt string `json:"lastBackupAt"`
	// +optional
	DataStored string `json:"dataStored"`
	// +optional
	Messages map[string]string `json:"messages"`
	// +optional
	BackingImageName string `json:"backingImageName"`
	// +optional
	BackingImageChecksum string `json:"backingImageChecksum"`
	// +optional
	LastSyncedAt metav1.Time `json:"lastSyncedAt"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BackupVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupVolumeSpec   `json:"spec,omitempty"`
	Status BackupVolumeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BackupVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupVolume `json:"items"`
}
