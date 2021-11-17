package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	BackupTargetConditionTypeUnavailable = "Unavailable"

	BackupTargetConditionReasonUnavailable = "Unavailable"
)

type BackupTargetSpec struct {
	// +optional
	BackupTargetURL string `json:"backupTargetURL"`
	// +optional
	CredentialSecret string `json:"credentialSecret"`
	// +optional
	PollInterval metav1.Duration `json:"pollInterval"`
	// +optional
	SyncRequestedAt metav1.Time `json:"syncRequestedAt"`
}

type BackupTargetStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	Available bool `json:"available"`
	// +optional
	Conditions map[string]Condition `json:"conditions"`
	// +optional
	LastSyncedAt metav1.Time `json:"lastSyncedAt"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BackupTarget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupTargetSpec   `json:"spec,omitempty"`
	Status BackupTargetStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BackupTargetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupTarget `json:"items"`
}
