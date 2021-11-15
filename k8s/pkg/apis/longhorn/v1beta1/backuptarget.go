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
// +kubebuilder:resource:shortName=lhbt
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.spec.backupTargetURL`,description="The backup target URL"
// +kubebuilder:printcolumn:name="Credential",type=string,JSONPath=`.spec.credentialSecret`,description="The backup target credential secret"
// +kubebuilder:printcolumn:name="LastBackupAt",type=string,JSONPath=`.spec.pollInterval`,description="The backup target poll interval"
// +kubebuilder:printcolumn:name="Available",type=boolean,JSONPath=`.status.available`,description="Indicate whether the backup target is available or not"
// +kubebuilder:printcolumn:name="LastSyncedAt",type=string,JSONPath=`.status.lastSyncedAt`,description="The backup target last synced time"
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
