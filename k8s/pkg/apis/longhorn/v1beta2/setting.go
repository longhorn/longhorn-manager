package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhs
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Value",type=string,JSONPath=`.value`,description="The value of the setting"
// +kubebuilder:printcolumn:name="Applied",type=boolean,JSONPath=`.status.applied`,description="The setting is applied"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Setting is where Longhorn stores setting object.
type Setting struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// The value of the setting.
	Value string `json:"value"`
	// The status of the setting.
	Status SettingStatus `json:"status,omitempty"`
	// Default values for the setting based on the data engine type.
	// If the value for a specific data engine type is not set, the default value will be used.
	DefaultsByEngine map[DataEngineType]string `json:"defaultsByEngine,omitempty"`
	// ApplicableEngines defines which data engines the setting is applicable to.
	// If empty, the setting is applicable to all data engines.
	ApplicableEngines map[DataEngineType]bool `json:"applicableEngines,omitempty"`
}

// SettingStatus defines the observed state of the Longhorn setting
type SettingStatus struct {
	// The setting is applied.
	Applied bool `json:"applied"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SettingList is a list of Settings.
type SettingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Setting `json:"items"`
}
