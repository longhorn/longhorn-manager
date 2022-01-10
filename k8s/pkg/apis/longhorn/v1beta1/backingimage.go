package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// BackingImageDownloadState is replaced by BackingImageState.
type BackingImageDownloadState string

type BackingImageState string

const (
	BackingImageStatePending          = BackingImageState("pending")
	BackingImageStateStarting         = BackingImageState("starting")
	BackingImageStateReadyForTransfer = BackingImageState("ready-for-transfer")
	BackingImageStateReady            = BackingImageState("ready")
	BackingImageStateInProgress       = BackingImageState("in-progress")
	BackingImageStateFailed           = BackingImageState("failed")
	BackingImageStateUnknown          = BackingImageState("unknown")
)

type BackingImageDiskFileStatus struct {
	State                   BackingImageState `json:"state"`
	Progress                int               `json:"progress"`
	Message                 string            `json:"message"`
	LastStateTransitionTime string            `json:"lastStateTransitionTime"`
}

// BackingImageSpec defines the desired state of the Longhorn backing image
type BackingImageSpec struct {
	Disks            map[string]struct{}        `json:"disks"`
	Checksum         string                     `json:"checksum"`
	SourceType       BackingImageDataSourceType `json:"sourceType"`
	SourceParameters map[string]string          `json:"sourceParameters"`

	// Deprecated: This kind of info will be included in the related BackingImageDataSource.
	ImageURL string `json:"imageURL"`
}

// BackingImageStatus defines the observed state of the Longhorn backing image status
type BackingImageStatus struct {
	OwnerID           string                                 `json:"ownerID"`
	UUID              string                                 `json:"uuid"`
	Size              int64                                  `json:"size"`
	Checksum          string                                 `json:"checksum"`
	DiskFileStatusMap map[string]*BackingImageDiskFileStatus `json:"diskFileStatusMap"`
	DiskLastRefAtMap  map[string]string                      `json:"diskLastRefAtMap"`

	// Deprecated: Replaced by field `State` in `DiskFileStatusMap`.
	DiskDownloadStateMap map[string]BackingImageDownloadState `json:"diskDownloadStateMap"`
	// Deprecated: Replaced by field `Progress` in `DiskFileStatusMap`.
	DiskDownloadProgressMap map[string]int `json:"diskDownloadProgressMap"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhbi
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`,description="The backing image name"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackingImage is where Longhorn stores backing image object.
type BackingImage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Spec BackingImageSpec `json:"spec,omitempty"`
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Status BackingImageStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BackingImageList is a list of BackingImages.
type BackingImageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackingImage `json:"items"`
}
