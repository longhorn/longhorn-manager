package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ImageState string

const (
	ImageStateDeploying = ImageState("deploying")
	ImageStateDeployed  = ImageState("deployed")
	ImageStateError     = ImageState("error")
	ImageStateUnknown   = ImageState("unknown")
)

const (
	PrePullImageName = "pre-pull-manager-images"
)

// ImageSpec defines the desired state of the Longhorn image
type ImageSpec struct {
	// +optional
	ImageURL string `json:"imageURL"`
}

// ImageStatus defines the observed state of the Longhorn image
type ImageStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	State ImageState `json:"state"`
	// +optional
	// +nullable
	NodeDeploymentMap map[string]bool `json:"nodeDeploymentMap"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhi
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="State of the image"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Image is where Longhorn stores image objects.
type Image struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageSpec   `json:"spec,omitempty"`
	Status ImageStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ImageList is a list of Images.
type ImageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Image `json:"items"`
}

// Hub defines the current version (v1beta2) is the storage version
// so mark this as Hub
func (i *Image) Hub() {}
