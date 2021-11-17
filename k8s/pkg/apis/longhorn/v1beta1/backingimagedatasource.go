package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	DataSourceTypeDownloadParameterURL = "url"
)

type BackingImageDataSourceType string

const (
	BackingImageDataSourceTypeDownload         = BackingImageDataSourceType("download")
	BackingImageDataSourceTypeUpload           = BackingImageDataSourceType("upload")
	BackingImageDataSourceTypeExportFromVolume = BackingImageDataSourceType("export-from-volume")
)

type BackingImageDataSourceSpec struct {
	// +optional
	NodeID string `json:"nodeID"`
	// +optional
	DiskUUID string `json:"diskUUID"`
	// +optional
	DiskPath string `json:"diskPath"`
	// +optional
	Checksum string `json:"checksum"`
	// +optional
	SourceType BackingImageDataSourceType `json:"sourceType"`
	// +optional
	Parameters map[string]string `json:"parameters"`
	// +optional
	FileTransferred bool `json:"fileTransferred"`
}

type BackingImageDataSourceStatus struct {
	// +optional
	OwnerID string `json:"ownerID"`
	// +optional
	RunningParameters map[string]string `json:"runningParameters"`
	// +optional
	CurrentState BackingImageState `json:"currentState"`
	// +optional
	Size int64 `json:"size"`
	// +optional
	Progress int `json:"progress"`
	// +optional
	Checksum string `json:"checksum"`
	// +optional
	Message string `json:"message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BackingImageDataSource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackingImageDataSourceSpec   `json:"spec,omitempty"`
	Status BackingImageDataSourceStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BackingImageDataSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackingImageDataSource `json:"items"`
}
