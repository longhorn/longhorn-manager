package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	DiskScheduleConditionTypeReady = "Ready"
)

type DiskScheduleState string

const (
	DiskScheduledStateScheduled = DiskScheduleState("scheduled")
	DiskScheduledStateRejected  = DiskScheduleState("rejected")
	DiskScheduledStateUnknown   = DiskScheduleState("unknown")
)

type DiskScheduledResourcesStatus struct {
	// Scheduled state
	State DiskScheduleState `json:"state"`
	// Scheduled size
	// +optional
	Size int64 `json:"scheduledSize"`
}

// DiskScheduleSpec defines the desired state of the Longhorn disk schedule
type DiskScheduleSpec struct {
	// Disk name from node CR
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Disk name is immutable"
	Name string `json:"name"`
	// Node ID that the disk locates
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="NodeID is immutable"
	NodeID string `json:"nodeID"`
	// Scheduling requests for volume replicas
	// +optional
	Replicas map[string]int64 `json:"replicas"`
	// Scheduling requests for backing images
	// +optional
	BackingImages map[string]int64 `json:"backingImages"`
}

// DiskScheduleStatus defines the observed state of the Longhorn disk schedule
type DiskScheduleStatus struct {
	// Scheduled status of volume replicas
	// +optional
	Replicas map[string]*DiskScheduledResourcesStatus `json:"replicas"`
	// Scheduled status of backing images
	// +optional
	BackingImages map[string]*DiskScheduledResourcesStatus `json:"backingImages"`
	// Sum of successfully scheduled storage
	// +optional
	StorageScheduled int64 `json:"storageScheduled"`
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhds
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeID`,description="The node of the disk"
// +kubebuilder:printcolumn:name="Disk",type=string,JSONPath=`.spec.name`,description="The disk that is serving"

// DiskSchedule is where Longhorn stores disk scheduling status.
type DiskSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DiskScheduleSpec   `json:"spec,omitempty"`
	Status DiskScheduleStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DiskScheduleList is a list of DiskSchedule.
type DiskScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DiskSchedule `json:"items"`
}

// Hub defines the current version (v1beta2) is the storage version
// so mark this as Hub
func (ds *DiskSchedule) Hub() {}

func (ds *DiskSchedule) GetReplicaRequirement(replicaName string) int64 {
	if ds.Spec.Replicas == nil {
		return 0
	}
	return ds.Spec.Replicas[replicaName]
}

// SetReplicaRequirement update replica disk requirement in the disk schedule spec, and the caller is responsible to apply changes.
// It removes the entry if size is 0.
func (ds *DiskSchedule) SetReplicaRequirement(replicaName string, size int64) {
	ds.setRequirement(&ds.Spec.Replicas, replicaName, size)
}

func (ds *DiskSchedule) GetReplicaScheduledStatus(replicaName string) *DiskScheduledResourcesStatus {
	return ds.getResourceStatus(ds.Status.Replicas, replicaName)
}

func (ds *DiskSchedule) GetBackingImageRequirement(biName string) int64 {
	if ds.Spec.BackingImages == nil {
		return 0
	}
	return ds.Spec.BackingImages[biName]
}

// SetBackingImageRequirement update backing image disk requirement in the disk schedule spec, and the caller is responsible to apply changes.
// It removes the entry if size is 0.
func (ds *DiskSchedule) SetBackingImageRequirement(biName string, size int64) {
	ds.setRequirement(&ds.Spec.BackingImages, biName, size)
}

func (ds *DiskSchedule) GetBackingImageScheduledStatus(biName string) *DiskScheduledResourcesStatus {
	return ds.getResourceStatus(ds.Status.BackingImages, biName)
}

func (ds *DiskSchedule) setRequirement(specMap *map[string]int64, resourceName string, size int64) {
	if size == 0 {
		delete(*specMap, resourceName)
	}

	if *specMap == nil {
		*specMap = make(map[string]int64)
	}
	(*specMap)[resourceName] = size
}

func (ds *DiskSchedule) getResourceStatus(statusMap map[string]*DiskScheduledResourcesStatus, resourceName string) *DiskScheduledResourcesStatus {
	var status *DiskScheduledResourcesStatus = nil
	if statusMap != nil {
		status = statusMap[resourceName]
	}
	if status != nil {
		return status
	}
	return &DiskScheduledResourcesStatus{
		State: DiskScheduledStateUnknown,
		Size:  0,
	}
}
