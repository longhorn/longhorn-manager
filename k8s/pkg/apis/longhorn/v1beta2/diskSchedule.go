package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	DiskScheduleConditionTypeReady = "Ready"
)

type DiskScheduleResult string

const (
	DiskScheduledResultScheduled = DiskScheduleResult("scheduled")
	DiskScheduledResultRejected  = DiskScheduleResult("rejected")
)

type DiskScheduledResourcesStatus struct {
	// Scheduled result
	Result DiskScheduleResult `json:"result"`
	// Required size
	// +optional
	RequiredSize int64 `json:"requiredSize"`
	// Scheduled size
	// +optional
	ScheduledSize int64 `json:"scheduledSize"`
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
	SchedulingReplicas map[string]int64 `json:"schedulingReplicas"`
	// Scheduling requests for backing images
	// +optional
	SchedulingBackingImages map[string]int64 `json:"schedulingBackingImages"`
}

// DiskScheduleStatus defines the observed state of the Longhorn disk schedule
type DiskScheduleStatus struct {
	// Scheduled status of volume replicas
	// +optional
	ScheduledReplicaStatus map[string]*DiskScheduledResourcesStatus `json:"scheduledReplicaStatus"`
	// Scheduled status of backing images
	// +optional
	ScheduledBackingImageStatus map[string]*DiskScheduledResourcesStatus `json:"scheduledBackingImageStatus"`
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

func (ds *DiskSchedule) GetReplicaScheduledStatus(replicaName string) *DiskScheduledResourcesStatus {
	if ds.Status.ScheduledReplicaStatus == nil {
		return nil
	}
	return ds.Status.ScheduledReplicaStatus[replicaName]
}

func (ds *DiskSchedule) GetReplicaScheduling(replicaName string) int64 {
	if ds.Spec.SchedulingReplicas == nil {
		return 0
	}
	return ds.Spec.SchedulingReplicas[replicaName]
}

// SetReplicaScheduling update replica disk requirement in the disk schedule spec, and the caller is responsible to apply changes.
// It removes the entry if size is 0.
func (ds *DiskSchedule) SetReplicaScheduling(replicaName string, size int64) *DiskSchedule {
	if size == 0 {
		delete(ds.Spec.SchedulingReplicas, replicaName)
		return ds
	}

	if ds.Spec.SchedulingReplicas == nil {
		ds.Spec.SchedulingReplicas = make(map[string]int64)
	}
	ds.Spec.SchedulingReplicas[replicaName] = size
	return ds
}

func (ds *DiskSchedule) GetBackingImageScheduledStatus(biName string) *DiskScheduledResourcesStatus {
	if ds == nil || ds.Status.ScheduledBackingImageStatus == nil {
		return nil
	}
	return ds.Status.ScheduledBackingImageStatus[biName]
}

func (ds *DiskSchedule) GetBackingImageScheduling(biName string) int64 {
	if ds.Spec.SchedulingBackingImages == nil {
		return 0
	}
	return ds.Spec.SchedulingBackingImages[biName]
}

// SetBackingImageScheduling update backing image disk requirement in the disk schedule spec, and the caller is responsible to apply changes.
// It removes the entry if size is 0.
func (ds *DiskSchedule) SetBackingImageScheduling(biName string, size int64) *DiskSchedule {
	if size == 0 {
		delete(ds.Spec.SchedulingBackingImages, biName)
		return ds
	}

	if ds.Spec.SchedulingBackingImages == nil {
		ds.Spec.SchedulingBackingImages = make(map[string]int64)
	}
	ds.Spec.SchedulingBackingImages[biName] = size
	return ds
}
