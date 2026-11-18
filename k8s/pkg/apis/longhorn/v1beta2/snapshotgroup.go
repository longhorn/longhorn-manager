package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SnapshotGroupPhase is the lifecycle phase of a SnapshotGroup. Empty until the
// first reconcile; a group then moves InProgress -> Ready | Failed. Ready and
// Failed are terminal: after either, the controller never creates another
// member snapshot.
// +kubebuilder:validation:Enum=InProgress;Ready;Failed;""
type SnapshotGroupPhase string

const (
	// SnapshotGroupPhaseInProgress means member snapshots are being taken
	// through the existing per-volume path.
	SnapshotGroupPhaseInProgress = SnapshotGroupPhase("InProgress")
	// SnapshotGroupPhaseReady means every member became ready with its creation
	// time at or before creationTimestamp + deadlineSeconds.
	SnapshotGroupPhaseReady = SnapshotGroupPhase("Ready")
	// SnapshotGroupPhaseFailed means the deadline passed before every member
	// snapshot was taken. Member snapshots already taken are kept until the
	// group is deleted; the group itself is never trusted as consistent.
	SnapshotGroupPhaseFailed = SnapshotGroupPhase("Failed")
)

const (
	// SnapshotGroupConditionTypeDegraded is set on a Ready group whose member
	// set is no longer complete (a member snapshot deleted out-of-band, or lost
	// in a restore). The group never takes replacement snapshots: a replacement
	// taken later would silently break the point-in-time set the group
	// represents.
	SnapshotGroupConditionTypeDegraded = "Degraded"
)

// SnapshotGroupMember identifies one member of the group: the volume and the
// name of its member Snapshot CR, generated at admission.
type SnapshotGroupMember struct {
	VolumeName   string `json:"volumeName"`
	SnapshotName string `json:"snapshotName"`
}

// SnapshotGroupSpec defines the desired state of the Longhorn SnapshotGroup.
// The whole spec is immutable after creation: a group is a point-in-time
// request, so changing members later has no meaning.
type SnapshotGroupSpec struct {
	// Volumes explicitly lists the member volumes. Exactly one of Volumes or
	// VolumeSelector may be set at creation; the mutating webhook resolves the
	// selection into Members at admission.
	// +optional
	// +nullable
	Volumes []string `json:"volumes"`
	// VolumeSelector selects the member volumes by their labels. Exactly one of
	// Volumes or VolumeSelector may be set at creation.
	// +optional
	VolumeSelector *metav1.LabelSelector `json:"volumeSelector"`
	// Labels are engine snapshot labels applied to every member (Snapshot
	// spec.labels). They are not visible to Kubernetes label selectors.
	// Reserved recurring-job label keys are rejected at admission.
	// +optional
	// +nullable
	Labels map[string]string `json:"labels"`
	// DeadlineSeconds is the deadline for taking every member snapshot,
	// measured from the group's metadata.creationTimestamp. When the field is
	// omitted, the CRD default applies. Go clients cannot omit the field: an
	// unset field arrives as 0, and the mutating webhook replaces the 0 with
	// the default.
	// +optional
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=3600
	DeadlineSeconds int64 `json:"deadlineSeconds"`
	// Members is the fixed member set, resolved from Volumes or VolumeSelector
	// and stamped by the mutating webhook at admission. It may not be set by
	// the user.
	// +optional
	// +nullable
	Members []SnapshotGroupMember `json:"members"`
}

// SnapshotGroupMemberStatus is the observed state of one member, mirrored from
// its Snapshot CR.
type SnapshotGroupMemberStatus struct {
	VolumeName   string `json:"volumeName"`
	SnapshotName string `json:"snapshotName"`
	// ReadyToUse mirrors the member Snapshot while the group is InProgress.
	// After the group is Ready, a member later deleted or unusable is recorded
	// here as false: member entries always tell the per-member truth.
	// +optional
	ReadyToUse bool `json:"readyToUse"`
	// CreationTime is the engine snapshot creation time, mirrored from the
	// member Snapshot. It is kept when the member is later lost.
	// +optional
	CreationTime string `json:"creationTime"`
	// Error is the last member error. A member Snapshot CR that disappeared
	// after the group became Ready is recorded with the synthetic error
	// "member snapshot deleted"; an unusable member keeps its own mirrored
	// error.
	// +optional
	Error string `json:"error,omitempty"`
}

// SnapshotGroupStatus defines the observed state of the Longhorn SnapshotGroup
type SnapshotGroupStatus struct {
	// OwnerID is the ID of the node that owns this SnapshotGroup.
	// +optional
	OwnerID string `json:"ownerID"`
	// Phase is the lifecycle phase: empty until the first reconcile, then
	// InProgress -> Ready | Failed.
	// +optional
	Phase SnapshotGroupPhase `json:"phase"`
	// Members is the observed state of every member, one entry per spec
	// member. It is the primary debugging signal: each entry carries the
	// member's last error and creation time.
	// +optional
	// +nullable
	Members []SnapshotGroupMemberStatus `json:"members"`
	// ReadyToUse is true when the group is Ready and every member snapshot
	// is still individually ready: group readiness is the AND of member
	// readiness, following the Kubernetes VolumeGroupSnapshot convention. It
	// drops to false when a member is lost or unusable after Ready and
	// recovers once every member is whole again; the Degraded condition
	// carries the per-member detail. A failed, partial, or empty group is
	// never reported ready.
	// +optional
	ReadyToUse bool `json:"readyToUse"`
	// CreationTime is the latest member creation time, set when the group
	// becomes Ready.
	// +optional
	CreationTime string `json:"creationTime"`
	// Error is set when the group fails (deadline, name collision).
	// +optional
	Error string `json:"error,omitempty"`
	// Conditions holds the latest observations of the SnapshotGroup's state,
	// such as Degraded.
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhsnapg
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`,description="The lifecycle phase of the group"
// +kubebuilder:printcolumn:name="ReadyToUse",type=boolean,JSONPath=`.status.readyToUse`,description="True when the group is Ready and every member is still usable"
// +kubebuilder:printcolumn:name="CreationTime",type=string,JSONPath=`.status.creationTime`,description="The latest member creation time, set when the group becomes Ready"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SnapshotGroup is the Schema for the snapshotgroups API
type SnapshotGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotGroupSpec   `json:"spec,omitempty"`
	Status SnapshotGroupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SnapshotGroupList contains a list of SnapshotGroup
type SnapshotGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapshotGroup `json:"items"`
}
