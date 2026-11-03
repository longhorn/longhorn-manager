package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SnapshotGroupPhase is the lifecycle phase of a SnapshotGroup. It is empty
// until the first reconcile, then moves InProgress -> Ready or Failed. A
// hooked group passes through RunningPreHooks first. Ready and Failed are
// terminal: the controller creates no more member snapshots after either.
// +kubebuilder:validation:Enum=RunningPreHooks;InProgress;Ready;Failed;""
type SnapshotGroupPhase string

const (
	// SnapshotGroupPhaseRunningPreHooks means the hook executor is running
	// the members' pre-commands. No member snapshot is taken in this phase.
	SnapshotGroupPhaseRunningPreHooks = SnapshotGroupPhase("RunningPreHooks")
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

// SnapshotGroupHooks selects whether VolumeHookPolicy commands run around
// the member snapshots. Any value other than none means the group is hooked.
// +kubebuilder:validation:Enum=none;command;""
type SnapshotGroupHooks string

const (
	// SnapshotGroupHooksNone means no hook runs. The group takes plain
	// snapshots.
	SnapshotGroupHooksNone = SnapshotGroupHooks("none")
	// SnapshotGroupHooksCommand means every member's pre-command must succeed
	// before any member snapshot is taken, and every member's post-command
	// runs afterwards. If any pre-command fails, the group ends Failed.
	SnapshotGroupHooksCommand = SnapshotGroupHooks("command")
)

// SnapshotGroupMember identifies one member of the group: the volume and the
// name of its member Snapshot CR, generated at admission.
type SnapshotGroupMember struct {
	VolumeName   string `json:"volumeName"`
	SnapshotName string `json:"snapshotName"`
	// HookPolicyRef is the VolumeHookPolicy that applies to this member. The
	// mutating webhook sets it at admission for a hooked group. It is unset
	// when Hooks is none.
	// +optional
	HookPolicyRef *VolumeHookPolicyRef `json:"hookPolicyRef,omitempty"`
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
	// DeadlineSeconds bounds how long the whole group may take, measured from
	// metadata.creationTimestamp. When omitted or 0, admission sets 300, or 60
	// for a hooked group because the deadline also bounds the time between the
	// pre- and post-commands. The default is set by the webhook, not the
	// schema, because it depends on Hooks.
	// +optional
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=3600
	DeadlineSeconds int64 `json:"deadlineSeconds"`
	// Hooks selects whether VolumeHookPolicy commands run around the member
	// snapshots: none (default) or command.
	// +optional
	// +kubebuilder:default=none
	Hooks SnapshotGroupHooks `json:"hooks,omitempty"`
	// HookOperation is the operation the hooks run for: snapshot or backup.
	// Set only for a hooked group; admission defaults it to snapshot.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="hookOperation is immutable"
	HookOperation HookOperation `json:"hookOperation,omitempty"`
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
	// Phase is the lifecycle phase. It is empty until the first reconcile,
	// then moves InProgress -> Ready or Failed. A hooked group passes through
	// RunningPreHooks first.
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
	// PreHooksDone is set by the hook executor once every member's
	// pre-command has succeeded. It is the controller's signal to leave
	// RunningPreHooks.
	// +optional
	PreHooksDone bool `json:"preHooksDone,omitempty"`
	// HooksStartedAt is when the executor started on the first member. The
	// time between creationTimestamp and this is queue wait, not hook time.
	// +optional
	HooksStartedAt *metav1.Time `json:"hooksStartedAt,omitempty"`
	// PreHooksDoneAt is when the last pre-command succeeded, set by the
	// executor. The application is paused from here until PostHooksDoneAt.
	// +optional
	PreHooksDoneAt *metav1.Time `json:"preHooksDoneAt,omitempty"`
	// PostHooksDoneAt is when every recorded pod reached PostDone or
	// PostFailed. Unset when nothing was paused.
	// +optional
	PostHooksDoneAt *metav1.Time `json:"postHooksDoneAt,omitempty"`
	// HookProgress summarizes hook progress for the HookProgress printer
	// column, for example "pre 2/3" or "post done". The hook executor
	// maintains it. Empty when Hooks is none.
	// +optional
	HookProgress string `json:"hookProgress,omitempty"`
	// Hooks records the hook outcome of each member. An entry's snapshotName
	// matches the member in spec.members. Only the hook executor writes it. It
	// never includes the command itself or its output.
	// +optional
	// +nullable
	Hooks []HookStatus `json:"hooks,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhsnapg
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`,description="The lifecycle phase of the group"
// +kubebuilder:printcolumn:name="HookProgress",type=string,JSONPath=`.status.hookProgress`,description="Pre and post hook command progress; empty when spec.hooks is none"
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
