package v1beta2

import (
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeHookPolicyDefaultHookName is the hook name used when a single-hook
// policy omits spec.hooks[0].name.
const VolumeHookPolicyDefaultHookName = "default"

// HookOperation is a Longhorn operation that hooks can run before and after.
// +kubebuilder:validation:Enum=snapshot;backup
type HookOperation string

const (
	// HookOperationSnapshot is a SnapshotGroup taking member snapshots.
	HookOperationSnapshot HookOperation = "snapshot"
	// HookOperationBackup is a SnapshotGroup taking member snapshots that
	// are then backed up.
	HookOperationBackup HookOperation = "backup"
)

// VolumeHookPolicyExec is a pre/post command pair run with pods/exec in the
// pods that mount a covered PVC. Commands are argv arrays and are not
// interpolated; a command that needs a shell must invoke one.
type VolumeHookPolicyExec struct {
	// PodSelector selects which of the pods mounting a covered PVC run this
	// hook. Every running pod that mounts a member volume must match exactly
	// one hook. May be omitted only when the policy has a single hook, in
	// which case the hook matches every pod.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`
	// Container is the container the commands run in. Required when a
	// matched pod has more than one regular container or native sidecar
	// (init container with restartPolicy Always).
	// +optional
	Container string `json:"container,omitempty"`
	// PreCommand runs in each matched pod before the operation starts. It
	// must be idempotent and leave the application in a consistent state
	// until PostCommand runs.
	// +kubebuilder:validation:MinItems=1
	PreCommand []string `json:"preCommand"`
	// PostCommand runs in each matched pod after the operation completes or
	// fails. It must be idempotent and safe to run even if PreCommand never
	// completed.
	// +kubebuilder:validation:MinItems=1
	PostCommand []string `json:"postCommand"`
}

// VolumeHookPolicyHook is one hook of a policy. Exec is the only handler.
type VolumeHookPolicyHook struct {
	// Name identifies the hook in status and events. Required when the
	// policy has more than one hook; a single hook defaults to "default".
	// +optional
	Name string `json:"name,omitempty"`
	// Operations lists the operations this hook runs before and after. When
	// omitted, the hook runs for every operation.
	// +optional
	// +listType=set
	Operations []HookOperation `json:"operations,omitempty"`
	// Exec runs a pre/post command pair in the selected pods.
	Exec *VolumeHookPolicyExec `json:"exec"`
}

// VolumeHookPolicySpec selects the PVCs a policy covers and the hooks to run
// in the pods that mount them. A policy covers only PVCs in its own
// namespace.
// +kubebuilder:validation:XValidation:rule="has(self.pvcSelector) != has(self.pvcNames)",message="exactly one of pvcSelector and pvcNames must be set"
type VolumeHookPolicySpec struct {
	// PVCSelector selects covered PersistentVolumeClaims by label. Set
	// exactly one of PVCSelector and PVCNames.
	// +optional
	PVCSelector *metav1.LabelSelector `json:"pvcSelector,omitempty"`
	// PVCNames lists covered PersistentVolumeClaims by name, for claims that
	// carry no labels to select on, such as those a StatefulSet creates. Set
	// exactly one of PVCSelector and PVCNames.
	// +optional
	PVCNames []string `json:"pvcNames,omitempty"`
	// Hooks lists the hooks. With more than one hook, every hook needs a Name
	// and a PodSelector so that each pod matches exactly one hook.
	// +kubebuilder:validation:MinItems=1
	Hooks []VolumeHookPolicyHook `json:"hooks"`
}

// VolumeHookPolicyRef records the policy, and the UID and generation of it,
// that a SnapshotGroup member was admitted with. The policy is not resolved
// again later: a policy edited (different generation) or deleted and
// recreated (different UID) at run time is reported as PolicyChanged rather
// than running commands the user did not review for this group.
type VolumeHookPolicyRef struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// UID is the policy's metadata.uid at admission.
	UID types.UID `json:"uid"`
	// Generation is the policy's metadata.generation at admission.
	// +kubebuilder:validation:Minimum=1
	Generation int64 `json:"generation"`
}

// The types below record a run of a policy's hooks. The policy itself has
// no status; the operation that runs the hooks stores one HookStatus per
// member, for example a SnapshotGroup in its status.hooks.

// HookPhase is the hook state of one member, or of one pod within a member:
// Pending -> PreRunning -> PreDone or PreFailed, then PostRunning ->
// PostDone or PostFailed.
// +kubebuilder:validation:Enum=Pending;PreRunning;PreDone;PreFailed;PostRunning;PostDone;PostFailed;""
type HookPhase string

const (
	HookPhasePending     = HookPhase("Pending")
	HookPhasePreRunning  = HookPhase("PreRunning")
	HookPhasePreDone     = HookPhase("PreDone")
	HookPhasePreFailed   = HookPhase("PreFailed")
	HookPhasePostRunning = HookPhase("PostRunning")
	HookPhasePostDone    = HookPhase("PostDone")
	HookPhasePostFailed  = HookPhase("PostFailed")
)

// HookReason is the machine-readable reason a hook step ended as it did.
// Member-level reasons say why no command could run; pod-level reasons
// describe the outcome of a command in one pod.
// +kubebuilder:validation:Enum=VolumeConflict;PodConflict;PolicyNotFound;PolicyChanged;PodNotFound;HookNotMatched;HookNotFound;ExitNonZero;Timeout;ExecFailed;Interrupted;""
type HookReason string

const (
	// HookReasonVolumeConflict means another hooked group includes the member
	// volume and has not finished its hooks yet.
	HookReasonVolumeConflict = HookReason("VolumeConflict")
	// HookReasonPodConflict means another hooked group ran its pre-command in
	// a pod mounting the member volume, through a different volume of the
	// same pod, and has not released it yet.
	HookReasonPodConflict = HookReason("PodConflict")
	// HookReasonPolicyNotFound means the recorded policy no longer exists.
	HookReasonPolicyNotFound = HookReason("PolicyNotFound")
	// HookReasonPolicyChanged means the policy's generation differs from the
	// one recorded at admission.
	HookReasonPolicyChanged = HookReason("PolicyChanged")
	// HookReasonPodNotFound means no running pod mounted the member when the
	// pre-command was due, or a recorded pod was gone when the post-command
	// was due.
	HookReasonPodNotFound = HookReason("PodNotFound")
	// HookReasonHookNotMatched means a pod matched zero or several hooks, or
	// its hook names no container while the pod has several.
	HookReasonHookNotMatched = HookReason("HookNotMatched")
	// HookReasonHookNotFound means the recorded hook name no longer exists in
	// the policy when the post-command is due.
	HookReasonHookNotFound = HookReason("HookNotFound")
	// HookReasonExitNonZero means the command exited non-zero.
	HookReasonExitNonZero = HookReason("ExitNonZero")
	// HookReasonTimeout means the command did not exit before its deadline.
	// The process may still be running in the pod.
	HookReasonTimeout = HookReason("Timeout")
	// HookReasonExecFailed means the exec session failed for a reason other
	// than exit code or timeout.
	HookReasonExecFailed = HookReason("ExecFailed")
	// HookReasonInterrupted means the executor stopped between running the
	// pre-command and recording its outcome, so whether it ran is unknown.
	// The member fails rather than running the pre-command again; the
	// post-command still runs.
	HookReasonInterrupted = HookReason("Interrupted")
)

// HookPodStatus is the hook outcome in one pod of a member. It records the
// pod and the hook that ran the pre-command, so the post-command can target
// the same pod and hook without resolving them again.
type HookPodStatus struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// UID is the pod's metadata.uid when the pre-command ran. A same-name pod
	// with a different UID is a replacement that never ran it.
	UID string `json:"uid"`
	// ContainerIDs are the IDs of every container in the pod when the
	// pre-command ran. A change before the group is Ready means a process
	// restarted in between.
	// +optional
	// +nullable
	ContainerIDs []string `json:"containerIDs"`
	// HookName is the policy hook that ran in this pod. The post-command is
	// taken from the same hook regardless of the pod's labels by then.
	HookName string `json:"hookName"`
	// Phase is this pod's hook state.
	// +optional
	Phase HookPhase `json:"phase"`
	// ExitCode is the last command's exit code, if it exited.
	// +optional
	ExitCode *int32 `json:"exitCode,omitempty"`
	// Reason is why the last step ended as it did; empty for a clean run. A
	// PostDone pod may carry PodNotFound or PolicyChanged, meaning the
	// post-command could not run as recorded.
	// +optional
	Reason HookReason `json:"reason,omitempty"`
	// ReleaseAttempts counts post-command attempts that may have started a
	// process in the pod. The executor increments it before each attempt, so
	// a restart mid-attempt cannot lose the count and exceed the retry cap.
	// +optional
	ReleaseAttempts int32 `json:"releaseAttempts,omitempty"`
}

// PVCRef names a PersistentVolumeClaim.
type PVCRef struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// HookStatus is the hook state of one member, owned by the hook executor.
type HookStatus struct {
	// SnapshotName matches the member in spec.members.
	SnapshotName string `json:"snapshotName"`
	// Phase is the member's overall hook state.
	// +optional
	Phase HookPhase `json:"phase"`
	// Reason is why no command could run for this member. Set only when Pods
	// is empty.
	// +optional
	Reason HookReason `json:"reason,omitempty"`
	// PVCRef is the member volume's claim, recorded so the member's pods can
	// still be listed after the volume's kubernetesStatus is gone.
	// +optional
	PVCRef *PVCRef `json:"pvcRef,omitempty"`
	// Pods are the pods the hooks ran in for this member.
	// +optional
	// +nullable
	Pods []HookPodStatus `json:"pods"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhvhp
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// VolumeHookPolicy is the Schema for the volumehookpolicies API. It lives
// in the application's namespace and has no status. Writing a policy is
// equivalent to pods/exec in its namespace, because the executor runs the
// author's commands in that namespace's pods.
type VolumeHookPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec VolumeHookPolicySpec `json:"spec,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeHookPolicyList contains a list of VolumeHookPolicy
type VolumeHookPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VolumeHookPolicy `json:"items"`
}
