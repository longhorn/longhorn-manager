package snapshotgroup

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type snapshotGroupValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &snapshotGroupValidator{ds: ds}
}

func (v *snapshotGroupValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "snapshotgroups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.SnapshotGroup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

// Create validates the mutated object: the mutator runs first and has
// already resolved spec.members.
func (v *snapshotGroupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	snapshotGroup, ok := newObj.(*longhorn.SnapshotGroup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SnapshotGroup", newObj), "")
	}

	if len(snapshotGroup.Name) > types.SnapshotGroupNameMaxLength {
		return werror.NewInvalidError(fmt.Sprintf("snapshot group name is longer than %v characters", types.SnapshotGroupNameMaxLength), "metadata.name")
	}
	if errs := validation.IsValidLabelValue(snapshotGroup.Name); len(errs) > 0 {
		return werror.NewInvalidError(fmt.Sprintf("snapshot group name must be a valid label value: %v", strings.Join(errs, "; ")), "metadata.name")
	}

	hasVolumes := len(snapshotGroup.Spec.Volumes) > 0
	hasSelector := snapshotGroup.Spec.VolumeSelector != nil
	if hasVolumes == hasSelector {
		return werror.NewInvalidError("exactly one of volumes or volumeSelector must be set", "spec")
	}

	duplicatedVolume := map[string]bool{}
	for _, volume := range snapshotGroup.Spec.Volumes {
		if duplicatedVolume[volume] {
			return werror.NewInvalidError(fmt.Sprintf("volume %v appears more than once in spec.volumes", volume), "spec.volumes")
		}
		duplicatedVolume[volume] = true
	}

	if err := util.VerifySnapshotLabels(snapshotGroup.Spec.Labels); err != nil {
		return werror.NewInvalidError(err.Error(), "spec.labels")
	}
	// A user-supplied recurring-job key would enroll every member into that
	// job's retention cleanup and let it delete members.
	if _, exists := snapshotGroup.Spec.Labels[types.RecurringJobLabel]; exists {
		return werror.NewInvalidError(fmt.Sprintf("label key %v is reserved for recurring jobs", types.RecurringJobLabel), "spec.labels")
	}
	if err := validateCSIReservedLabelKeys(snapshotGroup); err != nil {
		return err
	}

	if err := validateMembers(snapshotGroup); err != nil {
		return err
	}
	return v.validateMembersHooks(request, snapshotGroup)
}

// validateMembersHooks checks the hook fields of the member set. Format
// checks always run. Cluster-state checks run only when the group will run
// hooks and pause applications; a restored group CR already finished, no
// hook will run, so they are skipped.
func (v *snapshotGroupValidator) validateMembersHooks(request *admission.Request, snapshotGroup *longhorn.SnapshotGroup) error {
	if !snapshothook.IsHooked(snapshotGroup) {
		return validateUnhookedHookFields(snapshotGroup)
	}
	if err := validateHookReferences(snapshotGroup); err != nil {
		return err
	}
	if _, terminal := types.GetSnapshotGroupTerminalPhase(snapshotGroup); terminal {
		return nil
	}
	return v.validateHookClusterState(request, snapshotGroup)
}

// A CSI plugin that does not consume the hooks and deadlineSeconds class
// parameters (e.g. not yet updated during an upgrade) forwards them as
// snapshot labels, silently dropping the hooks.
func validateCSIReservedLabelKeys(snapshotGroup *longhorn.SnapshotGroup) error {
	if snapshotGroup.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType)] == "" {
		return nil
	}
	for _, key := range []string{types.SnapshotGroupCSIParameterHooks, types.SnapshotGroupCSIParameterDeadlineSeconds} {
		if _, exists := snapshotGroup.Spec.Labels[key]; exists {
			return werror.NewInvalidError(fmt.Sprintf("label key %v is a CSI class parameter this longhorn-manager consumes, but the CSI plugin that created this group predates it; the create is rejected until the plugin rollout completes", key), "spec.labels")
		}
	}
	return nil
}

// validateUnhookedHookFields rejects hook fields on a group whose spec.hooks
// is none.
func validateUnhookedHookFields(snapshotGroup *longhorn.SnapshotGroup) error {
	if snapshotGroup.Spec.HookOperation != "" {
		return werror.NewInvalidError(fmt.Sprintf("hookOperation %q is set but spec.hooks is none; no hook runs, so there is no operation to pause for", snapshotGroup.Spec.HookOperation), "spec.hookOperation")
	}
	for _, member := range snapshotGroup.Spec.Members {
		if member.HookPolicyRef != nil {
			return werror.NewInvalidError(fmt.Sprintf("member %v carries a hook policy but spec.hooks is none", member.VolumeName), "spec.members")
		}
	}
	return nil
}

// validateHookReferences checks that the hook fields on a hooked group are
// valid: a known hookOperation, and a complete hookPolicyRef on every member.
// It reads nothing from the cluster.
//
// The CRD schema enforces the same rules, so these checks fire only when the
// webhook runs ahead of the CRD, as during an upgrade.
func validateHookReferences(snapshotGroup *longhorn.SnapshotGroup) error {
	switch snapshotGroup.Spec.HookOperation {
	case longhorn.HookOperationSnapshot, longhorn.HookOperationBackup:
	case "":
		return werror.NewInvalidError("hookOperation is required when spec.hooks is command; the mutating webhook defaults it to snapshot", "spec.hookOperation")
	default:
		return werror.NewInvalidError(fmt.Sprintf("hookOperation %q is not one of %q, %q", snapshotGroup.Spec.HookOperation, longhorn.HookOperationSnapshot, longhorn.HookOperationBackup), "spec.hookOperation")
	}

	for _, member := range snapshotGroup.Spec.Members {
		ref := member.HookPolicyRef
		if ref == nil {
			return werror.NewInvalidError(fmt.Sprintf("member %v has no hookPolicyRef", member.VolumeName), "spec.members")
		}
		if ref.Namespace == "" || ref.Name == "" {
			return werror.NewInvalidError(fmt.Sprintf("member %v hookPolicyRef needs both namespace and name", member.VolumeName), "spec.members")
		}
		if ref.Generation <= 0 {
			return werror.NewInvalidError(fmt.Sprintf("member %v hookPolicyRef has no generation", member.VolumeName), "spec.members")
		}
	}
	return nil
}

// validateHookClusterState checks that the cluster is ready to run the
// group's hooks: the manager rollout is complete, every member has a running
// pod outside the Longhorn namespace, no other hooked group still holds a
// member, and the executor has access in every member namespace.
func (v *snapshotGroupValidator) validateHookClusterState(request *admission.Request, snapshotGroup *longhorn.SnapshotGroup) error {
	// During an upgrade, manager pods on different images may handle the
	// group differently. So a group with hooks is refused until every
	// manager pod runs the new image.
	outdated, err := v.ds.ListOutdatedManagerPods()
	if err != nil {
		return werror.NewInternalError(fmt.Sprintf("failed to check the longhorn-manager rollout: %v; retry the create", err))
	}
	if len(outdated) > 0 {
		return werror.NewInvalidError(datastore.OutdatedManagerPodsMessage(outdated), "spec.hooks")
	}

	// This is a warning, not a rejection, because the user may still back
	// up the snapshots themselves.
	if snapshotGroup.Spec.HookOperation == longhorn.HookOperationBackup &&
		snapshotGroup.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType)] != types.SnapshotGroupCSITypeBackup {
		request.AddWarning(fmt.Sprintf("hookOperation is %v but the group was not created by a CSI VolumeGroupSnapshot of type %v; Longhorn will not back up the member snapshots, so the backup hooks only matter if you back them up yourself", longhorn.HookOperationBackup, types.SnapshotGroupCSITypeBackup))
	}

	groups, err := v.ds.ListSnapshotGroupsRO()
	if err != nil {
		return werror.NewInternalError(fmt.Sprintf("failed to list snapshot groups: %v", err))
	}
	otherGroups := make([]*longhorn.SnapshotGroup, 0, len(groups))
	for _, other := range groups {
		if other.Name != snapshotGroup.Name {
			otherGroups = append(otherGroups, other)
		}
	}

	// Concurrent creates cannot see each other in the group list, so two
	// groups sharing a member can both pass the conflict check here. The
	// executor re-checks for conflicts before pausing a member.
	execNamespaces := map[string]bool{}
	for _, member := range snapshotGroup.Spec.Members {
		volume, err := v.ds.GetVolumeRO(member.VolumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				return werror.NewInvalidError(fmt.Sprintf("member volume %v does not exist", member.VolumeName), "spec.members")
			}
			return werror.NewInternalError(fmt.Sprintf("failed to get member volume %v: %v", member.VolumeName, err))
		}
		failure, warnings := snapshothook.CheckMemberClusterState(volume, snapshothook.PolicyRefNamespacedName(member.HookPolicyRef), otherGroups, v.ds.Namespace())
		if failure != nil {
			return werror.NewInvalidError(failure.Message, "spec.members")
		}
		for _, warning := range warnings {
			request.AddWarning(warning)
		}
		execNamespaces[volume.Status.KubernetesStatus.Namespace] = true
	}

	return v.verifyExecutorAccess(execNamespaces)
}

// verifyExecutorAccess asks the API server whether the executor's
// ServiceAccount has every permission it needs in every namespace the group
// will pause pods in. A denial is for the namespace admin to fix, so the
// rejection includes the RoleBinding manifest. A review that fails to
// complete is rejected as an internal error, to be retried.
func (v *snapshotGroupValidator) verifyExecutorAccess(namespaces map[string]bool) error {
	sorted := make([]string, 0, len(namespaces))
	for namespace := range namespaces {
		sorted = append(sorted, namespace)
	}
	sort.Strings(sorted)

	for _, namespace := range sorted {
		denial, err := v.ds.VerifyHookExecutorAccess(namespace)
		if err != nil {
			return werror.NewInternalError(fmt.Sprintf("%v; retry the create", err))
		}
		if denial != nil {
			return werror.NewInvalidError(denial.Message(), "spec.hooks")
		}
	}
	return nil
}

// validateMembers re-checks the stamped member set without re-resolving,
// catching any path that skipped the mutator.
func validateMembers(snapshotGroup *longhorn.SnapshotGroup) error {
	members := snapshotGroup.Spec.Members
	if len(members) == 0 {
		return werror.NewInvalidError("spec.members is empty; the volume selection must resolve to at least one member", "spec.members")
	}
	if len(members) > types.SnapshotGroupMaxMemberCount {
		return werror.NewInvalidError(fmt.Sprintf("spec.members has %v members, above the member cap %v", len(members), types.SnapshotGroupMaxMemberCount), "spec.members")
	}

	memberVolumes := map[string]bool{}
	memberVolumeBySnapshotName := map[string]string{}
	for _, member := range members {
		if member.VolumeName == "" {
			return werror.NewInvalidError("spec.members contains a member without a volume name", "spec.members")
		}
		if memberVolumes[member.VolumeName] {
			return werror.NewInvalidError(fmt.Sprintf("volume %v appears in more than one member", member.VolumeName), "spec.members")
		}
		memberVolumes[member.VolumeName] = true

		// The format is a persistence contract: the group name prefix ties a
		// member to its group, and the suffix length backs the group name
		// length bound.
		suffix, found := strings.CutPrefix(member.SnapshotName, snapshotGroup.Name+"-")
		if !found || len(suffix) != types.SnapshotGroupMemberSnapshotNameSuffixLength {
			return werror.NewInvalidError(fmt.Sprintf("member snapshot name %v for volume %v must be the group name plus a %v-character suffix",
				member.SnapshotName, member.VolumeName, types.SnapshotGroupMemberSnapshotNameSuffixLength), "spec.members")
		}

		if collidingVolume, exists := memberVolumeBySnapshotName[member.SnapshotName]; exists {
			return werror.NewInvalidError(fmt.Sprintf("volumes %v and %v share the same member snapshot name %v",
				collidingVolume, member.VolumeName, member.SnapshotName), "spec.members")
		}
		memberVolumeBySnapshotName[member.SnapshotName] = member.VolumeName
	}

	// When spec.volumes names the volumes, the members must name exactly
	// those volumes. A volumeSelector has no fixed list to compare against.
	if len(snapshotGroup.Spec.Volumes) > 0 {
		if len(snapshotGroup.Spec.Volumes) != len(members) {
			return werror.NewInvalidError(fmt.Sprintf("spec.members names %v volumes, spec.volumes %v", len(members), len(snapshotGroup.Spec.Volumes)), "spec.members")
		}
		for _, volume := range snapshotGroup.Spec.Volumes {
			if !memberVolumes[volume] {
				return werror.NewInvalidError(fmt.Sprintf("volume %v is in spec.volumes but has no member", volume), "spec.members")
			}
		}
	}

	return nil
}

func (v *snapshotGroupValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldSnapshotGroup, ok := oldObj.(*longhorn.SnapshotGroup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SnapshotGroup", oldObj), "")
	}
	newSnapshotGroup, ok := newObj.(*longhorn.SnapshotGroup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SnapshotGroup", newObj), "")
	}

	// A group is a point-in-time request; changing members later has no
	// meaning, so the whole spec is immutable. This also rejects a full-object
	// replace that drops spec.members - an explicit failure, never a silent
	// re-resolution at a later time.
	if !reflect.DeepEqual(oldSnapshotGroup.Spec, newSnapshotGroup.Spec) {
		return werror.NewInvalidError(fmt.Sprintf("snapshot group %v spec is immutable after creation; create a new group to capture a different set", oldSnapshotGroup.Name), "spec")
	}

	// Get and Delete match the group by this label, and the type decides
	// whether Delete sweeps the member backups.
	csiTypeLabelKey := types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType)
	if oldSnapshotGroup.Labels[csiTypeLabelKey] != newSnapshotGroup.Labels[csiTypeLabelKey] {
		return werror.NewInvalidError(fmt.Sprintf("label %v is immutable", csiTypeLabelKey), "metadata.labels")
	}

	// Get re-reads the recorded backup mode from this annotation each time it
	// creates a missing member backup, so an edit mid-group could mix upload
	// modes across members of one group.
	if oldSnapshotGroup.Annotations[types.SnapshotGroupAnnotationCSIParameters] != newSnapshotGroup.Annotations[types.SnapshotGroupAnnotationCSIParameters] {
		return werror.NewInvalidError(fmt.Sprintf("annotation %v is immutable", types.SnapshotGroupAnnotationCSIParameters), "metadata.annotations")
	}

	return nil
}
