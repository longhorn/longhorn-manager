package snapshotgroup

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type snapshotGroupMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &snapshotGroupMutator{ds: ds}
}

func (m *snapshotGroupMutator) Resource() admission.Resource {
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

// Create resolves the volume selection into the fixed member set and stamps
// the defaults. A restore of a finished group arrives with the members
// already stamped and keeps them.
func (m *snapshotGroupMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	snapshotGroup, ok := newObj.(*longhorn.SnapshotGroup)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SnapshotGroup", newObj), "")
	}

	var patchOps admission.PatchOps
	hooked := snapshothook.IsHooked(snapshotGroup)
	operation := snapshothook.EffectiveHookOperation(snapshotGroup.Spec.HookOperation)

	if len(snapshotGroup.Spec.Members) > 0 {
		// Pre-set members are only accepted from the restore of a finished
		// group, marked by the terminal-phase annotation. They stay as
		// stamped: resolving again could produce a different member set.
		// The validator still checks their format.
		if _, terminal := types.GetSnapshotGroupTerminalPhase(snapshotGroup); !terminal {
			return nil, werror.NewInvalidError("spec.members may only be pre-set on the restore of a finished group carrying the terminal-phase annotation; it is otherwise resolved from the volume selection at admission", "spec.members")
		}
	} else {
		candidates, err := m.ds.ResolveSnapshotGroupMemberCandidates(&snapshotGroup.Spec)
		if err != nil {
			return nil, werror.NewInvalidError(err.Error(), "spec")
		}

		var rejectionReasons []string
		members := make([]longhorn.SnapshotGroupMember, 0, len(candidates))
		usedSnapshotNames := map[string]bool{}
		for _, candidate := range candidates {
			if candidate.ValidationFailure != "" {
				rejectionReasons = append(rejectionReasons, candidate.ValidationFailure)
				continue
			}
			snapshotName := types.GenerateSnapshotGroupMemberSnapshotName(snapshotGroup.Name)
			for usedSnapshotNames[snapshotName] {
				snapshotName = types.GenerateSnapshotGroupMemberSnapshotName(snapshotGroup.Name)
			}
			usedSnapshotNames[snapshotName] = true
			member := longhorn.SnapshotGroupMember{
				VolumeName:   candidate.VolumeName,
				SnapshotName: snapshotName,
			}
			if hooked {
				// The policy, its UID and its generation are fixed here, once.
				// A policy edited or recreated after admission is detected as
				// PolicyChanged instead of silently running commands the user
				// never reviewed.
				policy, err := m.resolveHookPolicy(candidate.VolumeName)
				if err != nil {
					rejectionReasons = append(rejectionReasons, err.Error())
					continue
				}
				if !snapshothook.PolicyCoversOperation(policy, operation) {
					rejectionReasons = append(rejectionReasons, snapshothook.NoHookForOperationMessage(policy, operation, candidate.VolumeName))
					continue
				}
				member.HookPolicyRef = snapshothook.PolicyRef(policy)
			}
			members = append(members, member)
		}
		if len(rejectionReasons) > 0 {
			return nil, werror.NewInvalidError(strings.Join(rejectionReasons, "; "), "spec")
		}

		bytes, err := json.Marshal(members)
		if err != nil {
			err = errors.Wrapf(err, "failed to marshal members for snapshot group %v", snapshotGroup.Name)
			return nil, werror.NewInvalidError(err.Error(), "spec.members")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/members", "value": %v}`, string(bytes)))
	}

	if snapshotGroup.Spec.DeadlineSeconds == 0 {
		deadlineSeconds := types.SnapshotGroupDefaultDeadlineSeconds
		if hooked {
			deadlineSeconds = types.SnapshotGroupHookedDefaultDeadlineSeconds
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/deadlineSeconds", "value": %v}`, deadlineSeconds))
	}

	// The operation has no CRD default because it only means something for a
	// hooked group.
	if hooked && snapshotGroup.Spec.HookOperation == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/hookOperation", "value": "%v"}`, operation))
	}

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(snapshotGroup)
	if err != nil {
		err = errors.Wrapf(err, "failed to get finalizer patch for snapshot group %v", snapshotGroup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}

// resolveHookPolicy finds the one policy for a member volume. The volume was
// just resolved from the same cache, so a miss here is a real error, not a
// race.
func (m *snapshotGroupMutator) resolveHookPolicy(volumeName string) (*longhorn.VolumeHookPolicy, error) {
	volume, err := m.ds.GetVolumeRO(volumeName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get volume %v to resolve its volume hook policy", volumeName)
	}
	return m.ds.ResolveVolumeHookPolicyForVolume(volume)
}

// Update only maintains the finalizer: the spec is immutable after creation.
func (m *snapshotGroupMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	snapshotGroup, ok := newObj.(*longhorn.SnapshotGroup)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.SnapshotGroup", newObj), "")
	}

	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(snapshotGroup)
	if err != nil {
		err = errors.Wrapf(err, "failed to get finalizer patch for snapshot group %v", snapshotGroup.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
