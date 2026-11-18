package snapshotgroup

import (
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
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

	return validateMembers(snapshotGroup)
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
