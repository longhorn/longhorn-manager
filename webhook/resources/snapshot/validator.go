package snapshot

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type snapshotValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &snapshotValidator{ds: ds}
}

func (o *snapshotValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "snapshots",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Snapshot{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
			admissionregv1.Delete,
		},
	}
}

func (o *snapshotValidator) Create(request *admission.Request, newObj runtime.Object) error {
	snapshot, ok := newObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", newObj), "")
	}

	if err := util.VerifySnapshotLabels(snapshot.Labels); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if snapshot.Spec.Volume == "" {
		return werror.NewInvalidError("spec.volume is required", "spec.volume")
	}

	if isLinkedClone, err := o.ds.IsVolumeLinkedCloneVolume(snapshot.Spec.Volume); err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to check IsVolumeLinkedCloneVolume: %v", err), "")
	} else if isLinkedClone {
		return werror.NewInvalidError(fmt.Sprintf("snapshot is not allowed for linked-clone volume %v", snapshot.Spec.Volume), "")
	}

	if err := o.validateSnapshotCount(snapshot); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (o *snapshotValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldSnapshot, ok := oldObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", oldObj), "")
	}
	newSnapshot, ok := newObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", newObj), "")
	}

	if newSnapshot.Spec.Volume != oldSnapshot.Spec.Volume {
		return werror.NewInvalidError("spec.volume field is immutable", "spec.volume")
	}

	if len(oldSnapshot.OwnerReferences) != 0 && !reflect.DeepEqual(newSnapshot.OwnerReferences, oldSnapshot.OwnerReferences) {
		return werror.NewInvalidError("snapshot OwnerReferences field is immutable", "metadata.ownerReferences")
	}

	if _, ok := oldSnapshot.Labels[types.LonghornLabelVolume]; ok && newSnapshot.Labels[types.LonghornLabelVolume] != oldSnapshot.Labels[types.LonghornLabelVolume] {
		return werror.NewInvalidError(fmt.Sprintf("label %v is immutable", types.LonghornLabelVolume), "metadata.labels")
	}

	return nil
}

func (o *snapshotValidator) validateSnapshotCount(snapshot *longhorn.Snapshot) error {
	if !snapshot.Spec.CreateSnapshot {
		return nil
	}

	volume, err := o.ds.GetVolumeRO(snapshot.Spec.Volume)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume %v for snapshot count validation", snapshot.Spec.Volume)
	}

	snapshotMaxCount, err := o.getSnapshotMaxCount(volume)
	if err != nil {
		return err
	}

	existingSnapshots, err := o.ds.ListVolumeSnapshotsRO(snapshot.Spec.Volume)
	if err != nil {
		return errors.Wrapf(err, "failed to list snapshots for volume %s for snapshot count validation", snapshot.Spec.Volume)
	}

	currentSnapshotCount := countExistingSnapshots(existingSnapshots)

	if currentSnapshotCount >= snapshotMaxCount {
		return fmt.Errorf(
			"cannot create snapshot for volume %s: snapshot count usage %d is greater than or equal to snapshotMaxCount %d; remove snapshots or increase snapshotMaxCount",
			snapshot.Spec.Volume,
			currentSnapshotCount,
			snapshotMaxCount,
		)
	}
	return nil
}

// getSnapshotMaxCount returns the effective snapshot max count for the given volume.
// It prefers volume.Spec.SnapshotMaxCount (set at volume creation time from the global setting
// and not updated when the global setting changes afterward). If the field is unset
// (e.g. during upgrade/migration before the mutator has defaulted it), it falls back to
// the current global setting to ensure enforcement is never silently skipped.
func (o *snapshotValidator) getSnapshotMaxCount(volume *longhorn.Volume) (int, error) {
	if volume.Spec.SnapshotMaxCount > 0 {
		return volume.Spec.SnapshotMaxCount, nil
	}

	snapshotMaxCount, err := o.ds.GetSettingAsInt(types.SettingNameSnapshotMaxCount)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get setting %v for snapshot count validation", types.SettingNameSnapshotMaxCount)
	}
	return int(snapshotMaxCount), nil
}

func countExistingSnapshots(snapshots map[string]*longhorn.Snapshot) int {
	count := 0
	for _, s := range snapshots {
		if s != nil && s.DeletionTimestamp == nil && !s.Status.MarkRemoved && s.Status.UserCreated {
			count++
		}
	}
	return count
}
func (o *snapshotValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	snapshot, ok := oldObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", oldObj), "")
	}

	isEntrypoint, cloneNames, err := o.ds.IsSnapshotLinkedCloneEntrypoint(snapshot.Name)
	if err != nil {
		return werror.NewInternalError(fmt.Sprintf(
			"failed to check if snapshot %v is a linked-clone entrypoint: %v",
			snapshot.Name, err))
	}
	if isEntrypoint {
		sort.Strings(cloneNames)
		return werror.NewForbiddenError(fmt.Sprintf(
			"cannot delete snapshot %v: it is the entrypoint for linked-clone volume(s): %v",
			snapshot.Name, strings.Join(cloneNames, ", ")))
	}

	return nil
}
