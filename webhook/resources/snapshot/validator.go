package snapshot

import (
	"fmt"
	"reflect"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

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
		return werror.NewInvalidError(fmt.Sprintf("failed to to check IsVolumeLinkedCloneVolume: %v", err), "")
	} else if isLinkedClone {
		return werror.NewInvalidError(fmt.Sprintf("snapshot is not allowed for linked-clone volume %v", snapshot.Spec.Volume), "")
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
