package snapshot

import (
	"fmt"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
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
	_, ok := newObj.(*longhorn.Snapshot)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Snapshot", newObj), "")
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
		return werror.NewInvalidError(fmt.Sprintf("spec.volume field is immutable"), "spec.volume")
	}

	if len(oldSnapshot.OwnerReferences) != 0 && !reflect.DeepEqual(newSnapshot.OwnerReferences, oldSnapshot.OwnerReferences) {
		return werror.NewInvalidError(fmt.Sprintf("snapshot OwnerReferences field is immutable"), "metadata.ownerReferences")
	}

	if _, ok := oldSnapshot.Labels[types.LonghornLabelVolume]; ok && newSnapshot.Labels[types.LonghornLabelVolume] != oldSnapshot.Labels[types.LonghornLabelVolume] {
		return werror.NewInvalidError(fmt.Sprintf("label %v is immutable", types.LonghornLabelVolume), "metadata.labels")
	}

	return nil
}
