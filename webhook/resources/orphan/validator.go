package orphan

import (
	"fmt"
	"reflect"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type orphanValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &orphanValidator{ds: ds}
}

func (o *orphanValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "orphans",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Orphan{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (o *orphanValidator) Create(request *admission.Request, newObj runtime.Object) error {
	orphan := newObj.(*longhorn.Orphan)

	var err error
	switch {
	case orphan.Spec.Type == longhorn.OrphanTypeReplica:
		err = checkOrphanForReplicaDirectory(orphan)
	default:
		return werror.NewInvalidError(fmt.Sprintf("unknown orphan type %v for orphan %v", orphan.Spec.Type, orphan.Name), "")
	}
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to check orphan %v since %v", orphan.Name, err), "")
	}

	return nil
}

func (o *orphanValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldOrphan := oldObj.(*longhorn.Orphan)
	newOrphan := newObj.(*longhorn.Orphan)

	if err := checkOrphanParameters(newOrphan); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if !reflect.DeepEqual(oldOrphan.Spec, newOrphan.Spec) {
		return werror.NewInvalidError(fmt.Sprintf("orphan %v spec fields are immutable", oldOrphan.Name), "")
	}

	return nil
}

func checkOrphanParameters(orphan *longhorn.Orphan) error {
	switch {
	case orphan.Spec.Type == longhorn.OrphanTypeReplica:
		return checkOrphanForReplicaDirectory(orphan)
	}

	return werror.NewInvalidError(fmt.Sprintf("unknown orphan type %v for orphan %v", orphan.Spec.Type, orphan.Name), "")
}

func checkOrphanForReplicaDirectory(orphan *longhorn.Orphan) error {
	params := []string{
		longhorn.OrphanDataName,
		longhorn.OrphanDiskName,
		longhorn.OrphanDiskUUID,
		longhorn.OrphanDiskPath,
	}

	for _, param := range params {
		_, ok := orphan.Spec.Parameters[param]
		if !ok {
			return fmt.Errorf("parameter %v for orphan %v is missing", param, orphan.Name)
		}
	}

	return nil
}
