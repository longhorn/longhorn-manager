package orphan

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
	orphan, ok := newObj.(*longhorn.Orphan)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Orphan", newObj), "")
	}

	var err error
	switch orphan.Spec.Type {
	case longhorn.OrphanTypeReplica:
		err = checkOrphanForReplicaDataStore(orphan)
	default:
		return werror.NewInvalidError(fmt.Sprintf("unknown orphan type %v for orphan %v", orphan.Spec.Type, orphan.Name), "")
	}
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to check orphan %v since %v", orphan.Name, err), "")
	}

	return nil
}

func (o *orphanValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	newOrphan, ok := newObj.(*longhorn.Orphan)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Orphan", newObj), "")
	}

	if err := checkOrphanParameters(newOrphan); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func checkOrphanParameters(orphan *longhorn.Orphan) error {
	if orphan.Spec.Type == longhorn.OrphanTypeReplica {
		return checkOrphanForReplicaDataStore(orphan)
	}

	return werror.NewInvalidError(fmt.Sprintf("unknown orphan type %v for orphan %v", orphan.Spec.Type, orphan.Name), "")
}

func checkOrphanForReplicaDataStore(orphan *longhorn.Orphan) error {
	params := []string{
		longhorn.OrphanDataName,
		longhorn.OrphanDiskName,
		longhorn.OrphanDiskUUID,
		longhorn.OrphanDiskPath,
		longhorn.OrphanDiskType,
	}

	for _, param := range params {
		_, ok := orphan.Spec.Parameters[param]
		if !ok {
			return fmt.Errorf("parameter %v for orphan %v is missing", param, orphan.Name)
		}
	}

	return nil
}
