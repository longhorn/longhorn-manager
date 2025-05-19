package orphan

import (
	"fmt"
	"reflect"

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
	case longhorn.OrphanTypeReplicaData:
		err = checkOrphanForReplicaData(orphan)
	case longhorn.OrphanTypeEngineInstance, longhorn.OrphanTypeReplicaInstance:
		err = checkOrphanForInstance(orphan)
	default:
		return werror.NewInvalidError(fmt.Sprintf("unknown orphan type %v for orphan %v", orphan.Spec.Type, orphan.Name), "")
	}
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to check orphan %v since %v", orphan.Name, err), "")
	}

	return nil
}

func (o *orphanValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldOrphan, ok := oldObj.(*longhorn.Orphan)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Orphan", oldObj), "")
	}
	newOrphan, ok := newObj.(*longhorn.Orphan)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Orphan", newObj), "")
	}

	if err := checkOrphanImmutable(oldOrphan, newOrphan); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func checkOrphanForReplicaData(orphan *longhorn.Orphan) error {
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

func checkOrphanForInstance(orphan *longhorn.Orphan) error {
	params := []string{
		longhorn.OrphanInstanceName,
		longhorn.OrphanInstanceUUID,
		longhorn.OrphanInstanceManager,
	}

	for _, param := range params {
		val, ok := orphan.Spec.Parameters[param]
		if !ok {
			return fmt.Errorf("parameter %v for orphan %v is missing", param, orphan.Name)
		}
		if val == "" {
			return fmt.Errorf("parameter %v for orphan %v is empty", param, orphan.Name)
		}
	}

	switch orphan.Spec.DataEngine {
	case longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2:
		break
	default:
		return fmt.Errorf("invalid data engine type %v for orphan %v", orphan.Spec.DataEngine, orphan.Name)
	}

	return nil
}

func checkOrphanImmutable(oldOrphan, newOrphan *longhorn.Orphan) error {
	if !reflect.DeepEqual(oldOrphan.Spec, newOrphan.Spec) {
		return fmt.Errorf("orphan %s spec is immutable", oldOrphan.Name)
	}
	return nil
}
