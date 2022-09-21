package engine

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type engineValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &engineValidator{ds: ds}
}

func (e *engineValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "engines",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Engine{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Update,
		},
	}
}

func (e *engineValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldEngine := oldObj.(*longhorn.Engine)
	newEngine := newObj.(*longhorn.Engine)

	if oldEngine.Spec.BlockSize != newEngine.Spec.BlockSize {
		// Backward compatibility is allowable
		if oldEngine.Spec.BlockSize == 0 && newEngine.Spec.BlockSize == util.LegacyBlockSize {
			return nil
		}

		return werror.NewInvalidError(fmt.Sprintf("Changing blockSize from %v to %v is not allowable",
			oldEngine.Spec.BlockSize, newEngine.Spec.BlockSize), "")
	}

	return nil
}
