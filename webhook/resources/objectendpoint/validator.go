package objectendpoint

import (
	"fmt"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type objectEndpointValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &objectEndpointValidator{ds: ds}
}

func (v *objectEndpointValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "objectendpoints",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.ObjectEndpoint{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *objectEndpointValidator) Create(request *admission.Request, newObj runtime.Object) error {
	_ = newObj.(*longhorn.ObjectEndpoint)

	return nil
}

func (v *objectEndpointValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldOE := oldObj.(*longhorn.ObjectEndpoint)
	newOE := newObj.(*longhorn.ObjectEndpoint)

	if newOE.Spec.Size.Cmp(oldOE.Spec.Size) < 0 {
		return werror.NewInvalidError(fmt.Sprintf("cannot shink size from %v to %v", oldOE.Spec.Size.String(), newOE.Spec.Size.String()), "")
	}

	// TODO: handle additional logic if needed
	// for example, Credential cannot be changed

	return nil
}
