package instancemanager

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type instanceManagerValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &instanceManagerValidator{ds: ds}
}

func (i *instanceManagerValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "instancemanagers",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.InstanceManager{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (i *instanceManagerValidator) Create(request *admission.Request, newObj runtime.Object) error {
	if err := validate(newObj.(*longhorn.InstanceManager)); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (i *instanceManagerValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	if err := validate(newObj.(*longhorn.InstanceManager)); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func validate(im *longhorn.InstanceManager) error {
	if im.Labels == nil {
		return fmt.Errorf("labels for instanceManager %s is not set", im.Name)
	}

	if im.OwnerReferences == nil {
		return fmt.Errorf("ownerReferences for instanceManager %s is not set", im.Name)
	}

	if im.Spec.Type == "" {
		return fmt.Errorf("type for instanceManager %s is not set", im.Name)
	}

	if im.Spec.BackendStoreDriver == "" {
		return fmt.Errorf("backend store driver for instanceManager %s is not set", im.Name)
	}

	return nil
}
