package objectstore

import (
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	"k8s.io/apimachinery/pkg/runtime"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
)

var (
	ErrInvalidNamespace            = werror.NewInvalidError("metadata.namespace is invalid", "metadata.namespace")
	ErrCredentialsInvalidNamespace = werror.NewInvalidError("spec.credentials.namespace is invalid", "spec.cerentials.namespace")
)

type objectStoreValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &objectStoreValidator{ds: ds}
}

func (osv *objectStoreValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "objectstore",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.ObjectStore{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (osv *objectStoreValidator) Crete(req *admission.Request, newObj runtime.Object) (err error) {
	objectstore := newObj.(*longhorn.ObjectStore)

	if objectstore.ObjectMeta.Namespace != objectstore.Spec.Credentials.Namespace {
		return ErrCredentialsInvalidNamespace
	}

	return nil
}

func (osv *objectStoreValidator) Update(req *admission.Request, oldObj, newObj runtime.Object) (err error) {
	return nil
}
