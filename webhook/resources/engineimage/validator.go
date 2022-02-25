package engineimage

import (
	"fmt"

	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type engineImageValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &engineImageValidator{ds: ds}
}

func (e *engineImageValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "engineimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.EngineImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Delete,
		},
	}
}

func (e *engineImageValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	engineImage := oldObj.(*longhorn.EngineImage)

	defaultImage, err := e.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		err = errors.Wrap(err, "unable to delete engine image")
		return werror.NewInvalidError(err.Error(), "")
	}
	if engineImage.Spec.Image == defaultImage {
		return werror.NewInvalidError("unable to delete the default engine image", "")
	}
	if engineImage.Status.RefCount != 0 {
		return werror.NewInvalidError(fmt.Sprintf("unable to delete the engine image while being used and refCount is %v", engineImage.Status.RefCount), "")
	}
	return nil
}
