package engineimage

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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

func (e *engineImageValidator) Delete(request *admission.Request, obj runtime.Object) error {
	ei, ok := obj.(*longhorn.EngineImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineImage", obj), "")
	}

	defaultImage, err := e.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to get default engine image setting: %v", err), "")
	}

	if ei.Spec.Image == defaultImage {
		exists := false
		if ei.Annotations != nil {
			// Annotations `DeleteEngineImageFromLonghorn` is used to note that deleting engine image is by Longhorn during uninstalling.
			// When `exists` is true, the engine image is deleted by Longhorn.
			_, exists = ei.Annotations[types.GetLonghornLabelKey(types.DeleteEngineImageFromLonghorn)]
		}
		if !exists {
			return werror.NewInvalidError(fmt.Sprintf("deleting the default engine image %v (%v) is not allowed", defaultImage, ei.Spec.Image), "")
		}
	}

	if ei.Status.RefCount != 0 {
		return werror.NewInvalidError(fmt.Sprintf("deleting engine image %v (%v) is not allowed while being used", ei.Name, ei.Spec.Image), "")
	}

	return nil
}
