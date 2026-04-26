package enginefrontend

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type engineFrontendValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &engineFrontendValidator{ds: ds}
}

func (e *engineFrontendValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "enginefrontends",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.EngineFrontend{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (e *engineFrontendValidator) Create(request *admission.Request, newObj runtime.Object) error {
	engineFrontend, ok := newObj.(*longhorn.EngineFrontend)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineFrontend", newObj), "")
	}

	_, err := e.ds.GetVolume(engineFrontend.Spec.VolumeName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return werror.NewInvalidError("volume does not exist for engine frontend", "spec.volumeName")
		}
		err = errors.Wrap(err, "failed to get volume for engine frontend")
		return werror.NewInternalError(err.Error())
	}

	if !types.IsDataEngineV2(engineFrontend.Spec.DataEngine) {
		return werror.NewInvalidError(fmt.Sprintf("data engine %v is not supported for engine frontend", engineFrontend.Spec.DataEngine), "spec.dataEngine")
	}

	return nil
}

func (e *engineFrontendValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldEngineFrontend, ok := oldObj.(*longhorn.EngineFrontend)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineFrontend", oldObj), "")
	}
	newEngineFrontend, ok := newObj.(*longhorn.EngineFrontend)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.EngineFrontend", newObj), "")
	}

	if oldEngineFrontend.Spec.DataEngine != "" {
		if oldEngineFrontend.Spec.DataEngine != newEngineFrontend.Spec.DataEngine {
			err := fmt.Errorf("changing data engine for engine frontend %v is not supported", oldEngineFrontend.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
}
