package engine

import (
	"fmt"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
			admissionregv1.Create,
		},
	}
}

func (e *engineValidator) Create(request *admission.Request, newObj runtime.Object) error {
	engine := newObj.(*longhorn.Engine)

	volume, err := e.ds.GetVolume(engine.Spec.VolumeName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return werror.NewInvalidError("volume does not exist for engine", "spec.volumeName")
		}
		err = errors.Wrap(err, "failed to get volume for engine")
		return werror.NewInternalError(err.Error())
	}

	return e.validateNumberOfEngines(engine, volume)
}

func (e *engineValidator) validateNumberOfEngines(newEngine *longhorn.Engine, volume *longhorn.Volume) error {
	volumeEngines, err := e.ds.ListVolumeEnginesUncached(newEngine.Spec.VolumeName)
	if err != nil {
		err = errors.Wrap(err, "failed to list engines for volume")
		return werror.NewInternalError(err.Error())
	}

	newNumVolumeEngines := len(volumeEngines) + 1
	if volume.Spec.Migratable && newNumVolumeEngines > 2 {
		message := fmt.Sprintf("engine creation would result in %d engines for migratable volume", newNumVolumeEngines)
		return werror.NewInvalidError(message, "")
	}
	if !volume.Spec.Migratable && newNumVolumeEngines > 1 {
		message := fmt.Sprintf("engine creation would result in %d engines for non-migratable volume", newNumVolumeEngines)
		return werror.NewInvalidError(message, "")
	}

	return nil
}
