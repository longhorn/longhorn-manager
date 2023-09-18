package engine

import (
	"fmt"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
			admissionregv1.Update,
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

	if engine.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeV2 {
		v2DataEngineEnabled, err := e.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
		if err != nil {
			err = errors.Wrapf(err, "failed to get spdk setting")
			return werror.NewInvalidError(err.Error(), "")
		}
		if !v2DataEngineEnabled {
			return werror.NewInvalidError("v2 data engine is not enabled", "")
		}
	}

	return e.validateNumberOfEngines(engine, volume)
}

func (e *engineValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldEngine := oldObj.(*longhorn.Engine)
	newEngine := newObj.(*longhorn.Engine)

	if oldEngine.Spec.BackendStoreDriver != "" {
		if oldEngine.Spec.BackendStoreDriver != newEngine.Spec.BackendStoreDriver {
			err := fmt.Errorf("changing backend store driver for engine %v is not supported", oldEngine.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
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
