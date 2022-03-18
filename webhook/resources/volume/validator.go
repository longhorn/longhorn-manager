package volume

import (
	"fmt"

	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type volumeValidator struct {
	admission.DefaultValidator
	ds            *datastore.DataStore
	currentNodeID string
}

func NewValidator(ds *datastore.DataStore, currentNodeID string) admission.Validator {
	return &volumeValidator{ds: ds, currentNodeID: currentNodeID}
}

func (v *volumeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "volumes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Volume{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *volumeValidator) Create(request *admission.Request, newObj runtime.Object) error {
	volume := newObj.(*longhorn.Volume)

	if !util.ValidateName(volume.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", volume.Name), "")
	}

	if err := types.ValidateDataLocality(volume.Spec.DataLocality); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateAccessMode(volume.Spec.AccessMode); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaCount(volume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaAutoBalance(volume.Spec.ReplicaAutoBalance); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if volume.Spec.BackingImage != "" {
		if _, err := v.ds.GetBackingImage(volume.Spec.BackingImage); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if volume.Spec.EngineImage == "" {
		return werror.NewInvalidError("BUG: Invalid empty Setting.EngineImage", "")
	}

	if !volume.Spec.Standby {
		if volume.Spec.Frontend != longhorn.VolumeFrontendBlockDev && volume.Spec.Frontend != longhorn.VolumeFrontendISCSI {
			return werror.NewInvalidError(fmt.Sprintf("invalid volume frontend specified: %v", volume.Spec.Frontend), "")
		}
	}

	if volume.Spec.Migratable && volume.Spec.AccessMode != longhorn.AccessModeReadWriteMany {
		return werror.NewInvalidError("migratable volumes are only supported in ReadWriteMany (rwx) access mode", "")
	}

	// Check engine version before disable revision counter
	if volume.Spec.RevisionCounterDisabled {
		if ok, err := v.canDisableRevisionCounter(volume.Spec.EngineImage); !ok {
			err := errors.Wrapf(err, "can not create volume with current engine image that doesn't support disable revision counter")
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if volume.Spec.DataSource != "" {
		if err := v.verifyDataSourceForVolumeCreation(v.currentNodeID, volume.Spec.DataSource, volume.Spec.Size); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if err := datastore.CheckVolume(volume); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (v *volumeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	newVolume := newObj.(*longhorn.Volume)

	if err := types.ValidateDataLocality(newVolume.Spec.DataLocality); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateAccessMode(newVolume.Spec.AccessMode); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaCount(newVolume.Spec.NumberOfReplicas); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := types.ValidateReplicaAutoBalance(newVolume.Spec.ReplicaAutoBalance); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := datastore.CheckVolume(newVolume); err != nil {
		return err
	}

	return nil
}

func (v *volumeValidator) canDisableRevisionCounter(engineImage string) (bool, error) {
	cliAPIVersion, err := v.ds.GetEngineImageCLIAPIVersion(engineImage)
	if err != nil {
		return false, err
	}
	if cliAPIVersion < engineapi.CLIVersionFour {
		return false, fmt.Errorf("current engine image version %v doesn't support disable revision counter", cliAPIVersion)
	}

	return true, nil
}

func (v *volumeValidator) verifyDataSourceForVolumeCreation(currentNodeID string, dataSource longhorn.VolumeDataSource, requestSize int64) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to verify data source")
	}()

	if !types.IsValidVolumeDataSource(dataSource) {
		return fmt.Errorf("in valid value for data source: %v", dataSource)
	}

	if types.IsDataFromVolume(dataSource) {
		srcVolName := types.GetVolumeName(dataSource)
		srcVol, err := v.ds.GetVolume(srcVolName)
		if err != nil {
			return err
		}
		if requestSize != srcVol.Spec.Size {
			return fmt.Errorf("size of target volume (%v bytes) is different than size of source volume (%v bytes)", requestSize, srcVol.Spec.Size)
		}

		if snapName := types.GetSnapshotName(dataSource); snapName != "" {
			if _, err := v.getSnapshot(currentNodeID, snapName, srcVolName); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *volumeValidator) getSnapshot(currentNodeID, snapshotName, volumeName string) (*longhorn.Snapshot, error) {
	if currentNodeID == "" || volumeName == "" || snapshotName == "" {
		return nil, fmt.Errorf("currentNodeID, volume and snapshot name required")
	}
	engine, err := v.getEngineClient(currentNodeID, volumeName)
	if err != nil {
		return nil, err
	}
	snapshot, err := engine.SnapshotGet(snapshotName)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, fmt.Errorf("cannot find snapshot '%s' for volume '%s'", snapshotName, volumeName)
	}
	return snapshot, nil
}

func (v *volumeValidator) getEngineClient(currentNodeID, volumeName string) (client engineapi.EngineClient, err error) {
	var e *longhorn.Engine

	defer func() {
		err = errors.Wrapf(err, "cannot get client for volume %v", volumeName)
	}()
	es, err := v.ds.ListVolumeEngines(volumeName)
	if err != nil {
		return nil, err
	}
	if len(es) == 0 {
		return nil, fmt.Errorf("cannot find engine")
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("more than one engine exists")
	}
	for _, e = range es {
		break
	}
	if e.Status.CurrentState != longhorn.InstanceStateRunning {
		return nil, fmt.Errorf("engine is not running")
	}
	if isReady, err := v.ds.CheckEngineImageReadiness(e.Status.CurrentImage, currentNodeID); !isReady {
		if err != nil {
			return nil, fmt.Errorf("cannot get engine client with image %v: %v", e.Status.CurrentImage, err)
		}
		return nil, fmt.Errorf("cannot get engine client with image %v because it isn't deployed on this node", e.Status.CurrentImage)
	}

	engineCollection := &engineapi.EngineCollection{}
	return engineCollection.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:  e.Spec.VolumeName,
		EngineImage: e.Status.CurrentImage,
		IP:          e.Status.IP,
		Port:        e.Status.Port,
	})
}
