package common

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

func GetLonghornFinalizerPatchOpIfNeeded(obj runtime.Object) (string, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	if metadata.GetDeletionTimestamp() != nil {
		// We should not add a finalizer to an object that is being deleted.
		return "", nil
	}

	if err := util.AddFinalizer(longhornFinalizerKey, obj); err != nil {
		return "", err
	}

	finalizers := metadata.GetFinalizers()
	if finalizers == nil {
		finalizers = []string{}
	}
	bytes, err := json.Marshal(finalizers)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get JSON encoding finalizers of object %v ", metadata.GetName())
	}

	return fmt.Sprintf(`{"op": "replace", "path": "/metadata/finalizers", "value": %v}`, string(bytes)), nil
}

func GetLonghornLabelsPatchOp(obj runtime.Object, requiredLabels, removingLabels map[string]string) (string, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	for k := range removingLabels {
		delete(labels, k)
	}

	for k, v := range requiredLabels {
		labels[k] = v
	}

	volumeName := ""
	switch o := obj.(type) {
	case *longhorn.Volume:
		volumeName = o.Name
	case *longhorn.Engine:
		volumeName = o.Spec.VolumeName
	case *longhorn.Replica:
		volumeName = o.Spec.VolumeName
	}

	if volumeName != "" {
		volumeName = util.AutoCorrectName(volumeName, datastore.NameMaximumLength)

		for k, v := range types.GetVolumeLabels(volumeName) {
			labels[k] = v
		}
	}

	bytes, err := json.Marshal(labels)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get JSON encoding labels of %v ", metadata.GetName())
	}

	return fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)), nil
}

func ValidateRequiredDataEngineEnabled(ds *datastore.DataStore, dataEngine longhorn.DataEngineType) error {
	dataEngineSetting := types.SettingNameV1DataEngine
	if types.IsDataEngineV2(dataEngine) {
		dataEngineSetting = types.SettingNameV2DataEngine
	}

	enabled, err := ds.GetSettingAsBool(dataEngineSetting)
	if err != nil {
		err = errors.Wrapf(err, "failed to get %v setting", dataEngineSetting)
		return werror.NewInternalError(err.Error())
	}
	if !enabled {
		err := fmt.Errorf("%v data engine is not enabled", dataEngineSetting)
		return werror.NewForbiddenError(err.Error())
	}
	return nil
}

func IsRemovingLonghornFinalizer(oldObj runtime.Object, newObj runtime.Object) (bool, error) {
	oldMeta, err := meta.Accessor(oldObj)
	if err != nil {
		return false, err
	}
	newMeta, err := meta.Accessor(newObj)
	if err != nil {
		return false, err
	}
	oldFinalizers := oldMeta.GetFinalizers()
	newFinalizers := newMeta.GetFinalizers()

	if oldMeta.GetDeletionTimestamp() == nil {
		// The object is not being deleted.
		return false, nil
	}

	if len(newFinalizers) != len(oldFinalizers)-1 {
		// More or less than one finalizer is being removed.
		return false, nil
	}

	hadFinalizer := false
	for _, finalizer := range oldFinalizers {
		if finalizer == longhornFinalizerKey {
			hadFinalizer = true
			break
		}
	}
	if !hadFinalizer {
		// The old object didn't have the longhorn.io finalizer.
		return false, nil
	}

	hasFinalizer := false
	for _, finalizer := range newFinalizers {
		if finalizer == longhornFinalizerKey {
			hasFinalizer = true
			break
		}
	}
	if hasFinalizer {
		// The new object still has the longhorn.io finalizer.
		return false, nil
	}

	return true, nil
}

func GetBackupTargetInfoPatchOp(ds *datastore.DataStore, backupTargetName, backupTargetURL string, labels map[string]string) (admission.PatchOps, string, error) {
	var patchOps admission.PatchOps
	returningBackupTargetName := ""

	backupTarget, err := ds.GetBackupTargetRO(backupTargetName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, "", errors.Wrapf(err, "failed to get backup target")
		}
		if backupTargetURL != "" {
			backupTarget, err = ds.GetBackupTargetWithURLRO(backupTargetURL)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return nil, "", errors.Wrapf(err, "failed to get backup target by backup target URL")
				}
			}
		}
		if backupTarget == nil {
			backupTarget, err = ds.GetDefaultBackupTargetRO()
			if err != nil {
				return nil, "", errors.Wrapf(err, "failed to get default backup target")
			}
		}
		backupTargetName, err := json.Marshal(backupTarget.Name)
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to convert backup target name into JSON string")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupTargetName", "value": %s}`, string(backupTargetName)))
		returningBackupTargetName = backupTarget.Name
	}
	if backupTarget != nil {
		returningBackupTargetName = backupTarget.Name
	}

	patchLabels := true
	if labels == nil {
		labels = map[string]string{
			types.LonghornLabelBackupTarget: returningBackupTargetName,
		}
	} else if _, isExist := labels[types.LonghornLabelBackupTarget]; !isExist {
		labels[types.LonghornLabelBackupTarget] = returningBackupTargetName
	} else {
		patchLabels = false
	}
	if patchLabels {
		updatedJSONLabels, err := json.Marshal(labels)
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to convert backup labels into JSON string")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %s}`, string(updatedJSONLabels)))
	}

	if backupTargetURL != backupTarget.Spec.BackupTargetURL {
		backupTargetURL, err := json.Marshal(backupTarget.Spec.BackupTargetURL)
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to convert backup target url into JSON string")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupTargetURL", "value": %s}`, string(backupTargetURL)))
	}

	return patchOps, returningBackupTargetName, nil
}
