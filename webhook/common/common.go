package common

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

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
