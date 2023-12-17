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
