package common

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

func GetLonghornFinalizerPatchOp(obj runtime.Object) (string, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, obj); err != nil {
		return "", err
	}

	metadata, err := meta.Accessor(obj)
	if err != nil {
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

	if removingLabels != nil {
		for k := range removingLabels {
			delete(labels, k)
		}
	}
	if requiredLabels != nil {
		for k, v := range requiredLabels {
			labels[k] = v
		}
	}

	volumeName := ""
	switch obj.(type) {
	case *longhorn.Volume:
		volumeName = obj.(*longhorn.Volume).Name
	case *longhorn.Engine:
		volumeName = obj.(*longhorn.Engine).Spec.VolumeName
	case *longhorn.Replica:
		volumeName = obj.(*longhorn.Replica).Spec.VolumeName
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
