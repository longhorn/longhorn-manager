package common

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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

func GetLonghornLabelsPatchOp(obj runtime.Object, longhornLabels map[string]string) (string, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	for k, v := range longhornLabels {
		labels[k] = v
	}

	bytes, err := json.Marshal(labels)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get JSON encoding labels of %v ", metadata.GetName())
	}

	return fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)), nil
}
