package common

import (
	"fmt"
	"reflect"
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

func admissionDataEngineMode(ds *datastore.DataStore) (types.DataEngineMode, error) {
	if ds == nil {
		return "", werror.NewInternalError("missing installation data engine capability")
	}
	mode, err := types.ParseDataEngineMode(string(ds.DataEngineMode()))
	if err != nil {
		return "", werror.NewInternalError(err.Error())
	}
	return mode, nil
}

func ValidateDataEngineCapability(ds *datastore.DataStore, engine longhorn.DataEngineType) error {
	mode, err := admissionDataEngineMode(ds)
	if err != nil {
		return err
	}
	if mode == types.DataEngineModeV2Only && engine != longhorn.DataEngineTypeV2 {
		return werror.NewForbiddenError(fmt.Sprintf("data engine %q is outside the v2-only installation capability", engine))
	}
	return nil
}

// ValidateDataEngineCapabilityUpdate reports whether the request is a strictly
// bounded finalizer cleanup that must bypass mutation and activation checks.
func ValidateDataEngineCapabilityUpdate(ds *datastore.DataStore, oldObj, newObj runtime.Object) (bool, error) {
	mode, err := admissionDataEngineMode(ds)
	if err != nil {
		return false, err
	}
	if mode != types.DataEngineModeV2Only {
		return false, nil
	}
	oldEngine, err := resourceDataEngine(oldObj)
	if err != nil {
		return false, werror.NewInvalidError(err.Error(), "")
	}
	newEngine, err := resourceDataEngine(newObj)
	if err != nil {
		return false, werror.NewInvalidError(err.Error(), "")
	}
	if reflect.TypeOf(oldObj) != reflect.TypeOf(newObj) {
		return false, werror.NewInvalidError("data engine update requires matching Longhorn resource types", "")
	}
	cleanup, err := IsLonghornFinalizerOnlyRemoval(oldObj, newObj)
	if err != nil {
		return false, werror.NewInvalidError(err.Error(), "")
	}
	if cleanup {
		return true, nil
	}
	oldMeta, err := meta.Accessor(oldObj)
	if err != nil {
		return false, werror.NewInvalidError(err.Error(), "")
	}
	newMeta, err := meta.Accessor(newObj)
	if err != nil {
		return false, werror.NewInvalidError(err.Error(), "")
	}
	countFinalizer := func(finalizers []string) int {
		count := 0
		for _, finalizer := range finalizers {
			if finalizer == longhornFinalizerKey {
				count++
			}
		}
		return count
	}
	if countFinalizer(oldMeta.GetFinalizers()) > countFinalizer(newMeta.GetFinalizers()) {
		return false, werror.NewInvalidError("Longhorn finalizer removal must preserve the entire deleting resource", "metadata.finalizers")
	}
	if err := ValidateDataEngineCapability(ds, oldEngine); err != nil {
		return false, err
	}
	return false, ValidateDataEngineCapability(ds, newEngine)
}

func IsLonghornFinalizerOnlyRemoval(oldObj, newObj runtime.Object) (bool, error) {
	if _, err := resourceDataEngine(oldObj); err != nil {
		return false, err
	}
	if _, err := resourceDataEngine(newObj); err != nil {
		return false, err
	}
	if reflect.TypeOf(oldObj) != reflect.TypeOf(newObj) {
		return false, fmt.Errorf("finalizer cleanup requires matching resource types")
	}
	oldMeta, err := meta.Accessor(oldObj)
	if err != nil {
		return false, err
	}
	newMeta, err := meta.Accessor(newObj)
	if err != nil {
		return false, err
	}
	if oldMeta.GetName() == "" || oldMeta.GetNamespace() == "" || oldMeta.GetUID() == "" || oldMeta.GetResourceVersion() == "" ||
		oldMeta.GetName() != newMeta.GetName() || oldMeta.GetNamespace() != newMeta.GetNamespace() || oldMeta.GetUID() != newMeta.GetUID() ||
		oldMeta.GetDeletionTimestamp() == nil || newMeta.GetDeletionTimestamp() == nil || !oldMeta.GetDeletionTimestamp().Equal(newMeta.GetDeletionTimestamp()) {
		return false, nil
	}
	seen := map[string]bool{}
	expected := []string{}
	for _, finalizer := range oldMeta.GetFinalizers() {
		if seen[finalizer] {
			return false, nil
		}
		seen[finalizer] = true
		if finalizer != longhornFinalizerKey {
			expected = append(expected, finalizer)
		}
	}
	if !seen[longhornFinalizerKey] || !slices.Equal(expected, newMeta.GetFinalizers()) {
		return false, nil
	}
	oldCopy, newCopy := oldObj.DeepCopyObject(), newObj.DeepCopyObject()
	oldCopyMeta, err := meta.Accessor(oldCopy)
	if err != nil {
		return false, err
	}
	newCopyMeta, err := meta.Accessor(newCopy)
	if err != nil {
		return false, err
	}
	oldCopyMeta.SetFinalizers(nil)
	newCopyMeta.SetFinalizers(nil)
	oldCopyMeta.SetManagedFields(nil)
	newCopyMeta.SetManagedFields(nil)
	return reflect.DeepEqual(oldCopy, newCopy), nil
}

func resourceDataEngine(obj runtime.Object) (longhorn.DataEngineType, error) {
	switch object := obj.(type) {
	case *longhorn.Volume:
		if object != nil {
			return object.Spec.DataEngine, nil
		}
	case *longhorn.Engine:
		if object != nil {
			return object.Spec.DataEngine, nil
		}
	case *longhorn.Replica:
		if object != nil {
			return object.Spec.DataEngine, nil
		}
	case *longhorn.InstanceManager:
		if object != nil {
			return object.Spec.DataEngine, nil
		}
	}
	return "", fmt.Errorf("expected a non-nil Longhorn data engine resource, got %T", obj)
}
