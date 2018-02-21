package datastore

import (
	"fmt"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

const (
	longhornVolumeKey = "longhornvolume"
	// NameMaximumLength restricted the length due to Kubernetes name limitation
	NameMaximumLength = 32

	SettingName = "longhorn-manager-settings"
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

func (s *DataStore) CreateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	setting.Name = SettingName
	return s.lhClient.LonghornV1alpha1().Settings(s.namespace).Create(setting)
}

func (s *DataStore) UpdateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	setting.Name = SettingName
	return s.lhClient.LonghornV1alpha1().Settings(s.namespace).Update(setting)
}

func (s *DataStore) GetSetting() (*longhorn.Setting, error) {
	result, err := s.lhClient.LonghornV1alpha1().Settings(s.namespace).Get(SettingName,
		metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return result, err
}

func getVolumeLabels(volumeName string) map[string]string {
	return map[string]string{
		longhornVolumeKey: volumeName,
	}
}

func checkVolume(v *longhorn.Volume) error {
	size, err := util.ConvertSize(v.Spec.Size)
	if err != nil {
		return err
	}
	if v.Name == "" || size == 0 || v.Spec.NumberOfReplicas == 0 {
		return fmt.Errorf("BUG: missing required field %+v", v)
	}
	errs := validation.IsDNS1123Label(v.Name)
	if len(errs) != 0 {
		return fmt.Errorf("Invalid volume name: %+v", errs)
	}
	if len(v.Name) > NameMaximumLength {
		return fmt.Errorf("Volume name is too long %v, must be less than %v characters",
			v.Name, NameMaximumLength)
	}
	return nil
}

func fixupMetadata(volumeName string, obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if labels[longhornVolumeKey] == "" {
		labels[longhornVolumeKey] = volumeName
	}
	metadata.SetLabels(labels)

	if err := util.AddFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	return nil
}

func getVolumeSelector(volumeName string) (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: getVolumeLabels(volumeName),
	})
}

func (s *DataStore) CreateVolume(v *longhorn.Volume) (*longhorn.Volume, error) {
	if err := checkVolume(v); err != nil {
		return nil, err
	}
	if err := fixupMetadata(v.Name, v); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Create(v)
}

func (s *DataStore) UpdateVolume(v *longhorn.Volume) (*longhorn.Volume, error) {
	if err := checkVolume(v); err != nil {
		return nil, err
	}
	if err := fixupMetadata(v.Name, v); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Update(v)
}

// DeleteVolume won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteVolume(name string) error {
	return s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForVolume will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForVolume(name string) error {
	obj, err := s.GetVolume(name)
	if obj == nil {
		// already deleted
		return nil
	}
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err = s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for volume %v", name)
	}
	return nil
}

func (s *DataStore) GetVolume(name string) (*longhorn.Volume, error) {
	resultRO, err := s.vLister.Volumes(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) ListVolumes() (map[string]*longhorn.Volume, error) {
	itemMap := make(map[string]*longhorn.Volume)

	list, err := s.vLister.Volumes(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return map[string]*longhorn.Volume{}, nil
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

func checkEngine(engine *longhorn.Controller) error {
	if engine.Name == "" || engine.Spec.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", engine)
	}
	return nil
}

func (s *DataStore) CreateEngine(e *longhorn.Controller) (*longhorn.Controller, error) {
	if err := checkEngine(e); err != nil {
		return nil, err
	}
	if err := fixupMetadata(e.Spec.VolumeName, e); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().Controllers(s.namespace).Create(e)
}

func (s *DataStore) UpdateEngine(e *longhorn.Controller) (*longhorn.Controller, error) {
	if err := checkEngine(e); err != nil {
		return nil, err
	}
	if err := fixupMetadata(e.Spec.VolumeName, e); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().Controllers(s.namespace).Update(e)
}

// DeleteEngine won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteEngine(name string) error {
	return s.lhClient.LonghornV1alpha1().Controllers(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForEngine will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForEngine(name string) error {
	obj, err := s.GetEngine(name)
	if obj == nil {
		// already deleted
		return nil
	}
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err = s.lhClient.LonghornV1alpha1().Controllers(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for engine %v", name)
	}
	return nil
}

func (s *DataStore) GetEngine(name string) (*longhorn.Controller, error) {
	resultRO, err := s.eLister.Controllers(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetVolumeEngine(volumeName string) (*longhorn.Controller, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	list, err := s.eLister.Controllers(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	if len(list) > 1 {
		return nil, fmt.Errorf("find more than one engine for volume %v: %+v", volumeName, list)
	}
	return list[0], nil
}

func checkReplica(r *longhorn.Replica) error {
	if r.Name == "" || r.Spec.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", r)
	}
	if (r.Status.CurrentState == types.InstanceStateRunning) != (r.Status.IP != "") {
		return fmt.Errorf("BUG: instance state and IP wasn't in sync %+v", r)
	}
	if (r.Spec.RestoreFrom != "") != (r.Spec.RestoreName != "") {
		return fmt.Errorf("BUG: replica RestoreFrom and RestoreName value wasn't in sync %+v", r)
	}
	return nil
}

func (s *DataStore) CreateReplica(r *longhorn.Replica) (*longhorn.Replica, error) {
	if err := checkReplica(r); err != nil {
		return nil, err
	}
	if err := fixupMetadata(r.Spec.VolumeName, r); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Create(r)
}

func (s *DataStore) UpdateReplica(r *longhorn.Replica) (*longhorn.Replica, error) {
	if err := checkReplica(r); err != nil {
		return nil, err
	}
	if err := fixupMetadata(r.Spec.VolumeName, r); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Update(r)
}

// DeleteReplica won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteReplica(name string) error {
	return s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForReplica will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForReplica(name string) error {
	obj, err := s.GetReplica(name)
	if obj == nil {
		// already deleted
		return nil
	}
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err = s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for replica %v", name)
	}
	return nil
}

func (s *DataStore) GetReplica(name string) (*longhorn.Replica, error) {
	resultRO, err := s.rLister.Replicas(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetVolumeReplicas(volumeName string) (map[string]*longhorn.Replica, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	list, err := s.rLister.Replicas(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return map[string]*longhorn.Replica{}, nil
	}
	replicas := map[string]*longhorn.Replica{}
	for _, r := range list {
		// Cannot use cached object from lister
		replicas[r.Name] = r.DeepCopy()
	}
	return replicas, nil
}
