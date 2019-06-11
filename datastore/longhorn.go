package datastore

import (
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

const (
	LonghornVolumeKey = "longhornvolume"
	// NameMaximumLength restricted the length due to Kubernetes name limitation
	NameMaximumLength = 40
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group

	VerificationRetryInterval = 100 * time.Millisecond
	VerificationRetryCounts   = 20
)

func (s *DataStore) InitSettings() error {
	for _, sName := range types.SettingNameList {
		definition, ok := types.SettingDefinitions[sName]
		if !ok {
			return fmt.Errorf("BUG: setting %v is not defined", sName)
		}
		if _, err := s.sLister.Settings(s.namespace).Get(string(sName)); err != nil {
			if ErrorIsNotFound(err) {
				setting := &longhorn.Setting{
					ObjectMeta: metav1.ObjectMeta{
						Name: string(sName),
					},
					Setting: types.Setting{
						Value: definition.Default,
					},
				}
				if _, err := s.CreateSetting(setting); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
	return nil
}

func (s *DataStore) CreateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	// GetSetting automatically create default entry, so no need to double check
	return s.lhClient.LonghornV1alpha1().Settings(s.namespace).Create(setting)
}

func (s *DataStore) UpdateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	obj, err := s.lhClient.LonghornV1alpha1().Settings(s.namespace).Update(setting)
	if err != nil {
		return nil, err
	}
	verifyUpdate(setting.Name, obj, func(name string) (runtime.Object, error) {
		return s.getSettingRO(name)
	})
	return obj, nil
}

func (s *DataStore) getSettingRO(name string) (*longhorn.Setting, error) {
	return s.sLister.Settings(s.namespace).Get(name)
}

// GetSetting will automatically fill the non-existing setting if it's a valid
// setting name.
// The function will not return nil for *longhorn.Setting when error is nil
func (s *DataStore) GetSetting(sName types.SettingName) (*longhorn.Setting, error) {
	definition, ok := types.SettingDefinitions[sName]
	if !ok {
		return nil, fmt.Errorf("setting %v is not supported", sName)
	}
	resultRO, err := s.getSettingRO(string(sName))
	if err != nil {
		if !ErrorIsNotFound(err) {
			return nil, err
		}
		resultRO = &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(sName),
			},
			Setting: types.Setting{
				Value: definition.Default,
			},
		}
	}
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetSettingValueExisted(sName types.SettingName) (string, error) {
	setting, err := s.GetSetting(sName)
	if err != nil {
		return "", err
	}
	if setting.Value == "" {
		return "", fmt.Errorf("setting %v is empty", sName)
	}
	return setting.Value, nil
}

func (s *DataStore) ListSettings() (map[types.SettingName]*longhorn.Setting, error) {
	itemMap := make(map[types.SettingName]*longhorn.Setting)

	list, err := s.sLister.Settings(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		settingField := types.SettingName(itemRO.Name)
		// Ignore the items that we don't recongize
		if _, ok := types.SettingDefinitions[settingField]; ok {
			itemMap[settingField] = itemRO.DeepCopy()
		}
	}
	// fill up the missing entries
	for sName, definition := range types.SettingDefinitions {
		if _, ok := itemMap[sName]; !ok {
			itemMap[sName] = &longhorn.Setting{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(sName),
				},
				Setting: types.Setting{
					Value: definition.Default,
				},
			}
		}
	}
	return itemMap, nil
}

func (s *DataStore) GetCredentialFromSecret(secretName string) (map[string]string, error) {
	secret, err := s.kubeClient.CoreV1().Secrets(s.namespace).Get(secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	credentialSecret := make(map[string]string)
	if secret.Data != nil {
		credentialSecret[types.AWSAccessKey] = string(secret.Data[types.AWSAccessKey])
		credentialSecret[types.AWSSecretKey] = string(secret.Data[types.AWSSecretKey])
		credentialSecret[types.AWSEndPoint] = string(secret.Data[types.AWSEndPoint])
	}
	return credentialSecret, nil
}

func getVolumeLabels(volumeName string) map[string]string {
	return map[string]string{
		LonghornVolumeKey: volumeName,
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

func tagVolumeLabel(volumeName string, obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if labels[LonghornVolumeKey] == "" {
		labels[LonghornVolumeKey] = volumeName
	}
	metadata.SetLabels(labels)
	return nil
}

func fixupMetadata(volumeName string, obj runtime.Object) error {
	if err := tagVolumeLabel(volumeName, obj); err != nil {
		return err
	}
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
	ret, err := s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Create(v)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(v.Name, "volume", func(name string) (runtime.Object, error) {
		return s.getVolumeRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Volume)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for volume")
	}
	return ret, nil
}

func (s *DataStore) UpdateVolume(v *longhorn.Volume) (*longhorn.Volume, error) {
	if err := checkVolume(v); err != nil {
		return nil, err
	}
	if err := fixupMetadata(v.Name, v); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Update(v)
	if err != nil {
		return nil, err
	}
	verifyUpdate(v.Name, obj, func(name string) (runtime.Object, error) {
		return s.getVolumeRO(name)
	})
	return obj, nil
}

// DeleteVolume won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteVolume(name string) error {
	return s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForVolume will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForVolume(obj *longhorn.Volume) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1alpha1().Volumes(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for volume %v", obj.Name)
	}
	return nil
}

func (s *DataStore) GetVolume(name string) (*longhorn.Volume, error) {
	result, err := s.getVolume(name)
	if err != nil {
		return nil, err
	}
	return s.fixupVolume(result)
}

func (s *DataStore) getVolumeRO(name string) (*longhorn.Volume, error) {
	return s.vLister.Volumes(s.namespace).Get(name)
}

func (s *DataStore) getVolume(name string) (*longhorn.Volume, error) {
	resultRO, err := s.vLister.Volumes(s.namespace).Get(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) ListVolumesRO() ([]*longhorn.Volume, error) {
	return s.vLister.Volumes(s.namespace).List(labels.Everything())
}

func (s *DataStore) ListVolumes() (map[string]*longhorn.Volume, error) {
	itemMap := make(map[string]*longhorn.Volume)

	list, err := s.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name], err = s.fixupVolume(itemRO.DeepCopy())
		if err != nil {
			return nil, err
		}
	}
	return itemMap, nil
}

func (s *DataStore) ListStandbyVolumesRO() (map[string]*longhorn.Volume, error) {
	itemMap := make(map[string]*longhorn.Volume)

	list, err := s.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		if itemRO.Spec.Standby {
			itemMap[itemRO.Name] = itemRO
		}
	}
	return itemMap, nil
}

func (s *DataStore) fixupVolume(volume *longhorn.Volume) (*longhorn.Volume, error) {
	if volume.Status.Conditions == nil {
		volume.Status.Conditions = map[types.VolumeConditionType]types.Condition{}
	}
	// v0.3
	if !volume.Spec.Standby && volume.Spec.Frontend == "" {
		volume.Spec.Frontend = types.VolumeFrontendBlockDev
	}
	// v0.3
	if volume.Spec.EngineImage == "" {
		engines, err := s.ListVolumeEngines(volume.Name)
		if err != nil || len(engines) == 0 {
			return nil, fmt.Errorf("cannot fix up volume object, engine of %v cannot be found: %v", volume.Name, err)
		}
		if len(engines) != 1 {
			return nil, fmt.Errorf("cannot fix up volume object, detected multiple engines")
		}
		for _, e := range engines {
			volume.Spec.EngineImage = e.Spec.EngineImage
			break
		}
	}
	return volume, nil
}

func checkEngine(engine *longhorn.Engine) error {
	if engine.Name == "" || engine.Spec.VolumeName == "" {
		return fmt.Errorf("BUG: missing required field %+v", engine)
	}
	return nil
}

func (s *DataStore) CreateEngine(e *longhorn.Engine) (*longhorn.Engine, error) {
	if err := checkEngine(e); err != nil {
		return nil, err
	}
	if err := fixupMetadata(e.Spec.VolumeName, e); err != nil {
		return nil, err
	}
	if err := tagNodeLabel(e.Spec.NodeID, e); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1alpha1().Engines(s.namespace).Create(e)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(e.Name, "engine", func(name string) (runtime.Object, error) {
		return s.getEngineRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Engine)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for engine")
	}

	return ret, nil
}

func (s *DataStore) UpdateEngine(e *longhorn.Engine) (*longhorn.Engine, error) {
	if err := checkEngine(e); err != nil {
		return nil, err
	}
	if err := fixupMetadata(e.Spec.VolumeName, e); err != nil {
		return nil, err
	}
	if err := tagNodeLabel(e.Spec.NodeID, e); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1alpha1().Engines(s.namespace).Update(e)
	if err != nil {
		return nil, err
	}
	verifyUpdate(e.Name, obj, func(name string) (runtime.Object, error) {
		return s.getEngineRO(name)
	})
	return obj, nil
}

// DeleteEngine won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteEngine(name string) error {
	return s.lhClient.LonghornV1alpha1().Engines(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForEngine will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForEngine(obj *longhorn.Engine) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1alpha1().Engines(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for engine %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getEngineRO(name string) (*longhorn.Engine, error) {
	return s.eLister.Engines(s.namespace).Get(name)
}

func (s *DataStore) getEngine(name string) (*longhorn.Engine, error) {
	resultRO, err := s.getEngineRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetEngine(name string) (*longhorn.Engine, error) {
	result, err := s.eLister.Engines(s.namespace).Get(name)
	if err != nil {
		return nil, err
	}
	return s.fixupEngine(result)
}

func (s *DataStore) listEngines(selector labels.Selector) (map[string]*longhorn.Engine, error) {
	list, err := s.eLister.Engines(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	engines := map[string]*longhorn.Engine{}
	for _, e := range list {
		// Cannot use cached object from lister
		engines[e.Name], err = s.fixupEngine(e.DeepCopy())
		if err != nil {
			return nil, err
		}
	}
	return engines, nil
}

func (s *DataStore) ListEngines() (map[string]*longhorn.Engine, error) {
	return s.listEngines(labels.Everything())
}

func (s *DataStore) ListVolumeEngines(volumeName string) (map[string]*longhorn.Engine, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	return s.listEngines(selector)
}

func (s *DataStore) fixupEngine(engine *longhorn.Engine) (*longhorn.Engine, error) {
	// v0.3
	if engine.Spec.VolumeSize == 0 || engine.Spec.Frontend == "" {
		volume, err := s.getVolumeRO(engine.Spec.VolumeName)
		if err != nil {
			return nil, fmt.Errorf("BUG: cannot fix up engine object, volume %v cannot be found", engine.Spec.VolumeName)
		}
		engine.Spec.VolumeSize = volume.Spec.Size
		engine.Spec.Frontend = volume.Spec.Frontend
	}
	return engine, nil
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
	if err := tagNodeLabel(r.Spec.NodeID, r); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Create(r)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(r.Name, "replica", func(name string) (runtime.Object, error) {
		return s.getReplicaRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Replica)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for replica")
	}

	return ret, nil
}

func (s *DataStore) UpdateReplica(r *longhorn.Replica) (*longhorn.Replica, error) {
	if err := checkReplica(r); err != nil {
		return nil, err
	}
	if err := fixupMetadata(r.Spec.VolumeName, r); err != nil {
		return nil, err
	}
	if err := tagNodeLabel(r.Spec.NodeID, r); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Update(r)
	if err != nil {
		return nil, err
	}
	verifyUpdate(r.Name, obj, func(name string) (runtime.Object, error) {
		return s.getReplicaRO(name)
	})
	return obj, nil
}

// DeleteReplica won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteReplica(name string) error {
	return s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForReplica will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForReplica(obj *longhorn.Replica) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1alpha1().Replicas(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for replica %v", obj.Name)
	}
	return nil
}

func (s *DataStore) GetReplica(name string) (*longhorn.Replica, error) {
	result, err := s.getReplica(name)
	if err != nil {
		return nil, err
	}
	return s.fixupReplica(result)
}

func (s *DataStore) getReplicaRO(name string) (*longhorn.Replica, error) {
	return s.rLister.Replicas(s.namespace).Get(name)
}

func (s *DataStore) getReplica(name string) (*longhorn.Replica, error) {
	resultRO, err := s.rLister.Replicas(s.namespace).Get(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) listReplicas(selector labels.Selector) (map[string]*longhorn.Replica, error) {
	list, err := s.rLister.Replicas(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}

	itemMap := map[string]*longhorn.Replica{}
	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name], err = s.fixupReplica(itemRO.DeepCopy())
		if err != nil {
			return nil, err
		}
	}
	return itemMap, nil
}

func (s *DataStore) ListReplicas() (map[string]*longhorn.Replica, error) {
	return s.listReplicas(labels.Everything())
}

func (s *DataStore) ListVolumeReplicas(volumeName string) (map[string]*longhorn.Replica, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	return s.listReplicas(selector)
}

func (s *DataStore) fixupReplica(replica *longhorn.Replica) (*longhorn.Replica, error) {
	// v0.3
	if replica.Spec.EngineName == "" {
		engines, err := s.ListVolumeEngines(replica.Spec.VolumeName)
		if err != nil {
			return nil, err
		}
		if len(engines) != 1 {
			return nil, fmt.Errorf("cannot find the default engine the replica %v belong to", replica.Name)
		}
		for name := range engines {
			replica.Spec.EngineName = name
			break
		}
	}
	if replica.Spec.NodeID == "" {
		// allow scheduler to continue
		return replica, nil
	}
	if replica.Spec.DiskID == "" {
		// replica needs to be scheduled before assign diskID and dataPath
		node, err := s.GetNode(replica.Spec.NodeID)
		if err != nil {
			if ErrorIsNotFound(err) {
				return nil, fmt.Errorf("cannot find node %v for replica %v", replica.Spec.NodeID, replica.Name)
			}
			return nil, err
		}
		for fsid, disk := range node.Spec.Disks {
			if disk.Path == types.DefaultLonghornDirectory {
				replica.Spec.DiskID = fsid
				break
			}
		}
		if replica.Spec.DiskID == "" {
			return nil, fmt.Errorf("cannot find default disk on node %v for replica %v", replica.Spec.NodeID, replica.Name)
		}
	}
	if replica.Spec.DataPath == "" {
		replica.Spec.DataPath = filepath.Join(types.DefaultLonghornDirectory, "/replicas/", replica.Name)
		// We cannot tell if the field `Active` exists in the object since it's a bool
		// so if it's old version, we will set it
		replica.Spec.Active = true
	}
	return replica, nil
}

func (s *DataStore) CreateEngineImage(img *longhorn.EngineImage) (*longhorn.EngineImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, img); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1alpha1().EngineImages(s.namespace).Create(img)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(img.Name, "engine image", func(name string) (runtime.Object, error) {
		return s.getEngineImageRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.EngineImage)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for engine image")
	}

	return ret, nil
}

func (s *DataStore) UpdateEngineImage(img *longhorn.EngineImage) (*longhorn.EngineImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, img); err != nil {
		return nil, err
	}

	obj, err := s.lhClient.LonghornV1alpha1().EngineImages(s.namespace).Update(img)
	if err != nil {
		return nil, err
	}
	verifyUpdate(img.Name, obj, func(name string) (runtime.Object, error) {
		return s.getEngineImageRO(name)
	})
	return obj, nil
}

// DeleteEngineImage won't result in immediately deletion since finalizer was set by default
func (s *DataStore) DeleteEngineImage(name string) error {
	return s.lhClient.LonghornV1alpha1().EngineImages(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

// RemoveFinalizerForEngineImage will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForEngineImage(obj *longhorn.EngineImage) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1alpha1().EngineImages(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for engine image %v", obj.Name)
	}
	return nil
}

func (s *DataStore) getEngineImageRO(name string) (*longhorn.EngineImage, error) {
	return s.iLister.EngineImages(s.namespace).Get(name)
}

func (s *DataStore) getEngineImage(name string) (*longhorn.EngineImage, error) {
	resultRO, err := s.getEngineImageRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetEngineImage(name string) (*longhorn.EngineImage, error) {
	result, err := s.getEngineImage(name)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *DataStore) ListEngineImages() (map[string]*longhorn.EngineImage, error) {
	itemMap := map[string]*longhorn.EngineImage{}

	list, err := s.iLister.EngineImages(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

func (s *DataStore) CreateNode(node *longhorn.Node) (*longhorn.Node, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, node); err != nil {
		return nil, err
	}
	ret, err := s.lhClient.LonghornV1alpha1().Nodes(s.namespace).Create(node)
	if err != nil {
		return nil, err
	}
	if SkipListerCheck {
		return ret, nil
	}

	obj, err := verifyCreation(node.Name, "node", func(name string) (runtime.Object, error) {
		return s.getNodeRO(name)
	})
	if err != nil {
		return nil, err
	}
	ret, ok := obj.(*longhorn.Node)
	if !ok {
		return nil, fmt.Errorf("BUG: datastore: verifyCreation returned wrong type for node")
	}

	return ret, nil
}

// CreateDefaultNode will set default directory to node replica mount path
func (s *DataStore) CreateDefaultNode(name string) (*longhorn.Node, error) {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: types.NodeSpec{
			Name:            name,
			AllowScheduling: true,
		},
	}
	diskInfo, err := util.GetDiskInfo(types.DefaultLonghornDirectory)
	if err != nil {
		return nil, err
	}

	defaultDisk := map[string]types.DiskSpec{
		diskInfo.Fsid: {
			Path:            diskInfo.Path,
			AllowScheduling: true,
			StorageReserved: diskInfo.StorageMaximum * 30 / 100,
		},
	}
	node.Spec.Disks = defaultDisk

	return s.CreateNode(node)
}

func (s *DataStore) getNodeRO(name string) (*longhorn.Node, error) {
	return s.nLister.Nodes(s.namespace).Get(name)
}

func (s *DataStore) getNode(name string) (*longhorn.Node, error) {
	resultRO, err := s.getNodeRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetNode(name string) (*longhorn.Node, error) {
	node, err := s.getNode(name)
	if err != nil {
		return nil, err
	}
	if node.Status.Conditions == nil {
		node.Status.Conditions = map[types.NodeConditionType]types.Condition{}
	}
	return node, nil
}

func (s *DataStore) UpdateNode(node *longhorn.Node) (*longhorn.Node, error) {
	obj, err := s.lhClient.LonghornV1alpha1().Nodes(s.namespace).Update(node)
	if err != nil {
		return nil, err
	}
	verifyUpdate(node.Name, obj, func(name string) (runtime.Object, error) {
		return s.getNodeRO(name)
	})
	return obj, nil
}

func (s *DataStore) ListNodes() (map[string]*longhorn.Node, error) {
	itemMap := make(map[string]*longhorn.Node)

	nodeList, err := s.nLister.Nodes(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, node := range nodeList {
		// Cannot use cached object from lister
		result := node.DeepCopy()
		if result.Status.Conditions == nil {
			result.Status.Conditions = map[types.NodeConditionType]types.Condition{}
		}
		itemMap[node.Name] = result
	}
	return itemMap, nil
}

// RemoveFinalizerForNode will result in deletion if DeletionTimestamp was set
func (s *DataStore) RemoveFinalizerForNode(obj *longhorn.Node) error {
	if !util.FinalizerExists(longhornFinalizerKey, obj) {
		// finalizer already removed
		return nil
	}
	if err := util.RemoveFinalizer(longhornFinalizerKey, obj); err != nil {
		return err
	}
	_, err := s.lhClient.LonghornV1alpha1().Nodes(s.namespace).Update(obj)
	if err != nil {
		// workaround `StorageError: invalid object, Code: 4` due to empty object
		if obj.DeletionTimestamp != nil {
			return nil
		}
		return errors.Wrapf(err, "unable to remove finalizer for node %v", obj.Name)
	}
	return nil
}

func (s *DataStore) IsNodeDownOrDeleted(name string) (bool, error) {
	node, err := s.getNodeRO(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	cond := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
	if cond.Status == types.ConditionStatusFalse &&
		(cond.Reason == string(types.NodeConditionReasonKubernetesNodeGone) ||
			cond.Reason == string(types.NodeConditionReasonKubernetesNodeNotReady)) {
		return true, nil
	}
	return false, nil
}

func getNodeSelector(nodeName string) (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{
			types.LonghornNodeKey: nodeName,
		},
	})
}

func (s *DataStore) ListReplicasByNode(name string) (map[string][]*longhorn.Replica, error) {
	nodeSelector, err := getNodeSelector(name)
	if err != nil {
		return nil, err
	}
	replicaList, err := s.rLister.Replicas(s.namespace).List(nodeSelector)
	if err != nil {
		return nil, err
	}

	replicaDiskMap := map[string][]*longhorn.Replica{}
	for _, replica := range replicaList {
		if _, ok := replicaDiskMap[replica.Spec.DiskID]; !ok {
			replicaDiskMap[replica.Spec.DiskID] = []*longhorn.Replica{}
		}
		replicaDiskMap[replica.Spec.DiskID] = append(replicaDiskMap[replica.Spec.DiskID], replica.DeepCopy())
	}
	return replicaDiskMap, nil
}

func tagNodeLabel(nodeID string, obj runtime.Object) error {
	// fix longhornnode label for object
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[types.LonghornNodeKey] = nodeID
	metadata.SetLabels(labels)
	return nil
}

func (s *DataStore) GetSettingAsInt(settingName types.SettingName) (int64, error) {
	definition, ok := types.SettingDefinitions[settingName]
	if !ok {
		return 0, fmt.Errorf("setting %v is not supported", settingName)
	}
	settings, err := s.GetSetting(settingName)
	if err != nil {
		return 0, err
	}
	value := settings.Value

	if definition.Type == types.SettingTypeInt {
		result, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, err
		}
		return result, nil
	}

	return 0, fmt.Errorf("The %v setting value couldn't change to integer, value is %v ", string(settingName), value)
}

func (s *DataStore) GetSettingAsBool(settingName types.SettingName) (bool, error) {
	definition, ok := types.SettingDefinitions[settingName]
	if !ok {
		return false, fmt.Errorf("setting %v is not supported", settingName)
	}
	settings, err := s.GetSetting(settingName)
	if err != nil {
		return false, err
	}
	value := settings.Value

	if definition.Type == types.SettingTypeBool {
		result, err := strconv.ParseBool(value)
		if err != nil {
			return false, err
		}
		return result, nil
	}

	return false, fmt.Errorf("The %v setting value couldn't be converted to bool, value is %v ", string(settingName), value)
}

func (s *DataStore) UpdateVolumeAndOwner(v *longhorn.Volume) (*longhorn.Volume, error) {
	engines, err := s.ListVolumeEngines(v.Name)
	if err != nil {
		return nil, err
	}
	for _, engine := range engines {
		if engine.Spec.OwnerID != v.Spec.OwnerID {
			engine.Spec.OwnerID = v.Spec.OwnerID
			if _, err := s.UpdateEngine(engine); err != nil {
				return nil, err
			}
		}
	}

	replicas, err := s.ListVolumeReplicas(v.Name)
	if err != nil {
		return nil, err
	}
	for _, replica := range replicas {
		if replica.Spec.OwnerID != v.Spec.OwnerID {
			replica.Spec.OwnerID = v.Spec.OwnerID
			if _, err := s.UpdateReplica(replica); err != nil {
				return nil, err
			}
		}
	}

	v, err = s.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (s *DataStore) ResetEngineMonitoringStatus(e *longhorn.Engine) (*longhorn.Engine, error) {
	e.Status.Endpoint = ""
	e.Status.ReplicaModeMap = nil
	ret, err := s.UpdateEngine(e)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to reset engine status for %v", e.Name)
	}
	return ret, nil
}

func (s *DataStore) DeleteNode(name string) error {
	return s.lhClient.LonghornV1alpha1().Nodes(s.namespace).Delete(name, &metav1.DeleteOptions{})
}

func (s *DataStore) ListEnginesByNode(name string) ([]*longhorn.Engine, error) {
	nodeSelector, err := getNodeSelector(name)
	engineList, err := s.eLister.Engines(s.namespace).List(nodeSelector)
	if err != nil {
		return nil, err
	}
	return engineList, nil
}

func verifyCreation(name, kind string, getMethod func(name string) (runtime.Object, error)) (runtime.Object, error) {
	// WORKAROUND: The immedidate read after object's creation can fail.
	// See https://github.com/longhorn/longhorn/issues/133
	var (
		ret runtime.Object
		err error
	)
	for i := 0; i < VerificationRetryCounts; i++ {
		if ret, err = getMethod(name); err == nil {
			break
		}
		if !ErrorIsNotFound(err) {
			break
		}
		time.Sleep(VerificationRetryInterval)
	}
	if err != nil {
		return nil, fmt.Errorf("Unable to verify the existance of newly created %s %s: %v", kind, name, err)
	}
	return ret, nil
}

func verifyUpdate(name string, obj runtime.Object, getMethod func(name string) (runtime.Object, error)) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		logrus.Errorf("BUG: datastore: cannot verify update for %v (%+v) because cannot get accessor: %v", name, obj, err)
		return
	}
	minimalResourceVersion := accessor.GetResourceVersion()
	verified := false
	for i := 0; i < VerificationRetryCounts; i++ {
		ret, err := getMethod(name)
		if err != nil {
			logrus.Errorf("datastore: failed to get updated object %v", name)
			return
		}
		accessor, err := meta.Accessor(ret)
		if err != nil {
			logrus.Errorf("BUG: datastore: cannot verify update for %v because cannot get accessor for updated object: %v", name, err)
			return
		}
		if resourceVersionAtLeast(accessor.GetResourceVersion(), minimalResourceVersion) {
			verified = true
			break
		}
		time.Sleep(VerificationRetryInterval)
	}
	if !verified {
		logrus.Errorf("Unable to verify the update of %s", name)
	}
}

// resourceVersionAtLeast depends on the Kubernetes internal resource version implmentation
// See https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
func resourceVersionAtLeast(curr, min string) bool {
	// skip unit testing code
	if curr == "" || min == "" {
		return true
	}
	currVersion, err := strconv.ParseInt(curr, 10, 64)
	if err != nil {
		logrus.Errorf("datastore: failed to parse current resource version %v: %v", curr, err)
		return false
	}
	minVersion, err := strconv.ParseInt(min, 10, 64)
	if err != nil {
		logrus.Errorf("datastore: failed to parse minimal resource version %v: %v", min, err)
		return false
	}
	return currVersion >= minVersion
}
