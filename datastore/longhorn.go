package datastore

import (
	"fmt"
	"path/filepath"

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
	NameMaximumLength = 40
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

func (s *DataStore) CreateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	return s.lhClient.LonghornV1alpha1().Settings(s.namespace).Create(setting)
}

func (s *DataStore) UpdateSetting(setting *longhorn.Setting) (*longhorn.Setting, error) {
	return s.lhClient.LonghornV1alpha1().Settings(s.namespace).Update(setting)
}

// GetSetting will automatically fill the non-existing setting if it's a valid
// setting name.
// The function will not return nil for *longhorn.Setting when error is nil
func (s *DataStore) GetSetting(sName types.SettingName) (*longhorn.Setting, error) {
	definition, ok := types.SettingDefinitions[sName]
	if !ok {
		return nil, fmt.Errorf("setting %v is not supported", sName)
	}
	resultRO, err := s.sLister.Settings(s.namespace).Get(string(sName))
	if err != nil {
		if !apierrors.IsNotFound(err) {
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

func (s *DataStore) ListSettings() (map[types.SettingName]*longhorn.Setting, error) {
	itemMap := make(map[types.SettingName]*longhorn.Setting)

	list, err := s.sLister.Settings(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return map[types.SettingName]*longhorn.Setting{}, nil
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

func tagVolumeLabel(volumeName string, obj runtime.Object) error {
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
	resultRO, err := s.getVolumeRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return s.fixupVolume(resultRO.DeepCopy())
}

func (s *DataStore) getVolumeRO(name string) (*longhorn.Volume, error) {
	resultRO, err := s.vLister.Volumes(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return resultRO, nil
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
		itemMap[itemRO.Name], err = s.fixupVolume(itemRO.DeepCopy())
		if err != nil {
			return nil, err
		}
	}
	return itemMap, nil
}

func (s *DataStore) fixupVolume(volume *longhorn.Volume) (*longhorn.Volume, error) {
	if volume == nil {
		return nil, nil
	}
	// v0.3
	if volume.Spec.Frontend == "" {
		volume.Spec.Frontend = types.VolumeFrontendBlockDev
	}
	// v0.3
	if volume.Spec.EngineImage == "" {
		engine, err := s.getVolumeEngineRO(volume.Name)
		if err != nil || engine == nil {
			return nil, fmt.Errorf("cannot fix up volume object, engine of %v cannot be found: %v", volume.Name, err)
		}
		volume.Spec.EngineImage = engine.Spec.EngineImage
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
	return s.lhClient.LonghornV1alpha1().Engines(s.namespace).Create(e)
}

func (s *DataStore) UpdateEngine(e *longhorn.Engine) (*longhorn.Engine, error) {
	if err := checkEngine(e); err != nil {
		return nil, err
	}
	if err := fixupMetadata(e.Spec.VolumeName, e); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().Engines(s.namespace).Update(e)
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

func (s *DataStore) GetEngine(name string) (*longhorn.Engine, error) {
	resultRO, err := s.eLister.Engines(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	// Cannot use cached object from lister
	return s.fixupEngine(resultRO.DeepCopy())
}

func (s *DataStore) GetVolumeEngine(volumeName string) (*longhorn.Engine, error) {
	resultRO, err := s.getVolumeEngineRO(volumeName)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return s.fixupEngine(resultRO.DeepCopy())
}

func (s *DataStore) getVolumeEngineRO(volumeName string) (*longhorn.Engine, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	list, err := s.eLister.Engines(s.namespace).List(selector)
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

func (s *DataStore) fixupEngine(engine *longhorn.Engine) (*longhorn.Engine, error) {
	if engine == nil {
		return nil, nil
	}
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
	resultRO, err := s.getReplicaRO(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return s.fixupReplica(resultRO.DeepCopy())
}

func (s *DataStore) getReplicaRO(name string) (*longhorn.Replica, error) {
	resultRO, err := s.rLister.Replicas(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return resultRO, nil
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
		replicas[r.Name], err = s.fixupReplica(r.DeepCopy())
		if err != nil {
			return nil, err
		}
	}
	return replicas, nil
}

func (s *DataStore) fixupReplica(replica *longhorn.Replica) (*longhorn.Replica, error) {
	if replica == nil {
		return nil, nil
	}
	// v0.3
	if replica.Spec.DataPath == "" {
		replica.Spec.DataPath = filepath.Join(types.DefaultLonghornDirectory, "/replicas/", replica.Name)
		// We cannot judge if the field `Active` exists separately, but
		// if it's old version, we will set it
		replica.Spec.Active = true
	}
	return replica, nil
}

func (s *DataStore) CreateEngineImage(img *longhorn.EngineImage) (*longhorn.EngineImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, img); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().EngineImages(s.namespace).Create(img)
}

func (s *DataStore) UpdateEngineImage(img *longhorn.EngineImage) (*longhorn.EngineImage, error) {
	if err := util.AddFinalizer(longhornFinalizerKey, img); err != nil {
		return nil, err
	}
	return s.lhClient.LonghornV1alpha1().EngineImages(s.namespace).Update(img)
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

func (s *DataStore) GetEngineImage(name string) (*longhorn.EngineImage, error) {
	resultRO, err := s.iLister.EngineImages(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) ListEngineImages() (map[string]*longhorn.EngineImage, error) {
	itemMap := map[string]*longhorn.EngineImage{}

	list, err := s.iLister.EngineImages(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return itemMap, nil
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
	return s.lhClient.LonghornV1alpha1().Nodes(s.namespace).Create(node)
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
		Status: types.NodeStatus{
			State: types.NodeStateUp,
		},
	}
	return s.CreateNode(node)
}

func (s *DataStore) GetNode(name string) (*longhorn.Node, error) {
	result, err := s.nLister.Nodes(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return result.DeepCopy(), nil
}

func (s *DataStore) UpdateNode(node *longhorn.Node) (*longhorn.Node, error) {
	return s.lhClient.LonghornV1alpha1().Nodes(s.namespace).Update(node)
}

func (s *DataStore) ListNodes() (map[string]*longhorn.Node, error) {
	itemMap := make(map[string]*longhorn.Node)

	nodeList, err := s.nLister.Nodes(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	if len(nodeList) == 0 {
		return map[string]*longhorn.Node{}, nil
	}

	for _, node := range nodeList {
		// Cannot use cached object from lister
		itemMap[node.Name] = node.DeepCopy()
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
