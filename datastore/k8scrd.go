package datastore

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	apiv1 "k8s.io/api/core/v1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/longhorn-manager/types"

	"github.com/rancher/longhorn-manager/k8s"
	"github.com/rancher/longhorn-manager/k8s/crdclient"
	lh "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"

	fakeclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

type CRDStore struct {
	clientset lhclientset.Interface
	namespace string
}

const SettingName = "longhorn-manager-settings"

func NewCRDStore(kubeconfig string) (*CRDStore, error) {
	config, err := k8s.GetClientConfig(kubeconfig)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get client config")
	}

	cliset, err := apiextcs.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get k8s client")
	}

	if err := crdclient.CreateCRD(cliset, config); err != nil {
		return nil, errors.Wrapf(err, "unable to create CRDs")
	}

	clientset, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get clientset")
	}

	return &CRDStore{
		clientset: clientset,
		//FIXME: Need to detect the current namespace
		namespace: apiv1.NamespaceDefault,
	}, nil
}

func NewFakeCRDStore() *CRDStore {
	return &CRDStore{
		clientset: fakeclientset.NewSimpleClientset(),
	}
}

func (s *CRDStore) CreateNode(node *types.NodeInfo) error {
	if err := CheckNode(node); err != nil {
		return errors.Wrap(err, "failed checking node")
	}

	crdNode := &lh.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: node.Name,
		},
		NodeInfo: *node,
	}
	result, err := s.clientset.LonghornV1alpha1().Nodes(s.namespace).Create(crdNode)
	if err != nil {
		return errors.Wrap(err, "fail to create resource")
	}

	node.Metadata.ResourceVersion = result.ResourceVersion
	logrus.Infof("Add node %v name %v longhorn-manager IP %v", node.ID, node.Name, node.IP)
	return nil
}

func (s *CRDStore) UpdateNode(node *types.NodeInfo) error {
	if err := CheckNode(node); err != nil {
		return err
	}

	crdNode := &lh.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            node.Name,
			ResourceVersion: node.ResourceVersion,
		},
		NodeInfo: *node,
	}
	result, err := s.clientset.LonghornV1alpha1().Nodes(s.namespace).Update(crdNode)
	if err != nil {
		return errors.Wrap(err, "fail to update resource")
	}

	node.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) DeleteNode(nodeName string) error {
	err := s.clientset.LonghornV1alpha1().Nodes(s.namespace).Delete(nodeName, &metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "fail to delete node %v", nodeName)
	}

	return nil
}

func (s *CRDStore) GetNode(key string) (*types.NodeInfo, error) {
	result, err := s.clientset.LonghornV1alpha1().Nodes(s.namespace).Get(key,
		metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get node")
	}
	// Cannot use cached object
	resultCopy := result.DeepCopy()
	node := resultCopy.NodeInfo
	node.ResourceVersion = resultCopy.ResourceVersion

	return &node, nil
}

func (s *CRDStore) ListNodes() (map[string]*types.NodeInfo, error) {
	nodeMap := make(map[string]*types.NodeInfo)

	result, err := s.clientset.LonghornV1alpha1().Nodes(s.namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "fail to list resource")
	}
	if len(result.Items) <= 0 {
		return nil, nil
	}

	for _, item := range result.Items {
		// Cannot use cached object
		itemCopy := item.DeepCopy()
		node := itemCopy.NodeInfo
		node.Metadata = types.Metadata{
			ResourceVersion: itemCopy.ResourceVersion,
			Name:            itemCopy.Name,
		}
		nodeMap[itemCopy.ID] = &node
	}
	return nodeMap, nil
}

func (s *CRDStore) CreateSettings(settings *types.SettingsInfo) error {
	crdSetting := &lh.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: SettingName,
		},
		SettingsInfo: *settings,
	}
	result, err := s.clientset.LonghornV1alpha1().Settings(s.namespace).Create(crdSetting)
	if err != nil {
		return errors.Wrap(err, "fail to create resource")
	}

	settings.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) UpdateSettings(settings *types.SettingsInfo) error {
	crdSetting := &lh.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:            SettingName,
			ResourceVersion: settings.ResourceVersion,
		},
		SettingsInfo: *settings,
	}
	result, err := s.clientset.LonghornV1alpha1().Settings(s.namespace).Update(crdSetting)
	if err != nil {
		return errors.Wrap(err, "fail to update resource")
	}

	settings.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) GetSettings() (*types.SettingsInfo, error) {
	result, err := s.clientset.LonghornV1alpha1().Settings(s.namespace).Get(SettingName,
		metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "unable to get %v", SettingName)
	}
	// Cannot use cached object
	resultCopy := result.DeepCopy()
	settings := resultCopy.SettingsInfo
	settings.ResourceVersion = resultCopy.ResourceVersion

	return &settings, nil
}

func (s *CRDStore) getVolumeLabels(volumeName string) map[string]string {
	return map[string]string{
		"longhornvolume": volumeName,
	}
}

func (s *CRDStore) CreateVolume(volume *types.VolumeInfo) error {
	if err := CheckVolume(volume); err != nil {
		return err
	}
	resource := &lh.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   volume.Name,
			Labels: s.getVolumeLabels(volume.Name),
		},
		VolumeInfo: *volume,
	}
	result, err := s.clientset.LonghornV1alpha1().Volumes(s.namespace).Create(resource)
	if err != nil {
		return errors.Wrap(err, "fail to create resource")
	}

	volume.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) UpdateVolume(volume *types.VolumeInfo) error {
	if err := CheckVolume(volume); err != nil {
		return err
	}

	cr := &lh.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            volume.Name,
			ResourceVersion: volume.ResourceVersion,
			Labels:          s.getVolumeLabels(volume.Name),
		},
		VolumeInfo: *volume,
	}
	result, err := s.clientset.LonghornV1alpha1().Volumes(s.namespace).Update(cr)
	if err != nil {
		return err
	}

	volume.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) GetVolume(id string) (*types.VolumeInfo, error) {
	result, err := s.clientset.LonghornV1alpha1().Volumes(s.namespace).Get(id,
		metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get volume")
	}
	// Cannot use cached object
	resultCopy := result.DeepCopy()
	info := resultCopy.VolumeInfo
	info.ResourceVersion = resultCopy.ResourceVersion

	return &info, nil
}

func (s *CRDStore) DeleteVolume(name string) error {
	err := s.clientset.LonghornV1alpha1().Volumes(s.namespace).Delete(name, &metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to delete volume %v", name)
	}

	return nil
}

func (s *CRDStore) ListVolumes() (map[string]*types.VolumeInfo, error) {
	infoMap := make(map[string]*types.VolumeInfo)

	result, err := s.clientset.LonghornV1alpha1().Volumes(s.namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(result.Items) <= 0 {
		return nil, nil
	}

	for _, item := range result.Items {
		// Cannot use cached object
		itemCopy := item.DeepCopy()
		info := itemCopy.VolumeInfo
		info.Metadata = types.Metadata{
			ResourceVersion: itemCopy.ResourceVersion,
			Name:            itemCopy.Name,
		}
		infoMap[itemCopy.Name] = &info
	}
	return infoMap, nil
}

func (s *CRDStore) CreateVolumeController(info *types.ControllerInfo) error {
	if err := CheckVolumeInstance(&info.InstanceInfo); err != nil {
		return errors.Wrap(err, "check fail")
	}
	resource := &lh.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:   info.Name,
			Labels: s.getVolumeLabels(info.VolumeName),
		},
		ControllerInfo: *info,
	}
	result, err := s.clientset.LonghornV1alpha1().Controllers(s.namespace).Create(resource)
	if err != nil {
		return errors.Wrap(err, "fail to create resource")
	}

	info.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) UpdateVolumeController(info *types.ControllerInfo) error {
	if err := CheckVolumeInstance(&info.InstanceInfo); err != nil {
		return errors.Wrap(err, "check fail")
	}

	resource := &lh.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:            info.Name,
			ResourceVersion: info.ResourceVersion,
			Labels:          s.getVolumeLabels(info.VolumeName),
		},
		ControllerInfo: *info,
	}
	result, err := s.clientset.LonghornV1alpha1().Controllers(s.namespace).Update(resource)
	if err != nil {
		return errors.Wrap(err, "fail to update resource")
	}

	info.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) getControllerName(volumeName string) string {
	return volumeName + "-controller"
}

func (s *CRDStore) GetVolumeController(volumeName string) (*types.ControllerInfo, error) {
	id := s.getControllerName(volumeName)

	result, err := s.clientset.LonghornV1alpha1().Controllers(s.namespace).Get(id,
		metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get volume")
	}
	// Cannot use cached object
	resultCopy := result.DeepCopy()
	info := resultCopy.ControllerInfo
	info.ResourceVersion = resultCopy.ResourceVersion

	return &info, nil
}

func (s *CRDStore) DeleteVolumeController(volumeName string) error {
	id := s.getControllerName(volumeName)

	err := s.clientset.LonghornV1alpha1().Controllers(s.namespace).Delete(id, &metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to delete controller %v", id)
	}

	return nil
}

func (s *CRDStore) CreateVolumeReplica(info *types.ReplicaInfo) error {
	if err := CheckVolumeInstance(&info.InstanceInfo); err != nil {
		return errors.Wrap(err, "check fail")
	}
	resource := &lh.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:   info.Name,
			Labels: s.getVolumeLabels(info.VolumeName),
		},
		ReplicaInfo: *info,
	}
	result, err := s.clientset.LonghornV1alpha1().Replicas(s.namespace).Create(resource)
	if err != nil {
		return errors.Wrap(err, "fail to create resource")
	}

	info.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) UpdateVolumeReplica(info *types.ReplicaInfo) error {
	if err := CheckVolumeInstance(&info.InstanceInfo); err != nil {
		return errors.Wrap(err, "check fail")
	}

	resource := &lh.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            info.Name,
			ResourceVersion: info.ResourceVersion,
			Labels:          s.getVolumeLabels(info.VolumeName),
		},
		ReplicaInfo: *info,
	}
	result, err := s.clientset.LonghornV1alpha1().Replicas(s.namespace).Update(resource)
	if err != nil {
		return errors.Wrap(err, "fail to update resource")
	}

	info.Metadata.ResourceVersion = result.ResourceVersion
	return nil
}

func (s *CRDStore) GetVolumeReplica(volumeName, replicaName string) (*types.ReplicaInfo, error) {
	result, err := s.clientset.LonghornV1alpha1().Replicas(s.namespace).Get(replicaName,
		metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get volume")
	}
	// Cannot use cached object
	resultCopy := result.DeepCopy()
	info := resultCopy.ReplicaInfo
	info.ResourceVersion = resultCopy.ResourceVersion

	return &info, nil
}

func (s *CRDStore) DeleteVolumeReplica(volumeName, replicaName string) error {
	err := s.clientset.LonghornV1alpha1().Replicas(s.namespace).Delete(replicaName, &metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to delete replica %v", replicaName)
	}

	return nil
}

func (s *CRDStore) ListVolumeReplicas(volumeName string) (map[string]*types.ReplicaInfo, error) {
	infoMap := make(map[string]*types.ReplicaInfo)

	result, err := s.clientset.LonghornV1alpha1().Replicas(s.namespace).List(metav1.ListOptions{
		LabelSelector: "longhornvolume=" + volumeName,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fail to list resource")
	}
	if len(result.Items) <= 0 {
		return nil, nil
	}

	for _, item := range result.Items {
		// Cannot use cached object
		itemCopy := item.DeepCopy()
		info := itemCopy.ReplicaInfo
		info.Metadata = types.Metadata{
			ResourceVersion: itemCopy.ResourceVersion,
			Name:            itemCopy.Name,
		}
		infoMap[itemCopy.ID] = &info
	}
	return infoMap, nil
}

func (s *CRDStore) Nuclear(nuclearCode string) error {
	//Fake CRDStore doesn't persist
	return nil
}
