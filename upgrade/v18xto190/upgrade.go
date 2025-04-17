package v18xto190

import (
	"strings"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.8.x to v1.9.0: "
)

type listAndUpdateFunc func(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) error
type typedListAndUpdateFunc[K any] func(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*K, error)

func listAndUpdateResources[K any](listUpdateFunc typedListAndUpdateFunc[K]) listAndUpdateFunc {
	return func(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) error {
		_, err := listUpdateFunc(namespace, lhClient, resourceMaps)
		return err
	}
}

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := updateCRs(namespace, lhClient, kubeClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func updateCRs(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	// From v1.9.0, the v1beta1 API is deprecated, and v1beta2 is the storage version. Load all resource and write back into v1beta2.

	updates := map[string]listAndUpdateFunc{
		types.LonghornKindSetting:                listAndUpdateResources(upgradeutil.ListAndUpdateSettingsInProvidedCache),
		types.LonghornKindNode:                   listAndUpdateResources(upgradeutil.ListAndUpdateNodesInProvidedCache),
		types.LonghornKindInstanceManager:        listAndUpdateResources(upgradeutil.ListAndUpdateInstanceManagersInProvidedCache),
		types.LonghornKindShareManager:           listAndUpdateResources(upgradeutil.ListAndUpdateShareManagersInProvidedCache),
		types.LonghornKindEngine:                 listAndUpdateResources(upgradeutil.ListAndUpdateEnginesInProvidedCache),
		types.LonghornKindEngineImage:            listAndUpdateResources(upgradeutil.ListAndUpdateEngineImagesInProvidedCache),
		types.LonghornKindReplica:                listAndUpdateResources(upgradeutil.ListAndUpdateReplicasInProvidedCache),
		types.LonghornKindVolume:                 listAndUpdateResources(upgradeutil.ListAndUpdateVolumesInProvidedCache),
		types.LonghornKindBackupVolume:           listAndUpdateResources(upgradeutil.ListAndUpdateBackupVolumesInProvidedCache),
		types.LonghornKindBackup:                 listAndUpdateResources(upgradeutil.ListAndUpdateBackupsInProvidedCache),
		types.LonghornKindBackupTarget:           listAndUpdateResources(upgradeutil.ListAndUpdateBackupTargetsInProvidedCache),
		types.LonghornKindBackingImageManager:    listAndUpdateResources(upgradeutil.ListAndUpdateBackingImageManagersInProvidedCache),
		types.LonghornKindBackingImageDataSource: listAndUpdateResources(upgradeutil.ListAndUpdateBackingImageDataSourcesInProvidedCache),
		types.LonghornKindBackingImage:           listAndUpdateResources(upgradeutil.ListAndUpdateBackingImagesInProvidedCache),
		types.LonghornKindRecurringJob:           listAndUpdateResources(upgradeutil.ListAndUpdateRecurringJobsInProvidedCache),
	}
	for resourceKind, listUpdateFunc := range updates {
		if err := listUpdateFunc(namespace, lhClient, resourceMaps); err != nil {
			return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn %s during the %s upgrade", resourceKind, resourceKind)
		}
	}

	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeSettings(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumesMap, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumesMap {
		if v.Spec.OfflineRebuilding == "" {
			v.Spec.OfflineRebuilding = longhorn.VolumeOfflineRebuildingIgnored
		}
	}

	return nil
}

func upgradeSettings(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade settings failed")
	}()

	settingsMap, err := upgradeutil.ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn settings during the settings upgrade")
	}

	if err := updateOrphanResourceAutoDeletionSetting(settingsMap); err != nil {
		return errors.Wrapf(err, "failed to update orphan resource auto-deletion settings")
	}

	return nil
}

func updateOrphanResourceAutoDeletionSetting(settingsMap map[string]*longhorn.Setting) (err error) {
	oldSetting, exist := settingsMap[string(types.SettingNameOrphanAutoDeletion)]
	if !exist {
		return nil
	}
	newSetting, exist := settingsMap[string(types.SettingNameOrphanResourceAutoDeletion)]
	if !exist {
		newSetting = &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(types.SettingNameOrphanResourceAutoDeletion),
			},
		}
		settingsMap[string(types.SettingNameOrphanResourceAutoDeletion)] = newSetting
	}
	resourceTypes, err := types.UnmarshalOrphanResourceTypes(newSetting.Value)
	if err != nil {
		return err
	}

	resourceTypes[types.OrphanResourceTypeReplicaData] = oldSetting.Value == "true"
	enabledResourceType := make([]string, 0, len(resourceTypes))
	for rt, enabled := range resourceTypes {
		if enabled {
			enabledResourceType = append(enabledResourceType, string(rt))
		}
	}
	newSetting.Value = strings.Join(enabledResourceType, ";")
	return nil
}
