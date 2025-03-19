package v18xto190

import (
	"github.com/pkg/errors"

	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
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
			return errors.Wrapf(err, "upgrade from v1.8.x to v1.9.0: failed to list all existing Longhorn %s during the %s upgrade", resourceKind, resourceKind)
		}
	}
	return nil
}
