package v18xto190

import (
	"github.com/pkg/errors"

	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

type listAndUpdateResourcesInProvidedCacheFunc func(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) error
type typedListAndUpdateResourcesInProvidedCacheFunc[K any] func(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*K, error)

func toListAndUpdateResourcesInProvidedCacheFunc[K any](listUpdateFunc typedListAndUpdateResourcesInProvidedCacheFunc[K]) listAndUpdateResourcesInProvidedCacheFunc {
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

	updates := map[string]listAndUpdateResourcesInProvidedCacheFunc{
		types.LonghornKindSetting:                toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateSettingsInProvidedCache),
		types.LonghornKindNode:                   toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateNodesInProvidedCache),
		types.LonghornKindInstanceManager:        toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateInstanceManagersInProvidedCache),
		types.LonghornKindShareManager:           toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateShareManagersInProvidedCache),
		types.LonghornKindEngine:                 toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateEnginesInProvidedCache),
		types.LonghornKindEngineImage:            toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateEngineImagesInProvidedCache),
		types.LonghornKindReplica:                toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateReplicasInProvidedCache),
		types.LonghornKindVolume:                 toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateVolumesInProvidedCache),
		types.LonghornKindBackupVolume:           toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateBackupVolumesInProvidedCache),
		types.LonghornKindBackup:                 toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateBackupsInProvidedCache),
		types.LonghornKindBackupTarget:           toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateBackupTargetsInProvidedCache),
		types.LonghornKindBackingImageManager:    toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateBackingImageManagersInProvidedCache),
		types.LonghornKindBackingImageDataSource: toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateBackingImageDataSourcesInProvidedCache),
		types.LonghornKindBackingImage:           toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateBackingImagesInProvidedCache),
		types.LonghornKindRecurringJob:           toListAndUpdateResourcesInProvidedCacheFunc(upgradeutil.ListAndUpdateRecurringJobsInProvidedCache),
	}
	for resourceKind, listUpdateFunc := range updates {
		if err := listUpdateFunc(namespace, lhClient, resourceMaps); err != nil {
			return errors.Wrapf(err, "upgrade from v1.8.x to v1.9.0: failed to list all existing Longhorn %s during the %s upgrade", resourceKind, resourceKind)
		}
	}
	return nil
}
