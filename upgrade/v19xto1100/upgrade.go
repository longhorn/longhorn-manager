package v19xto1100

import (
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.9.x to v1.10.0: "
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

	if err := upgradeSettings(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func upgradeSettings(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade setting failed")
	}()

	settingMap, err := upgradeutil.ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn settings during the setting upgrade")
	}

	// Update Setting CRs
	var errs []error
	for _, s := range settingMap {
		definition, ok := types.GetSettingDefinition(types.SettingName(s.Name))
		if !ok {
			logrus.Warnf("Unknown setting %v found during upgrade, skipping", s.Name)
			continue
		}
		if !definition.DataEngineSpecific {
			continue
		}

		value, err := datastore.GetSettingValidValue(definition, s.Value)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to get valid value for setting %s", s.Name))
			continue
		}

		s.Value = value
	}

	return errors.Join(errs...)
}
