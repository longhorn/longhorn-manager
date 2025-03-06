package v18xto190

import (
	"github.com/pkg/errors"

	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.8.x to v1.9.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	// From v1.9.0, the v1beta1 API is deprecated. Load all resource and write back into v1beta2.

	updates := []func(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) error{
		upgradeSettings,
		upgradeNodes,
		upgradeInstanceManagers,
		upgradeShareManagers,
		upgradeEngines,
		upgradeEngineImages,
		upgradeReplicas,
		upgradeVolumes,
		upgradeBackupVolumes,
		upgradeBackups,
		upgradeBackupTargets,
		upgradeBackingImageManagers,
		upgradeBackingImageDataSources,
		upgradeBackingImages,
		upgradeRecurringJobs,
	}
	for _, updateResource := range updates {
		if err := updateResource(namespace, lhClient, resourceMaps); err != nil {
			return err
		}
	}
	return nil
}

func upgradeSettings(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn settings during the settings upgrade")
	}
	return nil
}

func upgradeNodes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn nodes during the nodes upgrade")
	}
	return nil
}

func upgradeInstanceManagers(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateInstanceManagersInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn instance managers during the instance manager upgrade")
	}
	return nil
}

func upgradeShareManagers(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateShareManagersInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn share managers during the share manager upgrade")
	}
	return nil
}

func upgradeEngines(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn engines during the engine upgrade")
	}
	return nil
}

func upgradeEngineImages(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateEngineImagesInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn engine images during the engine image upgrade")
	}
	return nil
}

func upgradeReplicas(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateReplicasInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn replicas during the replica upgrade")
	}
	return nil
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn volumes during the volume upgrade")
	}
	return nil
}

func upgradeBackupVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateBackupVolumesInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn backup volumes during the backup volume upgrade")
	}
	return nil
}

func upgradeBackups(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateBackupsInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn backups during the backup upgrade")
	}
	return nil
}

func upgradeBackupTargets(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateBackupTargetsInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn backup targets during the backup target upgrade")
	}
	return nil
}

func upgradeBackingImageManagers(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateBackingImageManagersInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn backing image manager during the backing image manager upgrade")
	}
	return nil
}

func upgradeBackingImageDataSources(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateBackingImageDataSourcesInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn backing image data sources during the backing image data source upgrade")
	}
	return nil
}

func upgradeBackingImages(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateBackingImagesInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn backing images during the backing image upgrade")
	}
	return nil
}

func upgradeRecurringJobs(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if _, err := upgradeutil.ListAndUpdateRecurringJobsInProvidedCache(namespace, lhClient, resourceMaps); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn recurring jobs during the recurring job upgrade")
	}
	return nil
}
