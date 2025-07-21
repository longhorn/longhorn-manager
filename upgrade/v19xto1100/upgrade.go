package v19xto1100

import (
	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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

	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeBackups(namespace, lhClient, resourceMaps); err != nil {
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
		if backupBlockSize, convertBlockSizeErr := util.ConvertSize(string(v.Spec.BackupBlockSize)); convertBlockSizeErr == nil && backupBlockSize == 0 {
			defaultBackupBlockSizeQuantity := resource.NewQuantity(backupstore.DEFAULT_BLOCK_SIZE, resource.BinarySI)
			v.Spec.BackupBlockSize = longhorn.BackupBlockSize(defaultBackupBlockSizeQuantity.String())
		}
	}

	return nil
}

func upgradeBackups(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup failed")
	}()

	backupsMap, err := upgradeutil.ListAndUpdateBackupsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn backups during the backup upgrade")
	}

	for _, b := range backupsMap {
		if backupBlockSize, convertBlockSizeErr := util.ConvertSize(string(b.Spec.BackupBlockSize)); convertBlockSizeErr == nil && backupBlockSize == 0 {
			defaultBackupBlockSizeQuantity := resource.NewQuantity(backupstore.DEFAULT_BLOCK_SIZE, resource.BinarySI)
			b.Spec.BackupBlockSize = longhorn.BackupBlockSize(defaultBackupBlockSizeQuantity.String())
		}
	}

	return nil
}
