package v16xto170

import (
	"fmt"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.6.x to v1.7.0: "

	oldDefaultBackupTargetName = "default"
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resources as well. See upgradeReplicas or previous Longhorn versions for
	// examples.
	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeEngines(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeReplicas(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackupTargets(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackupVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackups(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackupBackingImages(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeRecurringJobs(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return upgradeNodes(namespace, lhClient, resourceMaps)
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resource status as well. See upgradeEngineStatus or previous Longhorn
	// versions for examples.
	if err := upgradeEngineStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeBackupTargetsStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return upgradeSettingStatus(namespace, lhClient, resourceMaps)
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumeMap, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumeMap {
		v.Spec.OfflineReplicaRebuilding = longhorn.OfflineReplicaRebuildingDisabled

		if v.Spec.FreezeFilesystemForSnapshot == "" {
			v.Spec.FreezeFilesystemForSnapshot = longhorn.FreezeFilesystemForSnapshotDefault
		}
	}

	return nil
}

func upgradeReplicas(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade replica failed")
	}()

	replicaMap, err := upgradeutil.ListAndUpdateReplicasInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn replicas during the replica upgrade")
	}

	for _, r := range replicaMap {
		if r.Spec.LastHealthyAt == "" {
			// We could attempt to figure out if the replica is currently RW in an engine and set its
			// Spec.LastHealthyAt = now, but it is safer and easier to start updating it after the upgrade.
			r.Spec.LastHealthyAt = r.Spec.HealthyAt
		}
		if r.Spec.LastFailedAt == "" {
			// There is no way for us to know the right time for Spec.LastFailedAt if the replica isn't currently
			// failed. Start updating it after the upgrade.
			r.Spec.LastFailedAt = r.Spec.FailedAt
		}
	}

	return nil
}

func upgradeBackupTargets(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup target failed")
	}()

	backupTargetMap, err := upgradeutil.ListAndUpdateBackupTargetsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to list all existing Longhorn BackupTargets during the Longhorn BackupTarget upgrade")
	}

	backupTarget, exist := backupTargetMap[oldDefaultBackupTargetName]
	if !exist {
		return errors.Wrapf(err, "failed to get old default backup target during the Longhorn BackupTarget upgrade")
	}

	backupTarget.Spec.Default = true

	return nil
}

func upgradeBackupVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup volume failed")
	}()

	backupTarget, err := getOldDefaultBackupTarget(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to get old default backup target during the Longhorn BackupVolume upgrade")
	}

	backupVolumeMap, err := upgradeutil.ListAndUpdateBackupVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn BackupVolumes during the Longhorn BackupVolume upgrade")
	}

	for _, bv := range backupVolumeMap {
		if bv.Spec.BackupTargetName == "" {
			bv.Spec.BackupTargetName = backupTarget.Name
		}
		if bv.Spec.BackupTargetURL == "" {
			bv.Spec.BackupTargetURL = backupTarget.Spec.BackupTargetURL
		}
		if bv.Spec.VolumeName == "" {
			bv.Spec.VolumeName = bv.Name
		}
		if bv.Labels == nil {
			bv.Labels = make(map[string]string)
		}
		_, exists := bv.Labels[types.LonghornLabelBackupVolumeCRName]
		if !exists {
			bv.Labels[types.LonghornLabelBackupVolumeCRName] = bv.Spec.VolumeName + "-" + bv.Spec.BackupTargetName
		}
	}

	return nil
}

func getOldDefaultBackupTarget(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (backupTarget *longhorn.BackupTarget, err error) {
	backupTargetMap, err := upgradeutil.ListAndUpdateBackupTargetsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	backupTarget, exist := backupTargetMap[oldDefaultBackupTargetName]
	if !exist {
		return nil, fmt.Errorf("old default backup target %v does not exist", oldDefaultBackupTargetName)
	}
	return backupTarget, nil
}

func upgradeBackups(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup failed")
	}()

	backupTarget, err := getOldDefaultBackupTarget(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to get old default backup target during the Longhorn Backup upgrade")
	}

	backupMap, err := upgradeutil.ListAndUpdateBackupsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn Backups during the Longhorn Backup upgrade")
	}

	for _, b := range backupMap {
		if b.Annotations == nil {
			b.Annotations = make(map[string]string)
		}
		b.Annotations[types.UpgradedOldBackupFrom16x] = ""
		if b.Spec.BackupTargetName == "" {
			b.Spec.BackupTargetName = backupTarget.Name
		}
		if b.Spec.BackupTargetURL == "" {
			b.Spec.BackupTargetURL = backupTarget.Spec.BackupTargetURL
		}
	}

	return nil
}

func upgradeBackupBackingImages(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup backing images failed")
	}()

	backupTarget, err := getOldDefaultBackupTarget(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to get old default backup target during the Longhorn Backup Backing Images upgrade")
	}

	backupBackingImageMap, err := upgradeutil.ListAndUpdateBackupBackingImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn Backups during the Longhorn Backup Backing Images upgrade")
	}

	for _, bbi := range backupBackingImageMap {
		if bbi.Spec.BackingImage == "" {
			bbi.Spec.BackingImage = bbi.Status.BackingImage
		}
		if bbi.Spec.BackupTargetName == "" {
			bbi.Spec.BackupTargetName = backupTarget.Name
		}
		if bbi.Spec.BackupTargetURL == "" {
			bbi.Spec.BackupTargetURL = backupTarget.Spec.BackupTargetURL
		}
	}

	return nil
}

func upgradeRecurringJobs(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade recurring job failed")
	}()

	backupTarget, err := getOldDefaultBackupTarget(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to get old default backup target during the Longhorn RecurringJob upgrade")
	}

	recurringJobMap, err := upgradeutil.ListAndUpdateRecurringJobsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn RecurringJobs during the Longhorn RecurringJob upgrade")
	}

	for _, rj := range recurringJobMap {
		if rj.Spec.Task == longhorn.RecurringJobTypeBackup || rj.Spec.Task == longhorn.RecurringJobTypeBackupForceCreate {
			rj.Spec.BackupTargetName = backupTarget.Name
		}
	}

	return nil
}

func upgradeEngines(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade engine failed")
	}()

	backupTarget, err := getOldDefaultBackupTarget(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to get old default backup target during the engine upgrade")
	}

	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the engine upgrade")
	}

	for _, e := range engineMap {
		if e.Spec.BackupTargetName == "" {
			e.Spec.BackupTargetName = backupTarget.Name
		}
	}

	return nil
}

func upgradeNodes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade node failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn nodes during the node upgrade")
	}

	for key, node := range nodeMap {
		for i, disk := range node.Spec.Disks {
			if disk.Type == longhorn.DiskTypeBlock && disk.DiskDriver == "" {
				diskUpdate := disk
				diskUpdate.DiskDriver = longhorn.DiskDriverAio

				node.Spec.Disks[i] = diskUpdate
			}
		}

		nodeMap[key] = node
	}

	return nil
}

func upgradeBackupTargetsStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup target failed")
	}()

	backupTargetMap, err := upgradeutil.ListAndUpdateBackupTargetsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to list all existing Longhorn BackupTargets during the Longhorn BackupTarget status upgrade")
	}

	backupTarget, exist := backupTargetMap[oldDefaultBackupTargetName]
	if !exist {
		return errors.Wrapf(err, "failed to get old default backup target during the Longhorn BackupTarget status upgrade")
	}

	backupTarget.Status.Default = true
	return nil
}

func upgradeSettingStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
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

	for _, s := range settingMap {
		s.Status.Applied = true
	}

	return nil
}

func upgradeEngineStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade engines failed")
	}()

	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the engine status upgrade")
	}

	for _, e := range engineMap {
		if e.Status.ReplicaTransitionTimeMap == nil {
			e.Status.ReplicaTransitionTimeMap = map[string]string{}
		}
		for replicaName := range e.Status.ReplicaModeMap {
			// We don't have any historical information to rely on. Starting at the time of the upgrade.
			if _, ok := e.Status.ReplicaTransitionTimeMap[replicaName]; !ok {
				e.Status.ReplicaTransitionTimeMap[replicaName] = util.Now()
			}
		}
	}

	return nil
}
