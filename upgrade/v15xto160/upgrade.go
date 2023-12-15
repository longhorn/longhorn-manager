package v15xto160

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.5.x to v1.6.0: "

	oldDefaultBackupTargetName = "default"
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resources as well. See upgradeVolumes or previous Longhorn versions for
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

	if err := upgradeInstanceManagers(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := upgradeVolumeAttachments(namespace, lhClient, resourceMaps); err != nil {
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

	if err := upgradeRecurringJobs(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return deleteCSIServices(namespace, kubeClient)
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
		if v.Spec.ReplicaDiskSoftAntiAffinity == "" {
			v.Spec.ReplicaDiskSoftAntiAffinity = longhorn.ReplicaDiskSoftAntiAffinityDefault
		}

		if v.Spec.Image == "" {
			v.Spec.Image = v.Spec.EngineImage
			v.Spec.EngineImage = ""
		}
	}

	return nil
}

func upgradeInstanceManagers(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade instanceManager failed")
	}()

	imMap, err := upgradeutil.ListAndUpdateInstanceManagersInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn instanceManagers during the instanceManager upgrade")
	}

	for _, im := range imMap {
		if im.Labels != nil {
			im.Labels[types.GetLonghornLabelKey(types.LonghornLabelBackendStoreDriver)] = string(longhorn.BackendStoreDriverTypeV1)
		}

		if im.Spec.BackendStoreDriver == "" {
			im.Spec.BackendStoreDriver = longhorn.BackendStoreDriverTypeV1
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
		if e.Spec.Image == "" {
			e.Spec.Image = e.Spec.EngineImage
			e.Spec.EngineImage = ""
		}
		if e.Spec.BackupTargetName == "" {
			e.Spec.BackupTargetName = backupTarget.Name
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
		if r.Spec.Image == "" {
			r.Spec.Image = r.Spec.EngineImage
			r.Spec.EngineImage = ""
		}

		r.Spec.EvictionRequested = r.Status.EvictionRequested
	}

	return nil
}

func upgradeVolumeAttachments(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade VolumeAttachment failed")
	}()

	volumeAttachmentMap, err := upgradeutil.ListAndUpdateVolumeAttachmentsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn VolumeAttachments during the Longhorn VolumeAttachment upgrade")
	}

	snapshotMap, err := upgradeutil.ListAndUpdateSnapshotsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Snapshots during the Longhorn VolumeAttachment a upgrade")
	}

	ticketIDsForExistingSnapshotsMap := map[string]interface{}{}
	for snapshotName := range snapshotMap {
		ticketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeSnapshotController, snapshotName)
		ticketIDsForExistingSnapshotsMap[ticketID] = nil
	}

	// Previous Longhorn versions may have created attachmentTickets for snapshots that no longer exist. Clean these up.
	for _, volumeAttachment := range volumeAttachmentMap {
		for ticketID, ticket := range volumeAttachment.Spec.AttachmentTickets {
			if ticket.Type != longhorn.AttacherTypeSnapshotController {
				continue
			}
			if _, ok := ticketIDsForExistingSnapshotsMap[ticketID]; !ok {
				delete(volumeAttachment.Spec.AttachmentTickets, ticketID)
			}
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
		b.Annotations[types.UpgradedOldBackupFrom15x] = ""
		if b.Spec.BackupTargetName == "" {
			b.Spec.BackupTargetName = backupTarget.Name
		}
		if b.Spec.BackupTargetURL == "" {
			b.Spec.BackupTargetURL = backupTarget.Spec.BackupTargetURL
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

func deleteCSIServices(namespace string, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"delete CSI service failed")
	}()

	servicesToDelete := map[string]struct{}{
		types.CSIAttacherName:    {},
		types.CSIProvisionerName: {},
		types.CSIResizerName:     {},
		types.CSISnapshotterName: {},
	}

	servicesInCluster, err := kubeClient.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "failed to list all existing Longhorn services during the CSI service deletion")
	}

	for _, serviceInCluster := range servicesInCluster.Items {
		if _, ok := servicesToDelete[serviceInCluster.Name]; !ok {
			continue
		}
		if err = kubeClient.CoreV1().Services(namespace).Delete(context.TODO(), serviceInCluster.Name, metav1.DeleteOptions{}); err != nil {
			// Best effort. Dummy services have no function and no finalizer, so we can proceed on failure.
			logrus.Warnf("Deprecated CSI dummy service could not be deleted during upgrade: %v", err)
		}
	}

	return nil
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// Currently there are no statuses to upgrade. See UpgradeResources -> upgradeVolumes or previous Longhorn versions
	// for examples.
	if err := upgradeBackupTargets(namespace, lhClient, resourceMaps); err != nil {
		return err
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
