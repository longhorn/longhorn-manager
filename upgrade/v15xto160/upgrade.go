package v15xto160

import (
	"context"

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

	return deleteCSIServices(namespace, kubeClient)
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// Currently there are no statuses to upgrade. See UpgradeResources -> upgradeVolumes or previous Longhorn versions
	// for examples.
	if err := upgradeEngineImagesStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
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
			v.Spec.Image = v.Spec.EngineImage // nolint: staticcheck
			v.Spec.EngineImage = ""           // nolint: staticcheck
		}

		if v.Spec.BackendStoreDriver == "" {
			v.Spec.DataEngine = longhorn.DataEngineTypeV1
		} else {
			v.Spec.DataEngine = longhorn.DataEngineType(v.Spec.BackendStoreDriver)
		}

		v.Spec.SnapshotMaxCount = types.MaxSnapshotNum
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
			im.Labels[types.GetLonghornLabelKey(types.LonghornLabelDataEngine)] = string(longhorn.DataEngineTypeV1)
		}

		if im.Spec.DataEngine == "" {
			im.Spec.DataEngine = longhorn.DataEngineTypeV1
		}
	}

	return nil
}

func upgradeEngines(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade engine failed")
	}()

	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the engine upgrade")
	}

	for _, e := range engineMap {
		if e.Spec.Image == "" {
			e.Spec.Image = e.Spec.EngineImage // nolint: staticcheck
			e.Spec.EngineImage = ""           // nolint: staticcheck
		}

		if e.Spec.BackendStoreDriver == "" {
			e.Spec.DataEngine = longhorn.DataEngineTypeV1
		} else {
			e.Spec.DataEngine = longhorn.DataEngineType(e.Spec.BackendStoreDriver)
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
		if r.Spec.Image == "" {
			r.Spec.Image = r.Spec.EngineImage // nolint: staticcheck
			r.Spec.EngineImage = ""           // nolint: staticcheck
		}

		if r.Spec.BackendStoreDriver == "" {
			r.Spec.DataEngine = longhorn.DataEngineTypeV1
		} else {
			r.Spec.DataEngine = longhorn.DataEngineType(r.Spec.BackendStoreDriver)
		}

		r.Spec.EvictionRequested = r.Status.EvictionRequested // nolint: staticcheck
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

func upgradeEngineImagesStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade engine images failed")
	}()

	engineImages, err := upgradeutil.ListAndUpdateEngineImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing engine images during the upgrade")
	}

	for _, ei := range engineImages {
		ei.Status.Incompatible = ei.Status.State == longhorn.EngineImageStateIncompatible
	}

	return nil
}
