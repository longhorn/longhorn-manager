package v15xto160

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

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

	return upgradeVolumeAttachments(namespace, lhClient, resourceMaps)
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
			e.Spec.Image = e.Spec.EngineImage
			e.Spec.EngineImage = ""
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
			r.Spec.Image = r.Spec.EngineImage
			r.Spec.EngineImage = ""
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

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// Currently there are no statuses to upgrade. See UpgradeResources -> upgradeVolumes or previous Longhorn versions
	// for examples.
	return nil
}
