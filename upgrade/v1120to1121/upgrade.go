package v1120to1121

import (
	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.12.0 to v1.12.1: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
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
		if !types.IsDataFromVolume(v.Spec.DataSource) {
			continue
		}
		srcVolName := types.GetVolumeName(v.Spec.DataSource)
		if srcVolName == "" {
			continue
		}
		if v.Labels == nil {
			v.Labels = map[string]string{}
		}

		// Backfill clone-source-volume label for all clone volumes.
		labelKey := types.GetLonghornLabelKey(types.LonghornLabelCloneSourceVolume)
		if v.Labels[labelKey] == "" {
			v.Labels[labelKey] = srcVolName
		}

		// Mark existing linked-clone volumes as legacy (pre-entrypoint architecture).
		// All linked-clone volumes that exist at upgrade time predate the new architecture.
		if v.Spec.CloneMode == longhorn.CloneModeLinkedClone {
			legacyKey := types.GetLonghornLabelKey(types.LonghornLabelLegacyLinkedClone)
			v.Labels[legacyKey] = "true"
		}

		// Remove the deprecated linked-clone-source-volume label.
		delete(v.Labels, types.GetLonghornLabelKey("linked-clone-source-volume"))
	}

	return nil
}
