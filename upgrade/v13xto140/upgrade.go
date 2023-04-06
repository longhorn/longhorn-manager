package v13xto140

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.3.x to v1.4.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
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
		if v.Spec.SnapshotDataIntegrity == "" {
			v.Spec.SnapshotDataIntegrity = longhorn.SnapshotDataIntegrityIgnored
		}
		if v.Spec.RestoreVolumeRecurringJob == "" {
			v.Spec.RestoreVolumeRecurringJob = longhorn.RestoreVolumeRecurringJobDefault
		}
		if v.Spec.UnmapMarkSnapChainRemoved == "" {
			v.Spec.UnmapMarkSnapChainRemoved = longhorn.UnmapMarkSnapChainRemovedIgnored
		}
	}

	return nil
}
