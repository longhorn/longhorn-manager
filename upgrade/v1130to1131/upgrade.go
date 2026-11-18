package v1130to1131

import (
	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.13.0 to v1.13.1: "
)

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := updateExportBackingImageSnapshotsStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

// updateExportBackingImageSnapshotsStatus marks the snapshots created for backing image exporting as
// system-generated. Both the Engine status and the Snapshot status are corrected; otherwise the snapshot
// controller would restore the stale value from the Engine status for detached volumes.
func updateExportBackingImageSnapshotsStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade export backing image snapshots status failed")
	}()

	exportLabelKey := types.GetLonghornLabelKey(types.LonghornLabelSnapshotForExportingBackingImage)

	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the snapshots upgrade")
	}
	for _, engine := range engineMap {
		for _, snapshotInfo := range engine.Status.Snapshots {
			if snapshotInfo == nil {
				continue
			}
			if _, ok := snapshotInfo.Labels[exportLabelKey]; ok {
				snapshotInfo.UserCreated = false
			}
		}
	}

	snapshotMap, err := upgradeutil.ListAndUpdateSnapshotsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn snapshots during the snapshots upgrade")
	}
	for _, snapshot := range snapshotMap {
		if _, ok := snapshot.Status.Labels[exportLabelKey]; ok {
			snapshot.Status.UserCreated = false
		}
	}

	return nil
}
