package v112xto1130

import (
	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.12.x to v1.13.0: "
)

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := updateSnapshotsStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func updateSnapshotsStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade snapshots failed")
	}()

	snapshotMap, err := upgradeutil.ListAndUpdateSnapshotsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn snapshots during the snapshots upgrade")
	}
	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the snapshots upgrade")
	}
	for _, engine := range engineMap {
		if engine.Status.Snapshots == nil {
			continue
		}
		for snapshotName, snapshotInfo := range engine.Status.Snapshots {
			snapshot := snapshotMap[snapshotName]
			if snapshot == nil {
				continue
			}
			if snapshot.Status.RequestedTime == "" {
				snapshot.Status.RequestedTime = snapshotInfo.Created
			}
		}
	}

	return nil
}
