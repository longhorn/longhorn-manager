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

	if err := updateNodesStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

// updateNodesStatus backfills the disk Initialized condition. The node controller only sets it when the manager on
// the node syncs its disks, so a node that is down after the upgrade would otherwise have no Initialized condition
// even though its disk UUIDs are recorded.
func updateNodesStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade nodes failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn nodes during the nodes upgrade")
	}

	for _, node := range nodeMap {
		for diskName, diskStatus := range node.Status.DiskStatus {
			if diskStatus == nil {
				continue
			}
			types.SetDiskInitializedCondition(diskStatus, node.Name, diskName)
		}
	}

	return nil
}
