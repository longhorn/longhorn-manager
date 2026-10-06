package v113xto1140

import (
	"fmt"

	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.13.x to v1.14.0: "
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
			if types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeInitialized).Status != longhorn.ConditionStatusUnknown {
				continue
			}

			// Initialized mirrors the recorded disk UUID: a disk that has recorded one was initialized before.
			condValue := longhorn.ConditionStatusTrue
			condReason := ""
			condMessage := fmt.Sprintf("Disk %v(%v) on node %v is initialized with disk UUID %v", diskName, diskStatus.DiskPath, node.Name, diskStatus.DiskUUID)
			if diskStatus.DiskUUID == "" {
				condValue = longhorn.ConditionStatusFalse
				condReason = string(longhorn.DiskConditionReasonDiskUninitialized)
				condMessage = fmt.Sprintf("Disk %v(%v) on node %v has no disk UUID recorded yet", diskName, diskStatus.DiskPath, node.Name)
			}
			diskStatus.Conditions = types.SetCondition(diskStatus.Conditions,
				longhorn.DiskConditionTypeInitialized, condValue, condReason, condMessage)
		}
	}

	return nil
}
