package v111xto1120

import (
	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.11.x to v1.12.0: "
)

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if err := upgradeNodeStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func upgradeNodeStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade nodes failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn nodes during the node status upgrade")
	}

	for _, n := range nodeMap {
		// scheduled resources are moved and handled by DiskSchedules
		for _, diskStatus := range n.Status.DiskStatus {
			diskStatus.ScheduledReplica = nil
			diskStatus.ScheduledBackingImage = nil
			diskStatus.StorageScheduled = 0
		}
	}
	return nil
}
