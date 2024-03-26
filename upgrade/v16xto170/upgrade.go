package v16xto170

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.6.x to v1.7.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resources as well. See upgradeReplicas or previous Longhorn versions for
	// examples.
	if err := upgradeReplicas(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return upgradeNodes(namespace, lhClient, resourceMaps)
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resource status as well. See upgradeEngineStatus or previous Longhorn
	// versions for examples.
	return upgradeEngineStatus(namespace, lhClient, resourceMaps)
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
		if r.Spec.LastHealthyAt == "" {
			// We could attempt to figure out if the replica is currently RW in an engine and set its
			// Spec.LastHealthyAt = now, but it is safer and easier to start updating it after the upgrade.
			r.Spec.LastHealthyAt = r.Spec.HealthyAt
		}
		if r.Spec.LastFailedAt == "" {
			// There is no way for us to know the right time for Spec.LastFailedAt if the replica isn't currently
			// failed. Start updating it after the upgrade.
			r.Spec.LastFailedAt = r.Spec.FailedAt
		}
	}

	return nil
}

func upgradeEngineStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade engines failed")
	}()

	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the engine status upgrade")
	}

	for _, e := range engineMap {
		if e.Status.ReplicaTransitionTimeMap == nil {
			e.Status.ReplicaTransitionTimeMap = map[string]string{}
		}
		for replicaName := range e.Status.ReplicaModeMap {
			// We don't have any historical information to rely on. Starting at the time of the upgrade.
			if _, ok := e.Status.ReplicaTransitionTimeMap[replicaName]; !ok {
				e.Status.ReplicaTransitionTimeMap[replicaName] = util.Now()
			}
		}
	}

	return nil
}

func upgradeNodes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade node failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn nodes during the node upgrade")
	}

	for _, node := range nodeMap {
		for _, disk := range node.Spec.Disks {
			if disk.Type == longhorn.DiskTypeBlock && disk.DiskDriver == "" {
				disk.DiskDriver = longhorn.DiskDriverAio
			}
		}
	}

	return nil
}
