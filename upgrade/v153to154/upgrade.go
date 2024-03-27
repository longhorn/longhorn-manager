package v153to154

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.5.3 to v1.5.4: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resources as well. See upgradeVolumeAttachments or previous Longhorn
	// versions for examples.
	return upgradeReplicas(namespace, lhClient, resourceMaps)
}

<<<<<<< HEAD:upgrade/v153to154/upgrade.go
=======
func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resource status as well. See upgradeEngineStatus or previous Longhorn
	// versions for examples.
	return upgradeEngineStatus(namespace, lhClient, resourceMaps)
}

>>>>>>> 65bdc091 (Track replica mode transition times with replicaTransitionTimeMap):upgrade/v16xto170/upgrade.go
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
		// If Spec.EvictionRequested is already in use (e.g. in some version of v1.4.x), Status.EvictionRequested
		// mirrors it. Otherwise, Status.EvictionRequested was previously in use. Either way, it is fine to set the
		// (potentially new) Spec.EvictionRequested to Status.EvictionRequested.
		r.Spec.EvictionRequested = r.Status.EvictionRequested

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
		for replicaName := range e.Status.ReplicaModeMap {
			// We don't have any historical information to rely on. Starting at the time of the upgrade.
			if _, ok := e.Status.ReplicaTransitionTimeMap[replicaName]; !ok {
				e.Status.ReplicaTransitionTimeMap[replicaName] = util.Now()
			}
		}
	}

	return nil
}
