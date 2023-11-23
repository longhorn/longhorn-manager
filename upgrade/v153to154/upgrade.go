package v153to154

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.5.3 to v1.5.4: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// We will probably need to upgrade other resources as well. See upgradeVolumeAttachments or previous Longhorn
	// versions for examples.
	return upgradeReplicas(namespace, lhClient, resourceMaps)
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
		// If Spec.EvictionRequested is already in use (e.g. in some version of v1.4.x), Status.EvictionRequested
		// mirrors it. Otherwise, Status.EvictionRequested was previously in use. Either way, it is fine to set the
		// (potentially new) Spec.EvictionRequested to Status.EvictionRequested.
		r.Spec.EvictionRequested = r.Status.EvictionRequested
	}

	return nil
}
