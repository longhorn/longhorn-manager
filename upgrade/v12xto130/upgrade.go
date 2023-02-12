package v12xto130

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.2.x to v1.3.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if err := upgradeReplicas(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	return nil
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
		if r.Status.IP == "" {
			continue
		}

		if r.Status.StorageIP != "" {
			continue
		}

		r.Status.StorageIP = r.Status.IP
	}

	return nil
}
