package v110xto1110

import (
	"github.com/cockroachdb/errors"

	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.10.x to v1.11.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := updateCRs(namespace, lhClient, kubeClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func updateCRs(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) (err error) {

	if err := upgradeutil.CopyCSISidecarSettings(namespace, lhClient); err != nil {
		return err
	}

	return nil
}
