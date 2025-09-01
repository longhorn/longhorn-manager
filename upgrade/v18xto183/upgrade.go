package v18xto183

import (
	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.8.x to v1.8.3: "
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
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeNodes(namespace, lhClient, resourceMaps); err != nil {
		return err
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

	for _, n := range nodeMap {
		// Blindly clean up the legacy package checking condition. The node controller will attach this condition back during runtime if necessary.
		n.Status.Conditions = types.RemoveCondition(n.Status.Conditions, longhorn.NodeConditionTypeRequiredPackages)
	}

	return nil
}
