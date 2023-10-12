package v143to144

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	upgradeLogPrefix = "upgrade from v1.4.3 to v1.4.4: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) error {
	return deleteCSIServices(namespace, kubeClient)
}

func deleteCSIServices(namespace string, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"delete CSI service failed")
	}()

	servicesToDelete := map[string]struct{}{
		types.CSIAttacherName:    {},
		types.CSIProvisionerName: {},
		types.CSIResizerName:     {},
		types.CSISnapshotterName: {},
	}

	servicesInCluster, err := kubeClient.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "failed to list all existing Longhorn services during the CSI service deletion")
	}

	for _, serviceInCluster := range servicesInCluster.Items {
		if _, ok := servicesToDelete[serviceInCluster.Name]; !ok {
			continue
		}
		if err = kubeClient.CoreV1().Services(namespace).Delete(context.TODO(), serviceInCluster.Name, metav1.DeleteOptions{}); err != nil {
			// Best effort. Dummy services have no function and no finalizer, so we can proceed on failure.
			logrus.Warnf("Deprecated CSI dummy service could not be deleted during upgrade: %v", err)
		}
	}

	return nil
}
