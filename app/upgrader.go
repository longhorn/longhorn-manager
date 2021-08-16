package app

import (
	"context"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	PVCAnnotationCSIProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	PVAnnotationCSIProvisioner  = "pv.kubernetes.io/provisioned-by"
)

func upgradeLonghornRelatedComponents(kubeClient *clientset.Clientset, namespace string) error {
	logrus.Infof("Upgrading Longhorn related components for CSI v1.1.0")

	pvList, err := kubeClient.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, pv := range pvList.Items {
		// Make sure we are handling the Longhorn related PVs only
		if v, exist := pv.Annotations[PVAnnotationCSIProvisioner]; !exist || v != types.DeprecatedProvisionerName {
			continue
		}
		pv.Annotations[PVAnnotationCSIProvisioner] = types.LonghornDriverName
		if _, err := kubeClient.CoreV1().PersistentVolumes().Update(context.TODO(), &pv, metav1.UpdateOptions{}); err != nil {
			return err
		}

		// No related PVC. The PV provisioned by the static provisioner contains the annotation but no ClaimRef field.
		// e.g., PVs created by the local storage provisioner.
		// See https://github.com/longhorn/longhorn/issues/905 for more details.
		if pv.Spec.ClaimRef == nil {
			continue
		}
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(context.TODO(), pv.Spec.ClaimRef.Name, metav1.GetOptions{})
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				continue
			}
			return err
		}
		if v, exist := pvc.Annotations[PVCAnnotationCSIProvisioner]; !exist || v != types.DeprecatedProvisionerName {
			continue
		}
		pvc.Annotations[PVCAnnotationCSIProvisioner] = types.LonghornDriverName
		if _, err := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(context.TODO(), pvc, metav1.UpdateOptions{}); err != nil {
			return err
		}

	}

	return nil
}
