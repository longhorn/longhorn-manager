package app

import (
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"
)

const (
	PVCAnnotationCSIProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	PVAnnotationCSIProvisioner  = "pv.kubernetes.io/provisioned-by"
)

func upgradeLonghornRelatedComponents(kubeClient *clientset.Clientset, namespace string) error {
	logrus.Infof("Upgrading Longhorn related components for CSI v1.1.0")

	pvList, err := kubeClient.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, pv := range pvList.Items {
		if v, exist := pv.Annotations[PVAnnotationCSIProvisioner]; exist {
			if v == types.DeprecatedProvisionerName {
				pv.Annotations[PVAnnotationCSIProvisioner] = types.LonghornDriverName
				_, err := kubeClient.CoreV1().PersistentVolumes().Update(&pv)
				if err != nil {
					return err
				}
			}

			pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if v, exist := pvc.Annotations[PVCAnnotationCSIProvisioner]; exist && v == types.DeprecatedProvisionerName {
				pvc.Annotations[PVCAnnotationCSIProvisioner] = types.LonghornDriverName
				_, err := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(pvc)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
