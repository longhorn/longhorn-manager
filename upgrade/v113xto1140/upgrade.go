package v113xto1140

import (
	"context"

	"github.com/cockroachdb/errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.13.x to v1.14.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient clientset.Interface, resourceMaps map[string]any) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeBackingImageManager(namespace, lhClient, kubeClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

// upgradeBackingImageManager annotates existing running BIM pods with their disk's current
// Ready transition timestamp, so adding the recovery check does not restart healthy pods.
func upgradeBackingImageManager(namespace string, lhClient *lhclientset.Clientset, kubeClient clientset.Interface, resourceMaps map[string]any) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backing image manager pods failed")
	}()

	bims, err := upgradeutil.ListAndUpdateBackingImageManagersInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "failed to list backing image managers")
	}
	nodes, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "failed to list nodes")
	}

	annotationKey := types.GetLonghornLabelKey(types.BackingImageManagerStartingAtAnnotationKeySuffix)
	for _, bim := range bims {
		node := nodes[bim.Spec.NodeID]
		if node == nil || bim.Spec.DiskUUID == "" {
			continue
		}
		var diskStatus *longhorn.DiskStatus
		for diskName, status := range node.Status.DiskStatus {
			diskSpec, exists := node.Spec.Disks[diskName]
			if status != nil && status.DiskUUID == bim.Spec.DiskUUID && exists && diskSpec.Path == bim.Spec.DiskPath {
				diskStatus = status
				break
			}
		}
		if diskStatus == nil {
			continue
		}

		pod, err := kubeClient.CoreV1().Pods(namespace).Get(context.TODO(), bim.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return errors.Wrapf(err, "failed to get backing image manager pod %v", bim.Name)
		}
		if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil || pod.Spec.NodeName != bim.Spec.NodeID {
			continue
		}
		// Preserve even a stale value: a retry must not mask a later disk recovery.
		if _, exists := pod.Annotations[annotationKey]; exists {
			continue
		}
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[annotationKey] = types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeReady).LastTransitionTime
		if _, err := kubeClient.CoreV1().Pods(namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to annotate backing image manager pod %v", bim.Name)
		}
	}

	return nil
}
