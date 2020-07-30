package v100to101

import (
	"github.com/pkg/errors"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
)

// This upgrade is needed because we changed from using full image name (in v1.0.0)
// to use the checksum of image name (in v1.0.1) for instance manager labels.
// Therefore, we need to update all existing instance manager labels so that
// the updated Longhorn manager can correctly find them.
// Link to the original issue: https://github.com/longhorn/longhorn/issues/1323

const (
	upgradeLogPrefix = "upgrade from v1.0.0 to v1.0.1: "
)

func UpgradeInstanceManagerPods(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade instance manager pods failed")
	}()

	imList, err := lhClient.LonghornV1beta1().InstanceManagers(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing instance managers during the instance managers pods upgrade")
	}

	for _, im := range imList.Items {
		if im.Spec.Image == "" {
			continue
		}
		imPodsList, err := kubeClient.CoreV1().Pods(namespace).List(metav1.ListOptions{
			FieldSelector: "metadata.name=" + im.Name,
		})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return errors.Wrapf(err, upgradeLogPrefix+"failed to find pod for instance manager %v during the instance managers pods upgrade", im.Name)
		}
		for _, pod := range imPodsList.Items {
			if err = upgradeInstanceMangerPodLabel(&pod, &im, kubeClient, namespace); err != nil {
				return err
			}
		}
	}
	return nil
}

func upgradeInstanceMangerPodLabel(pod *v1.Pod, im *longhorn.InstanceManager, kubeClient *clientset.Clientset, namespace string) (err error) {
	metadata, err := meta.Accessor(pod)
	if err != nil {
		return err
	}
	metadata.SetLabels(types.GetInstanceManagerLabels(im.Spec.NodeID, im.Spec.Image, im.Spec.Type))
	if pod, err = kubeClient.CoreV1().Pods(namespace).Update(pod); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to update the spec for instance manager pod %v during the instance managers upgrade", pod.Name)
	}
	return nil
}

func UpgradeCRDs(namespace string, lhClient *lhclientset.Clientset) error {
	if err := doInstanceManagerUpgrade(namespace, lhClient); err != nil {
		return err
	}
	return nil
}

func doInstanceManagerUpgrade(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade instance manager failed")
	}()

	imList, err := lhClient.LonghornV1beta1().InstanceManagers(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing instance managers during the instance managers upgrade")
	}

	for _, im := range imList.Items {
		newInstanceManagerLabels := types.GetInstanceManagerLabels(im.Spec.NodeID, im.Spec.Image, im.Spec.Type)
		if im.Labels[types.LonghornLabelInstanceManagerImage] != newInstanceManagerLabels[types.LonghornLabelInstanceManagerImage] {
			if err := upgradeInstanceManagersLabels(&im, lhClient, namespace); err != nil {
				return err
			}
		}
	}
	return nil
}

func upgradeInstanceManagersLabels(im *longhorn.InstanceManager, lhClient *lhclientset.Clientset, namespace string) (err error) {
	metadata, err := meta.Accessor(im)
	if err != nil {
		return err
	}
	metadata.SetLabels(types.GetInstanceManagerLabels(im.Spec.NodeID, im.Spec.Image, im.Spec.Type))
	if im, err = lhClient.LonghornV1beta1().InstanceManagers(namespace).Update(im); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to update the spec for instance manager %v during the instance managers upgrade", im.Name)
	}
	return nil
}
