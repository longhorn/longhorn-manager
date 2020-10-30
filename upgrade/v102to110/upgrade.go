package v102to110

import (
	"fmt"

	"github.com/pkg/errors"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

// This upgrade is needed because we add one more field `controller: true`
// to the ownerReferences of instance manager pods so that `kubectl drain`
// can work without --force flag.
// Therefore, we need to updade the field for all existing instance manager pod
// Link to the original issue: https://github.com/longhorn/longhorn/issues/1286

const (
	upgradeLogPrefix = "upgrade from v1.0.2 to v1.1.0: "
)

func UpgradeInstanceManagerPods(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade instance manager pods failed")
	}()

	imPodsList, err := kubeClient.CoreV1().Pods(namespace).List(metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", types.GetLonghornLabelComponentKey(), types.LonghornLabelInstanceManager),
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing instance manager pods during the instance managers pods upgrade")
	}

	for _, pod := range imPodsList.Items {
		if err := upgradeInstanceMangerPodOwnerRef(&pod, kubeClient, namespace); err != nil {
			return err
		}
	}
	return nil
}

func upgradeInstanceMangerPodOwnerRef(pod *v1.Pod, kubeClient *clientset.Clientset, namespace string) (err error) {
	metadata, err := meta.Accessor(pod)
	if err != nil {
		return err
	}

	podOwnerRefs := metadata.GetOwnerReferences()
	isController := true
	needToUpdate := false
	for ind, ownerRef := range podOwnerRefs {
		if ownerRef.Kind == types.LonghornKindInstanceManager &&
			(ownerRef.Controller == nil || *ownerRef.Controller != true) {
			ownerRef.Controller = &isController
			needToUpdate = true
		}
		podOwnerRefs[ind] = ownerRef
	}

	if !needToUpdate {
		return nil
	}

	metadata.SetOwnerReferences(podOwnerRefs)

	if _, err = kubeClient.CoreV1().Pods(namespace).Update(pod); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to update the owner reference for instance manager pod %v during the instance managers pods upgrade", pod.GetName())
	}

	return nil
}

func UpgradeVolumes(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumeList, err := lhClient.LonghornV1beta1().Volumes(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumeList.Items {
		if v.Status.Robustness != types.VolumeRobustnessDegraded {
			continue
		}
		v.Status.LastDegradedAt = util.Now()
		if _, err := lhClient.LonghornV1beta1().Volumes(namespace).UpdateStatus(&v); err != nil {
			return err
		}
	}
	return nil
}
