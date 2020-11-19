package v102to110

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
		if err := updateIMPodLastAppliedTolerationsAnnotation(&pod, kubeClient, namespace); err != nil {
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

func UpgradeReplicas(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade replica failed")
	}()

	replicaList, err := lhClient.LonghornV1beta1().Replicas(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn replicas during the replica upgrade")
	}

	for _, r := range replicaList.Items {
		if r.Spec.DataPath == "" || r.Spec.NodeID == "" {
			continue
		}
		isFailedReplica := false
		node, err := lhClient.LonghornV1beta1().Nodes(namespace).Get(r.Spec.NodeID, metav1.GetOptions{})
		if err != nil {
			logrus.Errorf("%vFailed to get node %v during the replica %v upgrade: %v", upgradeLogPrefix, r.Spec.NodeID, r.Name, err)
			isFailedReplica = true
		} else {
			if diskStatus, exists := node.Status.DiskStatus[r.Spec.DiskID]; !exists {
				logrus.Errorf("%vCannot find disk status during the replica %v upgrade", upgradeLogPrefix, r.Name)
				isFailedReplica = true
			} else {
				if _, exists := node.Spec.Disks[r.Spec.DiskID]; !exists {
					logrus.Errorf("%vCannot find disk spec during the replica %v upgrade", upgradeLogPrefix, r.Name)
					isFailedReplica = true
				} else {
					pathElements := strings.Split(filepath.Clean(r.Spec.DataPath), "/replicas/")
					if len(pathElements) != 2 {
						logrus.Errorf("%vFound invalid data path %v during the replica %v upgrade", upgradeLogPrefix, r.Spec.DataPath, r.Name)
						isFailedReplica = true
					} else {
						r.Labels[types.LonghornDiskUUIDKey] = diskStatus.DiskUUID
						r.Spec.DiskID = diskStatus.DiskUUID
						// The disk path will be synced by node controller later.
						r.Spec.DiskPath = pathElements[0]
						r.Spec.DataDirectoryName = pathElements[1]
					}
				}
			}
		}
		if isFailedReplica && r.Spec.FailedAt == "" {
			r.Spec.FailedAt = util.Now()
		}
		r.Spec.DataPath = ""
		if _, err := lhClient.LonghornV1beta1().Replicas(namespace).Update(&r); err != nil {
			return err
		}
	}
	return nil
}

func UpdateDeploymentAndDaemonset(namespace string, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"UpdateLastAppliedTolerationsAnnotation failed")
	}()

	if err := updateDeploymentLastAppliedTolerationsAnnotation(kubeClient, namespace); err != nil {
		return err
	}
	if err := updateDaemonsetLastAppliedTolerationsAnnotation(kubeClient, namespace); err != nil {
		return err
	}
	return nil
}

func updateDaemonsetLastAppliedTolerationsAnnotation(kubeClient *clientset.Clientset, namespace string) (err error) {
	daemonsetList, err := kubeClient.AppsV1().DaemonSets(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list Longhorn daemonsets for toleration annotation update")
	}
	for _, ds := range daemonsetList.Items {
		dsp := &ds
		needToUpdate, err := needToUpdateTolerationAnnotation(dsp)
		if err != nil {
			return err
		}
		if !needToUpdate {
			continue
		}
		appliedTolerations := getNonDefaultTolerationList(dsp.Spec.Template.Spec.Tolerations)
		appliedTolerationsByte, err := json.Marshal(appliedTolerations)
		if err != nil {
			return err
		}
		if err := util.SetAnnotation(dsp, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix), string(appliedTolerationsByte)); err != nil {
			return err
		}
		if _, err := kubeClient.AppsV1().DaemonSets(namespace).Update(dsp); err != nil {
			return err
		}
	}
	return nil
}

func updateDeploymentLastAppliedTolerationsAnnotation(kubeClient *clientset.Clientset, namespace string) (err error) {
	deploymentList, err := kubeClient.AppsV1().Deployments(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list Longhorn deployments for toleration annotation update")
	}

	for _, dp := range deploymentList.Items {
		dpp := &dp
		needToUpdate, err := needToUpdateTolerationAnnotation(dpp)
		if err != nil {
			return err
		}
		if !needToUpdate {
			continue
		}
		appliedTolerations := getNonDefaultTolerationList(dpp.Spec.Template.Spec.Tolerations)
		appliedTolerationsByte, err := json.Marshal(appliedTolerations)
		if err != nil {
			return err
		}
		if err := util.SetAnnotation(dpp, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix), string(appliedTolerationsByte)); err != nil {
			return err
		}
		if _, err := kubeClient.AppsV1().Deployments(namespace).Update(dpp); err != nil {
			return err
		}
	}
	return nil
}

func updateIMPodLastAppliedTolerationsAnnotation(pod *v1.Pod, kubeClient *clientset.Clientset, namespace string) (err error) {
	needToUpdate, err := needToUpdateTolerationAnnotation(pod)
	if err != nil {
		return err
	}
	if !needToUpdate {
		return nil
	}
	appliedTolerations := getNonDefaultTolerationList(pod.Spec.Tolerations)
	appliedTolerationsByte, err := json.Marshal(appliedTolerations)
	if err != nil {
		return err
	}
	if err := util.SetAnnotation(pod, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix), string(appliedTolerationsByte)); err != nil {
		return err
	}
	if _, err = kubeClient.CoreV1().Pods(namespace).Update(pod); err != nil {
		return errors.Wrapf(err, "failed to update toleration annotation for instance manager pod %v", pod.GetName())
	}
	return nil
}

func getNonDefaultTolerationList(tolerations []v1.Toleration) []v1.Toleration {
	result := []v1.Toleration{}
	for _, t := range tolerations {
		if !util.IsKubernetesDefaultToleration(t) {
			result = append(result, t)
		}
	}
	return result
}

func needToUpdateTolerationAnnotation(obj runtime.Object) (bool, error) {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return false, err
	}

	annos := objMeta.GetAnnotations()
	if annos == nil {
		return true, nil
	}

	_, ok := annos[types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix)]
	if ok {
		return false, nil
	}

	return true, nil
}
