package v102to110

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
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

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeReplicas(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeInstanceManagerPods(namespace, kubeClient, resourceMaps); err != nil {
		return err
	}
	return nil
}

func upgradeInstanceManagerPods(namespace string, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade instance manager pods failed")
	}()

	imPods, err := upgradeutil.ListIMPods(namespace, kubeClient)
	if err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing instance manager pods before updating Pod's owner reference")
	}
	for _, pod := range imPods {
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
			(ownerRef.Controller == nil || !*ownerRef.Controller) {
			ownerRef.Controller = &isController
			needToUpdate = true
		}
		podOwnerRefs[ind] = ownerRef
	}

	if !needToUpdate {
		return nil
	}

	metadata.SetOwnerReferences(podOwnerRefs)

	if _, err = kubeClient.CoreV1().Pods(namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to update the owner reference for instance manager pod %v during the instance managers pods upgrade", pod.GetName())
	}

	return nil
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumeMap, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumeMap {
		// in pr https://github.com/longhorn/longhorn-manager/pull/789
		// we added a new access mode field, that is exposed to the ui
		// so we add the previously only supported rwo access mode
		if v.Spec.AccessMode == "" {
			v.Spec.AccessMode = longhorn.AccessModeReadWriteOnce
		}

		if v.Status.Robustness == longhorn.VolumeRobustnessDegraded && v.Status.LastDegradedAt == "" {
			v.Status.LastDegradedAt = util.Now()
		}
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
		if r.Spec.DataPath == "" || r.Spec.NodeID == "" {
			continue
		}
		isFailedReplica := false
		node, err := upgradeutil.GetNodeFromProvidedCache(namespace, lhClient, resourceMaps, r.Spec.NodeID)
		if err != nil {
			logrus.WithError(err).Errorf("%vFailed to get node %v during the replica %v upgrade", upgradeLogPrefix, r.Spec.NodeID, r.Name)
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
	}
	return nil
}
