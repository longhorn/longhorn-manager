package v112xto1130

import (
	"context"

	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.12.x to v1.13.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeRecurringJobs(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	if err := createDefaultControlPathSetting(namespace, lhClient); err != nil {
		return err
	}

	return nil
}

func createDefaultControlPathSetting(namespace string, lhClient lhclientset.Interface) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"create default control path setting failed")
	}()

	_, err = lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNameDefaultControlPath), metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameDefaultControlPath),
		},
		Value: types.DefaultControlPath,
	}
	if _, err = lhClient.LonghornV1beta2().Settings(namespace).Create(context.TODO(), setting, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	return nil
}

func upgradeRecurringJobs(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade recurring jobs failed")
	}()

	recurringJobsMap, err := upgradeutil.ListAndUpdateRecurringJobsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn recurring jobs during the recurring jobs upgrade")
	}

	for _, rj := range recurringJobsMap {
		if rj.Spec.RetentionPolicy == "" {
			rj.Spec.RetentionPolicy = longhorn.RecurringJobRetentionPolicyCountBased
		}
	}

	return nil
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := updateSnapshotsStatus(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func updateSnapshotsStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade snapshots failed")
	}()

	snapshotMap, err := upgradeutil.ListAndUpdateSnapshotsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn snapshots during the snapshots upgrade")
	}
	engineMap, err := upgradeutil.ListAndUpdateEnginesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn engines during the snapshots upgrade")
	}
	for _, engine := range engineMap {
		if engine.Status.Snapshots == nil {
			continue
		}
		for snapshotName, snapshotInfo := range engine.Status.Snapshots {
			snapshot := snapshotMap[snapshotName]
			if snapshot == nil {
				continue
			}
			if snapshot.Status.RequestedTime == "" {
				snapshot.Status.RequestedTime = snapshotInfo.Created
			}
		}
	}

	return nil
}
