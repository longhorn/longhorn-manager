package v17xto180

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
	upgradeLogPrefix = "upgrade from v1.7.x to v1.8.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	return upgradeBackingImages(namespace, lhClient, resourceMaps)
}

func upgradeBackingImages(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backing image failed")
	}()

	backingImageMap, err := upgradeutil.ListAndUpdateBackingImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn backing images during the backing image upgrade")
	}

	for _, bi := range backingImageMap {
		if bi.Spec.MinNumberOfCopies == 0 {
			bi.Spec.MinNumberOfCopies = types.DefaultMinNumberOfCopies
		}
		if string(bi.Spec.DataEngine) == "" {
			bi.Spec.DataEngine = longhorn.DataEngineTypeV1
		}

		// before v1.8.0, there should not have any v2 data engine disk in the backing image.
		for bi.Spec.DiskFileSpecMap != nil {
			for diskUUID := range bi.Spec.DiskFileSpecMap {
				bi.Spec.DiskFileSpecMap[diskUUID].DataEngine = longhorn.DataEngineTypeV1
			}
		}

		// before v1.8.0, there should not have any v2 data engine disk in the backing image.
		for bi.Status.DiskFileStatusMap != nil {
			for diskUUID := range bi.Status.DiskFileStatusMap {
				bi.Status.DiskFileStatusMap[diskUUID].DataEngine = longhorn.DataEngineTypeV1
			}
		}
	}

	return nil
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	// Currently there are no statuses to upgrade. See UpgradeResources -> upgradeVolumes or previous Longhorn versions
	// for examples.
	return nil
}
