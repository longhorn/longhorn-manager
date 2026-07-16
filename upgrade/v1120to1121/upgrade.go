package v1120to1121

import (
	"github.com/cockroachdb/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.12.0 to v1.12.1: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
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
		return errors.Wrapf(err, "failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumeMap {
		if !types.IsDataFromVolume(v.Spec.DataSource) {
			continue
		}
		srcVolName := types.GetVolumeName(v.Spec.DataSource)
		if srcVolName == "" {
			continue
		}
		labelKey := types.GetLonghornLabelKey(types.LonghornLabelCloneSourceVolume)
		if v.Labels == nil {
			v.Labels = map[string]string{}
		}
		if v.Labels[labelKey] == "" {
			v.Labels[labelKey] = srcVolName
		}
		// Remove the deprecated linked-clone-source-volume label.
		delete(v.Labels, types.GetLonghornLabelKey("linked-clone-source-volume"))
	}

	return nil
}
