package v110xto1110

import (
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.10.x to v1.11.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := updateCRs(namespace, lhClient, kubeClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func updateCRs(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeSettings(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func upgradeSettings(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade setting failed")
	}()

	settingMap, err := upgradeutil.ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn settings during the setting upgrade")
	}

	storageNetwork, ok := settingMap[string(types.SettingNameStorageNetwork)]
	if !ok {
		logrus.Warnf("Setting %v not found. Skipping the upgrade for endpoint network for RWX volume.", types.SettingNameStorageNetwork)
		return nil
	}

	storageNetworkForRWXVolumeEnabled, ok := settingMap[string(types.SettingNameStorageNetworkForRWXVolumeEnabled)]
	if !ok {
		logrus.Warnf("Setting %v not found. Skipping the upgrade for endpoint network for RWX volume.", types.SettingNameStorageNetworkForRWXVolumeEnabled)
		return nil
	}

	endpointNetworkForRWXVolume, ok := settingMap[string(types.SettingNameEndpointNetworkForRWXVolume)]
	if !ok {
		return errors.New("cannot find endpoint network for RWX volume setting during the setting upgrade")
	}

	// Inherit the storage network to endpoint network for RWX volume if:
	// 	1. endpoint network for RWX volume is not set.
	// 	2. storage network is set.
	// 	3. storage network for RWX volume is enabled.
	if endpointNetworkForRWXVolume.Value == types.CniNetworkNone &&
		storageNetwork.Value != types.CniNetworkNone &&
		storageNetworkForRWXVolumeEnabled.Value == "true" {
		endpointNetworkForRWXVolume.Value = storageNetwork.Value
	}

	return nil
}
