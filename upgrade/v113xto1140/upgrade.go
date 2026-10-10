package v113xto1140

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

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

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	return upgradeSettings(namespace, lhClient, resourceMaps)
}

func upgradeSettings(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade settings failed")
	}()

	settingMap, err := upgradeutil.ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn settings during the setting upgrade")
	}

	newSetting, err := migrateIobufLargePoolSizeSetting(settingMap)
	if err != nil {
		return err
	}
	if newSetting == nil {
		return nil
	}

	// The new setting is created immediately, while the reset value of
	// data-engine-iobuf-large-pool-size in settingMap is persisted later by
	// upgradeutil.UpdateResources.
	created, err := lhClient.LonghornV1beta2().Settings(namespace).Create(context.TODO(), newSetting, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create setting %v", newSetting.Name)
		}
		if created, err = lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), newSetting.Name, metav1.GetOptions{}); err != nil {
			return errors.Wrapf(err, "failed to get setting %v", newSetting.Name)
		}
	}
	settingMap[created.Name] = created

	return nil
}

// migrateIobufLargePoolSizeSetting migrates the setting data-engine-iobuf-large-pool-size, which
// configured the SPDK iobuf large_pool_count before v1.14.0, to data-engine-iobuf-large-pool-count.
// data-engine-iobuf-large-pool-size now represents the SPDK iobuf large_bufsize, so its value is
// reset to the default. The returned setting is the data-engine-iobuf-large-pool-count setting to
// be created, or nil if it already exists or there is nothing to migrate. The settings in
// settingMap are updated in place.
func migrateIobufLargePoolSizeSetting(settingMap map[string]*longhorn.Setting) (*longhorn.Setting, error) {
	oldSetting, ok := settingMap[string(types.SettingNameDataEngineIobufLargePoolSize)]
	if !ok {
		return nil, nil
	}

	largePoolSizeDefinition, ok := types.GetSettingDefinition(types.SettingNameDataEngineIobufLargePoolSize)
	if !ok {
		return nil, errors.Errorf("setting %v is not defined", types.SettingNameDataEngineIobufLargePoolSize)
	}

	var newSetting *longhorn.Setting
	if _, exists := settingMap[string(types.SettingNameDataEngineIobufLargePoolCount)]; !exists {
		largePoolCountDefinition, ok := types.GetSettingDefinition(types.SettingNameDataEngineIobufLargePoolCount)
		if !ok {
			return nil, errors.Errorf("setting %v is not defined", types.SettingNameDataEngineIobufLargePoolCount)
		}

		value := oldSetting.Value
		if err := types.ValidateSetting(string(types.SettingNameDataEngineIobufLargePoolCount), value); err != nil {
			logrus.WithError(err).Warnf("%vCannot copy value %v of setting %v to setting %v, using the default value %v",
				upgradeLogPrefix, value, types.SettingNameDataEngineIobufLargePoolSize, types.SettingNameDataEngineIobufLargePoolCount, largePoolCountDefinition.Default)
			value = largePoolCountDefinition.Default
		}

		logrus.Infof("%vCopying value %v of setting %v to setting %v",
			upgradeLogPrefix, value, types.SettingNameDataEngineIobufLargePoolSize, types.SettingNameDataEngineIobufLargePoolCount)
		newSetting = &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(types.SettingNameDataEngineIobufLargePoolCount),
			},
			Value: value,
		}
	}

	// Reset the value when the large pool count is migrated by this upgrade, or when the existing
	// value is still a legacy large_pool_count that is invalid as a large_bufsize (e.g. a retried upgrade).
	if newSetting != nil || types.ValidateSetting(string(types.SettingNameDataEngineIobufLargePoolSize), oldSetting.Value) != nil {
		logrus.Infof("%vUpdating setting %v value from %v to %v",
			upgradeLogPrefix, types.SettingNameDataEngineIobufLargePoolSize, oldSetting.Value, largePoolSizeDefinition.Default)
		oldSetting.Value = largePoolSizeDefinition.Default
	}

	return newSetting, nil
}
