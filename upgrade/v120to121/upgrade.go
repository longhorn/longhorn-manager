package v120to121

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.2.0 to v1.2.1: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, resources map[string]interface{}) (err error) {
	if err := upgradeSettings(namespace, lhClient, resources); err != nil {
		return err
	}
	return nil
}

func upgradeSettings(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade settings failed")
	}()

	// Skip the upgrade if this deprecated setting is unavailable.
	disableReplicaRebuildSetting, err := upgradeutil.GetSettingFromProvidedCache(namespace, lhClient, resourceMaps, string(types.SettingNameDisableReplicaRebuild))
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	disabled, err := strconv.ParseBool(disableReplicaRebuildSetting.Value)
	if err != nil {
		logrus.WithError(err).Warnf(upgradeLogPrefix+"Failed to parse boolean value for setting %v, will skip checking it", types.SettingNameDisableReplicaRebuild)
		return nil
	}
	if !disabled {
		return nil
	}

	// The rebuilding is already disabled. This new setting should be consistent with it.
	concurrentRebuildLimitSetting, err := upgradeutil.GetSettingFromProvidedCache(namespace, lhClient, resourceMaps, string(types.SettingNameConcurrentReplicaRebuildPerNodeLimit))
	if err != nil {
		return err
	}
	concurrentRebuildLimit, err := strconv.ParseInt(concurrentRebuildLimitSetting.Value, 10, 32)
	if err != nil {
		return err
	}
	if concurrentRebuildLimit != 0 {
		concurrentRebuildLimitSetting.Value = "0"
	}

	// This old/deprecated setting can be unset or cleaned up now.
	disableReplicaRebuildSetting.Value = "false"

	return nil
}
