package v110xto1110

import (
	"encoding/json"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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

	if err := updateSettings(namespace, lhClient, kubeClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func updateSettings(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade settings failed")
	}()

	settingMap, err := upgradeutil.ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn settings during the settings upgrade")
	}

	// Migrate the offline replica rebuilding setting from data engine specific to general
	offlineRebuildingSetting, exists := settingMap[string(types.SettingNameOfflineReplicaRebuilding)]
	if !exists {
		return nil
	}
	offlineRebuildingSetting.Value = newOfflineReplicaRebuildingSettingValue(offlineRebuildingSetting.Value)

	return nil
}

func newOfflineReplicaRebuildingSettingValue(oldValue string) string {
	var offlineRebuildingJsonValues map[longhorn.DataEngineType]string

	if err := json.Unmarshal([]byte(oldValue), &offlineRebuildingJsonValues); err != nil {
		logrus.WithError(err).Infof("failed to parse setting JSON-formatted value %v, it might be upgraded", oldValue)
		_, err := strconv.ParseBool(oldValue)
		if err == nil {
			return oldValue
		}
		return "false"
	}

	offlineRebuildingNewValue := "false"
	for dataEngine, value := range offlineRebuildingJsonValues {
		if dataEngine != longhorn.DataEngineTypeV1 && dataEngine != longhorn.DataEngineTypeV2 {
			continue
		}
		if value == "true" {
			offlineRebuildingNewValue = "true"
			break
		}
	}

	return offlineRebuildingNewValue
}

func UpgradeResourcesStatus(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	return upgradeNodesStatus(namespace, lhClient, resourceMaps)
}

func upgradeNodesStatus(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade node status failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn nodes during the node upgrade")
	}

	for _, n := range nodeMap {
		// Blindly clean up the legacy package checking condition. The node controller will attach this condition back during runtime if necessary.
		n.Status.Conditions = types.RemoveCondition(n.Status.Conditions, longhorn.NodeConditionTypeRequiredPackages)
	}

	return nil
}

// TODO: upgrade clondeMode from "" to full-clone
