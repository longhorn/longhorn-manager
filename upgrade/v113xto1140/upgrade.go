package v113xto1140

import (
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
	upgradeLogPrefix = "upgrade from v1.13.x to v1.14.0: "
)

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset, resourceMaps map[string]interface{}) error {
	if resourceMaps == nil {
		return errors.New("resourceMaps cannot be nil")
	}

	if err := upgradeNodeDataEngineResources(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

// The instance manager fields are not cleared: resources are written in no fixed order, so the values
// would be lost if the node update failed after the instance manager update.
func upgradeNodeDataEngineResources(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade node data engine resources failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn nodes")
	}
	imMap, err := upgradeutil.ListAndUpdateInstanceManagersInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn instance managers")
	}
	settingMap, err := upgradeutil.ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, "failed to list all existing Longhorn settings")
	}
	defaultIMImage := ""
	if setting, ok := settingMap[string(types.SettingNameDefaultInstanceManagerImage)]; ok {
		defaultIMImage = setting.Value
	}

	nodeIMs := map[string][]*longhorn.InstanceManager{}
	for _, im := range imMap {
		if !types.IsDataEngineV2(im.Spec.DataEngine) {
			continue
		}
		nodeIMs[im.Spec.NodeID] = append(nodeIMs[im.Spec.NodeID], im)
	}

	for nodeName, ims := range nodeIMs {
		node, ok := nodeMap[nodeName]
		if !ok {
			continue
		}
		source := sourceInstanceManager(ims, defaultIMImage)
		for _, im := range ims {
			if im != source && im.Spec.DataEngineSpec.V2 != source.Spec.DataEngineSpec.V2 {
				logrus.Warnf(upgradeLogPrefix+"Ignoring V2 data engine overrides %+v of instance manager %v, node %v takes them from instance manager %v",
					im.Spec.DataEngineSpec.V2, im.Name, nodeName, source.Name)
			}
		}
		migrateV2DataEngineSpec(node, source)
	}

	return nil
}

// Other V2 instance managers on the node are older ones still holding instances.
func sourceInstanceManager(ims []*longhorn.InstanceManager, defaultIMImage string) *longhorn.InstanceManager {
	var newest *longhorn.InstanceManager
	for _, im := range ims {
		if im.Spec.Image == defaultIMImage {
			return im
		}
		if newest == nil || newest.CreationTimestamp.Before(&im.CreationTimestamp) {
			newest = im
		}
	}
	return newest
}

func migrateV2DataEngineSpec(node *longhorn.Node, im *longhorn.InstanceManager) {
	spec := im.Spec.DataEngineSpec.V2

	var cpuMask *string
	if spec.CPUMask != "" {
		normalized, err := types.NormalizeCPUMask(spec.CPUMask)
		if err != nil {
			logrus.WithError(err).Warnf(upgradeLogPrefix+"Skipping invalid CPU mask %v of instance manager %v", spec.CPUMask, im.Name)
		} else {
			cpuMask = &normalized
		}
	}
	var cpuIsolationEnabled *bool
	if spec.CPUIsolationEnabled != "" {
		enabled := spec.CPUIsolationEnabled == longhorn.TrueValue
		cpuIsolationEnabled = &enabled
	}
	if cpuMask == nil && cpuIsolationEnabled == nil {
		return
	}

	if node.Spec.DataEngineResources == nil {
		node.Spec.DataEngineResources = &longhorn.NodeDataEngineResources{}
	}
	if node.Spec.DataEngineResources.V2 == nil {
		node.Spec.DataEngineResources.V2 = &longhorn.NodeV2DataEngineResources{}
	}
	// A retried upgrade must not overwrite values already on the node.
	resources := node.Spec.DataEngineResources.V2
	if resources.CPUMask == nil {
		resources.CPUMask = cpuMask
	}
	if resources.CPUIsolationEnabled == nil {
		resources.CPUIsolationEnabled = cpuIsolationEnabled
	}
}
