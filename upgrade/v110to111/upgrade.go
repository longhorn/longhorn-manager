package v110to111

import (
	"math"
	"strconv"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	upgradeLogPrefix = "upgrade from v1.1.0 to v1.1.1: "
)

// This upgrade allow Longhorn switch to the new CPU settings as well as
// deprecating the old setting automatically:
// https://github.com/longhorn/longhorn/issues/2207

func UpgradeCRs(namespace string, lhClient *lhclientset.Clientset) error {
	if err := doLonghornNodeUpgrade(namespace, lhClient); err != nil {
		return err
	}
	return nil
}

func doLonghornNodeUpgrade(namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade longhorn node failed")
	}()

	deprecatedCPUSetting, err := lhClient.LonghornV1beta1().Settings(namespace).Get(string(types.SettingNameGuaranteedEngineCPU), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if deprecatedCPUSetting.Value == "" {
		return nil
	}

	requestedCPU, err := strconv.ParseFloat(deprecatedCPUSetting.Value, 64)
	if err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to convert the deprecated setting %v value %v to an float", types.SettingNameGuaranteedEngineCPU, deprecatedCPUSetting.Value)
	}
	// Convert to milli value
	requestedMilliCPU := int(math.Round(requestedCPU * 1000))

	nodeList, err := lhClient.LonghornV1beta1().Nodes(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing Longhorn nodes")
	}

	for _, node := range nodeList.Items {
		updateRequired := false
		if node.Spec.EngineManagerCPURequest == 0 {
			node.Spec.EngineManagerCPURequest = requestedMilliCPU
			updateRequired = true
		}
		if node.Spec.ReplicaManagerCPURequest == 0 {
			node.Spec.ReplicaManagerCPURequest = requestedMilliCPU
			updateRequired = true
		}
		if updateRequired == true {
			if _, err := lhClient.LonghornV1beta1().Nodes(namespace).Update(&node); err != nil {
				return err
			}
		}
	}

	deprecatedCPUSetting.Value = ""
	if _, err := lhClient.LonghornV1beta1().Settings(namespace).Update(deprecatedCPUSetting); err != nil {
		return err
	}

	return nil
}
