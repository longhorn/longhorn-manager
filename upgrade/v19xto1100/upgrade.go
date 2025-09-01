package v19xto1100

import (
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.9.x to v1.10.0: "
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

	if err := upgradeNodes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeSettings(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeVolumes(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	if err := upgradeBackups(namespace, lhClient, resourceMaps); err != nil {
		return err
	}

	return nil
}

func upgradeNodes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade node failed")
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

	// Update Setting CRs
	var errs []error
	for _, s := range settingMap {
		definition, ok := types.GetSettingDefinition(types.SettingName(s.Name))
		if !ok {
			logrus.Warnf("Unknown setting %v found during upgrade, skipping", s.Name)
			continue
		}
		if !definition.DataEngineSpecific {
			continue
		}

		value, err := datastore.GetSettingValidValue(definition, s.Value)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to get valid value for setting %s", s.Name))
			continue
		}

		s.Value = value
	}

	return errors.Join(errs...)
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	volumesMap, err := upgradeutil.ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn volumes during the volume upgrade")
	}

	for _, v := range volumesMap {
		if v.Spec.BackupBlockSize == 0 {
			v.Spec.BackupBlockSize = types.BackupBlockSize2Mi
		}
	}

	return nil
}

func upgradeBackups(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade backup failed")
	}()

	backupsMap, err := upgradeutil.ListAndUpdateBackupsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing Longhorn backups during the backup upgrade")
	}

	for _, b := range backupsMap {
		if b.Spec.BackupBlockSize == 0 {
			b.Spec.BackupBlockSize = types.BackupBlockSize2Mi
		}
	}

	return nil
}
