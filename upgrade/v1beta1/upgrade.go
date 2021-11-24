package v1beta1

import (
	"context"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"

	longhornV1beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	"github.com/longhorn/longhorn-manager/upgrade/v1beta1/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	upgradeLogPrefix = "upgrade from v1beta1 to v1beta2:"
)

func UpgradeFromV1beta1ToV1beta2(config *restclient.Config, namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+" failed")
	}()

	lhClientV1beta1, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get clientset for v1beta1")
	}

	scheme := runtime.NewScheme()
	if err := longhornV1beta1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "unable to create scheme for v1beta1")
	}

	if err := upgradeVolumes(namespace, lhClientV1beta1, lhClient); err != nil {
		return err
	}
	if err := upgradeEngineImages(namespace, lhClientV1beta1, lhClient); err != nil {
		return err
	}
	if err := upgradeBackupTargets(namespace, lhClientV1beta1, lhClient); err != nil {
		return err
	}
	if err := upgradeNodes(namespace, lhClientV1beta1, lhClient); err != nil {
		return err
	}

	logrus.Infof("%v: completed", upgradeLogPrefix)
	return nil
}

func copyConditions(fromConditions map[string]longhornV1beta1.Condition) ([]longhorn.Condition, error) {
	toConditions := []longhorn.Condition{}
	for _, fromCon := range fromConditions {
		toCon := longhorn.Condition{}
		if err := copier.Copy(&toCon, &fromCon); err != nil {
			return nil, err
		}
		toConditions = append(toConditions, toCon)
	}
	return toConditions, nil
}

func CanUpgrade(config *restclient.Config, namespace string) (bool, error) {
	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return false, errors.Wrap(err, "unable to get clientset for v1beta1")
	}

	scheme := runtime.NewScheme()
	if err := longhornV1beta1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return false, errors.Wrap(err, "unable to create scheme for v1beta1")
	}

	setting, err := lhClient.LonghornV1beta1().Settings(namespace).Get(context.TODO(), string(types.SettingNameDefaultEngineImage), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// cannot find the setting,
			logrus.Info("Longhorn CRD API v1beta1 not found")
			return true, nil
		}

		return false, errors.Wrap(err, "unable to verify if version matches v1beta1")
	}

	return false, errors.Wrapf(err, "unable to upgrade from %v directly", setting.Value)
}

func upgradeVolumes(namespace string, lhClientV1beta1 *lhclientset.Clientset, lhClient *lhclientset.Clientset) error {
	volumes, err := lhClientV1beta1.LonghornV1beta1().Volumes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "unable to list volumes")
	}
	for _, old := range volumes.Items {
		new := &longhorn.Volume{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "upgrade: copy volume conditions")
		}
		new.Status.Conditions = conditions

		new, err = lhClient.LonghornV1beta2().Volumes(namespace).UpdateStatus(context.TODO(), new, metav1.UpdateOptions{})
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta1 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
			continue
		}
	}
	return nil
}

func upgradeEngineImages(namespace string, lhClientV1beta1 *lhclientset.Clientset, lhClient *lhclientset.Clientset) error {
	engineImages, err := lhClientV1beta1.LonghornV1beta1().EngineImages(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "unable to list engine images")
	}
	for _, old := range engineImages.Items {
		new := &longhorn.EngineImage{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "upgrade: copy engineImage conditions")
		}
		new.Status.Conditions = conditions

		new, err = lhClient.LonghornV1beta2().EngineImages(namespace).UpdateStatus(context.TODO(), new, metav1.UpdateOptions{})
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta2 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
			continue
		}
	}
	return nil
}

func upgradeBackupTargets(namespace string, lhClientV1beta1 *lhclientset.Clientset, lhClient *lhclientset.Clientset) error {
	backupTargets, err := lhClientV1beta1.LonghornV1beta1().BackupTargets(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "unable to list engine images")
	}
	for _, old := range backupTargets.Items {
		new := &longhorn.BackupTarget{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "upgrade: copy backupTarget conditions")
		}
		new.Status.Conditions = conditions

		new, err = lhClient.LonghornV1beta2().BackupTargets(namespace).UpdateStatus(context.TODO(), new, metav1.UpdateOptions{})
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta2 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
			continue
		}
	}
	return nil
}

func upgradeNodes(namespace string, lhClientV1beta1 *lhclientset.Clientset, lhClient *lhclientset.Clientset) error {
	nodes, err := lhClientV1beta1.LonghornV1beta1().Nodes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "unable to list engine images")
	}
	for _, old := range nodes.Items {
		new := &longhorn.Node{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "upgrade: copy node conditions")
		}
		new.Status.Conditions = conditions

		new, err = lhClient.LonghornV1beta2().Nodes(namespace).UpdateStatus(context.TODO(), new, metav1.UpdateOptions{})
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta2 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
			continue
		}
	}
	return nil
}
