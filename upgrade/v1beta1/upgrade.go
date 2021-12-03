package v1beta1

import (
	"context"
	"fmt"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"

	longhornV1beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	upgradeLogPrefix = "upgrade from v1beta1 to v1beta2:"
)

func UpgradeCRFromV1beta1ToV1beta2(config *restclient.Config, namespace string, lhClient *lhclientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+" failed")
	}()

	if err := upgradeVolumes(namespace, lhClient); err != nil {
		return err
	}
	if err := upgradeEngineImages(namespace, lhClient); err != nil {
		return err
	}
	if err := upgradeBackupTargets(namespace, lhClient); err != nil {
		return err
	}
	if err := upgradeNodes(namespace, lhClient); err != nil {
		return err
	}

	logrus.Infof("%v completed", upgradeLogPrefix)
	return nil
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

	_, err = lhClient.LonghornV1beta1().Settings(namespace).Get(context.TODO(), string(types.SettingNameDefaultEngineImage), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("setting %v not found", string(types.SettingNameDefaultEngineImage))
			return true, nil
		}

		return false, errors.Wrap(err, fmt.Sprintf("unable to get setting %v", string(types.SettingNameDefaultEngineImage)))
	}

	// The CRD API version is v1alpha1 if SettingNameCRDAPIVersion is "" and SettingNameDefaultEngineImage is set.
	// Longhorn no longer supports the upgrade from v1alpha1 to v1beta2 directly.
	return false, errors.Wrapf(err, "unable to upgrade from v1alpha1 directly")
}

func upgradeVolumes(namespace string, lhClient *lhclientset.Clientset) error {
	volumes, err := lhClient.LonghornV1beta1().Volumes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to list volumes")
	}

	for _, old := range volumes.Items {
		new := longhorn.Volume{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "fail to copy volume Status.Conditions")
		}
		new.Status.Conditions = conditions

		if err := copier.Copy(&new.Status.KubernetesStatus, &old.Status.KubernetesStatus); err != nil {
			return errors.Wrap(err, "failed to copy volume Status.KubernetesStatus")
		}

		if err := copier.Copy(&new.Status.CloneStatus, &old.Status.CloneStatus); err != nil {
			return errors.Wrap(err, "failed to copy volume Status.CloneStatus")
		}

		_, err = lhClient.LonghornV1beta2().Volumes(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
		}
	}
	logrus.Info("Finished upgrading volumes")
	return nil
}

func upgradeEngineImages(namespace string, lhClient *lhclientset.Clientset) error {
	engineImages, err := lhClient.LonghornV1beta1().EngineImages(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to list engineImages")
	}

	for _, old := range engineImages.Items {
		new := longhorn.EngineImage{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "failed to copy engineImage Status.Conditions")
		}
		new.Status.Conditions = conditions

		if err := copier.Copy(&new.Status.EngineVersionDetails, &old.Status.EngineVersionDetails); err != nil {
			return errors.Wrap(err, "failed to copy engineImage Status.EngineVersionDetails")
		}

		_, err = lhClient.LonghornV1beta2().EngineImages(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
		}
	}
	logrus.Info("Finished upgrading engineImages")
	return nil
}

func upgradeBackupTargets(namespace string, lhClient *lhclientset.Clientset) error {
	backupTargets, err := lhClient.LonghornV1beta1().BackupTargets(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to list backupTargets")
	}

	for _, old := range backupTargets.Items {
		new := longhorn.BackupTarget{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "failed to copy backupTarget Status.Conditions")
		}
		new.Status.Conditions = conditions

		_, err = lhClient.LonghornV1beta2().BackupTargets(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
		}
	}
	logrus.Info("Finished upgrading backupTargets")
	return nil
}

func upgradeNodes(namespace string, lhClient *lhclientset.Clientset) error {
	nodes, err := lhClient.LonghornV1beta1().Nodes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to list nodes")
	}

	for _, old := range nodes.Items {
		new := longhorn.Node{}

		if err := copier.Copy(&new, &old); err != nil {
			return err
		}

		new.Status.Conditions, err = copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "failed to copy node Status.Conditions")
		}

		new.Status.DiskStatus, err = copyDiskStatus(old.Status.DiskStatus)
		if err != nil {
			return errors.Wrap(err, "failed to copy node Status.DiskStatus")
		}

		_, err = lhClient.LonghornV1beta2().Nodes(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", old.Kind, old.Name)
		}
	}
	logrus.Info("Finished upgrading nodes")
	return nil
}

func copyConditions(srcConditions map[string]longhornV1beta1.Condition) ([]longhorn.Condition, error) {
	dstConditions := []longhorn.Condition{}
	for _, src := range srcConditions {
		dst := longhorn.Condition{}
		if err := copier.Copy(&dst, &src); err != nil {
			return nil, err
		}
		dstConditions = append(dstConditions, dst)
	}
	return dstConditions, nil
}

func copyDiskStatus(srcDiskStatus map[string]*longhornV1beta1.DiskStatus) (map[string]*longhorn.DiskStatus, error) {
	dstDiskStatus := make(map[string]*longhorn.DiskStatus)
	for key, src := range srcDiskStatus {
		dst := &longhorn.DiskStatus{}
		if err := copier.Copy(dst, src); err != nil {
			return nil, err
		}

		conditions, err := copyConditions(src.Conditions)
		if err != nil {
			return nil, err
		}
		dst.Conditions = conditions

		dstDiskStatus[key] = dst
	}

	return dstDiskStatus, nil
}
