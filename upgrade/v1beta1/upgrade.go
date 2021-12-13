package v1beta1

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
	if err := upgradeRecurringJobs(namespace, lhClient); err != nil {
		return err
	}
	if err := upgradeBackups(namespace, lhClient); err != nil {
		return err
	}
	if err := upgradeBackingImageDataSources(namespace, lhClient); err != nil {
		return err
	}
	if err := upgradeBackingImageManagers(namespace, lhClient); err != nil {
		return err
	}
	if err := upgradeBackingImages(namespace, lhClient); err != nil {
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
	if err := fixupVolumes(namespace, lhClient); err != nil {
		return errors.Wrapf(err, "unable to fix up volumes")
	}

	volumes, err := lhClient.LonghornV1beta1().Volumes(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return errors.Wrapf(err, "unable to list volumes")
	}

	for _, old := range volumes.Items {
		new := longhorn.Volume{}

		if err := copier.Copy(&new, &old); err != nil {
			return errors.Wrap(err, "fail to copy volume")
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

		obj, err := lhClient.LonghornV1beta2().Volumes(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", new.Kind, new.Name)
		}

		if err := tagCRLabelCRDAPIversion(obj); err != nil {
			return errors.Wrapf(err, "failed to add label to %v %v", obj.Kind, obj.Name)
		}
		if _, err := lhClient.LonghornV1beta2().Volumes(namespace).Update(context.TODO(), obj, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(err, "failed to update for %v %v", obj.Kind, obj.Name)
		}
	}
	logrus.Info("Finished upgrading volumes")
	return nil
}

func fixupVolumes(namespace string, lhClient *lhclientset.Clientset) error {
	volumes, err := lhClient.LonghornV1beta1().Volumes(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return err
	}

	for _, obj := range volumes.Items {
		existing := obj.DeepCopy()

		if obj.Spec.DiskSelector == nil {
			obj.Spec.DiskSelector = []string{}
		}
		if obj.Spec.NodeSelector == nil {
			obj.Spec.NodeSelector = []string{}
		}
		if obj.Spec.RecurringJobs == nil {
			obj.Spec.RecurringJobs = make([]longhornV1beta1.VolumeRecurringJobSpec, 0)
		}
		for i, src := range obj.Spec.RecurringJobs {
			dst := longhornV1beta1.VolumeRecurringJobSpec{}
			if err := copier.Copy(&dst, &src); err != nil {
				return err
			}
			if dst.Groups == nil {
				dst.Groups = []string{}
			}
			if dst.Labels == nil {
				dst.Labels = make(map[string]string, 0)
			}
			obj.Spec.RecurringJobs[i] = dst
		}
		if reflect.DeepEqual(&obj, existing) {
			continue
		}
		if _, err = lhClient.LonghornV1beta1().Volumes(namespace).Update(context.TODO(), &obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func upgradeEngineImages(namespace string, lhClient *lhclientset.Clientset) error {
	engineImages, err := lhClient.LonghornV1beta1().EngineImages(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return errors.Wrapf(err, "unable to list engineImages")
	}

	for _, old := range engineImages.Items {
		new := longhorn.EngineImage{}

		if err := copier.Copy(&new, &old); err != nil {
			return errors.Wrap(err, "fail to copy engineImage")
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "failed to copy engineImage Status.Conditions")
		}
		new.Status.Conditions = conditions

		if err := copier.Copy(&new.Status.EngineVersionDetails, &old.Status.EngineVersionDetails); err != nil {
			return errors.Wrap(err, "failed to copy engineImage Status.EngineVersionDetails")
		}

		obj, err := lhClient.LonghornV1beta2().EngineImages(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", new.Kind, new.Name)
		}

		if err := tagCRLabelCRDAPIversion(obj); err != nil {
			return errors.Wrapf(err, "failed to add label to %v %v", obj.Kind, obj.Name)
		}
		if _, err := lhClient.LonghornV1beta2().EngineImages(namespace).Update(context.TODO(), obj, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(err, "failed to update for %v %v", obj.Kind, obj.Name)
		}
	}
	logrus.Info("Finished upgrading engineImages")
	return nil
}

func upgradeBackupTargets(namespace string, lhClient *lhclientset.Clientset) error {
	backupTargets, err := lhClient.LonghornV1beta1().BackupTargets(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return errors.Wrapf(err, "unable to list backupTargets")
	}

	for _, old := range backupTargets.Items {
		new := longhorn.BackupTarget{}

		if err := copier.Copy(&new, &old); err != nil {
			return errors.Wrap(err, "fail to copy backupTarget")
		}

		conditions, err := copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "failed to copy backupTarget Status.Conditions")
		}
		new.Status.Conditions = conditions

		obj, err := lhClient.LonghornV1beta2().BackupTargets(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", new.Kind, new.Name)
		}

		if err := tagCRLabelCRDAPIversion(obj); err != nil {
			return errors.Wrapf(err, "failed to add label to %v %v", obj.Kind, obj.Name)
		}
		if _, err := lhClient.LonghornV1beta2().BackupTargets(namespace).Update(context.TODO(), obj, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(err, "failed to update for %v %v", obj.Kind, obj.Name)
		}
	}
	logrus.Info("Finished upgrading backupTargets")
	return nil
}

func upgradeNodes(namespace string, lhClient *lhclientset.Clientset) error {
	if err := fixupNodes(namespace, lhClient); err != nil {
		return errors.Wrapf(err, "unable to fix up nodes")
	}

	nodes, err := lhClient.LonghornV1beta1().Nodes(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return errors.Wrapf(err, "unable to list nodes")
	}

	for _, old := range nodes.Items {
		new := longhorn.Node{}

		if err := copier.Copy(&new, &old); err != nil {
			return errors.Wrap(err, "fail to copy node")
		}

		new.Status.Conditions, err = copyConditions(old.Status.Conditions)
		if err != nil {
			return errors.Wrap(err, "failed to copy node Status.Conditions")
		}

		new.Status.DiskStatus, err = copyDiskStatus(old.Status.DiskStatus)
		if err != nil {
			return errors.Wrap(err, "failed to copy node Status.DiskStatus")
		}

		obj, err := lhClient.LonghornV1beta2().Nodes(namespace).UpdateStatus(context.TODO(), &new, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update for %v status %v", new.Kind, new.Name)
		}

		if err := tagCRLabelCRDAPIversion(obj); err != nil {
			return errors.Wrapf(err, "failed to add label to %v %v", obj.Kind, obj.Name)
		}

		if _, err := lhClient.LonghornV1beta2().Nodes(namespace).Update(context.TODO(), obj, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(err, "failed to update for %v %v", obj.Kind, obj.Name)
		}
	}
	logrus.Info("Finished upgrading nodes")
	return nil
}

func fixupNodes(namespace string, lhClient *lhclientset.Clientset) error {
	nodes, err := lhClient.LonghornV1beta1().Nodes(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return err
	}

	for _, obj := range nodes.Items {
		existing := obj.DeepCopy()

		if obj.Spec.Disks == nil {
			obj.Spec.Disks = make(map[string]longhornV1beta1.DiskSpec, 0)
		}
		for key, src := range obj.Spec.Disks {
			if src.Tags == nil {
				dst := longhornV1beta1.DiskSpec{}

				if err := copier.Copy(&dst, &src); err != nil {
					return err
				}
				dst.Tags = []string{}

				obj.Spec.Disks[key] = dst
			}
		}
		if obj.Spec.Tags == nil {
			obj.Spec.Tags = []string{}
		}
		if reflect.DeepEqual(&obj, existing) {
			continue
		}
		if _, err = lhClient.LonghornV1beta1().Nodes(namespace).Update(context.TODO(), &obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func upgradeRecurringJobs(namespace string, lhClient *lhclientset.Clientset) error {
	if err := fixupRecurringJobs(namespace, lhClient); err != nil {
		return errors.Wrapf(err, "unable to fix up recurringJobs")
	}
	return nil
}

func fixupRecurringJobs(namespace string, lhClient *lhclientset.Clientset) error {
	recurringJobs, err := lhClient.LonghornV1beta1().RecurringJobs(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return err
	}

	for _, obj := range recurringJobs.Items {
		existing := obj.DeepCopy()

		if obj.Spec.Groups == nil {
			obj.Spec.Groups = []string{}
		}
		if obj.Spec.Labels == nil {
			obj.Spec.Labels = make(map[string]string, 0)
		}
		if reflect.DeepEqual(&obj, existing) {
			continue
		}
		if _, err = lhClient.LonghornV1beta1().RecurringJobs(namespace).Update(context.TODO(), &obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func upgradeBackups(namespace string, lhClient *lhclientset.Clientset) error {
	if err := fixupBackups(namespace, lhClient); err != nil {
		return errors.Wrapf(err, "unable to fix up backups")
	}
	return nil
}

func fixupBackups(namespace string, lhClient *lhclientset.Clientset) error {
	backups, err := lhClient.LonghornV1beta1().Backups(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return err
	}

	for _, obj := range backups.Items {
		existing := obj.DeepCopy()

		if obj.Spec.Labels == nil {
			obj.Spec.Labels = make(map[string]string, 0)
		}
		if reflect.DeepEqual(&obj, existing) {
			continue
		}
		if _, err = lhClient.LonghornV1beta1().Backups(namespace).Update(context.TODO(), &obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func upgradeBackingImageDataSources(namespace string, lhClient *lhclientset.Clientset) error {
	if err := fixupBackingImageDataSources(namespace, lhClient); err != nil {
		return errors.Wrapf(err, "unable to fix up backingImageDataSources")
	}
	return nil
}

func fixupBackingImageDataSources(namespace string, lhClient *lhclientset.Clientset) error {
	backingImageDataSources, err := lhClient.LonghornV1beta1().BackingImageDataSources(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return err
	}

	for _, obj := range backingImageDataSources.Items {
		existing := obj.DeepCopy()

		if obj.Spec.Parameters == nil {
			obj.Spec.Parameters = make(map[string]string, 0)
		}
		if reflect.DeepEqual(&obj, existing) {
			continue
		}
		if _, err = lhClient.LonghornV1beta1().BackingImageDataSources(namespace).Update(context.TODO(), &obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func upgradeBackingImageManagers(namespace string, lhClient *lhclientset.Clientset) error {
	if err := fixupBackingImageManagers(namespace, lhClient); err != nil {
		return errors.Wrapf(err, "unable to fix up backingImageManagers")
	}
	return nil
}

func fixupBackingImageManagers(namespace string, lhClient *lhclientset.Clientset) error {
	backingImageManagers, err := lhClient.LonghornV1beta1().BackingImageManagers(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return err
	}

	for _, obj := range backingImageManagers.Items {
		existing := obj.DeepCopy()

		if obj.Spec.BackingImages == nil {
			obj.Spec.BackingImages = make(map[string]string, 0)
		}
		if reflect.DeepEqual(&obj, existing) {
			continue
		}
		if _, err = lhClient.LonghornV1beta1().BackingImageManagers(namespace).Update(context.TODO(), &obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func upgradeBackingImages(namespace string, lhClient *lhclientset.Clientset) error {
	if err := fixupBackingImages(namespace, lhClient); err != nil {
		return errors.Wrapf(err, "unable to fix up backingImages")
	}
	return nil
}

func fixupBackingImages(namespace string, lhClient *lhclientset.Clientset) error {
	backingImages, err := lhClient.LonghornV1beta1().BackingImages(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getLabelSelectorNotEqualV1beta2APIVersion(),
	})
	if err != nil {
		return err
	}

	for _, obj := range backingImages.Items {
		existing := obj.DeepCopy()

		if obj.Spec.Disks == nil {
			obj.Spec.Disks = make(map[string]string, 0)
		}
		if obj.Spec.SourceParameters == nil {
			obj.Spec.SourceParameters = make(map[string]string, 0)
		}
		if reflect.DeepEqual(&obj, existing) {
			continue
		}
		if _, err = lhClient.LonghornV1beta1().BackingImages(namespace).Update(context.TODO(), &obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func tagCRLabelCRDAPIversion(obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labels[types.GetLonghornLabelCRDAPIVersionKey()] = strings.Split(types.CRDAPIVersionV1beta2, "/")[1]
	metadata.SetLabels(labels)

	return nil
}

func getLabelSelectorNotEqualV1beta2APIVersion() string {
	return types.GetLonghornLabelCRDAPIVersionKey() + "!=" + strings.Split(types.CRDAPIVersionV1beta2, "/")[1]
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
