package v1alpha1

import (
	"fmt"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	restclient "k8s.io/client-go/rest"

	longhorn_v1alpha1 "github.com/longhorn/longhorn-manager/upgrade/v1alpha1/k8s/pkg/apis/longhorn/v1alpha1"
	lhclientset_v1alpha1 "github.com/longhorn/longhorn-manager/upgrade/v1alpha1/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/upgrade/v1alpha1/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const upgradeLogPrefix = "upgrade from v1alpha1 to v1beta1"

func IsCRDVersionMatch(config *restclient.Config, namespace string) (bool, error) {
	lhClientV1alpha1, err := lhclientset_v1alpha1.NewForConfig(config)
	if err != nil {
		return false, errors.Wrap(err, "unable to get clientset for v1alpha1")
	}

	scheme := runtime.NewScheme()
	if err := longhorn_v1alpha1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return false, errors.Wrap(err, "unable to create scheme for v1alpha1")
	}

	if _, err := lhClientV1alpha1.LonghornV1alpha1().Settings(namespace).Get(string(types.SettingNameDefaultEngineImage), metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			// cannot find the setting,
			logrus.Infof("Longhorn CRD API v1alpha1 not found")
			return false, nil
		}
		return false, errors.Wrap(err, "unable to verify if version matches v1alpha1")
	}
	logrus.Infof("Detected Longhorn CRD API v1alpha1")
	return true, nil
}

func UpgradeFromV1alpha1ToV1beta1(config *restclient.Config, namespace string, lhClient *lhclientset.Clientset) error {
	lhClientV1alpha1, err := lhclientset_v1alpha1.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get clientset for v1alpha1")
	}

	scheme := runtime.NewScheme()
	if err := longhorn_v1alpha1.SchemeBuilder.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "unable to create scheme for v1alpha1")
	}

	volumes, err := lhClientV1alpha1.LonghornV1alpha1().Volumes(namespace).List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "upgrade: v1alpha1: unable to list volumes")
	}

	for _, old := range volumes.Items {
		v := &longhorn.Volume{}

		copyObjectMetaFromV1alpha1(&v.ObjectMeta, &old.ObjectMeta)
		copier.Copy(&v.Spec, &old.Spec)
		v, err = lhClient.LonghornV1beta1().Volumes(namespace).Create(v)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: creating %v %v v1beta1 but it's already exist, skipping creation", upgradeLogPrefix, old.Kind, old.Name)
			v, err = lhClient.LonghornV1beta1().Volumes(namespace).Get(old.Name, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, "cannot get %v %v for upgrading v1alpha1 to v1beta1", old.Kind, old.Name)
			}
		}

		copier.Copy(&v.Status, &old.Status)
		v.Status.OwnerID = old.Spec.OwnerID
		// The volume cannot be in the intermediate state, e.g. restoring
		v.Status.CurrentNodeID = old.Spec.NodeID
		v.Status.PendingNodeID = old.Spec.PendingNodeID
		v.Status.FrontendDisabled = old.Spec.DisableFrontend
		v.Status.InitialRestorationRequired = old.Spec.InitialRestorationRequired
		// The volume must complete restoration before upgrade
		v.Status.RestoreInitiated = true

		v, err = lhClient.LonghornV1beta1().Volumes(namespace).UpdateStatus(v)
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta1 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
		}
	}

	engines, err := lhClientV1alpha1.LonghornV1alpha1().Engines(namespace).List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "upgrade: v1alpha1: unable to list engines")
	}
	for _, old := range engines.Items {
		e := &longhorn.Engine{}

		copyObjectMetaFromV1alpha1(&e.ObjectMeta, &old.ObjectMeta)
		copier.Copy(&e.Spec, &old.Spec)
		e, err = lhClient.LonghornV1beta1().Engines(namespace).Create(e)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: creating %v %v v1beta1 but it's already exist, skipping creation", upgradeLogPrefix, old.Kind, old.Name)
			e, err = lhClient.LonghornV1beta1().Engines(namespace).Get(old.Name, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, "cannot get %v %v for upgrading v1alpha1 to v1beta1", old.Kind, old.Name)
			}
		}

		copier.Copy(&e.Status, &old.Status)
		e.Status.OwnerID = old.Spec.OwnerID
		e.Status.LogFetched = old.Spec.LogRequested
		e.Status.CurrentReplicaAddressMap = old.Spec.ReplicaAddressMap
		e, err = lhClient.LonghornV1beta1().Engines(namespace).UpdateStatus(e)
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta1 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
		}
	}

	replicas, err := lhClientV1alpha1.LonghornV1alpha1().Replicas(namespace).List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "upgrade: v1alpha1: unable to list replicas")
	}
	for _, old := range replicas.Items {
		new := &longhorn.Replica{}

		copyObjectMetaFromV1alpha1(&new.ObjectMeta, &old.ObjectMeta)
		copier.Copy(&new.Spec, &old.Spec)
		new, err = lhClient.LonghornV1beta1().Replicas(namespace).Create(new)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: creating %v %v v1beta1 but it's already exist, skipping creation", upgradeLogPrefix, old.Kind, old.Name)
			new, err = lhClient.LonghornV1beta1().Replicas(namespace).Get(old.Name, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, "cannot get %v %v for upgrading v1alpha1 to v1beta1", old.Kind, old.Name)
			}
		}

		copier.Copy(&new.Status, &old.Status)
		new.Status.OwnerID = old.Spec.OwnerID
		new.Status.LogFetched = old.Spec.LogRequested
		new, err = lhClient.LonghornV1beta1().Replicas(namespace).UpdateStatus(new)
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta1 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
		}
	}

	engineImages, err := lhClientV1alpha1.LonghornV1alpha1().EngineImages(namespace).List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "upgrade: v1alpha1: unable to list engine images")
	}
	for _, old := range engineImages.Items {
		new := &longhorn.EngineImage{}

		copyObjectMetaFromV1alpha1(&new.ObjectMeta, &old.ObjectMeta)
		copier.Copy(&new.Spec, &old.Spec)
		new, err = lhClient.LonghornV1beta1().EngineImages(namespace).Create(new)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: creating %v %v v1beta1 but it's already exist, skipping creation", upgradeLogPrefix, old.Kind, old.Name)
			new, err = lhClient.LonghornV1beta1().EngineImages(namespace).Get(old.Name, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, "cannot get %v %v for upgrading v1alpha1 to v1beta1", old.Kind, old.Name)
			}
		}

		copier.Copy(&new.Status, &old.Status)
		new.Status.OwnerID = old.Spec.OwnerID
		new, err = lhClient.LonghornV1beta1().EngineImages(namespace).UpdateStatus(new)
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta1 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
		}
	}

	instanceManagers, err := lhClientV1alpha1.LonghornV1alpha1().InstanceManagers(namespace).List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "upgrade: v1alpha1: unable to list engine images")
	}
	for _, old := range instanceManagers.Items {
		new := &longhorn.InstanceManager{}

		copyObjectMetaFromV1alpha1(&new.ObjectMeta, &old.ObjectMeta)
		copier.Copy(&new.Spec, &old.Spec)
		new, err = lhClient.LonghornV1beta1().InstanceManagers(namespace).Create(new)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: creating %v %v v1beta1 but it's already exist, skipping creation", upgradeLogPrefix, old.Kind, old.Name)
			new, err = lhClient.LonghornV1beta1().InstanceManagers(namespace).Get(old.Name, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, "cannot get %v %v for upgrading v1alpha1 to v1beta1", old.Kind, old.Name)
			}
		}

		copier.Copy(&new.Status, &old.Status)
		new.Status.OwnerID = old.Spec.OwnerID
		new, err = lhClient.LonghornV1beta1().InstanceManagers(namespace).UpdateStatus(new)
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta1 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
		}
	}

	nodes, err := lhClientV1alpha1.LonghornV1alpha1().Nodes(namespace).List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "upgrade: v1alpha1: unable to list engine images")
	}
	for _, old := range nodes.Items {
		new := &longhorn.Node{}

		copyObjectMetaFromV1alpha1(&new.ObjectMeta, &old.ObjectMeta)
		copier.Copy(&new.Spec, &old.Spec)
		new, err = lhClient.LonghornV1beta1().Nodes(namespace).Create(new)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: creating %v %v v1beta1 but it's already exist, skipping creation", upgradeLogPrefix, old.Kind, old.Name)
			new, err = lhClient.LonghornV1beta1().Nodes(namespace).Get(old.Name, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, "cannot get %v %v for upgrading v1alpha1 to v1beta1", old.Kind, old.Name)
			}
		}

		copier.Copy(&new.Status, &old.Status)
		new, err = lhClient.LonghornV1beta1().Nodes(namespace).UpdateStatus(new)
		if err != nil {
			if !apierrors.IsConflict(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v status %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: update status for %v %v v1beta1 result in conflict, skipping", upgradeLogPrefix, old.Kind, old.Name)
		}
	}

	settings, err := lhClientV1alpha1.LonghornV1alpha1().Settings(namespace).List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "upgrade: v1alpha1: unable to list engine images")
	}
	for _, old := range settings.Items {
		new := &longhorn.Setting{}

		copyObjectMetaFromV1alpha1(&new.ObjectMeta, &old.ObjectMeta)
		new.Value = old.Value
		new, err = lhClient.LonghornV1beta1().Settings(namespace).Create(new)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to convert v1alpha1 to v1beta1 for %v %v", old.Kind, old.Name)
			}
			logrus.Warnf("%v: creating %v %v v1beta1 but it's already exist, skipping creation", upgradeLogPrefix, old.Kind, old.Name)
		}
	}

	return fmt.Errorf("upgrade wasn't completed")
}

func copyObjectMetaFromV1alpha1(to, from *metav1.ObjectMeta) {
	to.Name = from.Name
	to.Labels = from.Labels
	to.Annotations = from.Annotations
	to.OwnerReferences = from.OwnerReferences
	to.Finalizers = []string{}
	for _, f := range from.Finalizers {
		if f == longhorn_v1alpha1.SchemeGroupVersion.Group {
			to.Finalizers = append(to.Finalizers, longhorn.SchemeGroupVersion.Group)
		} else {
			to.Finalizers = append(to.Finalizers, f)
		}
	}
}
