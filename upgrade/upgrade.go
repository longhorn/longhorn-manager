package upgrade

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"

	"github.com/longhorn/longhorn-manager/upgrade/v1alpha1"
)

const (
	leaseLockName = "longhorn-manager-upgrade-lock"
)

func Upgrade(kubeconfigPath, currentNodeID string) error {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get clientset")
	}

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "unable to create scheme")
	}

	if err := migrateEngineBinaries(); err != nil {
		return err
	}

	if err := upgrade(currentNodeID, namespace, config, lhClient, kubeClient); err != nil {
		return err
	}

	return nil
}

func upgrade(currentNodeID, namespace string, config *restclient.Config, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseLockName,
			Namespace: namespace,
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: currentNodeID,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   20 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				if err := doAPIVersionUpgrade(namespace, config, lhClient); err != nil {
					logrus.Errorf("cannot finish APIVersion upgrade: %v", err)
					return
				}
				if err := doInstanceManagerUpgrade(namespace, lhClient); err != nil {
					logrus.Errorf("cannot finish InstanceManager upgrade: %v", err)
				}
				// we only need to run upgrade once
				cancel()
			},
			OnStoppedLeading: func() {
				logrus.Infof("Upgrade leader lost: %s", currentNodeID)
			},
			OnNewLeader: func(identity string) {
				if identity == currentNodeID {
					return
				}
				logrus.Infof("New upgrade leader elected: %s", identity)
			},
		},
	})

	return nil
}

func doAPIVersionUpgrade(namespace string, config *restclient.Config, lhClient *lhclientset.Clientset) error {
	defer func() {
		logrus.Info("Upgrade APIVersion completed")
	}()
	logrus.Info("Start the APIVersion upgrade process")

	crdAPIVersion := ""

	crdAPIVersionSetting, err := lhClient.LonghornV1beta1().Settings(namespace).Get(string(types.SettingNameCRDAPIVersion), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// v1alpha1 doesn't have a setting entry for crd-api-version, need to create
		crdAPIVersionSetting = &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(types.SettingNameCRDAPIVersion),
			},
			Setting: types.Setting{
				Value: "",
			},
		}
		crdAPIVersionSetting, err = lhClient.LonghornV1beta1().Settings(namespace).Create(crdAPIVersionSetting)
		if err != nil {
			return errors.Wrap(err, "cannot create CRDAPIVersionSetting")
		}
	} else {
		crdAPIVersion = crdAPIVersionSetting.Value
	}

	if crdAPIVersion != "" &&
		crdAPIVersion != types.CRDAPIVersionV1alpha1 &&
		crdAPIVersion != types.CRDAPIVersionV1beta1 {
		return fmt.Errorf("unrecognized CRD API version %v", crdAPIVersion)
	}
	if crdAPIVersion == types.CurrentCRDAPIVersion {
		logrus.Info("No upgrade is needed")
		return nil
	}

	switch crdAPIVersion {
	case "":
		// it can be a new installation or v1alpha1
		isV1alpha1, err := v1alpha1.IsCRDVersionMatch(config, namespace)
		if err != nil {
			logrus.Warnf("Cannot verify current CRD version, assume it's not v1alpha1: %v", err)
		}
		if !isV1alpha1 {
			crdAPIVersionSetting.Value = types.CurrentCRDAPIVersion
			if _, err := lhClient.LonghornV1beta1().Settings(namespace).Update(crdAPIVersionSetting); err != nil {
				return errors.Wrapf(err, "cannot finish CRD API upgrade by setting the CRDAPIVersionSetting to %v", types.CurrentCRDAPIVersion)
			}
			logrus.Infof("Initialized CRD API Version to %v", types.CurrentCRDAPIVersion)
			break
		}
		fallthrough
	case types.CRDAPIVersionV1alpha1:
		if types.CurrentCRDAPIVersion != types.CRDAPIVersionV1beta1 {
			return fmt.Errorf("cannot upgrade from %v to %v directly", types.CRDAPIVersionV1alpha1, types.CurrentCRDAPIVersion)
		}
		if err := v1alpha1.UpgradeFromV1alpha1ToV1beta1(config, namespace, lhClient); err != nil {
			return err
		}
		crdAPIVersionSetting.Value = types.CRDAPIVersionV1beta1
		if _, err := lhClient.LonghornV1beta1().Settings(namespace).Update(crdAPIVersionSetting); err != nil {
			return errors.Wrapf(err, "cannot finish CRD API upgrade by setting the CRDAPIVersionSetting to %v", types.CurrentCRDAPIVersion)
		}
		logrus.Infof("CRD has been upgraded to %v", crdAPIVersionSetting.Value)
	default:
		return fmt.Errorf("don't support upgrade from %v to %v", crdAPIVersion, types.CurrentCRDAPIVersion)
	}

	return nil
}

func doInstanceManagerUpgrade(namespace string, lhClient *lhclientset.Clientset) error {
	defer func() {
		logrus.Info("Upgrade instance managers completed")
	}()
	logrus.Info("Start the instance managers upgrade process")

	nodeMap := map[string]longhorn.Node{}
	nodeList, err := lhClient.LonghornV1beta1().Nodes(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all nodes during the instance managers upgrade")
	}
	for _, node := range nodeList.Items {
		nodeMap[node.Name] = node
	}

	imList, err := lhClient.LonghornV1beta1().InstanceManagers(namespace).List(metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to list all existing instance managers during the instance managers upgrade")
	}
	for _, im := range imList.Items {
		if im.Spec.Image != "" {
			continue
		}
		im := &im
		if types.ValidateEngineImageChecksumName(im.Spec.EngineImage) {
			ei, err := lhClient.LonghornV1beta1().EngineImages(namespace).Get(im.Spec.EngineImage, metav1.GetOptions{})
			if err != nil {
				return errors.Wrapf(err, "failed to find out the related engine image %v during the instance managers upgrade", im.Spec.EngineImage)
			}
			im.Spec.EngineImage = ei.Spec.Image
		}
		im.Spec.Image = im.Spec.EngineImage
		node, exist := nodeMap[im.Spec.NodeID]
		if !exist {
			return fmt.Errorf("cannot to find node %v for instance manager %v during the instance manager upgrade", im.Spec.NodeID, im.Name)
		}
		metadata, err := meta.Accessor(im)
		if err != nil {
			return err
		}
		metadata.SetOwnerReferences(datastore.GetOwnerReferencesForNode(&node))
		metadata.SetLabels(types.GetInstanceManagerLabels(im.Spec.NodeID, im.Spec.Image, im.Spec.Type))
		if im, err = lhClient.LonghornV1beta1().InstanceManagers(namespace).Update(im); err != nil {
			return errors.Wrapf(err, "failed to update the spec for instance manager %v during the instance managers upgrade", im.Name)
		}

		im.Status.APIMinVersion = engineapi.IncompatibleInstanceManagerAPIVersion
		im.Status.APIVersion = engineapi.IncompatibleInstanceManagerAPIVersion
		if _, err = lhClient.LonghornV1beta1().InstanceManagers(namespace).UpdateStatus(im); err != nil {
			return errors.Wrapf(err, "failed to update the version status for instance manager %v during the instance managers upgrade", im.Name)
		}
	}

	return nil
}

func migrateEngineBinaries() error {
	return util.CopyHostDirectoryContent(types.DeprecatedEngineBinaryDirectoryOnHost, types.EngineBinaryDirectoryOnHost)
}
