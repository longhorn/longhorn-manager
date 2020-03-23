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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"

	"github.com/longhorn/longhorn-manager/upgrade/v070to080"
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

	if err := upgradeLocalNode(); err != nil {
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
				if err := doCRDUpgrade(namespace, lhClient); err != nil {
					logrus.Errorf("cannot finish CRD upgrade: %v", err)
					return
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

func doCRDUpgrade(namespace string, lhClient *lhclientset.Clientset) error {
	defer func() {
		logrus.Info("Upgrade CRD completed")
	}()
	logrus.Info("Start CRD upgrade process")
	if err := v070to080.UpgradeCRDs(namespace, lhClient); err != nil {
		return err
	}
	return nil
}

func upgradeLocalNode() error {
	defer func() {
		logrus.Info("Upgrade local node completed")
	}()
	logrus.Info("Start local node upgrade process")
	if err := v070to080.UpgradeLocalNode(); err != nil {
		return err
	}
	return nil
}
