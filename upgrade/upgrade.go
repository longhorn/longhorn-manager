package upgrade

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"

	"github.com/longhorn/longhorn-manager/upgrade/v1alpha1"
)

func Upgrade(kubeconfigPath string) error {
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

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get clientset")
	}

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "unable to create scheme")
	}

	crdAPIVersion := ""

	crdAPIVersionSetting, err := lhClient.LonghornV1beta1().Settings(namespace).Get(string(types.SettingNameCRDAPIVersion), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// v1alpha1 doesn't have a setting entry for crd-api-version
	} else {
		crdAPIVersion = crdAPIVersionSetting.Value
	}

	if crdAPIVersion != "" &&
		crdAPIVersion != types.CRDAPIVersionV1alpha1 &&
		crdAPIVersion != types.CRDAPIVersionV1beta1 {
		return fmt.Errorf("unrecognized CRD API version %v", crdAPIVersion)
	}

	if crdAPIVersion == types.CurrentCRDAPIVersion {
		// No upgrade is needed
		return nil
	}

	switch crdAPIVersion {
	case "":
		// it can be a new installation or v1alpha1
		isV1alpha1, err := v1alpha1.IsCRDVersionMatch(config, namespace)
		if err != nil {
			return err
		}
		if !isV1alpha1 {
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

	default:
		return fmt.Errorf("don't support upgrade from %v to %v", crdAPIVersion, types.CurrentCRDAPIVersion)
	}

	return nil
}
