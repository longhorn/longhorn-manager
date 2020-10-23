package upgrade

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	envLonghornStorageClassSettingPath = "LONGHORN_STORAGECLASS_SETTING_PATH"
)

func doLonghornStorageClassUpgrade(namespace string, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrap(err, "error while upgrading Longhorn storageclass")
	}()
	sc, err := getLonghornStorageClassSetting()
	if err != nil {
		return err
	}

	existingSC, err := kubeClient.StorageV1().StorageClasses().Get(types.DefaultStorageClassName, metav1.GetOptions{})
	if err != nil {
		logrus.Warn("Cannot get the existing Longhorn storageclass. Will overwrite it with new Longhorn storageclass")
	}

	if storageclassesHaveSameValues(existingSC, sc) {
		logrus.Info("The Longhorn storageclass is already up to date")
		return nil
	}

	err = kubeClient.StorageV1().StorageClasses().Delete(types.DefaultStorageClassName, nil)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		logrus.Warnf("cannot delete the existing Longhorn storageclass. Skip updating Longhorn storageclass: %v", err)
		return nil
	}

	_, err = kubeClient.StorageV1().StorageClasses().Create(sc)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			logrus.Warnf("An up to date Longhorn storageclass already existed.")
			return nil
		}
		return err
	}

	return nil
}

func getLonghornStorageClassSetting() (*storagev1.StorageClass, error) {
	settingPath := os.Getenv(envLonghornStorageClassSettingPath)
	if settingPath == "" {
		return nil, fmt.Errorf("LonghornStorageClassSettingPath is empty")
	}

	data, err := ioutil.ReadFile(settingPath)

	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, _, err := decode(data, nil, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error while decoding YAML object")
	}

	sc, ok := obj.(*storagev1.StorageClass)
	if !ok {
		return nil, fmt.Errorf("%v is not a storageclass yaml", settingPath)
	}
	return sc, nil
}

// storageclassesHaveSameValues compare the values of SC1 and SC2, ignoring their ObjectMeta and TypeMeta
func storageclassesHaveSameValues(sc1, sc2 *storagev1.StorageClass) bool {
	if sc1 == nil || sc2 == nil {
		return false
	}

	sc1 = sc1.DeepCopy()
	sc2 = sc2.DeepCopy()

	sc1.ObjectMeta = metav1.ObjectMeta{}
	sc1.TypeMeta = metav1.TypeMeta{}
	sc2.ObjectMeta = metav1.ObjectMeta{}
	sc2.TypeMeta = metav1.TypeMeta{}

	return reflect.DeepEqual(sc1, sc2)
}
