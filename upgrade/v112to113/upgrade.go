package v112to113

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.1.2 to v1.1.3: "

	longhornFinalizerKey = "longhorn.io"
)

func UpgradeCRs(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"UpgradeCRs failed")
	}()
	if err := upgradeInstanceManagers(namespace, lhClient, kubeClient); err != nil {
		return err
	}

	return nil
}

func upgradeInstanceManagers(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade instance managers failed")
	}()

	// The pod update should happen before IM CR update. Hence we should not abstract this part as a function call in `doPodsUpgrade`.
	blockOwnerDeletion := true
	imPodList, err := upgradeutil.ListIMPods(namespace, kubeClient)
	if err != nil {
		return err
	}
	for _, imPod := range imPodList {
		if imPod.OwnerReferences == nil || len(imPod.OwnerReferences) == 0 {
			im, err := lhClient.LonghornV1beta1().InstanceManagers(namespace).Get(imPod.Name, metav1.GetOptions{})
			if err != nil {
				logrus.Errorf("cannot find the instance manager CR for the instance manager pod %v that has no owner reference during v1.2.0 upgrade: %v", imPod.Name, err)
				continue
			}
			imPod.OwnerReferences = datastore.GetOwnerReferencesForInstanceManager(im)
			if _, err = kubeClient.CoreV1().Pods(namespace).Update(&imPod); err != nil {
				return err
			}
			continue
		}
		if imPod.OwnerReferences[0].BlockOwnerDeletion == nil || !*imPod.OwnerReferences[0].BlockOwnerDeletion {
			imPod.OwnerReferences[0].BlockOwnerDeletion = &blockOwnerDeletion
			if _, err = kubeClient.CoreV1().Pods(namespace).Update(&imPod); err != nil {
				return err
			}
		}
	}

	imList, err := lhClient.LonghornV1beta1().InstanceManagers(namespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, im := range imList.Items {
		if !util.FinalizerExists(longhornFinalizerKey, &im) {
			// finalizer already removed
			return nil
		}
		if err := util.RemoveFinalizer(longhornFinalizerKey, &im); err != nil {
			return err
		}
		if _, err := lhClient.LonghornV1beta1().InstanceManagers(namespace).Update(&im); err != nil {
			return err
		}
	}

	return nil
}
