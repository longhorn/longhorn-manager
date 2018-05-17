package datastore

import (
	"fmt"

	appsv1beta2 "k8s.io/api/apps/v1beta2"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/rancher/longhorn-manager/types"
)

func (s *DataStore) getManagerLabel() map[string]string {
	return map[string]string{
		//TODO standardize key
		//longhornSystemKey: longhornSystemManager,
		"app": "longhorn-manager",
	}
}

func (s *DataStore) getManagerSelector() (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: s.getManagerLabel(),
	})
}

func (s *DataStore) GetManagerNodeIPMap() (map[string]string, error) {
	selector, err := s.getManagerSelector()
	if err != nil {
		return nil, err
	}
	podList, err := s.pLister.Pods(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if len(podList) == 0 {
		return nil, fmt.Errorf("cannot find manager pods by label %v", s.getManagerLabel())
	}
	nodeIPMap := make(map[string]string)
	for _, pod := range podList {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if nodeIPMap[pod.Spec.NodeName] != "" {
			return nil, fmt.Errorf("multiple managers on the node %v", pod.Spec.NodeName)
		}
		nodeIPMap[pod.Spec.NodeName] = pod.Status.PodIP
	}
	return nodeIPMap, nil
}

// ListVolumeCronJobROs returns a map of read-only CronJobs for the volume
func (s *DataStore) ListVolumeCronJobROs(volumeName string) (map[string]*batchv1beta1.CronJob, error) {
	selector, err := getVolumeSelector(volumeName)
	if err != nil {
		return nil, err
	}
	itemMap := map[string]*batchv1beta1.CronJob{}
	list, err := s.cjLister.CronJobs(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	for _, cj := range list {
		itemMap[cj.Name] = cj
	}
	return itemMap, nil
}

func (s *DataStore) CreateVolumeCronJob(volumeName string, cronJob *batchv1beta1.CronJob) (*batchv1beta1.CronJob, error) {
	if err := tagVolumeLabel(volumeName, cronJob); err != nil {
		return nil, err
	}
	return s.kubeClient.BatchV1beta1().CronJobs(s.namespace).Create(cronJob)
}

func (s *DataStore) UpdateVolumeCronJob(volumeName string, cronJob *batchv1beta1.CronJob) (*batchv1beta1.CronJob, error) {
	if err := tagVolumeLabel(volumeName, cronJob); err != nil {
		return nil, err
	}
	return s.kubeClient.BatchV1beta1().CronJobs(s.namespace).Update(cronJob)
}

func (s *DataStore) DeleteCronJob(cronJobName string) error {
	err := s.kubeClient.BatchV1beta1().CronJobs(s.namespace).Delete(cronJobName, &metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func getEngineUpgradeImageSelector() (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: types.GetEngineUpgradeImageLabel(),
	})
}

func (s *DataStore) ListEngineUpgradeImageDaemonSet() (map[string]string, error) {
	selector, err := getEngineUpgradeImageSelector()
	if err != nil {
		return nil, err
	}
	imageDSMap := map[string]string{}
	list, err := s.dsLister.DaemonSets(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}
	for _, ds := range list {
		name := ds.Spec.Template.Spec.Containers[0].Image
		imageDSMap[name] = ds.Name
	}
	return imageDSMap, nil
}

func (s *DataStore) CreateEngineUpgradeImageDaemonSet(ds *appsv1beta2.DaemonSet) error {
	if _, err := s.kubeClient.AppsV1beta2().DaemonSets(s.namespace).Create(ds); err != nil {
		return err
	}
	//TODO wait until daemon set deployed successfully
	return nil
}

func (s *DataStore) DeleteEngineUpgradeImageDaemonSet(name string) error {
	propagation := metav1.DeletePropagationForeground
	err := s.kubeClient.AppsV1beta2().DaemonSets(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
