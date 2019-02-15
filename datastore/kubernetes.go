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

const (
	LonghornManagerDaemonSetName = "longhorn-manager"
)

func (s *DataStore) getManagerLabel() map[string]string {
	return map[string]string{
		//TODO standardize key
		//longhornSystemKey: longhornSystemManager,
		"app": LonghornManagerDaemonSetName,
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
	propagation := metav1.DeletePropagationBackground
	err := s.kubeClient.BatchV1beta1().CronJobs(s.namespace).Delete(cronJobName,
		&metav1.DeleteOptions{
			PropagationPolicy: &propagation,
		})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func getEngineImageSelector() (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: types.GetEngineImageLabel(),
	})
}

func (s *DataStore) CreateEngineImageDaemonSet(ds *appsv1beta2.DaemonSet) error {
	if ds.Labels == nil {
		ds.Labels = map[string]string{}
	}
	for k, v := range types.GetEngineImageLabel() {
		ds.Labels[k] = v
	}
	if _, err := s.kubeClient.AppsV1beta2().DaemonSets(s.namespace).Create(ds); err != nil {
		return err
	}
	return nil
}

func (s *DataStore) GetEngineImageDaemonSet(name string) (*appsv1beta2.DaemonSet, error) {
	resultRO, err := s.dsLister.DaemonSets(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetDaemonSet(name string) (*appsv1beta2.DaemonSet, error) {
	return s.dsLister.DaemonSets(s.namespace).Get(name)
}

func (s *DataStore) DeleteDaemonSet(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.kubeClient.AppsV1beta2().DaemonSets(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

func (s *DataStore) GetManagerDaemonSet() (*appsv1beta2.DaemonSet, error) {
	return s.GetDaemonSet(LonghornManagerDaemonSetName)
}

func (s *DataStore) DeleteManagerDaemonSet() error {
	return s.DeleteDaemonSet(LonghornManagerDaemonSetName)
}

func (s *DataStore) ListManagerPods() ([]*corev1.Pod, error) {
	selector, err := s.getManagerSelector()
	if err != nil {
		return nil, err
	}
	podList, err := s.pLister.Pods(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}

	pList := []*corev1.Pod{}
	for _, item := range podList {
		pList = append(pList, item.DeepCopy())
	}

	return pList, nil
}

func (s *DataStore) GetLonghornEventList() (*corev1.EventList, error) {
	return s.kubeClient.CoreV1().Events(s.namespace).List(metav1.ListOptions{FieldSelector: "involvedObject.apiVersion=longhorn.rancher.io/v1alpha1"})
}

func (s *DataStore) GetAllEventsList() (*corev1.EventList, error) {
	return s.kubeClient.CoreV1().Events(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetKubernetesNode(name string) (*corev1.Node, error) {
	return s.kubeClient.CoreV1().Nodes().Get(name, metav1.GetOptions{})
}
