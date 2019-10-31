package datastore

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Event list is uncached but current lister doesn't field selector, so we have no choice but read from API directly

func (s *DataStore) GetLonghornEventList() (*corev1.EventList, error) {
	return s.kubeClient.CoreV1().Events(s.namespace).List(metav1.ListOptions{FieldSelector: "involvedObject.apiVersion=longhorn.io/v1beta1"})
}

// The following APIs are not cached, should only be used in e.g. the support bundle

func (s *DataStore) GetAllPodsList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Pods(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetAllServicesList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Services(s.namespace).List(metav1.ListOptions{})
}
func (s *DataStore) GetAllDeploymentsList() (runtime.Object, error) {
	return s.kubeClient.AppsV1().Deployments(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetAllDaemonSetsList() (runtime.Object, error) {
	return s.kubeClient.AppsV1().DaemonSets(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetAllStatefulSetsList() (runtime.Object, error) {
	return s.kubeClient.AppsV1().StatefulSets(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetAllJobsList() (runtime.Object, error) {
	return s.kubeClient.BatchV1().Jobs(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetAllCronJobsList() (runtime.Object, error) {
	return s.kubeClient.BatchV1beta1().CronJobs(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetAllNodesList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
}

func (s *DataStore) GetLonghornNamespace() (*corev1.Namespace, error) {
	return s.kubeClient.CoreV1().Namespaces().Get(s.namespace, metav1.GetOptions{})
}

func (s *DataStore) GetAllEventsList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Events(s.namespace).List(metav1.ListOptions{})
}

func (s *DataStore) GetAllConfigMaps() (runtime.Object, error) {
	return s.kubeClient.CoreV1().ConfigMaps(s.namespace).List(metav1.ListOptions{})
}
