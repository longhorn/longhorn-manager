package datastore

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// GetLonghornEventList returns a list of longhorn events for the given namespace
// Event list is uncached but current lister doesn't field selector, so we have no choice but read from API directly
func (s *DataStore) GetLonghornEventList() (*corev1.EventList, error) {
	return s.kubeClient.CoreV1().Events(s.namespace).List(metav1.ListOptions{FieldSelector: "involvedObject.apiVersion=longhorn.io/v1beta1"})
}

// The following APIs are not cached, should only be used in e.g. the support bundle

// GetAllPodsList returns a list of pods for the given namespace
func (s *DataStore) GetAllPodsList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Pods(s.namespace).List(metav1.ListOptions{})
}

// GetAllServicesList returns a list of services for the given namespace
func (s *DataStore) GetAllServicesList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Services(s.namespace).List(metav1.ListOptions{})
}

// GetAllDeploymentsList returns a list of deployments for the given namespace
func (s *DataStore) GetAllDeploymentsList() (runtime.Object, error) {
	return s.kubeClient.AppsV1().Deployments(s.namespace).List(metav1.ListOptions{})
}

// GetAllDaemonSetsList returns a list of daemonsets for the given namespace
func (s *DataStore) GetAllDaemonSetsList() (runtime.Object, error) {
	return s.kubeClient.AppsV1().DaemonSets(s.namespace).List(metav1.ListOptions{})
}

// GetAllStatefulSetsList returns a list of statefulsets for the given namespace
func (s *DataStore) GetAllStatefulSetsList() (runtime.Object, error) {
	return s.kubeClient.AppsV1().StatefulSets(s.namespace).List(metav1.ListOptions{})
}

// GetAllJobsList returns a list of jobs for the given namespace
func (s *DataStore) GetAllJobsList() (runtime.Object, error) {
	return s.kubeClient.BatchV1().Jobs(s.namespace).List(metav1.ListOptions{})
}

// GetAllCronJobsList returns a list of cronjobs for the given namespace
func (s *DataStore) GetAllCronJobsList() (runtime.Object, error) {
	return s.kubeClient.BatchV1beta1().CronJobs(s.namespace).List(metav1.ListOptions{})
}

// GetAllNodesList returns a list of nodes for the given namespace
func (s *DataStore) GetAllNodesList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
}

// GetLonghornNamespace returns an namespace object for the given namespace name
func (s *DataStore) GetLonghornNamespace() (*corev1.Namespace, error) {
	return s.kubeClient.CoreV1().Namespaces().Get(s.namespace, metav1.GetOptions{})
}

// GetAllEventsList returns a list of events for the given namespace
func (s *DataStore) GetAllEventsList() (runtime.Object, error) {
	return s.kubeClient.CoreV1().Events(s.namespace).List(metav1.ListOptions{})
}

// GetAllConfigMaps returns a list of configmaps for the given namespace
func (s *DataStore) GetAllConfigMaps() (runtime.Object, error) {
	return s.kubeClient.CoreV1().ConfigMaps(s.namespace).List(metav1.ListOptions{})
}

// GetAllVolumeAttachments returns a list of volumeattachments for the given namespace
func (s *DataStore) GetAllVolumeAttachments() (runtime.Object, error) {
	return s.kubeClient.StorageV1().VolumeAttachments().List(metav1.ListOptions{})
}
