package datastore

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/rest"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

const (
	KubeStatusPollCount    = 5
	KubeStatusPollInterval = 1 * time.Second
)

func (s *DataStore) getManagerLabel() map[string]string {
	return map[string]string{
		//TODO standardize key
		//longhornSystemKey: longhornSystemManager,
		"app": types.LonghornManagerDaemonSetName,
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

func (s *DataStore) CreateEngineImageDaemonSet(ds *appsv1.DaemonSet) error {
	if ds.Labels == nil {
		ds.Labels = map[string]string{}
	}
	for k, v := range types.GetEngineImageLabels(types.GetEngineImageNameFromDaemonSetName(ds.Name)) {
		ds.Labels[k] = v
	}
	if _, err := s.kubeClient.AppsV1().DaemonSets(s.namespace).Create(ds); err != nil {
		return err
	}
	return nil
}

func (s *DataStore) GetEngineImageDaemonSet(name string) (*appsv1.DaemonSet, error) {
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

func (s *DataStore) CreatePod(pod *corev1.Pod) (*corev1.Pod, error) {
	return s.kubeClient.CoreV1().Pods(s.namespace).Create(pod)
}

func (s *DataStore) DeletePod(name string) error {
	return s.kubeClient.CoreV1().Pods(s.namespace).Delete(name, nil)
}

func (s *DataStore) GetInstanceManagerPod(name string) (*corev1.Pod, error) {
	resultRO, err := s.pLister.Pods(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return resultRO.DeepCopy(), nil
}

func (s *DataStore) GetDaemonSet(name string) (*appsv1.DaemonSet, error) {
	return s.dsLister.DaemonSets(s.namespace).Get(name)
}

func (s *DataStore) ListDaemonSet() ([]*appsv1.DaemonSet, error) {
	return s.dsLister.DaemonSets(s.namespace).List(labels.Everything())
}

func (s *DataStore) UpdateDaemonSet(obj *appsv1.DaemonSet) (*appsv1.DaemonSet, error) {
	return s.kubeClient.AppsV1().DaemonSets(s.namespace).Update(obj)
}

func (s *DataStore) DeleteDaemonSet(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.kubeClient.AppsV1().DaemonSets(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

func (s *DataStore) GetDeployment(name string) (*appsv1.Deployment, error) {
	return s.dpLister.Deployments(s.namespace).Get(name)
}

func (s *DataStore) ListDeployment() ([]*appsv1.Deployment, error) {
	return s.dpLister.Deployments(s.namespace).List(labels.Everything())
}

func (s *DataStore) UpdateDeployment(obj *appsv1.Deployment) (*appsv1.Deployment, error) {
	return s.kubeClient.AppsV1().Deployments(s.namespace).Update(obj)
}

func (s *DataStore) DeleteDeployment(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.kubeClient.AppsV1().Deployments(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

func (s *DataStore) DeleteCSIDriver(name string) error {
	return s.kubeClient.StorageV1beta1().CSIDrivers().Delete(name, &metav1.DeleteOptions{})
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

func getInstanceManagerComponentSelector() (labels.Selector, error) {
	return metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: types.GetInstanceManagerComponentLabel(),
	})
}

func (s *DataStore) ListInstanceManagerPods() ([]*corev1.Pod, error) {
	selector, err := getInstanceManagerComponentSelector()
	if err != nil {
		return nil, err
	}

	podList, err := s.pLister.Pods(s.namespace).List(selector)
	if err != nil {
		return nil, err
	}

	res := []*corev1.Pod{}
	for _, item := range podList {
		res = append(res, item.DeepCopy())
	}
	return res, nil
}

func (s *DataStore) GetKubernetesNode(name string) (*corev1.Node, error) {
	return s.knLister.Get(name)
}

func (s *DataStore) CreatePersisentVolume(pv *corev1.PersistentVolume) (*corev1.PersistentVolume, error) {
	return s.kubeClient.CoreV1().PersistentVolumes().Create(pv)
}

func (s *DataStore) DeletePersisentVolume(pvName string) error {
	return s.kubeClient.CoreV1().PersistentVolumes().Delete(pvName, &metav1.DeleteOptions{})
}

func (s *DataStore) UpdatePersisentVolume(pv *corev1.PersistentVolume) (*corev1.PersistentVolume, error) {
	return s.kubeClient.CoreV1().PersistentVolumes().Update(pv)
}

func (s *DataStore) GetPersisentVolume(pvName string) (*corev1.PersistentVolume, error) {
	return s.pvLister.Get(pvName)
}

func (s *DataStore) CreatePersisentVolumeClaim(ns string, pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	return s.kubeClient.CoreV1().PersistentVolumeClaims(ns).Create(pvc)
}

func (s *DataStore) DeletePersisentVolumeClaim(ns, pvcName string) error {
	return s.kubeClient.CoreV1().PersistentVolumeClaims(ns).Delete(pvcName, &metav1.DeleteOptions{})
}

func (s *DataStore) GetPersisentVolumeClaim(namespace, pvcName string) (*corev1.PersistentVolumeClaim, error) {
	return s.pvcLister.PersistentVolumeClaims(namespace).Get(pvcName)
}

func (s *DataStore) GetPriorityClass(pcName string) (*schedulingv1.PriorityClass, error) {
	return s.pcLister.Get(pcName)
}

func (s *DataStore) GetPodContainerLogRequest(podName, containerName string) *rest.Request {
	return s.kubeClient.CoreV1().Pods(s.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  containerName,
		Timestamps: true,
	})
}

func (s *DataStore) GetKubernetesVersion() (*version.Info, error) {
	return s.kubeClient.Discovery().ServerVersion()
}

func NewPVManifest(v *longhorn.Volume, pvName, storageClassName, fsType string) *corev1.PersistentVolume {
	defaultVolumeMode := corev1.PersistentVolumeFilesystem

	diskSelector := strings.Join(v.Spec.DiskSelector, ",")
	nodeSelector := strings.Join(v.Spec.NodeSelector, ",")

	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: *resource.NewQuantity(v.Spec.Size, resource.BinarySI),
			},
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},

			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,

			VolumeMode: &defaultVolumeMode,

			StorageClassName: storageClassName,

			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: types.LonghornDriverName,
					FSType: fsType,
					VolumeAttributes: map[string]string{
						"diskSelector":        diskSelector,
						"nodeSelector":        nodeSelector,
						"numberOfReplicas":    strconv.Itoa(v.Spec.NumberOfReplicas),
						"staleReplicaTimeout": strconv.Itoa(v.Spec.StaleReplicaTimeout),
					},
					VolumeHandle: v.Name,
				},
			},
		},
	}
}

func NewPVCManifest(v *longhorn.Volume, pvName, ns, pvcName, storageClassName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: ns,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(v.Spec.Size, resource.BinarySI),
				},
			},
			StorageClassName: &storageClassName,
			VolumeName:       pvName,
		},
	}
}
