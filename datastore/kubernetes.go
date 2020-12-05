package datastore

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
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
	// KubeStatusPollCount is the number of retry to validate The KubernetesStatus
	KubeStatusPollCount = 5
	// KubeStatusPollInterval is the waiting time between each KubeStatusPollCount
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

// GetManagerNodeIPMap returns an object contains podIPs from list
// of running pods with app=longhorn-manager
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

// CreateVolumeCronJob sets CronJob labels in volume meta and
// creates a CronJob resource for the given namespace
func (s *DataStore) CreateVolumeCronJob(volumeName string, cronJob *batchv1beta1.CronJob) (*batchv1beta1.CronJob, error) {
	if err := tagVolumeLabel(volumeName, cronJob); err != nil {
		return nil, err
	}
	return s.kubeClient.BatchV1beta1().CronJobs(s.namespace).Create(cronJob)
}

// UpdateVolumeCronJob sets CronJob labels in volume meta and
// updates CronJobs for the given namespace
func (s *DataStore) UpdateVolumeCronJob(volumeName string, cronJob *batchv1beta1.CronJob) (*batchv1beta1.CronJob, error) {
	if err := tagVolumeLabel(volumeName, cronJob); err != nil {
		return nil, err
	}
	return s.kubeClient.BatchV1beta1().CronJobs(s.namespace).Update(cronJob)
}

// DeleteCronJob delete CronJob for the given name and namespace.
// The dependents will be deleted in the background
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

// CreateEngineImageDaemonSet sets EngineImage labels in DaemonSet label and
// creates a DaemonSet resource in the given namespace
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

// GetEngineImageDaemonSet get DaemonSet for the given name and namspace, and
// returns a new DaemonSet object
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

// CreatePDB creates a PodDisruptionBudget resource for the given PDB object and namespace
func (s *DataStore) CreatePDB(pdp *policyv1beta1.PodDisruptionBudget) (*policyv1beta1.PodDisruptionBudget, error) {
	return s.kubeClient.PolicyV1beta1().PodDisruptionBudgets(s.namespace).Create(pdp)
}

// DeletePDB deletes PodDisruptionBudget for the given name and namespace
func (s *DataStore) DeletePDB(name string) error {
	return s.kubeClient.PolicyV1beta1().PodDisruptionBudgets(s.namespace).Delete(name, nil)
}

// GetPDBRO gets PDB for the given name and namespace.
// This function returns direct reference to the internal cache object and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) GetPDBRO(name string) (*policyv1beta1.PodDisruptionBudget, error) {
	return s.pdbLister.PodDisruptionBudgets(s.namespace).Get(name)
}

// ListPDBs gets a map of PDB in s.namespace
func (s *DataStore) ListPDBs() (map[string]*policyv1beta1.PodDisruptionBudget, error) {
	itemMap := map[string]*policyv1beta1.PodDisruptionBudget{}

	list, err := s.pdbLister.PodDisruptionBudgets(s.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for _, itemRO := range list {
		// Cannot use cached object from lister
		itemMap[itemRO.Name] = itemRO.DeepCopy()
	}
	return itemMap, nil
}

// CreatePod creates a Pod resource for the given pod object and namespace
func (s *DataStore) CreatePod(pod *corev1.Pod) (*corev1.Pod, error) {
	return s.kubeClient.CoreV1().Pods(s.namespace).Create(pod)
}

// DeletePod deletes Pod for the given name and namespace
func (s *DataStore) DeletePod(name string) error {
	return s.kubeClient.CoreV1().Pods(s.namespace).Delete(name, nil)
}

// DeleteLease deletes Lease with the given name in s.namespace
func (s *DataStore) DeleteLease(name string) error {
	return s.kubeClient.CoordinationV1().Leases(s.namespace).Delete(name, nil)
}

// GetStorageClassRO gets StorageClass with the given name
// This function returns direct reference to the internal cache object and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) GetStorageClassRO(scName string) (*storagev1.StorageClass, error) {
	return s.storageclassLister.Get(scName)
}

// DeleteStorageClass deletes StorageClass with the given name
func (s *DataStore) DeleteStorageClass(scName string) error {
	return s.kubeClient.StorageV1().StorageClasses().Delete(scName, nil)
}

// CreateStorageClass creates StorageClass with the given object
func (s *DataStore) CreateStorageClass(sc *storagev1.StorageClass) (*storagev1.StorageClass, error) {
	return s.kubeClient.StorageV1().StorageClasses().Create(sc)
}

// GetPod returns a mutable Pod object for the given name and namspace
func (s *DataStore) GetPod(name string) (*corev1.Pod, error) {
	resultRO, err := s.pLister.Pods(s.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return resultRO.DeepCopy(), nil
}

// GetDaemonSet gets the DaemonSet for the given name and namespace
func (s *DataStore) GetDaemonSet(name string) (*appsv1.DaemonSet, error) {
	return s.dsLister.DaemonSets(s.namespace).Get(name)
}

// ListDaemonSet gets a list of all DaemonSet for the given namespace
func (s *DataStore) ListDaemonSet() ([]*appsv1.DaemonSet, error) {
	return s.dsLister.DaemonSets(s.namespace).List(labels.Everything())
}

// UpdateDaemonSet updates the DaemonSet for the given DaemonSet object and namespace
func (s *DataStore) UpdateDaemonSet(obj *appsv1.DaemonSet) (*appsv1.DaemonSet, error) {
	return s.kubeClient.AppsV1().DaemonSets(s.namespace).Update(obj)
}

// DeleteDaemonSet deletes DaemonSet for the given name and namespace.
// The dependents will be deleted in the forground
func (s *DataStore) DeleteDaemonSet(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.kubeClient.AppsV1().DaemonSets(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

// GetDeployment gets the Deployment for the given name and namespace
func (s *DataStore) GetDeployment(name string) (*appsv1.Deployment, error) {
	return s.dpLister.Deployments(s.namespace).Get(name)
}

// ListDeployment gets a list of all Deployment for the given namespace
func (s *DataStore) ListDeployment() ([]*appsv1.Deployment, error) {
	return s.dpLister.Deployments(s.namespace).List(labels.Everything())
}

// UpdateDeployment updates Deployment for the given Deployment object and namespace
func (s *DataStore) UpdateDeployment(obj *appsv1.Deployment) (*appsv1.Deployment, error) {
	return s.kubeClient.AppsV1().Deployments(s.namespace).Update(obj)
}

// DeleteDeployment deletes Deployment for the given name and namespace.
// The dependents will be deleted in the forground
func (s *DataStore) DeleteDeployment(name string) error {
	propagation := metav1.DeletePropagationForeground
	return s.kubeClient.AppsV1().Deployments(s.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}

// DeleteCSIDriver deletes CSIDriver for the given name and namespace
func (s *DataStore) DeleteCSIDriver(name string) error {
	return s.kubeClient.StorageV1beta1().CSIDrivers().Delete(name, &metav1.DeleteOptions{})
}

// ListManagerPods returns a list of Pods marked with app=longhorn-manager
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

// ListInstanceManagerPods returns a list of Pod marked with component=instance-manager
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

// GetKubernetesNode gets the Node from the index for the given name
func (s *DataStore) GetKubernetesNode(name string) (*corev1.Node, error) {
	return s.knLister.Get(name)
}

// CreatePersisentVolume creates a PersistentVolume resource for the given
// PersistentVolume object
func (s *DataStore) CreatePersisentVolume(pv *corev1.PersistentVolume) (*corev1.PersistentVolume, error) {
	return s.kubeClient.CoreV1().PersistentVolumes().Create(pv)
}

// DeletePersisentVolume deletes the PersistentVolume for the given
// PersistentVolume name
func (s *DataStore) DeletePersisentVolume(pvName string) error {
	return s.kubeClient.CoreV1().PersistentVolumes().Delete(pvName, &metav1.DeleteOptions{})
}

// UpdatePersisentVolume updates the PersistentVolume for the given
// PersistentVolume object
func (s *DataStore) UpdatePersisentVolume(pv *corev1.PersistentVolume) (*corev1.PersistentVolume, error) {
	return s.kubeClient.CoreV1().PersistentVolumes().Update(pv)
}

// GetPersisentVolume gets the PersistentVolume from the index for the
// given name
func (s *DataStore) GetPersisentVolume(pvName string) (*corev1.PersistentVolume, error) {
	return s.pvLister.Get(pvName)
}

// CreatePersisentVolumeClaim creates a PersistentVolumeClaim resource
// for the given PersistentVolumeclaim object and namespace
func (s *DataStore) CreatePersisentVolumeClaim(ns string, pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	return s.kubeClient.CoreV1().PersistentVolumeClaims(ns).Create(pvc)
}

// DeletePersisentVolumeClaim deletes the PersistentVolumeClaim for the
// given name and namespace
func (s *DataStore) DeletePersisentVolumeClaim(ns, pvcName string) error {
	return s.kubeClient.CoreV1().PersistentVolumeClaims(ns).Delete(pvcName, &metav1.DeleteOptions{})
}

// GetPersisentVolumeClaim gets the PersistentVolumeClaim from the
// index for the given name and namespace
func (s *DataStore) GetPersisentVolumeClaim(namespace, pvcName string) (*corev1.PersistentVolumeClaim, error) {
	return s.pvcLister.PersistentVolumeClaims(namespace).Get(pvcName)
}

// GetConfigMapRO gets ConfigMap with the given name in s.namespace
// This function returns direct reference to the internal cache object and should not be mutated.
// Consider using this function when you can guarantee read only access and don't want the overhead of deep copies
func (s *DataStore) GetConfigMapRO(namespace, name string) (*corev1.ConfigMap, error) {
	return s.cfmLister.ConfigMaps(namespace).Get(name)
}

// GetConfigMap return a new ConfigMap object for the given namespace and name
func (s *DataStore) GetConfigMap(namespace, name string) (*corev1.ConfigMap, error) {
	resultRO, err := s.cfmLister.ConfigMaps(namespace).Get(name)
	if err != nil {
		return nil, err
	}
	// Cannot use cached object from lister
	return resultRO.DeepCopy(), nil
}

// GetPriorityClass gets the PriorityClass from the index for the
// given name
func (s *DataStore) GetPriorityClass(pcName string) (*schedulingv1.PriorityClass, error) {
	return s.pcLister.Get(pcName)
}

// GetPodContainerLogRequest returns the Pod log for the given pod name,
// container name and namespace
func (s *DataStore) GetPodContainerLogRequest(podName, containerName string) *rest.Request {
	return s.kubeClient.CoreV1().Pods(s.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  containerName,
		Timestamps: true,
	})
}

// GetKubernetesVersion returns the server version
func (s *DataStore) GetKubernetesVersion() (*version.Info, error) {
	return s.kubeClient.Discovery().ServerVersion()
}

// NewPVManifest returns a new PersistentVolume object
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

// NewPVCManifest returns a new PersistentVolumeClaim object
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
