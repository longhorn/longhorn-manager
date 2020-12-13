package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type ShareManagerController struct {
	*baseController

	namespace      string
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	smStoreSynced cache.InformerSynced
	vStoreSynced  cache.InformerSynced
	pStoreSynced  cache.InformerSynced
}

func NewShareManagerController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,

	shareManagerInformer lhinformers.ShareManagerInformer,
	volumeInformer lhinformers.VolumeInformer,
	podInformer coreinformers.PodInformer,

	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string) *ShareManagerController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	c := &ShareManagerController{
		baseController: newBaseController("longhorn-share-manager", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-share-manager-controller"}),

		ds: ds,

		smStoreSynced: shareManagerInformer.Informer().HasSynced,
		vStoreSynced:  volumeInformer.Informer().HasSynced,
		pStoreSynced:  podInformer.Informer().HasSynced,
	}

	// need shared volume manager informer
	shareManagerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueShareManager,
		UpdateFunc: func(old, cur interface{}) { c.enqueueShareManager(cur) },
		DeleteFunc: c.enqueueShareManager,
	})

	// need information for volumes, to be able to claim them
	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueShareManagerForVolume,
		UpdateFunc: func(old, cur interface{}) { c.enqueueShareManagerForVolume(cur) },
		DeleteFunc: c.enqueueShareManagerForVolume,
	})

	// we are only interested in pods for which we are responsible for managing
	podInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: isShareManagerPod,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    c.enqueueShareManagerForPod,
			UpdateFunc: func(old, cur interface{}) { c.enqueueShareManagerForPod(cur) },
			DeleteFunc: c.enqueueShareManagerForPod,
		},
	})

	return c
}

func getLoggerForShareManager(logger logrus.FieldLogger, sm *longhorn.ShareManager) logrus.FieldLogger {
	return logger.WithFields(
		logrus.Fields{
			"shareManager": sm.Name,
			"owner":        sm.Status.OwnerID,
		},
	)
}

func (c *ShareManagerController) enqueueShareManager(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.AddRateLimited(key)
}

func (c *ShareManagerController) enqueueShareManagerForVolume(obj interface{}) {
	volume, isVolume := obj.(*longhorn.Volume)
	if !isVolume {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue the ShareManager
		volume, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Volume object: %#v", deletedState.Obj))
			return
		}
	}

	if volume.Spec.AccessMode == types.AccessModeReadWriteMany {
		// we can queue the key directly since a share manager only manages a single volume from it's own namespace
		// and there is no need for us to retrieve the whole object, since we already know the volume name
		getLoggerForVolume(c.logger, volume).Trace("Enqueuing share manager for volume")
		key := volume.Namespace + "/" + volume.Name
		c.queue.AddRateLimited(key)
		return
	}
}

func (c *ShareManagerController) enqueueShareManagerForPod(obj interface{}) {
	pod, isPod := obj.(*v1.Pod)
	if !isPod {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue the ShareManager
		pod, ok = deletedState.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Pod object: %#v", deletedState.Obj))
			return
		}
	}

	// we can queue the key directly since a share manager only manages pods from it's own namespace
	// and there is no need for us to retrieve the whole object, since the share manager name is stored in the label
	smName := pod.Labels[types.GetLonghornLabelKey(types.LonghornLabelShareManager)]
	c.logger.WithField("pod", pod.Name).WithField("shareManager", smName).Trace("Enqueuing share manager for pod")
	key := pod.Namespace + "/" + smName
	c.queue.AddRateLimited(key)
	return
}

func isShareManagerPod(obj interface{}) bool {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*v1.Pod)
		if !ok {
			return false
		}
	}

	podContainers := pod.Spec.Containers
	for _, con := range podContainers {
		if con.Name == types.LonghornLabelShareManager {
			return true
		}
	}
	return false
}

func (c *ShareManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Infof("Start Longhorn share manager controller")
	defer c.logger.Infof("Shutting down Longhorn share manager controller")

	if !cache.WaitForNamedCacheSync("longhorn-share-manager-controller", stopCh,
		c.smStoreSynced, c.vStoreSynced, c.pStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (c *ShareManagerController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *ShareManagerController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	err := c.syncShareManager(key.(string))
	c.handleErr(err, key)
	return true
}

func (c *ShareManagerController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		c.logger.WithError(err).Warnf("Error syncing Longhorn share manager %v", key)
		c.queue.AddRateLimited(key)
		return
	}

	c.logger.WithError(err).Warnf("Dropping Longhorn share manager %v out of the queue", key)
	c.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (c *ShareManagerController) cleanupShareManagerPod(sm *longhorn.ShareManager) error {
	if err := c.ds.DeletePod(sm.Name); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

func (c *ShareManagerController) syncShareManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}

	sm, err := c.ds.GetShareManager(name)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			c.logger.WithField("shareManager", name).WithError(err).Error("Failed to retrieve share manager from datastore")
			return err
		}

		c.logger.WithField("shareManager", name).Debug("Can't find share manager, may have been deleted")
		return nil
	}
	log := getLoggerForShareManager(c.logger, sm)

	// check if we need to claim ownership
	if sm.Status.OwnerID != c.controllerID {
		if !c.isResponsibleFor(sm) {
			return nil
		}

		// on owner ship transfer we set the state to unknown,
		// which leads to cleaning up any of the old pods
		sm.Status.OwnerID = c.controllerID
		sm.Status.State = types.ShareManagerStateUnknown
		sm, err = c.ds.UpdateShareManagerStatus(sm)
		if err != nil {
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Share manager got new owner %v", c.controllerID)
		log = getLoggerForShareManager(c.logger, sm)
	}

	if sm.DeletionTimestamp != nil {
		if err := c.cleanupShareManagerPod(sm); err != nil {
			return err
		}
		return c.ds.RemoveFinalizerForShareManager(sm)
	}

	// update at the end, after the whole reconcile loop
	existingShareManager := sm.DeepCopy()
	defer func() {
		if !reflect.DeepEqual(existingShareManager.Status, sm.Status) {
			// we end up overwriting any prior errors
			// but if the update works, the share manager will be
			// enqueued again anyway
			_, err = c.ds.UpdateShareManagerStatus(sm)
		}

		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debug("Requeue share manager due to conflict")
			c.enqueueShareManager(sm)
			err = nil
		}
	}()

	if err = c.syncShareManagerPod(sm); err != nil {
		return err
	}

	if sm.Status.State == types.ShareManagerStateRunning {
		// running is once the pod is in ready state
		// which means the nfs server is up and running with the volume attached
		// the cluster service ip doesn't change for the lifetime of the volume
		service, err := c.ds.GetService(sm.Namespace, sm.Name)
		if err != nil {
			return err
		}
		sm.Status.Endpoint = fmt.Sprintf("nfs://%v/%v", service.Spec.ClusterIP, sm.Name)
	} else {
		sm.Status.Endpoint = ""
	}

	return nil
}

func (c *ShareManagerController) syncShareManagerPod(sm *longhorn.ShareManager) error {
	log := getLoggerForShareManager(c.logger, sm)
	var pod *v1.Pod
	var err error
	if pod, err = c.ds.GetPod(getPodNameForShareManager(sm)); err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Error("failed to retrieve pod for share manager from datastore")
		return err
	}

	// TODO: compare pod image via label and recreate if share manager has different image (i.e. updated image)
	if pod != nil && pod.DeletionTimestamp != nil {
		if sm.Status.State != types.ShareManagerStateStopped {
			sm.Status.State = types.ShareManagerStateError
		}

		if nodeFailed, _ := c.ds.IsNodeDownOrDeleted(pod.Spec.NodeName); nodeFailed {
			sm.Status.State = types.ShareManagerStateError
			log.Debug("node of share manager pod is down, force deleting pod to allow fail over")
			gracePeriod := int64(0)
			if err := c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
				log.WithError(err).Debugf("failed to force delete share manager pod")
			}
		}

		return nil
	}

	// no point in starting a pod if we don't have the associated volume anymore
	volume, err := c.ds.GetVolume(sm.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			sm.Status.State = types.ShareManagerStateStopped
			return c.cleanupShareManagerPod(sm)
		}
		return err
	}

	// no need to expose standby volumes via a share manager
	if volume.Spec.Standby || volume.Status.IsStandby {
		sm.Status.State = types.ShareManagerStateStopped
		return c.cleanupShareManagerPod(sm)
	}

	// no active workload, there is no need to keep the share manager around and the volume can be detached
	hasActiveWorkload := volume.Status.KubernetesStatus.LastPodRefAt == "" && volume.Status.KubernetesStatus.LastPVCRefAt == "" &&
		len(volume.Status.KubernetesStatus.WorkloadsStatus) > 0
	if !hasActiveWorkload {
		isVolumeAttached := volume.Status.State == types.VolumeStateAttached || volume.Status.State == types.VolumeStateAttaching
		if sm.Status.State != types.ShareManagerStateStopped && isVolumeAttached && !volume.Status.FrontendDisabled {
			log.WithField("volume", volume.Name).Infof("Stopping share manager, requesting Volume detach from node %v", volume.Spec.NodeID)
			volume.Spec.NodeID = ""
			if volume, err = c.ds.UpdateVolume(volume); err != nil {
				return err
			}
		}
		sm.Status.State = types.ShareManagerStateStopped
		return c.cleanupShareManagerPod(sm)
	}

	if pod == nil {
		if pod, err = c.createShareManagerPod(sm); err != nil {
			sm.Status.State = types.ShareManagerStateError
			log.WithError(err).Error("failed to create pod for share manager")
			return err
		}
		sm.Status.State = types.ShareManagerStateStarting
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		if sm.Status.State != types.ShareManagerStateStarting {
			sm.Status.State = types.ShareManagerStateError
			log.Errorf("Share Manager has state %v but the related pod is pending.", sm.Status.State)
		}
	case v1.PodRunning:
		// pod readiness is based on the availability of the nfs server
		// nfs server is only started after the volume is attached and mounted
		allContainersReady := true
		for _, st := range pod.Status.ContainerStatuses {
			allContainersReady = allContainersReady && st.Ready
		}

		if !allContainersReady {
			// ensure that the shared volume is attached to this share managers node, if the share manager
			// switches nodes this will ensure, that the volumes will travel with the manager
			if volume.Spec.NodeID != sm.Status.OwnerID {
				// HACK: at the moment the consumer needs to control the state transitions of the volume
				//  since it's not possible to just specify the desired state, we should fix that down the line.
				//  for the actual state transition we need to request detachment then wait for detachment
				//  afterwards we can request attachment to the new node.
				if volume.Status.State == types.VolumeStateAttached || volume.Status.State == types.VolumeStateAttaching {
					log.WithField("volume", volume.Name).Infof("Requesting Volume detach from previous node %v", volume.Spec.NodeID)
					volume.Spec.NodeID = ""
				}

				if volume.Status.State == types.VolumeStateDetached {
					log.WithField("volume", volume.Name).Info("Requesting Volume attach to share manager node")
					volume.Spec.NodeID = sm.Status.OwnerID
				}

				if volume, err = c.ds.UpdateVolume(volume); err != nil {
					return err
				}
			} else {
				// manually requeue the share manager so we can check again for container ready just in case
				key := volume.Namespace + "/" + volume.Name
				c.queue.AddRateLimited(key)
			}
		} else if sm.Status.State == types.ShareManagerStateStarting {
			sm.Status.State = types.ShareManagerStateRunning
		} else if sm.Status.State != types.ShareManagerStateRunning {
			sm.Status.State = types.ShareManagerStateError
		}

	default:
		sm.Status.State = types.ShareManagerStateError
	}

	// in the case where the node name and the controller don't fit it means that there was an ownership transfer of the share manager
	// so we need to cleanup the old pod, most likely it's on a now defective node, in the case where the share manager fails
	// for whatever reason we just delete and recreate it.
	if pod.Spec.NodeName != sm.Status.OwnerID || sm.Status.State == types.ShareManagerStateError {
		sm.Status.State = types.ShareManagerStateError
		return c.cleanupShareManagerPod(sm)
	}

	return nil
}

// createShareManagerPod ensures existence of service, it's assumed that the pvc for this share manager already exists
func (c *ShareManagerController) createShareManagerPod(sm *longhorn.ShareManager) (*v1.Pod, error) {
	setting, err := c.ds.GetSetting(types.SettingNameTaintToleration)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get taint toleration setting before creating share manager pod")
	}
	tolerations, err := types.UnmarshalTolerations(setting.Value)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal taint toleration setting before creating share manager pod")
	}

	tolerationsByte, err := json.Marshal(tolerations)
	if err != nil {
		return nil, err
	}
	annotations := map[string]string{types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix): string(tolerationsByte)}

	imagePullPolicy, err := c.ds.GetSettingImagePullPolicy()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get image pull policy before creating share manager pod")
	}

	setting, err = c.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get registry secret setting before creating share manager pod")
	}
	registrySecret := setting.Value

	setting, err = c.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get priority class setting before creating share manager pod")
	}
	priorityClass := setting.Value

	// check if we need to create the service
	if _, err := c.ds.GetService(c.namespace, sm.Name); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to get service for share manager %v", sm.Name)
		}

		if _, err = c.ds.CreateService(c.namespace, c.createServiceManifest(sm)); err != nil {
			return nil, errors.Wrapf(err, "failed to create service for share manager %v", sm.Name)
		}
	}

	pod, err := c.ds.CreatePod(c.createPodManifest(sm, annotations, tolerations, imagePullPolicy, nil, registrySecret, priorityClass))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create pod for share manager %v", sm.Name)
	}
	getLoggerForShareManager(c.logger, sm).WithField("pod", pod.Name).Info("Created pod for share manager")
	return pod, nil
}

func (c *ShareManagerController) createServiceManifest(sm *longhorn.ShareManager) *v1.Service {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            sm.Name,
			Namespace:       c.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForShareManager(sm, false),
			Labels:          types.GetShareManagerInstanceLabel(sm.Name),
		},
		Spec: v1.ServiceSpec{
			ClusterIP: "", // we let the cluster assign a random ip
			Type:      v1.ServiceTypeClusterIP,
			Selector:  types.GetShareManagerInstanceLabel(sm.Name),
			Ports: []v1.ServicePort{
				{
					Name:     "nfs",
					Port:     2049,
					Protocol: v1.ProtocolTCP,
				},
			},
		},
	}

	return service
}

func getPodNameForShareManager(sm *longhorn.ShareManager) string {
	return types.LonghornLabelShareManager + "-" + sm.Name
}

func (c *ShareManagerController) createPodManifest(sm *longhorn.ShareManager, annotations map[string]string, tolerations []v1.Toleration,
	pullPolicy v1.PullPolicy, resourceReq *v1.ResourceRequirements, registrySecret string, priorityClass string) *v1.Pod {
	privileged := true
	podSpec := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            getPodNameForShareManager(sm),
			Namespace:       sm.Namespace,
			Labels:          types.GetShareManagerLabels(sm.Name, sm.Spec.Image),
			Annotations:     annotations,
			OwnerReferences: datastore.GetOwnerReferencesForShareManager(sm, true),
		},
		Spec: v1.PodSpec{
			ServiceAccountName: c.serviceAccount,
			Tolerations:        util.GetDistinctTolerations(tolerations),
			PriorityClassName:  priorityClass,
			NodeName:           sm.Status.OwnerID,
			Containers: []v1.Container{
				{
					Name:            types.LonghornLabelShareManager,
					Image:           sm.Spec.Image,
					ImagePullPolicy: pullPolicy,
					// Command: []string{"longhorn-share-manager"},
					Args: []string{"--debug", "daemon", "--volume", sm.Name},
					ReadinessProbe: &v1.Probe{
						Handler: v1.Handler{
							Exec: &v1.ExecAction{
								Command: []string{"cat", "/var/run/ganesha.pid"},
							},
						},
						InitialDelaySeconds: managerProbeInitialDelay,
						PeriodSeconds:       managerProbePeriodSeconds,
						FailureThreshold:    managerLivenessProbeFailureThreshold,
					},
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	// host mount the devices, so we can mount the shared longhorn-volume into the export folder
	hostToContainer := v1.MountPropagationHostToContainer
	podSpec.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
		{
			MountPath:        "/host/dev",
			Name:             "dev",
			MountPropagation: &hostToContainer,
		},
	}
	podSpec.Spec.Volumes = []v1.Volume{
		{
			Name: "dev",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/dev",
				},
			},
		},
	}

	if registrySecret != "" {
		podSpec.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}

	if resourceReq != nil {
		podSpec.Spec.Containers[0].Resources = *resourceReq
	}

	return podSpec
}

func (c *ShareManagerController) isResponsibleFor(sm *longhorn.ShareManager) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, sm.Name, "", sm.Status.OwnerID)
}
