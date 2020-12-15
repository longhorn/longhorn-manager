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

func getLoggerForShareManager(logger logrus.FieldLogger, sm *longhorn.ShareManager) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"shareManager": sm.Name,
			"volume":       sm.Name,
			"owner":        sm.Status.OwnerID,
			"state":        sm.Status.State,
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
		if !c.shouldClaimOwnership(sm) {
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

	if err = c.syncShareManagerVolume(sm); err != nil {
		return err
	}

	if err = c.syncShareManagerPod(sm); err != nil {
		return err
	}

	if err = c.syncShareManagerEndpoint(sm); err != nil {
		return err
	}

	return nil
}

func (c *ShareManagerController) syncShareManagerEndpoint(sm *longhorn.ShareManager) error {
	// running is once the pod is in ready state
	// which means the nfs server is up and running with the volume attached
	// the cluster service ip doesn't change for the lifetime of the volume
	if sm.Status.State != types.ShareManagerStateRunning {
		sm.Status.Endpoint = ""
		return nil
	}

	service, err := c.ds.GetService(sm.Namespace, sm.Name)
	if err != nil {
		return err
	}

	sm.Status.Endpoint = fmt.Sprintf("nfs://%v/%v", service.Spec.ClusterIP, sm.Name)
	return nil
}

// isShareManagerRequiredForVolume checks if a share manager should export a volume
// a nil volume does not require a share manager
func (c *ShareManagerController) isShareManagerRequiredForVolume(volume *longhorn.Volume) bool {
	if volume == nil {
		return false
	}

	if volume.Spec.AccessMode != types.AccessModeReadWriteMany {
		return false
	}

	// let the auto salvage take care of it
	if volume.Status.Robustness == types.VolumeRobustnessFaulted {
		return false
	}

	// let the normal restore process take care of it
	if volume.Status.RestoreRequired {
		return false
	}

	// volume is used in maintenance mode
	if volume.Spec.DisableFrontend || volume.Status.FrontendDisabled {
		return false
	}

	// no need to expose (DR) standby volumes via a share manager
	if volume.Spec.Standby || volume.Status.IsStandby {
		return false
	}

	// no active workload, there is no need to keep the share manager around
	hasActiveWorkload := volume.Status.KubernetesStatus.LastPodRefAt == "" && volume.Status.KubernetesStatus.LastPVCRefAt == "" &&
		len(volume.Status.KubernetesStatus.WorkloadsStatus) > 0
	if !hasActiveWorkload {
		return false
	}

	return true
}

func (c ShareManagerController) detachShareManagerVolume(sm *longhorn.ShareManager) error {
	log := getLoggerForShareManager(c.logger, sm)
	volume, err := c.ds.GetVolume(sm.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Error("failed to retrieve volume for share manager from datastore")
		return err
	} else if volume == nil {
		return nil
	}

	// we don't want to detach volumes that we don't control
	isMaintenanceMode := volume.Spec.DisableFrontend || volume.Status.FrontendDisabled
	shouldDetach := !isMaintenanceMode && volume.Spec.AccessMode == types.AccessModeReadWriteMany && volume.Spec.NodeID != ""
	if shouldDetach {
		log.Infof("requesting Volume detach from node %v", volume.Spec.NodeID)
		volume.Spec.NodeID = ""
		volume, err = c.ds.UpdateVolume(volume)
		return err
	}

	return nil
}

// syncShareManagerVolume controls volume attachment and provides the following state transitions
// running -> running (do nothing)
// stopped -> stopped (rest state)
// starting, stopped, error -> starting (requires pod, volume attachment)
// starting, running, error -> stopped (no longer required, volume detachment)
// controls transitions to starting, stopped
func (c *ShareManagerController) syncShareManagerVolume(sm *longhorn.ShareManager) (err error) {
	var isNotNeeded bool
	defer func() {
		// ensure volume gets detached if share manager needs to be cleaned up and hasn't stopped yet
		// we need the isNotNeeded var so we don't accidentally detach manually attached volumes,
		// while the share manager is no longer running (only run cleanup once)
		if isNotNeeded && sm.Status.State != types.ShareManagerStateStopped {
			getLoggerForShareManager(c.logger, sm).Info("stopping share manager")
			if err = c.detachShareManagerVolume(sm); err == nil {
				sm.Status.State = types.ShareManagerStateStopped
			}
		}
	}()

	log := getLoggerForShareManager(c.logger, sm)
	volume, err := c.ds.GetVolume(sm.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Error("failed to retrieve volume for share manager from datastore")
		return err
	}

	if !c.isShareManagerRequiredForVolume(volume) {
		if sm.Status.State != types.ShareManagerStateStopped {
			log.Info("share manager is no longer required")
			isNotNeeded = true
		}
		return nil
	} else if sm.Status.State == types.ShareManagerStateRunning {
		return nil
	}

	// in a single node cluster, there is no other manager that can claim ownership so we are prevented from creation
	// of the share manager pod and need to ensure that the volume gets detached, so that the engine can be stopped as well
	// we only check for running, since we don't want to nuke valid pods, not schedulable only means no new pods.
	// in the case of a drain kubernetes will terminate the running pod, which we will mark as error in the sync pod method
	if !isNodeSchedulable(c.ds, sm.Status.OwnerID) {
		if sm.Status.State != types.ShareManagerStateStopped {
			log.Info("cannot start share manager, node is not schedulable")
			isNotNeeded = true
		}
		return nil
	}

	// we manage volume auto detach/attach in the starting state, once the pod is running
	// the volume health check will be responsible for failing the pod which will lead to error state
	if sm.Status.State != types.ShareManagerStateStarting {
		log.Debug("starting share manager")
		sm.Status.State = types.ShareManagerStateStarting
	}

	if volume.Spec.NodeID != sm.Status.OwnerID || volume.Status.CurrentNodeID != sm.Status.OwnerID {
		// HACK: at the moment the consumer needs to control the state transitions of the volume
		//  since it's not possible to just specify the desired state, we should fix that down the line.
		//  for the actual state transition we need to request detachment then wait for detachment
		//  afterwards we can request attachment to the new node.
		shouldDetach := volume.Status.State == types.VolumeStateAttached ||
			(volume.Status.State == types.VolumeStateAttaching && volume.Spec.NodeID != sm.Status.OwnerID)
		if shouldDetach {
			log.WithField("volume", volume.Name).Infof("Requesting Volume detach from previous node %v", volume.Status.CurrentNodeID)
			volume.Spec.NodeID = ""
		}

		if volume.Status.State == types.VolumeStateDetached {
			log.WithField("volume", volume.Name).Info("Requesting Volume attach to share manager node")
			volume.Spec.NodeID = sm.Status.OwnerID
		}

		if volume, err = c.ds.UpdateVolume(volume); err != nil {
			return err
		}
	}

	return nil
}

func (c *ShareManagerController) cleanupShareManagerPod(sm *longhorn.ShareManager) error {
	log := getLoggerForShareManager(c.logger, sm)

	podName := getPodNameForShareManager(sm)
	pod, err := c.ds.GetPod(podName)
	if err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).WithField("pod", podName).Error("failed to retrieve pod for share manager from datastore")
		return err
	} else if pod == nil {
		return nil
	}

	if err := c.ds.DeletePod(podName); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if nodeFailed, _ := c.ds.IsNodeDownOrDeleted(pod.Spec.NodeName); nodeFailed {
		log.Debug("node of share manager pod is down, force deleting pod to allow fail over")
		gracePeriod := int64(0)
		err := c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(podName, &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod})
		if err != nil && !apierrors.IsNotFound(err) {
			log.WithError(err).Debugf("failed to force delete share manager pod")
			return err
		}
	}

	return nil
}

// syncShareManagerPod controls pod existence and provides the following state transitions
// stopped -> stopped (rest state)
// starting -> starting (pending, volume attachment)
// starting ,running, error -> error (restart, remount volumes)
// starting, running -> running (share ready to use)
// controls transitions to running, error
func (c *ShareManagerController) syncShareManagerPod(sm *longhorn.ShareManager) (err error) {
	defer func() {
		if sm.Status.State == types.ShareManagerStateError || sm.Status.State == types.ShareManagerStateStopped {
			err = c.cleanupShareManagerPod(sm)
		}
	}()

	// if we are in stopped state there is nothing to do but cleanup any outstanding pods
	// no need for remount, since we don't have any active workloads in this state
	if sm.Status.State == types.ShareManagerStateStopped {
		return nil
	}

	log := getLoggerForShareManager(c.logger, sm)
	pod, err := c.ds.GetPod(getPodNameForShareManager(sm))
	if err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Error("failed to retrieve pod for share manager from datastore")
		return err
	} else if pod == nil {

		// there should only ever be no pod if we are in pending state
		// if there is no pod in any other state transition to error so we start over
		if sm.Status.State != types.ShareManagerStateStarting {
			log.Debug("Share Manager has no pod but is not in starting state, requires cleanup with remount")
			sm.Status.State = types.ShareManagerStateError
			return nil
		}

		if pod, err = c.createShareManagerPod(sm); err != nil {
			log.WithError(err).Error("failed to create pod for share manager")
			return err
		}
	}

	// in the case where the node name and the controller don't fit it means that there was an ownership transfer of the share manager
	// so we need to cleanup the old pod, most likely it's on a now defective node, in the case where the share manager fails
	// for whatever reason we just delete and recreate it.
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != sm.Status.OwnerID {
		if sm.Status.State != types.ShareManagerStateStopped {
			log.Debug("Share Manager pod requires cleanup with remount")
			sm.Status.State = types.ShareManagerStateError
		}

		return nil
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		if sm.Status.State != types.ShareManagerStateStarting {
			log.Errorf("Share Manager has state %v but the related pod is pending.", sm.Status.State)
			sm.Status.State = types.ShareManagerStateError
		}
	case v1.PodRunning:
		// pod readiness is based on the availability of the nfs server
		// nfs server is only started after the volume is attached and mounted
		allContainersReady := true
		for _, st := range pod.Status.ContainerStatuses {
			allContainersReady = allContainersReady && st.Ready
		}

		if !allContainersReady {
			key := sm.Namespace + "/" + sm.Name
			c.queue.AddRateLimited(key)
		} else if sm.Status.State == types.ShareManagerStateStarting {
			sm.Status.State = types.ShareManagerStateRunning
		} else if sm.Status.State != types.ShareManagerStateRunning {
			sm.Status.State = types.ShareManagerStateError
		}

	default:
		sm.Status.State = types.ShareManagerStateError
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

// shouldClaimOwnership in most controllers we only checks if the node of the current owner is down
// but in the case where the node is unschedulable and the sharemanager is in error state
// we want to transfer ownership, since otherwise we would keep creating a share manager pod on the
// unschedulable node which would force the engine to also be created on this node
// this scenario would lead to the node no longer being able ot be drained
// since the volume would never be unattached which prevents the engine from being removed
func (c *ShareManagerController) shouldClaimOwnership(sm *longhorn.ShareManager) bool {
	shouldClaim := isControllerResponsibleFor(c.controllerID, c.ds, sm.Name, "", sm.Status.OwnerID)
	isCurrentNodeSchedulable := isNodeSchedulable(c.ds, c.controllerID)
	if shouldClaim {
		return isCurrentNodeSchedulable
	}

	// in the case where we shouldn't claim this,
	// we need to check if the owner allows for new pod creation
	// we only transfer ownership if it's in error state, since we don't want to move running or stopped pods
	isOwnersNodeSchedulable := isNodeSchedulable(c.ds, sm.Status.OwnerID)
	if !isOwnersNodeSchedulable && sm.Status.State == types.ShareManagerStateError && isCurrentNodeSchedulable {
		getLoggerForShareManager(c.logger, sm).Debugf("suggesting ownership transfer to %v because current node %v "+
			"is no longer schedulable and share manager is in error", c.controllerID, sm.Status.OwnerID)
		return true
	}

	return false
}

func isNodeSchedulable(ds *datastore.DataStore, node string) bool {
	kubeNode, err := ds.GetKubernetesNode(node)
	if err != nil {
		return false
	}
	return !kubeNode.Spec.Unschedulable
}
