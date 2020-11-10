package controller

import (
	"fmt"
	"reflect"
	"sync"
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
	"github.com/longhorn/longhorn-manager/shareapi"
	"github.com/longhorn/longhorn-manager/types"
)

const ShareManagerDefaultVolumeSize = int64(1 * 1024 * 1024 * 1024)
const ShareManagerDefaultVolumeFS = "ext4"

func (c *ShareManagerController) hasMonitor(sm string) *shareapi.ShareManagerMonitor {
	c.lock.RLock()
	monitor := c.monitors[sm]
	c.lock.RUnlock()
	return monitor
}

func (c *ShareManagerController) enableMonitor(sm *longhorn.ShareManager) (*shareapi.ShareManagerMonitor, error) {
	// this optimizes lock contention, since we only create a monitor once per manager
	// and there can be multiple readers so no need to write lock unless we need to
	if monitor := c.hasMonitor(sm.Name); monitor != nil {
		return monitor, nil
	}

	monitor, err := shareapi.NewShareManagerMonitor(c.logger, sm, c.enqueueShareManagerForMonitor)
	if err != nil {
		return nil, err
	}

	c.lock.Lock()
	c.monitors[sm.Name] = monitor
	c.lock.Unlock()
	return monitor, nil
}

func (c *ShareManagerController) disableMonitor(sm string) error {
	if monitor := c.hasMonitor(sm); monitor != nil {
		c.lock.Lock()
		delete(c.monitors, sm)
		c.lock.Unlock()
		return monitor.Close()
	}
	return nil
}

type ShareManagerController struct {
	*baseController

	namespace      string
	controllerID   string
	serviceAccount string

	// size of the volume for the share manager pod
	// this volume is used as a cache on the nfs server
	// as well as the recovery state for the nfs-clients
	volumeSize int64

	monitors map[string]*shareapi.ShareManagerMonitor
	lock     sync.RWMutex

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

		volumeSize: ShareManagerDefaultVolumeSize,

		monitors: map[string]*shareapi.ShareManagerMonitor{},
		lock:     sync.RWMutex{},

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
			"node":         sm.Spec.NodeID,
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

func (c *ShareManagerController) enqueueShareManagerForMonitor(key string) {
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

	if volume.Status.ShareManager != "" {
		// we can queue the key directly since a share manager only manages volumes from it's own namespace
		// and there is no need for us to retrieve the whole object, since we already know the share manager name
		getLoggerForVolume(c.logger, volume).Debug("Enqueuing share manager for volume")
		key := volume.Namespace + "/" + volume.Status.ShareManager
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
	// and there is no need for us to retrieve the whole object, since we already know the share manager name
	c.logger.WithField("pod", pod.Name).WithField("shareManager", pod.Name).Debug("Enqueuing share manager for pod")
	key := pod.Namespace + "/" + pod.Name
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
		if con.Name == types.LognhornLabelShareManager {
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

func (c *ShareManagerController) cleanupShareManager(sm *longhorn.ShareManager) error {

	pod, err := c.ds.GetPod(sm.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if pod != nil {
		// TODO: graceful shutdown for the ganesha server
		//  we can implement the graceful shutdown via a sigterm handler
		//  on the nfs-control software
		if err := c.ds.DeletePod(sm.Name); err != nil {
			return err
		}
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

		c.logger.WithField("shareManager", name).Info("Can't find share manager, may have been deleted")
		return c.disableMonitor(name)
	}
	log := getLoggerForShareManager(c.logger, sm)

	// check if we need to claim ownership
	if sm.Status.OwnerID != c.controllerID {
		if !c.isResponsibleFor(sm) {
			return nil
		}

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
		if err := c.cleanupShareManager(sm); err != nil {
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

	monitor, err := c.syncShareManagerMonitor(sm)
	if err != nil {
		return err
	}

	// in the case where the share manager is not running monitor will be nil
	// and we set all share states to unknown
	if err = c.syncShareManagerVolumes(sm, monitor); err != nil {
		return err
	}

	return nil
}

func (c *ShareManagerController) syncShareManagerMonitor(sm *longhorn.ShareManager) (*shareapi.ShareManagerMonitor, error) {
	switch sm.Status.State {
	case types.ShareManagerStateRunning:
		monitor, err := c.enableMonitor(sm)
		return monitor, err
	default:
		return nil, c.disableMonitor(sm.Name)
	}
}

func (c *ShareManagerController) share(sm *longhorn.ShareManager, monitor *shareapi.ShareManagerMonitor, volume string) error {
	if sm.Status.Volumes == nil {
		sm.Status.Volumes = map[string]types.Share{}
	}

	if monitor == nil {
		sm.Status.Volumes[volume] = types.Share{
			Volume: volume,
			State:  types.ShareStateUnknown,
		}
		return nil
	}

	log := getLoggerForShareManager(c.logger, sm).WithField("volume", volume)
	log.Debug("Calling monitor share for volume")
	share, err := monitor.Share(volume)
	if err != nil {
		return err
	}

	// TODO: work on this
	sm.Status.Volumes[volume] = share
	return nil
}

func (c *ShareManagerController) unshare(sm *longhorn.ShareManager, monitor *shareapi.ShareManagerMonitor, volume string) error {
	if monitor == nil {
		return nil
	}

	log := getLoggerForShareManager(c.logger, sm).WithField("volume", volume)
	log.Debug("Calling monitor unshare for volume")
	err := monitor.Unshare(volume)
	if err != nil {
		return err
	}

	// TODO: work on this
	delete(sm.Status.Volumes, volume)
	return nil
}

func (c *ShareManagerController) syncShare(sm *longhorn.ShareManager, monitor *shareapi.ShareManagerMonitor, volume string) error {
	if sm.Status.Volumes == nil {
		sm.Status.Volumes = map[string]types.Share{}
	}

	if monitor == nil {
		sm.Status.Volumes[volume] = types.Share{
			Volume: volume,
			State:  types.ShareStateUnknown,
		}
		return nil
	}

	share, err := monitor.GetShare(volume)
	if err != nil {
		return err
	}

	// TODO: work on this
	switch sm.Status.State {
	case types.ShareManagerStateError:
		share.State = types.ShareStateError
		share.Error = "share manager is in error state"
		share.Endpoint = ""
	case types.ShareManagerStateStarting:
		share.State = types.ShareStatePending
		share.Error = ""
		share.Endpoint = ""
	case types.ShareManagerStateRunning:
		if share.State == types.ShareStateReady {
			share.Error = ""
			if share.Endpoint == "" {
				service, err := c.ds.GetService(sm.Namespace, sm.Name)
				if err != nil {
					return err
				}
				share.Endpoint = fmt.Sprintf("nfs4://%v/%v", service.Spec.ClusterIP, share.Volume)
			}
		}
	default:
		share.State = types.ShareStateUnknown
		share.Error = ""
		share.Endpoint = ""
	}

	sm.Status.Volumes[volume] = share
	return nil
}

func (c *ShareManagerController) syncShareManagerVolumes(sm *longhorn.ShareManager, monitor *shareapi.ShareManagerMonitor) error {
	log := getLoggerForShareManager(c.logger, sm)
	volErrors := make(map[string]error)

	// check for newly desired volumes
	for v := range sm.Spec.Volumes {
		volume, err := c.ds.GetVolume(v)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.WithError(err).WithField("volume", v).Error("Failed to retrieve volume for share manager")
				volErrors[v] = err
				continue
			}

			if _, isShared := sm.Status.Volumes[v]; isShared {
				log.WithField("volume", v).Info("Remove Share for deleted volume")
				if err := c.unshare(sm, monitor, v); err != nil {
					volErrors[v] = err
				}
			}

			log.WithField("volume", v).Debug("Skip Volume that no longer exists")
			continue
		}

		// ensure that the volume has given us the right to control it
		// lets say that for some reason there are 2 share managers that both
		// have the volume in their spec, that's fine since the volume status
		// will only point to one of them.
		if volume.Status.ShareManager != sm.Name {
			if _, isShared := sm.Status.Volumes[v]; isShared {
				log.WithField("volume", v).Info("Remove Share for volume that is not under our control")
				if err := c.unshare(sm, monitor, v); err != nil {
					volErrors[v] = err
				}
				continue
			}

			log.WithField("volume", v).Debug("Skip Volume that is not under our control")
			continue
		}

		// ensure each shared volume is attached to this share managers node, if the share manager
		// switches nodes this will ensure, that the volumes will travel with the manager
		// TODO: only testing ownerID we need a different way of tracking current node,
		//  since we don't want service distributions, once the failed node is back.
		if sm.Status.State == types.ShareManagerStateRunning && volume.Spec.NodeID != sm.Status.OwnerID {

			// HACK: at the moment the consumer needs to control the state transitions of the volume
			//  since it's not possible to just specify the desired state, we should fix that down the line.
			//  for the actual state transition we need to request detachment then wait for detachment
			//  afterwards we can request attachment to the new node.
			if volume.Status.State == types.VolumeStateAttached || volume.Status.State == types.VolumeStateAttaching {
				log.WithField("volume", v).Infof("Requesting Volume detach from previous node %v", volume.Spec.NodeID)
				volume.Spec.NodeID = ""
			}

			if volume.Status.State == types.VolumeStateDetached {
				log.WithField("volume", v).Info("Requesting Volume attach to share manager node")
				volume.Spec.NodeID = sm.Status.OwnerID
			}

			if _, err := c.ds.UpdateVolume(volume); err != nil {
				volErrors[v] = err
			}
		}

		// ensure there is a share for each volume
		if err := c.share(sm, monitor, v); err != nil {
			volErrors[v] = err
			continue
		}
	}

	// sync all shared volumes
	for v := range sm.Status.Volumes {
		if _, desiredVolume := sm.Spec.Volumes[v]; !desiredVolume {
			log.WithField("volume", v).Info("Remove Share for no longer desired volume")
			if err := c.unshare(sm, monitor, v); err != nil {
				volErrors[v] = err
			}
			continue
		}

		if err := c.syncShare(sm, monitor, v); err != nil {
			volErrors[v] = err
			continue
		}
	}

	if len(volErrors) > 0 {
		return fmt.Errorf("while syncing %v shares encountered %v errors",
			len(sm.Status.Volumes), len(volErrors))
	}

	return nil
}

func (c *ShareManagerController) syncShareManagerPod(sm *longhorn.ShareManager) error {
	log := getLoggerForShareManager(c.logger, sm)
	var pod *v1.Pod
	var err error
	if pod, err = c.ds.GetPod(sm.Name); err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Error("failed to retrieve pod for share manager from datastore")
		return err
	}

	if pod == nil {
		// no need to start a pod if we don't have any volumes
		if len(sm.Spec.Volumes) == 0 {
			sm.Status.State = types.ShareManagerStateStopped
			return nil
		}

		if pod, err = c.createShareManagerPod(sm); err != nil {
			log.WithError(err).Error("failed to create pod for share manager")
			return err
		}
		sm.Status.State = types.ShareManagerStateStarting
	}

	// TODO: compare pod image via label and recreate if share manager has different image (i.e. updated image)
	if pod.DeletionTimestamp != nil {
		if sm.Status.State != types.ShareManagerStateError {
			sm.Status.State = types.ShareManagerStateStopping
		}

		if nodeFailed, _ := c.ds.IsNodeDownOrDeleted(pod.Spec.NodeName); nodeFailed {
			log.Debug("node of share manager pod is down, force deleting pod to allow fail over")
			gracePeriod := int64(0)
			if err := c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
				log.WithError(err).Debugf("failed to force delete share manager pod")
			}
		}

		return nil
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		if sm.Status.State != types.ShareManagerStateStarting {
			sm.Status.State = types.ShareManagerStateError
			log.Errorf("Share Manager has state %v but the related pod is pending.", sm.Status.State)
		}
	case v1.PodRunning:
		// the share manager readyness status is based on the nfs-servers availability
		// which only impacts the share availability, grpc availability is based on the pod running status
		/*for _, st := range pod.Status.ContainerStatuses {
			if !st.Ready {
				return nil
			}
		}*/

		if sm.Status.State == types.ShareManagerStateStarting || sm.Status.State == types.ShareManagerStateUnknown {
			sm.Status.State = types.ShareManagerStateRunning
		} else if sm.Status.State != types.ShareManagerStateRunning {
			sm.Status.State = types.ShareManagerStateError
		}

		// the ip is used for the internal grpc communication nfs goes via the service endpoint
		if sm.Status.State == types.ShareManagerStateRunning {
			sm.Status.IP = pod.Status.PodIP
		}
	default:
		sm.Status.State = types.ShareManagerStateError
	}

	// in the case where the node name and the controller don't fit it means that there was an ownership transfer of the share manager
	// so we need to cleanup the old pod, most likely it's on a now defective node, in the case where the share manager fails
	// for whatever reason we just delete and recreate it.
	if pod.Spec.NodeName != c.controllerID || sm.Status.State == types.ShareManagerStateError {
		if err = c.ds.DeletePod(sm.Name); err != nil {
			log.WithError(err).Error("Failed to delete pod for share manager")
			return err
		}
	}

	return nil
}

// createShareManagerPod ensures existence of service, volume, pv, pvc and pod for a ShareManager
func (c *ShareManagerController) createShareManagerPod(sm *longhorn.ShareManager) (*v1.Pod, error) {
	setting, err := c.ds.GetSetting(types.SettingNameTaintToleration)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get taint toleration setting before creating share manager pod")
	}
	tolerations, err := types.UnmarshalTolerations(setting.Value)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal taint toleration setting before creating share manager pod")
	}

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

	// check if we need to create the longhorn volume
	if _, err := c.ds.GetVolume(sm.Name); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to get volume for share manager %v", sm.Name)
		}

		engineImage, err := func() (string, error) {
			image, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
			if image == "" || err != nil {
				return "", fmt.Errorf("cannot create longhorn volume with invalid Setting.EngineImage %v", image)
			}

			ei, err := c.ds.GetEngineImage(types.GetEngineImageChecksumName(image))
			if err != nil {
				return "", errors.Wrapf(err, "unable to get engine image %v", image)
			}
			if ei.Status.State != types.EngineImageStateReady {
				return "", fmt.Errorf("engine image %v (%v) is not ready, it's %v", ei.Name, image, ei.Status.State)
			}

			return image, nil
		}()
		if err != nil {
			return nil, err
		}

		replicaCount, err := c.ds.GetSettingAsInt(types.SettingNameDefaultReplicaCount)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get replica count for share manager volume %v", sm.Name)
		}

		if _, err = c.ds.CreateVolume(c.createVolumeManifest(sm, engineImage, int(replicaCount))); err != nil {
			return nil, errors.Wrapf(err, "failed to create longhorn volume for share manager %v", sm.Name)
		}
	}

	// check if we need to create the pv
	if _, err := c.ds.GetPersisentVolume(sm.Name); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to get pv for share manager %v", sm.Name)
		}

		storageClassName, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultLonghornStaticStorageClass)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get storage class for share manager pv %v", sm.Name)
		}

		if _, err = c.ds.CreatePersisentVolume(c.createPersistentVolumeManifest(sm, storageClassName)); err != nil {
			return nil, errors.Wrapf(err, "failed to create pv for share manager %v", sm.Name)
		}
	}

	// check if we need to create the pvc
	if _, err := c.ds.GetPersisentVolumeClaim(c.namespace, sm.Name); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to get pvc for share manager %v", sm.Name)
		}

		storageClassName, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultLonghornStaticStorageClass)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get storage class for share manager pvc %v", sm.Name)
		}

		if _, err = c.ds.CreatePersisentVolumeClaim(c.namespace, c.createPersistentVolumeClaimManifest(sm, storageClassName)); err != nil {
			return nil, errors.Wrapf(err, "failed to create pvc for share manager %v", sm.Name)
		}
	}

	// TODO: do we want to set guaranteed cpu?
	pod, err := c.ds.CreatePod(c.createPodManifest(sm, tolerations, imagePullPolicy, nil, registrySecret, priorityClass))
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

func (c *ShareManagerController) createVolumeManifest(sm *longhorn.ShareManager, engineImage string, replicaCount int) *longhorn.Volume {
	vol := &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sm.Name,
			Namespace: sm.Namespace,
			Labels:    types.GetShareManagerInstanceLabel(sm.Name),
			// currently a volume only supports a single owner ref (engine)
		},
		Spec: types.VolumeSpec{
			Size:             c.volumeSize,
			Frontend:         types.VolumeFrontendBlockDev,
			EngineImage:      engineImage,
			NumberOfReplicas: replicaCount,
			DataLocality:     types.DataLocalityBestEffort,
		},
	}

	return vol
}

func (c *ShareManagerController) createPersistentVolumeManifest(sm *longhorn.ShareManager, storageClass string) *v1.PersistentVolume {
	pv := datastore.NewPVManifest(c.volumeSize, sm.Name, sm.Name, storageClass, ShareManagerDefaultVolumeFS, nil)
	pv.Labels = types.GetShareManagerInstanceLabel(sm.Name)
	pv.OwnerReferences = datastore.GetOwnerReferencesForShareManager(sm, false)

	// don't need this data anymore after deletion of the pvc
	// since it's just runtime data for the nfs-server
	pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimDelete
	return pv
}

func (c *ShareManagerController) createPersistentVolumeClaimManifest(sm *longhorn.ShareManager, storageClass string) *v1.PersistentVolumeClaim {
	pvc := datastore.NewPVCManifest(c.volumeSize, sm.Name, sm.Namespace, sm.Name, storageClass)
	pvc.Labels = types.GetShareManagerInstanceLabel(sm.Name)
	pvc.OwnerReferences = datastore.GetOwnerReferencesForShareManager(sm, false)
	return pvc
}

func (c *ShareManagerController) createPodManifest(sm *longhorn.ShareManager, tolerations []v1.Toleration, pullPolicy v1.PullPolicy,
	resourceReq *v1.ResourceRequirements, registrySecret string, priorityClass string) *v1.Pod {
	privileged := true
	podSpec := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            sm.Name,
			Namespace:       sm.Namespace,
			Labels:          types.GetShareManagerLabels(sm.Name, sm.Status.OwnerID, sm.Spec.Image),
			OwnerReferences: datastore.GetOwnerReferencesForShareManager(sm, true),
		},
		Spec: v1.PodSpec{
			ServiceAccountName: c.serviceAccount,
			Tolerations:        tolerations,
			PriorityClassName:  priorityClass,
			Containers: []v1.Container{
				{
					Name:            types.LognhornLabelShareManager,
					Image:           sm.Spec.Image,
					ImagePullPolicy: pullPolicy,
					// Command: []string{"longhorn-share-manager"},
					Args: []string{"--debug", "daemon", "--listen", "0.0.0.0:8500"},
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
					LivenessProbe: &v1.Probe{
						Handler: v1.Handler{
							Exec: &v1.ExecAction{
								Command: []string{"ls", "/export"},
							},
						},
						InitialDelaySeconds: managerProbeInitialDelay,
						PeriodSeconds:       managerProbePeriodSeconds,
						FailureThreshold:    managerLivenessProbeFailureThreshold,
					},
					// TODO: replace with grpc based ready & liveness probe
					/*
						LivenessProbe: &v1.Probe{
							Handler: v1.Handler{
								Exec: &v1.ExecAction{
									Command: []string{"/usr/local/bin/grpc_health_probe", "-addr=:8500"},
								},
							},
							InitialDelaySeconds: managerProbeInitialDelay,
							PeriodSeconds:       managerProbePeriodSeconds,
							FailureThreshold:    managerLivenessProbeFailureThreshold,
						},*/
					SecurityContext: &v1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			NodeName:      sm.Status.OwnerID, // TODO: using ownerID as current nodeID for testing
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	// host mount the devices, so we can mount each
	// shared longhorn-volume into the export folder
	hostToContainer := v1.MountPropagationHostToContainer
	podSpec.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
		{
			MountPath:        "/host/dev",
			Name:             "dev",
			MountPropagation: &hostToContainer,
		},
		{
			MountPath: "/export",
			Name:      "export",
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
		{
			Name: "export",
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: sm.Name,
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
	return isControllerResponsibleFor(c.controllerID, c.ds, sm.Name, sm.Spec.NodeID, sm.Status.OwnerID)
}
