package controller

import (
	"fmt"
	"reflect"
	"strings"
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
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	imapi "github.com/longhorn/longhorn-engine/pkg/instance-manager/api"
	imclient "github.com/longhorn/longhorn-engine/pkg/instance-manager/client"
	imutil "github.com/longhorn/longhorn-engine/pkg/instance-manager/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

const (
	unknownReplicaPrefix = "UNKNOWN-"

	EngineFrontendBlockDev = "tgt-blockdev"
	EngineFrontendISCSI    = "tgt-iscsi"
)

var (
	EnginePollInterval = 5 * time.Second
	EnginePollTimeout  = 30 * time.Second
)

type EngineController struct {
	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	eStoreSynced  cache.InformerSynced
	imStoreSynced cache.InformerSynced

	backoff *flowcontrol.Backoff
	queue   workqueue.RateLimitingInterface

	instanceHandler *InstanceHandler

	engines                  engineapi.EngineClientCollection
	engineMonitorMutex       *sync.RWMutex
	engineMonitorMap         map[string]chan struct{}
	engineMonitoringRemoveCh chan string
}

type EngineMonitor struct {
	namespace     string
	ds            *datastore.DataStore
	eventRecorder record.EventRecorder

	Name    string
	engines engineapi.EngineClientCollection
	stopCh  chan struct{}

	controllerID string
	// used to notify the controller that monitoring has stopped
	monitoringRemoveCh chan string
}

func NewEngineController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	engineInformer lhinformers.EngineInformer,
	instanceManagerInformer lhinformers.InstanceManagerInformer,
	kubeClient clientset.Interface,
	engines engineapi.EngineClientCollection,
	namespace string, controllerID string) *EngineController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	ec := &EngineController{
		ds:        ds,
		namespace: namespace,

		controllerID:  controllerID,
		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-engine-controller"}),
		eStoreSynced:  engineInformer.Informer().HasSynced,
		imStoreSynced: instanceManagerInformer.Informer().HasSynced,

		backoff: flowcontrol.NewBackOff(time.Second*10, time.Minute*5),
		queue:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-engine"),

		engines:                  engines,
		engineMonitorMutex:       &sync.RWMutex{},
		engineMonitorMap:         map[string]chan struct{}{},
		engineMonitoringRemoveCh: make(chan string, 1),
	}
	ec.instanceHandler = NewInstanceHandler(ds, ec, ec.eventRecorder)

	engineInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			e := obj.(*longhorn.Engine)
			ec.enqueueEngine(e)
		},
		UpdateFunc: func(old, cur interface{}) {
			curE := cur.(*longhorn.Engine)
			ec.enqueueEngine(curE)
		},
		DeleteFunc: func(obj interface{}) {
			e := obj.(*longhorn.Engine)
			ec.enqueueEngine(e)
		},
	})
	instanceManagerInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			ec.enqueueInstanceManagerChange(im)
		},
		UpdateFunc: func(old, cur interface{}) {
			curIM := cur.(*longhorn.InstanceManager)
			ec.enqueueInstanceManagerChange(curIM)
		},
		DeleteFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			ec.enqueueInstanceManagerChange(im)
		},
	})

	go ec.monitoringUpdater()
	return ec
}

func (ec *EngineController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ec.queue.ShutDown()

	logrus.Infof("Start Longhorn engine controller")
	defer logrus.Infof("Shutting down Longhorn engine controller")

	if !controller.WaitForCacheSync("longhorn engines", stopCh, ec.eStoreSynced, ec.imStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(ec.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (ec *EngineController) worker() {
	for ec.processNextWorkItem() {
	}
}

func (ec *EngineController) processNextWorkItem() bool {
	key, quit := ec.queue.Get()

	if quit {
		return false
	}
	defer ec.queue.Done(key)

	err := ec.syncEngine(key.(string))
	ec.handleErr(err, key)

	return true
}

func (ec *EngineController) handleErr(err error, key interface{}) {
	if err == nil {
		ec.queue.Forget(key)
		return
	}

	if ec.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn engine %v: %v", key, err)
		ec.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn engine %v out of the queue: %v", key, err)
	ec.queue.Forget(key)
}

func (ec *EngineController) syncEngine(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync engine for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != ec.namespace {
		// Not ours, don't do anything
		return nil
	}

	engine, err := ec.ds.GetEngine(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			logrus.Infof("Longhorn engine %v has been deleted", key)
			return nil
		}
		return err
	}

	if engine.Status.OwnerID != ec.controllerID {
		if !ec.isResponsibleFor(engine) {
			// Not ours
			return nil
		}
		engine.Status.OwnerID = ec.controllerID
		engine, err = ec.ds.UpdateEngineStatus(engine)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		logrus.Debugf("Engine controller %v picked up %v", ec.controllerID, engine.Name)
	}

	if engine.DeletionTimestamp != nil {
		if err := ec.DeleteInstance(engine); err != nil {
			if !types.ErrorIsNotFound(err) {
				return errors.Wrapf(err, "failed to cleanup the related engine process before deleting engine %v", engine.Name)
			}
		}
		return ec.ds.RemoveFinalizerForEngine(engine)
	}

	existingEngine := engine.DeepCopy()
	defer func() {
		// we're going to update engine assume things changes
		if err == nil && !reflect.DeepEqual(existingEngine.Status, engine.Status) {
			_, err = ec.ds.UpdateEngineStatus(engine)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict: %v", key, err)
			ec.enqueueEngine(engine)
			err = nil
		}
	}()

	isCLIAPIVersionOne := false
	if engine.Status.CurrentImage != "" {
		isCLIAPIVersionOne, err = ec.ds.IsEngineImageCLIAPIVersionOne(engine.Status.CurrentImage)
		if err != nil {
			return err
		}
	}

	if len(engine.Spec.UpgradedReplicaAddressMap) != 0 {
		if err := ec.Upgrade(engine); err != nil {
			return err
		}
	} else {
		engine.Status.CurrentReplicaAddressMap = engine.Spec.ReplicaAddressMap
	}

	if err := ec.instanceHandler.ReconcileInstanceState(engine, &engine.Spec.InstanceSpec, &engine.Status.InstanceStatus); err != nil {
		return err
	}

	// For incompatible engine, skip starting engine monitor and clean up fields when the engine is not running
	if isCLIAPIVersionOne {
		if engine.Status.CurrentState != types.InstanceStateRunning {
			engine.Status.Endpoint = ""
			engine.Status.ReplicaModeMap = nil
		}
		return nil
	}

	if engine.Status.CurrentState == types.InstanceStateRunning {
		// we allow across monitoring temporaily due to migration case
		if !ec.isMonitoring(engine) {
			ec.startMonitoring(engine)
		} else if engine.Status.ReplicaModeMap != nil {
			if err := ec.ReconcileEngineState(engine); err != nil {
				return err
			}
		}
	} else if ec.isMonitoring(engine) {
		// engine is not running
		ec.stopMonitoring(engine)
	}

	return nil
}

func (ec *EngineController) enqueueEngine(e *longhorn.Engine) {
	key, err := controller.KeyFunc(e)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", e, err))
		return
	}

	ec.queue.AddRateLimited(key)
}

func (ec *EngineController) enqueueInstanceManagerChange(im *longhorn.InstanceManager) {
	imType, err := datastore.CheckInstanceManagerType(im)
	if err != nil || imType != types.InstanceManagerTypeEngine {
		return
	}

	engineMap := map[string]*longhorn.Engine{}

	es, err := ec.ds.ListEnginesRO()
	if err != nil {
		logrus.Warnf("Engine controller: failed to list engines: %v", err)
	}
	for _, e := range es {
		// when attaching, instance manager name is not available
		// when detaching, node ID is not available
		if e.Spec.NodeID == im.Spec.NodeID || e.Status.InstanceManagerName == im.Name {
			engineMap[e.Name] = e
		}
	}

	for _, e := range engineMap {
		if e.Status.OwnerID == ec.controllerID {
			ec.enqueueEngine(e)
		}
	}
	return
}

func (ec *EngineController) getEngineManagerClient(instanceManagerName string) (*imclient.EngineManagerClient, error) {
	im, err := ec.ds.GetInstanceManager(instanceManagerName)
	if err != nil {
		return nil, fmt.Errorf("cannot find Instance Manager %v", instanceManagerName)
	}
	if im.Status.CurrentState != types.InstanceManagerStateRunning || im.Status.IP == "" {
		return nil, fmt.Errorf("invalid Instance Manager %v", instanceManagerName)
	}

	return imclient.NewEngineManagerClient(imutil.GetURL(im.Status.IP, engineapi.InstanceManagerDefaultPort)), nil
}

func (ec *EngineController) CreateInstance(obj interface{}) (*types.InstanceProcess, error) {
	e, ok := obj.(*longhorn.Engine)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for engine process creation: %v", obj)
	}
	if e.Spec.VolumeName == "" || e.Spec.NodeID == "" {
		return nil, fmt.Errorf("missing parameters for engine process creation: %v", e)
	}

	im, err := ec.ds.GetInstanceManagerByInstance(obj)
	if err != nil {
		return nil, err
	}

	frontend := ""
	if e.Spec.Frontend == types.VolumeFrontendBlockDev {
		frontend = EngineFrontendBlockDev
	} else if e.Spec.Frontend == types.VolumeFrontendISCSI {
		frontend = EngineFrontendISCSI
	} else if e.Spec.Frontend == "" || e.Spec.DisableFrontend {
		frontend = ""
	} else {
		return nil, fmt.Errorf("unknown volume frontend %v", e.Spec.Frontend)
	}

	if e.Spec.DisableFrontend {
		frontend = ""
	}

	replicas := []string{}
	for _, addr := range e.Status.CurrentReplicaAddressMap {
		replicas = append(replicas, engineapi.GetBackendReplicaURL(addr))
	}

	c, err := ec.getEngineManagerClient(im.Name)
	if err != nil {
		return nil, err
	}

	engineProcess, err := c.EngineCreate(
		e.Spec.VolumeSize, e.Name, e.Spec.VolumeName,
		types.DefaultEngineBinaryPath, "", "0.0.0.0", frontend, []string{}, replicas)
	if err != nil {
		return nil, err
	}

	return engineapi.EngineProcessToInstanceProcess(engineProcess), nil
}

func (ec *EngineController) DeleteInstance(obj interface{}) error {
	e, ok := obj.(*longhorn.Engine)
	if !ok {
		return fmt.Errorf("BUG: invalid object for engine process deletion: %v", obj)
	}

	if err := ec.deleteInstanceWithCLIAPIVersionOne(e); err != nil {
		return err
	}

	// Not assigned, safe to delete
	if e.Status.InstanceManagerName == "" {
		return nil
	}

	im, err := ec.ds.GetInstanceManager(e.Status.InstanceManagerName)
	if err != nil {
		return err
	}

	// Node down
	if im.Spec.NodeID != im.Status.OwnerID {
		isDown, err := ec.ds.IsNodeDownOrDeleted(im.Spec.NodeID)
		if err != nil {
			return err
		}
		if isDown {
			delete(im.Status.Instances, e.Name)
			if _, err := ec.ds.UpdateInstanceManagerStatus(im); err != nil {
				return err
			}
			return nil
		}
	}

	c, err := ec.getEngineManagerClient(e.Status.InstanceManagerName)
	if err != nil {
		return err
	}

	if _, err := c.EngineDelete(e.Name); err != nil && !types.ErrorIsNotFound(err) {
		return err
	}

	return nil
}

func (ec *EngineController) deleteInstanceWithCLIAPIVersionOne(e *longhorn.Engine) (err error) {
	isCLIAPIVersionOne := false
	if e.Status.CurrentImage != "" {
		isCLIAPIVersionOne, err = ec.ds.IsEngineImageCLIAPIVersionOne(e.Status.CurrentImage)
		if err != nil {
			return err
		}
	}

	if isCLIAPIVersionOne {
		pod, err := ec.kubeClient.CoreV1().Pods(ec.namespace).Get(e.Name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to get pod for old engine %v", e.Name)
		}
		if apierrors.IsNotFound(err) {
			pod = nil
		}

		logrus.Debugf("Prepared to delete old version engine %v with running pod", e.Name)
		if err := ec.deleteOldEnginePod(pod, e); err != nil {
			return err
		}
	}
	return nil
}

func (ec *EngineController) deleteOldEnginePod(pod *v1.Pod, e *longhorn.Engine) (err error) {
	// pod already stopped
	if pod == nil {
		return nil
	}

	if pod.DeletionTimestamp != nil {
		if pod.DeletionGracePeriodSeconds != nil && *pod.DeletionGracePeriodSeconds != 0 {
			// force deletion in the case of node lost
			deletionDeadline := pod.DeletionTimestamp.Add(time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second)
			now := time.Now().UTC()
			if now.After(deletionDeadline) {
				logrus.Debugf("engine pod %v still exists after grace period %v passed, force deletion: now %v, deadline %v",
					pod.Name, pod.DeletionGracePeriodSeconds, now, deletionDeadline)
				gracePeriod := int64(0)
				if err := ec.kubeClient.CoreV1().Pods(ec.namespace).Delete(pod.Name, &metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod}); err != nil {
					logrus.Debugf("failed to force deleting engine pod %v: %v ", pod.Name, err)
					return nil
				}
			}
		}
		return nil
	}

	if err := ec.kubeClient.CoreV1().Pods(ec.namespace).Delete(pod.Name, nil); err != nil {
		ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedStopping, "Error stopping pod for old engine %v: %v", pod.Name, err)
		return nil
	}
	ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonStop, "Stops pod for old engine %v", pod.Name)
	return nil
}

func (ec *EngineController) GetInstance(obj interface{}) (*types.InstanceProcess, error) {
	e, ok := obj.(*longhorn.Engine)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for engine process get: %v", obj)
	}

	c, err := ec.getEngineManagerClient(e.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}

	engineProcess, err := c.EngineGet(e.Name)
	if err != nil {
		return nil, err
	}

	return engineapi.EngineProcessToInstanceProcess(engineProcess), nil
}

func (ec *EngineController) LogInstance(obj interface{}) (*imapi.LogStream, error) {
	e, ok := obj.(*longhorn.Engine)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for engine process log: %v", obj)
	}

	c, err := ec.getEngineManagerClient(e.Status.InstanceManagerName)
	if err != nil {
		return nil, err
	}
	stream, err := c.EngineLog(e.Name)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

// monitoringUpdater will listen to the event from engineMonitoringRemoveCh and
// remove the entry from the current monitor map
// the engine will be added to the map when monitoring thread started
func (ec *EngineController) monitoringUpdater() {
	for engineName := range ec.engineMonitoringRemoveCh {
		ec.removeFromEngineMonitorMap(engineName)
	}
}

func (ec *EngineController) removeFromEngineMonitorMap(engineName string) {
	ec.engineMonitorMutex.Lock()
	defer ec.engineMonitorMutex.Unlock()

	stopCh, ok := ec.engineMonitorMap[engineName]
	if ok {
		close(stopCh)
		delete(ec.engineMonitorMap, engineName)
	}
}

func (ec *EngineController) isMonitoring(e *longhorn.Engine) bool {
	ec.engineMonitorMutex.RLock()
	defer ec.engineMonitorMutex.RUnlock()

	_, ok := ec.engineMonitorMap[e.Name]
	return ok
}

func (ec *EngineController) startMonitoring(e *longhorn.Engine) {
	//it's possible for monitor and engineController to send stop signal at
	//the same time, don't make it block
	stopCh := make(chan struct{}, 1)
	monitor := &EngineMonitor{
		Name:               e.Name,
		namespace:          e.Namespace,
		ds:                 ec.ds,
		eventRecorder:      ec.eventRecorder,
		engines:            ec.engines,
		stopCh:             stopCh,
		controllerID:       ec.controllerID,
		monitoringRemoveCh: ec.engineMonitoringRemoveCh,
	}

	ec.engineMonitorMutex.Lock()
	defer ec.engineMonitorMutex.Unlock()

	if _, ok := ec.engineMonitorMap[e.Name]; ok {
		logrus.Warnf("BUG: Monitoring for %v already exists", e.Name)
		return
	}
	ec.engineMonitorMap[e.Name] = stopCh

	go monitor.Run()
}

func (ec *EngineController) stopMonitoring(e *longhorn.Engine) {
	if _, err := ec.ds.ResetMonitoringEngineStatus(e); err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to update engine %v to stop monitoring", e.Name))
		// better luck next time
		return
	}

	ec.engineMonitorMutex.Lock()
	defer ec.engineMonitorMutex.Unlock()

	stopCh, ok := ec.engineMonitorMap[e.Name]
	if !ok {
		logrus.Warnf("engine %v: stop monitoring called when there is no monitoring", e.Name)
		return
	}
	stopCh <- struct{}{}
	return
}

func (m *EngineMonitor) Run() {
	logrus.Debugf("Start monitoring %v", m.Name)
	defer func() {
		m.monitoringRemoveCh <- m.Name
		logrus.Debugf("Stop monitoring %v", m.Name)
	}()

	wait.Until(func() {
		for {
			engine, err := m.ds.GetEngine(m.Name)
			if err != nil {
				if datastore.ErrorIsNotFound(err) {
					logrus.Infof("stop engine %v monitoring because the engine no longer exists", m.Name)
					m.stop(engine)
					return
				}
				utilruntime.HandleError(errors.Wrapf(err, "fail to get engine %v for monitoring", m.Name))
				return
			}

			// when engine stopped, nodeID will be empty as well
			if engine.Status.OwnerID != m.controllerID {
				logrus.Infof("stop engine %v monitoring because the engine is no longer running on node %v",
					m.Name, m.controllerID)
				m.stop(engine)
				return
			}

			// engine is maybe starting
			if engine.Status.CurrentState != types.InstanceStateRunning {
				return
			}

			// engine is upgrading
			if engine.Status.CurrentImage != engine.Spec.EngineImage {
				return
			}

			if err := m.refresh(engine); err == nil || !apierrors.IsConflict(errors.Cause(err)) {
				utilruntime.HandleError(errors.Wrapf(err, "fail to update status for engine %v", m.Name))
				break
			}
			// Retry if the error is due to conflict
		}
	}, EnginePollInterval, m.stopCh)
}

func (m *EngineMonitor) stop(e *longhorn.Engine) {
	if e != nil {
		if _, err := m.ds.ResetMonitoringEngineStatus(e); err != nil {
			utilruntime.HandleError(errors.Wrapf(err, "failed to update engine %v to stop monitoring", m.Name))
			// better luck next time
			return
		}
	}
	m.stopCh <- struct{}{}
}

func (m *EngineMonitor) refresh(engine *longhorn.Engine) error {
	addressReplicaMap := map[string]string{}
	for replica, address := range engine.Status.CurrentReplicaAddressMap {
		if addressReplicaMap[address] != "" {
			return fmt.Errorf("invalid ReplicaAddressMap: duplicate addresses")
		}
		addressReplicaMap[address] = replica
	}

	client, err := GetClientForEngine(engine, m.engines, engine.Status.CurrentImage)
	if err != nil {
		return err
	}

	replicaURLModeMap, err := client.ReplicaList()
	if err != nil {
		return err
	}

	currentReplicaModeMap := map[string]types.ReplicaMode{}
	for url, r := range replicaURLModeMap {
		addr := engineapi.GetAddressFromBackendReplicaURL(url)
		replica, exists := addressReplicaMap[addr]
		if !exists {
			// we have a entry doesn't exist in our spec
			replica = unknownReplicaPrefix + url
		}
		currentReplicaModeMap[replica] = r.Mode

		if engine.Status.ReplicaModeMap != nil {
			if r.Mode != engine.Status.ReplicaModeMap[replica] {
				switch r.Mode {
				case types.ReplicaModeERR:
					m.eventRecorder.Eventf(engine, v1.EventTypeWarning, EventReasonFaulted, "Detected replica %v (%v) in error", replica, addr)
				case types.ReplicaModeWO:
					m.eventRecorder.Eventf(engine, v1.EventTypeNormal, EventReasonRebuilding, "Detected rebuilding replica %v (%v)", replica, addr)
				case types.ReplicaModeRW:
					m.eventRecorder.Eventf(engine, v1.EventTypeNormal, EventReasonRebuilded, "Detected replica %v (%v) has been rebuilded", replica, addr)
				default:
					logrus.Errorf("Invalid engine replica mode %v", r.Mode)
				}
			}
		}
	}
	if !reflect.DeepEqual(engine.Status.ReplicaModeMap, currentReplicaModeMap) {
		engine.Status.ReplicaModeMap = currentReplicaModeMap
	}

	endpoint, err := engineapi.Endpoint(engine.Status.IP, engine.Name)
	if err != nil {
		return err
	}
	engine.Status.Endpoint = endpoint

	backupStatusList, err := client.SnapshotBackupStatus()
	if err != nil {
		logrus.Errorf("engine monitor: engine %v: failed to get backup status: %v", engine.Name, err)
	} else {
		engine.Status.BackupStatus = backupStatusList
	}

	isRestoring := false
	isConsensual := true
	rsMap, err := client.BackupRestoreStatus()
	if err != nil {
		logrus.Errorf("engine monitor: engine %v: failed to get restore status: %v", engine.Name, err)
	} else {
		engine.Status.RestoreStatus = rsMap

		lastRestored := ""
		for _, status := range rsMap {
			if status.IsRestoring {
				isRestoring = true
			}
		}

		//Engine is not restoring, pick the lastRestored from replica then update LastRestoredBackup
		if !isRestoring {
			for _, status := range rsMap {
				if lastRestored != "" && status.LastRestored != lastRestored {
					// this error shouldn't prevent the engine from updating the other status
					logrus.Errorf("BUG: engine %v: different lastRestored values even though engine is not restoring", engine.Name)
					isConsensual = false
				}
				lastRestored = status.LastRestored
			}
			if isConsensual {
				engine.Status.LastRestoredBackup = lastRestored
			}
		}
	}

	purgeStatus, err := client.SnapshotPurgeStatus()
	if err != nil {
		logrus.Errorf("engine monitor: engine %v: failed to get snapshot purge status: %v", engine.Name, err)
	} else {
		engine.Status.PurgeStatus = purgeStatus
	}

	engine, err = m.ds.UpdateEngineStatus(engine)
	if err != nil {
		return err
	}

	if err = restoreBackup(isRestoring, isConsensual, engine, client, m.ds); err != nil {
		return err
	}

	return nil
}

func needRestoration(isRestoring, isConsensual bool, engine *longhorn.Engine) bool {
	if !isRestoring && isConsensual && engine.Spec.RequestedBackupRestore != "" && engine.Spec.RequestedBackupRestore != engine.Status.LastRestoredBackup {
		return true
	}
	return false
}

func restoreBackup(isRestoring, isConsensual bool, engine *longhorn.Engine, client engineapi.EngineClient, ds *datastore.DataStore) error {
	if !needRestoration(isRestoring, isConsensual, engine) {
		return nil
	}

	if engine.Spec.BackupVolume == "" {
		return fmt.Errorf("BUG: backup volume is empty for backup restoration of engine %v", engine.Name)
	}

	backupTarget, err := manager.GenerateBackupTarget(ds)
	if err != nil {
		return errors.Wrapf(err, "cannot generate BackupTarget for backup restoration of engine %v", engine.Name)
	}

	credential, err := manager.GetBackupCredentialConfig(ds)
	if err != nil {
		return errors.Wrapf(err, "cannot get backup credential config for backup restoration of engine %v", engine.Name)
	}

	logrus.Infof("engine %v prepared to restore backup, backup target: %v, backup volume: %v, requested restored backup name: %v, last restored backup name: %v",
		engine.Name, backupTarget.URL, engine.Spec.BackupVolume, engine.Spec.RequestedBackupRestore, engine.Status.LastRestoredBackup)

	// If engine.Status.LastRestoredBackup is empty, it's full restoration
	if err = client.BackupRestore(backupTarget.URL, engine.Spec.RequestedBackupRestore, engine.Spec.BackupVolume, engine.Status.LastRestoredBackup, credential); err != nil {
		return errors.Wrapf(err, "failed to restore backup %v with last restored backup %v in engine monitor",
			engine.Spec.RequestedBackupRestore, engine.Status.LastRestoredBackup)
	}
	return nil
}

func (ec *EngineController) ReconcileEngineState(e *longhorn.Engine) error {
	if err := ec.removeUnknownReplica(e); err != nil {
		return err
	}
	if err := ec.rebuildingNewReplica(e); err != nil {
		return err
	}
	return nil
}

func GetClientForEngine(e *longhorn.Engine, engines engineapi.EngineClientCollection, image string) (client engineapi.EngineClient, err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot get client for engine %v", e.Name)
	}()
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil, fmt.Errorf("engine is not running")
	}
	if image == "" {
		return nil, fmt.Errorf("require specify engine image")
	}
	if e.Status.IP == "" || e.Status.Port == 0 {
		return nil, fmt.Errorf("require IP and Port")
	}

	client, err = engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:  e.Spec.VolumeName,
		EngineImage: image,
		IP:          e.Status.IP,
		Port:        e.Status.Port,
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (ec *EngineController) removeUnknownReplica(e *longhorn.Engine) error {
	unknownReplicaURLs := []string{}
	for replica := range e.Status.ReplicaModeMap {
		// unknown replicas have been named as `unknownReplicaPrefix-<replica URL>`
		if strings.HasPrefix(replica, unknownReplicaPrefix) {
			unknownReplicaURLs = append(unknownReplicaURLs, strings.TrimPrefix(replica, unknownReplicaPrefix))
		}
	}
	if len(unknownReplicaURLs) == 0 {
		return nil
	}

	client, err := GetClientForEngine(e, ec.engines, e.Status.CurrentImage)
	if err != nil {
		return err
	}
	for _, url := range unknownReplicaURLs {
		go func(url string) {
			if err := client.ReplicaRemove(url); err != nil {
				ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedDeleting, "Failed to remove unknown replica %v from engine: %v", url, err)
			} else {
				ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonDelete, "Removed unknown replica %v from engine", url)
			}
		}(url)
	}
	return nil
}

func (ec *EngineController) rebuildingNewReplica(e *longhorn.Engine) error {
	rebuildingInProgress := false
	replicaExists := make(map[string]bool)
	for replica, mode := range e.Status.ReplicaModeMap {
		replicaExists[replica] = true
		if mode == types.ReplicaModeWO {
			rebuildingInProgress = true
			break
		}
	}
	// We cannot rebuild more than one replica at one time
	if rebuildingInProgress {
		logrus.Debugf("Skip rebuilding for volume %v because there is rebuilding in process", e.Spec.VolumeName)
		return nil
	}
	for replica, addr := range e.Status.CurrentReplicaAddressMap {
		// one is enough
		if !replicaExists[replica] {
			return ec.startRebuilding(e, replica, addr)
		}
	}
	return nil
}

func doesAddressExistInEngine(addr string, client engineapi.EngineClient) (bool, error) {
	replicaURLModeMap, err := client.ReplicaList()
	if err != nil {
		return false, err
	}
	for url := range replicaURLModeMap {
		// the replica has been rebuilt or in the process already
		if addr == engineapi.GetAddressFromBackendReplicaURL(url) {
			return true, nil
		}
	}
	return false, nil
}

func (ec *EngineController) startRebuilding(e *longhorn.Engine, replica, addr string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start rebuild for %v of %v", replica, e.Name)
	}()

	client, err := GetClientForEngine(e, ec.engines, e.Status.CurrentImage)
	if err != nil {
		return err
	}

	// we need to know the current status, since ReplicaAddressMap may
	// haven't been updated since last rebuild
	alreadyExists, err := doesAddressExistInEngine(addr, client)
	if err != nil {
		return err
	}
	// replica has already been added to the engine
	if alreadyExists {
		logrus.Debugf("replica %v address %v has been added to the engine already", replica, addr)
		return nil
	}

	replicaURL := engineapi.GetBackendReplicaURL(addr)
	go func() {
		// start rebuild
		ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonRebuilding, "Start rebuilding replica %v with Address %v for %v", replica, addr, e.Spec.VolumeName)
		if err := client.ReplicaAdd(replicaURL); err != nil {
			logrus.Errorf("Failed rebuilding %v of %v: %v", addr, e.Spec.VolumeName, err)
			ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedRebuilding, "Failed rebuilding replica with Address %v: %v", addr, err)
			// we've sent out event to notify user. we don't want to
			// automatically handle it because it may cause chain
			// reaction to create numerous new replicas if we set
			// the replica to failed.
			// user can decide to delete it then we will try again
			if err := client.ReplicaRemove(replicaURL); err != nil {
				logrus.Errorf("Failed to remove rebuilding replica %v of %v due to rebuilding failure: %v", addr, e.Spec.VolumeName, err)
				ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedDeleting,
					"Failed to remove rebuilding replica %v with address %v for %v due to rebuilding failure: %v", replica, addr, e.Spec.VolumeName, err)
			} else {
				logrus.Errorf("Removed failed rebuilding replica %v of %v", addr, e.Spec.VolumeName)
			}
			// Before we mark the Replica as Failed automatically, we want to check the Backoff to avoid recreating new
			// Replicas too quickly. If the Replica is still in the Backoff period, we will leave the Replica alone. If
			// it is past the Backoff period, we'll try to mark the Replica as Failed and increase the Backoff period
			// for the next failure.
			if !ec.backoff.IsInBackOffSinceUpdate(e.Name, time.Now()) {
				rep, err := ec.ds.GetReplica(replica)
				if err != nil {
					logrus.Errorf("Could not get replica %v to mark failed rebuild: %v", replica, err)
					return
				}
				rep.Spec.FailedAt = util.Now()
				rep.Spec.DesireState = types.InstanceStateStopped
				if _, err := ec.ds.UpdateReplica(rep); err != nil {
					logrus.Errorf("Could not mark failed rebuild on replica %v: %v", replica, err)
					return
				}
				// Now that the Replica can actually be recreated, we can move up the Backoff.
				ec.backoff.Next(e.Name, time.Now())
				backoffTime := ec.backoff.Get(e.Name).Seconds()
				logrus.Infof("Marked failed rebuild on replica %v, backoff period is now %v seconds", replica, backoffTime)
				return
			}
			logrus.Infof("Engine %v is still in backoff for replica %v rebuild failure", e.Name, replica)
			return
		}
		// Replica rebuild succeeded, clear Backoff.
		ec.backoff.DeleteEntry(e.Name)
		ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonRebuilded,
			"Replica %v with Address %v has been rebuilded for volume %v", replica, addr, e.Spec.VolumeName)
	}()
	//wait until engine confirmed that rebuild started
	if err := wait.PollImmediate(EnginePollInterval, EnginePollTimeout, func() (bool, error) {
		return doesAddressExistInEngine(addr, client)
	}); err != nil {
		return err
	}
	return nil
}

func (ec *EngineController) Upgrade(e *longhorn.Engine) (err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot live upgrade image for %v", e.Name)
	}()

	client, err := GetClientForEngine(e, ec.engines, e.Spec.EngineImage)
	if err != nil {
		return err
	}
	version, err := client.Version(false)
	if err != nil {
		return err
	}
	// Don't use image with different image name but same commit here. It
	// will cause live replica to be removed. Volume controller should filter those.
	if err != nil || version.ClientVersion.GitCommit != version.ServerVersion.GitCommit {
		replicaURLs := []string{}
		for _, addr := range e.Spec.UpgradedReplicaAddressMap {
			replicaURLs = append(replicaURLs, engineapi.GetBackendReplicaURL(addr))
		}
		binary := types.GetEngineBinaryDirectoryInContainerForImage(e.Spec.EngineImage) + "/longhorn"
		logrus.Debugf("About to upgrade %v from %v to %v for %v",
			e.Name, e.Status.CurrentImage, e.Spec.EngineImage, e.Spec.VolumeName)

		c, err := ec.getEngineManagerClient(e.Status.InstanceManagerName)
		if err != nil {
			return err
		}
		engineProcess, err := c.EngineUpgrade(e.Spec.VolumeSize, e.Name, binary, replicaURLs)
		if err != nil {
			return err
		}
		e.Status.Port = int(engineProcess.ProcessStatus.PortStart)
	}
	logrus.Debugf("Engine %v has been upgraded from %v to %v", e.Name, e.Status.CurrentImage, e.Spec.EngineImage)
	e.Status.CurrentImage = e.Spec.EngineImage
	e.Status.CurrentReplicaAddressMap = e.Spec.UpgradedReplicaAddressMap
	// reset ReplicaModeMap to reflect the new replicas
	e.Status.ReplicaModeMap = nil
	return nil
}

func (ec *EngineController) isResponsibleFor(e *longhorn.Engine) bool {
	return isControllerResponsibleFor(ec.controllerID, ec.ds, e.Name, e.Spec.NodeID, e.Status.OwnerID)
}
