package controller

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

const (
	unknownReplicaPrefix = "UNKNOWN-"

	EngineFrontendBlockDev = "tgt-blockdev"
	EngineFrontendISCSI    = "tgt-iscsi"

	engineReadinessProbeInitialDelay     = 1
	engineReadinessProbePeriodSeconds    = 1
	engineReadinessProbeFailureThreshold = 15
)

var (
	ownerKindEngine = longhorn.SchemeGroupVersion.WithKind("Engine").String()

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

	eStoreSynced cache.InformerSynced
	pStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

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
	podInformer coreinformers.PodInformer,
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
		pStoreSynced:  podInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-engine"),

		engines:                  engines,
		engineMonitorMutex:       &sync.RWMutex{},
		engineMonitorMap:         map[string]chan struct{}{},
		engineMonitoringRemoveCh: make(chan string, 1),
	}
	ec.instanceHandler = NewInstanceHandler(podInformer, kubeClient, namespace, ec, ec.eventRecorder)

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
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ec.enqueueControlleeChange(obj)
		},
		UpdateFunc: func(old, cur interface{}) {
			ec.enqueueControlleeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			ec.enqueueControlleeChange(obj)
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

	if !controller.WaitForCacheSync("longhorn engines", stopCh, ec.eStoreSynced, ec.pStoreSynced) {
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

	// Not ours
	if engine.Spec.OwnerID != ec.controllerID {
		return nil
	}

	if engine.DeletionTimestamp != nil {
		// don't go through state transition because it can go wrong
		if err := ec.instanceHandler.DeleteInstanceForObject(engine); err != nil {
			return err
		}
		return ec.ds.RemoveFinalizerForEngine(engine)
	}

	existingEngine := engine.DeepCopy()
	defer func() {
		// we're going to update engine assume things changes
		if err == nil && !reflect.DeepEqual(existingEngine, engine) {
			_, err = ec.ds.UpdateEngine(engine)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict", key)
			ec.enqueueEngine(engine)
			err = nil
		}
	}()

	if err := ec.instanceHandler.ReconcileInstanceState(engine, &engine.Spec.InstanceSpec, &engine.Status.InstanceStatus); err != nil {
		return err
	}

	if engine.Status.CurrentState == types.InstanceStateRunning {
		// we allow across monitoring temporaily due to migration case
		if !ec.isMonitoring(engine) {
			ec.startMonitoring(engine)
		} else if engine.Status.CurrentImage != engine.Spec.EngineImage && len(engine.Spec.UpgradedReplicaAddressMap) != 0 {
			if err := ec.Upgrade(engine); err != nil {
				return err
			}
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

func validateEngine(e *longhorn.Engine) error {
	if e.Spec.VolumeName == "" ||
		len(e.Spec.ReplicaAddressMap) == 0 ||
		e.Spec.NodeID == "" {
		return fmt.Errorf("missing required field %+v", e)
	}
	return nil
}

func (ec *EngineController) CreatePodSpec(obj interface{}) (*v1.Pod, error) {
	var (
		frontend         string
		readinessHandler v1.Handler
		err              error
	)
	e, ok := obj.(*longhorn.Engine)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for engine pod spec creation: %v", obj)
	}
	if err := validateEngine(e); err != nil {
		logrus.Errorf("Invalid spec for create controller: %v", e)
		return nil, err
	}

	if e.Spec.Frontend == types.VolumeFrontendBlockDev {
		frontend = EngineFrontendBlockDev
		readinessHandler = v1.Handler{
			Exec: &v1.ExecAction{
				Command: []string{
					"ls", "/dev/longhorn/" + e.Spec.VolumeName,
				},
			},
		}
	} else if e.Spec.Frontend == types.VolumeFrontendISCSI {
		frontend = EngineFrontendISCSI
		readinessHandler, err = ec.getDefaultReadinessHandler()
		if err != nil {
			return nil, err
		}
	} else if e.Spec.Frontend == "" {
		frontend = ""
		readinessHandler, err = ec.getDefaultReadinessHandler()
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("unknown volume frontend %v", e.Spec.Frontend)
	}

	cmd := []string{
		"engine-launcher", "start",
		"--launcher-listen", "0.0.0.0:" + engineapi.EngineLauncherDefaultPort,
		"--longhorn-binary", types.DefaultEngineBinaryPath,
		"--listen", "0.0.0.0:" + engineapi.ControllerDefaultPort,
		"--size", strconv.FormatInt(e.Spec.VolumeSize, 10),
	}
	if frontend != "" {
		cmd = append(cmd, "--frontend", frontend)
	}

	for _, ip := range e.Spec.ReplicaAddressMap {
		url := engineapi.GetReplicaDefaultURL(ip)
		cmd = append(cmd, "--replica", url)
	}
	cmd = append(cmd, e.Spec.VolumeName)

	privilege := true
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      e.Name,
			Namespace: e.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: longhorn.SchemeGroupVersion.String(),
					Kind:       ownerKindEngine,
					UID:        e.UID,
					Name:       e.Name,
				},
			},
		},
		Spec: v1.PodSpec{
			NodeName:      e.Spec.NodeID,
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:    e.Name,
					Image:   e.Spec.EngineImage,
					Command: cmd,
					SecurityContext: &v1.SecurityContext{
						Privileged: &privilege,
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "dev",
							MountPath: "/host/dev",
						},
						{
							Name:      "proc",
							MountPath: "/host/proc",
						},
						{
							Name:      "engine-binaries",
							MountPath: types.EngineBinaryDirectoryInContainer,
						},
					},
					ReadinessProbe: &v1.Probe{
						Handler:             readinessHandler,
						InitialDelaySeconds: engineReadinessProbeInitialDelay,
						PeriodSeconds:       engineReadinessProbePeriodSeconds,
						FailureThreshold:    engineReadinessProbeFailureThreshold,
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "dev",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/dev",
						},
					},
				},
				{
					Name: "proc",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/proc",
						},
					},
				},
				{
					Name: "engine-binaries",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: types.EngineBinaryDirectoryOnHost,
						},
					},
				},
			},
		},
	}
	resourceReq, err := GetGuaranteedResourceRequirement(ec.ds)
	if err != nil {
		return nil, err
	}
	if resourceReq != nil {
		pod.Spec.Containers[0].Resources = *resourceReq
	}
	return pod, nil
}

func (ec *EngineController) enqueueControlleeChange(obj interface{}) {
	metaObj, err := meta.Accessor(obj)
	if err != nil {
		logrus.Warnf("BUG: %v cannot be convert to metav1.Object: %v", obj, err)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		if ref.Kind != ownerKindEngine {
			continue
		}
		namespace := metaObj.GetNamespace()
		ec.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (ec *EngineController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != ownerKindEngine {
		return
	}
	engine, err := ec.ds.GetEngine(ref.Name)
	if err != nil {
		return
	}
	if engine.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	// Not ours
	if engine.Spec.OwnerID != ec.controllerID {
		return
	}
	ec.enqueueEngine(engine)
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
	if _, err := ec.ds.ResetEngineMonitoringStatus(e); err != nil {
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
		if engine.Spec.OwnerID != m.controllerID {
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

		if err := m.refresh(engine); err != nil {
			utilruntime.HandleError(errors.Wrapf(err, "fail to update status for engine %v", m.Name))
		}
	}, EnginePollInterval, m.stopCh)
}

func (m *EngineMonitor) stop(e *longhorn.Engine) {
	if e != nil {
		if _, err := m.ds.ResetEngineMonitoringStatus(e); err != nil {
			utilruntime.HandleError(errors.Wrapf(err, "failed to update engine %v to stop monitoring", m.Name))
			// better luck next time
			return
		}
	}
	m.stopCh <- struct{}{}
}

func (m *EngineMonitor) refresh(engine *longhorn.Engine) error {
	addressReplicaMap := map[string]string{}
	for replica, address := range engine.Spec.ReplicaAddressMap {
		if addressReplicaMap[address] != "" {
			return fmt.Errorf("invalid ReplicaAddressMap: duplicate IPs")
		}
		addressReplicaMap[address] = replica
	}

	client, err := GetClientForEngine(engine, m.engines, engine.Status.CurrentImage)
	if err != nil {
		return err
	}

	endpoint, err := client.Endpoint()
	if err != nil {
		return err
	}
	engine.Status.Endpoint = endpoint

	replicaURLModeMap, err := client.ReplicaList()
	if err != nil {
		return err
	}

	currentReplicaModeMap := map[string]types.ReplicaMode{}
	for url, r := range replicaURLModeMap {
		ip := engineapi.GetIPFromURL(url)
		replica, exists := addressReplicaMap[ip]
		if !exists {
			// we have a entry doesn't exist in our spec
			replica = unknownReplicaPrefix + ip
		}
		currentReplicaModeMap[replica] = r.Mode

		if engine.Status.ReplicaModeMap != nil {
			if r.Mode != engine.Status.ReplicaModeMap[replica] {
				switch r.Mode {
				case types.ReplicaModeERR:
					m.eventRecorder.Eventf(engine, v1.EventTypeWarning, EventReasonFaulted, "Detected replica %v (%v) in error", replica, ip)
				case types.ReplicaModeWO:
					m.eventRecorder.Eventf(engine, v1.EventTypeNormal, EventReasonRebuilding, "Detected rebuilding replica %v (%v)", replica, ip)
				case types.ReplicaModeRW:
					m.eventRecorder.Eventf(engine, v1.EventTypeNormal, EventReasonRebuilded, "Detected replica %v (%v) has been rebuilded", replica, ip)
				default:
					logrus.Errorf("Invalid engine replica mode %v", r.Mode)
				}
			}
		}
	}
	if !reflect.DeepEqual(engine.Status.ReplicaModeMap, currentReplicaModeMap) {
		engine.Status.ReplicaModeMap = currentReplicaModeMap
	}

	needRestore := false
	if engine.Status.LastRestoredBackup != "" {
		info, err := client.Info()
		if err != nil {
			return err
		}
		// for those engine just restored from backup, info.LastRestored is empty
		if info.LastRestored != "" {
			engine.Status.LastRestoredBackup = info.LastRestored
		}
		if !info.IsRestoring && engine.Spec.RequestedBackupRestore != engine.Status.LastRestoredBackup {
			needRestore = true
		}
	}

	engine, err = m.ds.UpdateEngine(engine)
	if err != nil {
		return err
	}

	if !needRestore {
		return nil
	}

	go func(client engineapi.EngineClient, engine *longhorn.Engine) {
		if engine.Spec.RequestedBackupRestore == "" || engine.Spec.RequestedBackupRestore == engine.Status.LastRestoredBackup {
			return
		}

		backupTarget, err := manager.GenerateBackupTarget(m.ds)
		if err != nil {
			logrus.Errorf("cannot generate BackupTarget for engine %v: %v", engine.Name, err)
			return
		}

		logrus.Infof("engine %v prepared to do incremental restore, backup target %v, backup volume %v, backup name %v",
			engine.Name, backupTarget.URL, engine.Spec.BackupVolume, engine.Spec.RequestedBackupRestore)

		err = client.BackupRestoreIncrementally(backupTarget.URL, engine.Spec.RequestedBackupRestore, engine.Spec.BackupVolume, engine.Status.LastRestoredBackup)
		if err != nil {
			logrus.Errorf("failed to do incremental restoration in engine monitor: %v", err)
			return
		}

		engine.Status.LastRestoredBackup = engine.Spec.RequestedBackupRestore
		engine, err = m.ds.UpdateEngine(engine)
		if err != nil {
			logrus.Errorf("failed to do update engine %v after incremental restoration: %v", engine.Name, err)
			return
		}
	}(client, engine)

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
	client, err = engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:  e.Spec.VolumeName,
		EngineImage: image,
		IP:          e.Status.IP,
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (ec *EngineController) removeUnknownReplica(e *longhorn.Engine) error {
	unknownReplicaIPs := []string{}
	for replica := range e.Status.ReplicaModeMap {
		// unknown replicas have been named as `unknownReplicaPrefix-<IP>`
		if strings.HasPrefix(replica, unknownReplicaPrefix) {
			unknownReplicaIPs = append(unknownReplicaIPs, strings.TrimPrefix(replica, unknownReplicaPrefix))
		}
	}
	if len(unknownReplicaIPs) == 0 {
		return nil
	}

	client, err := GetClientForEngine(e, ec.engines, e.Status.CurrentImage)
	if err != nil {
		return err
	}
	for _, ip := range unknownReplicaIPs {
		go func(ip string) {
			url := engineapi.GetReplicaDefaultURL(ip)
			if err := client.ReplicaRemove(url); err != nil {
				ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedDeleting, "Failed to remove unknown replica IP %v from engine: %v", ip, err)
			} else {
				ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonDelete, "Removed unknown replica IP %v from engine", ip)
			}
		}(ip)
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
	for replica, ip := range e.Spec.ReplicaAddressMap {
		// one is enough
		if !replicaExists[replica] {
			return ec.startRebuilding(e, replica, ip)
		}
	}
	return nil
}

func doesIPExistInEngine(ip string, client engineapi.EngineClient) (bool, error) {
	replicaURLModeMap, err := client.ReplicaList()
	if err != nil {
		return false, err
	}
	for url := range replicaURLModeMap {
		// the replica has been rebuilt or in the process already
		if ip == engineapi.GetIPFromURL(url) {
			return true, nil
		}
	}
	return false, nil
}

func (ec *EngineController) startRebuilding(e *longhorn.Engine, replica, ip string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start rebuild for %v of %v", replica, e.Name)
	}()

	client, err := GetClientForEngine(e, ec.engines, e.Status.CurrentImage)
	if err != nil {
		return err
	}

	// we need to know the current status, since ReplicaAddressMap may
	// haven't been updated since last rebuild
	alreadyExists, err := doesIPExistInEngine(ip, client)
	if err != nil {
		return err
	}
	// replica has already been added to the engine
	if alreadyExists {
		logrus.Debugf("replica %v ip %v has been added to the engine already", replica, ip)
		return nil
	}

	replicaURL := engineapi.GetReplicaDefaultURL(ip)
	go func() {
		// start rebuild
		ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonRebuilding, "Start rebuilding replica %v with IP %v for %v", replica, ip, e.Spec.VolumeName)
		if err := client.ReplicaAdd(replicaURL); err != nil {
			logrus.Errorf("Failed rebuilding %v of %v: %v", ip, e.Spec.VolumeName, err)
			ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedRebuilding, "Failed rebuilding replica with IP %v: %v", ip, err)
			// we've sent out event to notify user. we don't want to
			// automatically handle it because it may cause chain
			// reaction to create numerous new replicas if we set
			// the replica to failed.
			// user can decide to delete it then we will try again
			if err := client.ReplicaRemove(replicaURL); err != nil {
				logrus.Errorf("Failed to remove rebuilding replica %v of %v due to rebuilding failure: %v", ip, e.Spec.VolumeName, err)
				ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedDeleting,
					"Failed to remove rebuilding replica %v with ip %v for %v due to rebuilding failure: %v", replica, ip, e.Spec.VolumeName, err)
			} else {
				logrus.Errorf("Removed failed rebuilding replica %v of %v", ip, e.Spec.VolumeName)
			}
			return
		}
		ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonRebuilded,
			"Replica %v with IP %v has been rebuilded for volume %v", replica, ip, e.Spec.VolumeName)
	}()
	//wait until engine confirmed that rebuild started
	if err := wait.PollImmediate(EnginePollInterval, EnginePollTimeout, func() (bool, error) {
		return doesIPExistInEngine(ip, client)
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
		for _, ip := range e.Spec.UpgradedReplicaAddressMap {
			replicaURLs = append(replicaURLs, engineapi.GetReplicaDefaultURL(ip))
		}
		binary := types.GetEngineBinaryDirectoryInContainerForImage(e.Spec.EngineImage) + "/longhorn"
		logrus.Debugf("About to upgrade %v from %v to %v for %v",
			e.Name, e.Status.CurrentImage, e.Spec.EngineImage, e.Spec.VolumeName)
		if err := client.Upgrade(binary, replicaURLs); err != nil {
			return err
		}
	}
	logrus.Debugf("Engine %v has been upgraded from %v to %v", e.Name, e.Status.CurrentImage, e.Spec.EngineImage)
	e.Status.CurrentImage = e.Spec.EngineImage
	// reset ReplicaModeMap to reflect the new replicas
	e.Status.ReplicaModeMap = nil
	e.Spec.ReplicaAddressMap = e.Spec.UpgradedReplicaAddressMap
	e.Spec.UpgradedReplicaAddressMap = map[string]string{}
	return nil
}

func (ec *EngineController) getDefaultReadinessHandler() (readinessHandler v1.Handler, err error) {
	readinessHandler = v1.Handler{
		Exec: &v1.ExecAction{
			Command: []string{"/usr/bin/grpc_health_probe", fmt.Sprintf("-addr=:%s", engineapi.ControllerDefaultPort)},
		},
	}
	return readinessHandler, err
}
