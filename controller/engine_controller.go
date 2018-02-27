package controller

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

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

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

const (
	unknownReplicaPrefix = "UNKNOWN-"
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

	engines            engineapi.EngineClientCollection
	engineMonitorMutex *sync.RWMutex
	engineMonitorMap   map[string]*EngineMonitor
}

type EngineMonitor struct {
	namespace     string
	ds            *datastore.DataStore
	eventRecorder record.EventRecorder

	Name         string
	engineClient engineapi.EngineClient
	stopCh       chan struct{}
}

func NewEngineController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	engineInformer lhinformers.ControllerInformer,
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

		engines:            engines,
		engineMonitorMutex: &sync.RWMutex{},
		engineMonitorMap:   make(map[string]*EngineMonitor),
	}
	ec.instanceHandler = NewInstanceHandler(podInformer, kubeClient, namespace, ec, ec.eventRecorder)

	engineInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			e := obj.(*longhorn.Controller)
			ec.enqueueEngine(e)
		},
		UpdateFunc: func(old, cur interface{}) {
			curE := cur.(*longhorn.Controller)
			ec.enqueueEngine(curE)
		},
		DeleteFunc: func(obj interface{}) {
			e := obj.(*longhorn.Controller)
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
		return err
	}
	if engine == nil {
		logrus.Infof("Longhorn engine %v has been deleted", key)
		return nil
	}

	// Not ours
	if engine.Spec.OwnerID != ec.controllerID {
		return nil
	}

	if engine.DeletionTimestamp != nil {
		ec.stopMonitoring(engine)

		// don't go through state transition because it can go wrong
		if err := ec.instanceHandler.DeleteInstanceForObject(engine); err != nil {
			return err
		}
		return ec.ds.RemoveFinalizerForEngine(engine)
	}

	defer func() {
		// we're going to update engine assume things changes
		if err == nil {
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
		if !ec.isMonitoring(engine) {
			ec.startMonitoring(engine)
		} else if engine.Status.ReplicaModeMap != nil {
			// wait until monitoring updated for the first time
			if err := ec.ReconcileEngineState(engine); err != nil {
				return err
			}
		}
	} else if ec.isMonitoring(engine) {
		// not running
		ec.stopMonitoring(engine)
	}

	return nil
}

func (ec *EngineController) enqueueEngine(e *longhorn.Controller) {
	key, err := controller.KeyFunc(e)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", e, err))
		return
	}

	ec.queue.AddRateLimited(key)
}

func validateEngine(e *longhorn.Controller) error {
	if e.Spec.VolumeName == "" ||
		len(e.Spec.ReplicaAddressMap) == 0 ||
		e.Spec.NodeID == "" {
		return fmt.Errorf("missing required field %+v", e)
	}
	return nil
}

func (ec *EngineController) CreatePodSpec(obj interface{}) (*v1.Pod, error) {
	e, ok := obj.(*longhorn.Controller)
	if !ok {
		return nil, fmt.Errorf("BUG: invalid object for engine pod spec creation: %v", obj)
	}
	if err := validateEngine(e); err != nil {
		logrus.Errorf("Invalid spec for create controller: %v", e)
		return nil, err
	}

	cmd := []string{
		"launch", "controller",
		"--listen", "0.0.0.0:9501",
		"--frontend", "tgt",
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
					},
					ReadinessProbe: &v1.Probe{
						Handler: v1.Handler{
							Exec: &v1.ExecAction{
								Command: []string{
									"ls", "/dev/longhorn/" + e.Spec.VolumeName,
								},
							},
						},
						InitialDelaySeconds: 5,
						PeriodSeconds:       5,
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
			},
		},
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
	if err != nil || engine == nil {
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

func (ec *EngineController) isMonitoring(e *longhorn.Controller) bool {
	ec.engineMonitorMutex.RLock()
	defer ec.engineMonitorMutex.RUnlock()

	return ec.engineMonitorMap[e.Name] != nil
}

func (ec *EngineController) startMonitoring(e *longhorn.Controller) {
	client, err := ec.getClientForEngine(e)
	if err != nil {
		logrus.Warnf("Failed to start monitoring %v, cannot create engine client", e.Name)
		return
	}
	endpoint := client.Endpoint()
	if endpoint == "" {
		logrus.Warnf("Failed to start monitoring %v, cannot connect", e.Name)
		return
	}

	e.Status.Endpoint = endpoint

	monitor := &EngineMonitor{
		Name:          e.Name,
		namespace:     e.Namespace,
		ds:            ec.ds,
		eventRecorder: ec.eventRecorder,
		engineClient:  client,
		stopCh:        make(chan struct{}),
	}

	ec.engineMonitorMutex.Lock()
	defer ec.engineMonitorMutex.Unlock()

	if ec.engineMonitorMap[e.Name] != nil {
		logrus.Warnf("BUG: Monitoring for %v already exists", e.Name)
		return
	}
	ec.engineMonitorMap[e.Name] = monitor
	go monitor.Run()
}

func (ec *EngineController) stopMonitoring(e *longhorn.Controller) {
	ec.engineMonitorMutex.Lock()
	defer ec.engineMonitorMutex.Unlock()

	monitor := ec.engineMonitorMap[e.Name]
	if monitor == nil {
		return
	}
	monitor.stopCh <- struct{}{}
	e.Status.Endpoint = ""
	e.Status.ReplicaModeMap = nil
	delete(ec.engineMonitorMap, e.Name)
}

func (m *EngineMonitor) Run() {
	logrus.Debugf("Start monitoring %v", m.Name)
	defer logrus.Debugf("Stop monitoring %v", m.Name)

	wait.Until(func() {
		if err := m.Refresh(); err != nil {
			utilruntime.HandleError(errors.Wrapf(err, "fail to update status for engine %v", m.Name))
		}
	}, EnginePollInterval, m.stopCh)
}

func (m *EngineMonitor) Refresh() error {
	engine, err := m.ds.GetEngine(m.Name)
	if err != nil {
		return err
	}
	if engine == nil {
		return fmt.Errorf("engine doesn't exist")
	}

	// Wait for stop monitoring
	if engine.Status.CurrentState != types.InstanceStateRunning {
		return nil
	}

	addressReplicaMap := map[string]string{}
	for replica, address := range engine.Spec.ReplicaAddressMap {
		if addressReplicaMap[address] != "" {
			return fmt.Errorf("invalid ReplicaAddressMap: duplicate IPs")
		}
		addressReplicaMap[address] = replica
	}

	replicaURLModeMap, err := m.engineClient.ReplicaList()
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
					m.eventRecorder.Eventf(engine, v1.EventTypeWarning, EventReasonFaulted, "Detected replica %v (%v) faulted", replica, ip)
				case types.ReplicaModeWO:
					m.eventRecorder.Eventf(engine, v1.EventTypeNormal, EventReasonRebuilding, "Start rebuilding replica %v (%v)", replica, ip)
				case types.ReplicaModeRW:
					m.eventRecorder.Eventf(engine, v1.EventTypeNormal, EventReasonRebuilded, "Replica %v (%v) has been rebuilded", replica, ip)
				default:
					logrus.Errorf("Invalid engine replica mode %v", r.Mode)
				}
			}
		}
	}
	if !reflect.DeepEqual(engine.Status.ReplicaModeMap, currentReplicaModeMap) {
		engine.Status.ReplicaModeMap = currentReplicaModeMap
		_, err = m.ds.UpdateEngine(engine)
		return err
	}
	return nil
}

func (ec *EngineController) ReconcileEngineState(e *longhorn.Controller) error {
	if err := ec.removeUnknownReplica(e); err != nil {
		return err
	}
	if err := ec.rebuildingNewReplica(e); err != nil {
		return err
	}
	return nil
}

func (ec *EngineController) getClientForEngine(e *longhorn.Controller) (client engineapi.EngineClient, err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot get client for engine %v", e.Name)
	}()
	if e.Status.CurrentState != types.InstanceStateRunning {
		return nil, fmt.Errorf("engine is not running")
	}
	client, err = ec.engines.NewEngineClient(&engineapi.EngineClientRequest{
		VolumeName:    e.Spec.VolumeName,
		ControllerURL: engineapi.GetControllerDefaultURL(e.Status.IP),
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (ec *EngineController) removeUnknownReplica(e *longhorn.Controller) error {
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

	client, err := ec.getClientForEngine(e)
	if err != nil {
		return err
	}
	for _, ip := range unknownReplicaIPs {
		go func(ip string) {
			url := engineapi.GetReplicaDefaultURL(ip)
			if err := client.ReplicaRemove(url); err != nil {
				ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedDeleting, "Failed to remove replica IP %v from engine: %v", ip, err)
			} else {
				ec.eventRecorder.Eventf(e, v1.EventTypeNormal, EventReasonDelete, "Removed replica IP %v from engine", ip)
			}
		}(ip)
	}
	return nil
}

func (ec *EngineController) rebuildingNewReplica(e *longhorn.Controller) error {
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

func (ec *EngineController) startRebuilding(e *longhorn.Controller, replica, ip string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start rebuild for %v of %v", replica, e.Name)
	}()

	client, err := ec.getClientForEngine(e)
	if err != nil {
		return err
	}
	replicaURL := engineapi.GetReplicaDefaultURL(ip)
	go func() {
		// start rebuild
		if err := client.ReplicaAdd(replicaURL); err != nil {
			ec.eventRecorder.Eventf(e, v1.EventTypeWarning, EventReasonFailedRebuilding, "Failed rebuilding replica with IP %v", ip)
		}
	}()
	//wait until engine confirmed that rebuild started
	if err := wait.PollImmediate(EnginePollInterval, EnginePollTimeout, func() (bool, error) {
		replicaURLModeMap, err := client.ReplicaList()
		if err != nil {
			return false, err
		}
		for url := range replicaURLModeMap {
			if ip == engineapi.GetIPFromURL(url) {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	return nil
}
