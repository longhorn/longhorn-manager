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
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-engine/pkg/instance-manager/api"
	"github.com/longhorn/longhorn-engine/pkg/instance-manager/client"
	imutil "github.com/longhorn/longhorn-engine/pkg/instance-manager/util"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

const (
	managerProbeInitialDelay  = 3
	managerProbePeriodSeconds = 5

	managerLivenessProbeFailureThreshold = 3

	// MaxPollCount, MinPollCount, PollInterval determines how often we sync instance map

	MaxPollCount = 60
	MinPollCount = 1
	PollInterval = 500 * time.Millisecond
)

var (
	hostToContainer = v1.MountPropagationHostToContainer
)

type InstanceManagerController struct {
	namespace      string
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	imStoreSynced cache.InformerSynced
	pStoreSynced  cache.InformerSynced

	queue workqueue.RateLimitingInterface

	instanceManagerMonitorMutex    *sync.RWMutex
	instanceManagerMonitorMap      map[string]chan struct{}
	instanceManagerMonitorRemoveCh chan string
}

type InstanceManagerMonitor struct {
	Name         string
	controllerID string

	instanceManagerUpdater InstanceManagerUpdater
	ds                     *datastore.DataStore
	lock                   *sync.Mutex
	updateNotification     bool
	stopCh                 chan struct{}
	done                   bool

	// used to notify the controller that monitoring has stopped
	monitoringRemoveCh chan string
}

type InstanceManagerUpdater interface {
	GetNotifier() (InstanceManagerNotifier, error)
	Poll() (map[string]types.InstanceProcess, error)
}

type InstanceManagerNotifier interface {
	Recv() (struct{}, error)
	Close()
}

type EngineManagerUpdater struct {
	client *client.EngineManagerClient
}

type ReplicaManagerUpdater struct {
	client *client.ProcessManagerClient
}

type EngineManagerNotifier struct {
	stream *api.EngineStream
}

type ReplicaManagerNotifier struct {
	stream *api.ProcessStream
}

func NewInstanceManagerController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	imInformer lhinformers.InstanceManagerInformer,
	pInformer coreinformers.PodInformer,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string) *InstanceManagerController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	imc := &InstanceManagerController{
		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-instance-manager-controller"}),

		ds: ds,

		imStoreSynced: imInformer.Informer().HasSynced,
		pStoreSynced:  pInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-instance-manager"),

		instanceManagerMonitorMutex:    &sync.RWMutex{},
		instanceManagerMonitorMap:      map[string]chan struct{}{},
		instanceManagerMonitorRemoveCh: make(chan string, 1),
	}

	imInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			imc.enqueueInstanceManager(im)
		},
		UpdateFunc: func(old, cur interface{}) {
			curIM := cur.(*longhorn.InstanceManager)
			imc.enqueueInstanceManager(curIM)
		},
		DeleteFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			imc.enqueueInstanceManager(im)
		},
	})

	pInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			switch t := obj.(type) {
			case *v1.Pod:
				return imc.filterInstanceManagerPod(t)
			default:
				utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", imc, obj))
				return false
			}
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*v1.Pod)
				imc.enqueueInstanceManagerPod(pod)
			},
			UpdateFunc: func(old, cur interface{}) {
				newPod := cur.(*v1.Pod)
				imc.enqueueInstanceManagerPod(newPod)
			},
			DeleteFunc: func(obj interface{}) {
				pod := obj.(*v1.Pod)
				imc.enqueueInstanceManagerPod(pod)
			},
		},
	})

	go imc.monitorUpdater()
	return imc
}

func (imc *InstanceManagerController) filterInstanceManagerPod(obj *v1.Pod) bool {
	isInstanceManager := false
	podContainers := obj.Spec.Containers
	for _, con := range podContainers {
		if con.Name == "engine-manager" || con.Name == "replica-manager" {
			isInstanceManager = true
			break
		}
	}
	return isInstanceManager
}

func (imc *InstanceManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer imc.queue.ShutDown()

	logrus.Infof("Starting Longhorn instance manager controller")
	defer logrus.Infof("Shutting down Longhorn instance manager controller")

	if !controller.WaitForCacheSync("longhorn instance manager", stopCh, imc.imStoreSynced, imc.pStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(imc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (imc *InstanceManagerController) worker() {
	for imc.processNextWorkItem() {
	}
}

func (imc *InstanceManagerController) processNextWorkItem() bool {
	key, quit := imc.queue.Get()

	if quit {
		return false
	}
	defer imc.queue.Done(key)

	err := imc.syncInstanceManager(key.(string))
	imc.handleErr(err, key)

	return true
}

func (imc *InstanceManagerController) handleErr(err error, key interface{}) {
	if err == nil {
		imc.queue.Forget(key)
		return
	}

	if imc.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn instance manager %v: %v", key, err)
		imc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn instance manager %v out of the queue: %v", key, err)
	imc.queue.Forget(key)
}

func (imc *InstanceManagerController) syncInstanceManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync instance manager for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != imc.namespace {
		return nil
	}

	im, err := imc.ds.GetInstanceManager(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			logrus.Infof("Longhorn instance manager %v has been deleted", key)
			return nil
		}
		return err
	}

	if im.Status.OwnerID != imc.controllerID {
		if !imc.isResponsibleFor(im) {
			// Not ours
			return nil
		}
		im.Status.OwnerID = imc.controllerID
		im, err = imc.ds.UpdateInstanceManagerStatus(im)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		logrus.Debugf("Instance Manager Controller %v picked up %v", imc.controllerID, im.Name)
	}

	if im.DeletionTimestamp != nil {
		if err := imc.cleanupInstanceManager(im); err != nil {
			return err
		}
		return imc.ds.RemoveFinalizerForInstanceManager(im)
	}

	existingIM := im.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingIM.Status, im.Status) {
			_, err = imc.ds.UpdateInstanceManagerStatus(im)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict: %v", key, err)
			imc.enqueueInstanceManager(im)
			err = nil
		}
	}()

	image, err := imc.ds.GetEngineImage(im.Spec.EngineImage)
	if err != nil {
		im.Status.CurrentState = types.InstanceManagerStateError
		if datastore.ErrorIsNotFound(err) {
			logrus.Infof("Engine image %v for instance manager %v has been deleted", im.Spec.EngineImage, key)
			return nil
		}
		return err
	}

	pod, err := imc.ds.GetInstanceManagerPod(im.Name)
	if err != nil {
		return errors.Wrapf(err, "cannot get pod for instance manager %v", im.Name)
	}

	// We're handling the Instance Manager from a Node that isn't supposed to be responsible for it. This could mean
	// that the Node it belongs to went down, or its newly scheduled.
	if im.Spec.NodeID != imc.controllerID {
		im.Status.CurrentState = types.InstanceManagerStateUnknown
		return nil
	}

	// Sync instance manager status with related pod
	if pod == nil {
		if im.Status.CurrentState != types.InstanceManagerStateError {
			im.Status.CurrentState = types.InstanceManagerStateStopped
		}
	} else {
		// TODO: Will remove this reference kind correcting after all Longhorn components having used the new kinds
		if len(pod.OwnerReferences) < 1 || pod.OwnerReferences[0].Kind != types.LonghornKindInstanceManager {
			pod.OwnerReferences = datastore.GetOwnerReferencesForInstanceManager(im)
			pod, err = imc.kubeClient.CoreV1().Pods(imc.namespace).Update(pod)
			if err != nil {
				return err
			}
		}

		switch pod.Status.Phase {
		case v1.PodPending:
			if im.Status.CurrentState == types.InstanceManagerStateUnknown {
				if len(pod.Status.ContainerStatuses) != 0 && pod.Status.ContainerStatuses[0].State.Terminated != nil {
					im.Status.CurrentState = types.InstanceManagerStateError
					logrus.Errorf("Instance Manager %v pod terminated. Details %+v. Set state to Error.", im.Name,
						pod.Status.ContainerStatuses[0].State.Terminated)
				} else {
					im.Status.CurrentState = types.InstanceManagerStateStarting
				}
			} else if im.Status.CurrentState != types.InstanceManagerStateStarting {
				im.Status.CurrentState = types.InstanceManagerStateError
				logrus.Errorf("Instance Manager %v is state %v but the related pod is pending.", im.Name, im.Status.CurrentState)
			}
		case v1.PodRunning:
			// Make sure readiness probe has passed.
			for _, st := range pod.Status.ContainerStatuses {
				if !st.Ready {
					return nil
				}
			}

			if im.Status.CurrentState == types.InstanceManagerStateStarting || im.Status.CurrentState == types.InstanceManagerStateUnknown {
				im.Status.CurrentState = types.InstanceManagerStateRunning
				im.Status.IP = pod.Status.PodIP
			} else if im.Status.CurrentState != types.InstanceManagerStateRunning {
				im.Status.CurrentState = types.InstanceManagerStateError
			}
		default:
			im.Status.CurrentState = types.InstanceManagerStateError
		}
	}

	if im.Status.CurrentState == types.InstanceManagerStateRunning {
		if !imc.isMonitoring(im.Name) {
			switch im.Spec.Type {
			case types.InstanceManagerTypeEngine:
				imc.startMonitoring(im, NewEngineManagerUpdater(im))
			case types.InstanceManagerTypeReplica:
				imc.startMonitoring(im, NewReplicaManagerUpdater(im))
			default:
				logrus.Errorf("BUG: instance manager %v has invalid type %v", im.Name, im.Spec.Type)
				im.Status.CurrentState = types.InstanceManagerStateError
			}
		}
	}

	// Restart instance manager automatically. Otherwise users need to take care of it then we need to expose API
	// for operating instance manager.
	if im.Status.CurrentState == types.InstanceManagerStateError || im.Status.CurrentState == types.InstanceManagerStateStopped {
		logrus.Warnf("Starts to clean up then recreates pod for instance manager %v with state %v", im.Name, im.Status.CurrentState)
		if err := imc.cleanupInstanceManager(im); err != nil {
			return err
		}
		if err := imc.createInstanceManagerPod(im, image); err != nil {
			return err
		}
		im.Status.CurrentState = types.InstanceManagerStateStarting
		return nil
	}

	return nil
}

func (imc *InstanceManagerController) enqueueInstanceManager(instanceManager *longhorn.InstanceManager) {
	key, err := controller.KeyFunc(instanceManager)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", instanceManager, err))
		return
	}

	imc.queue.AddRateLimited(key)
}

func (imc *InstanceManagerController) enqueueInstanceManagerPod(pod *v1.Pod) {
	im, err := imc.ds.GetInstanceManager(pod.Name)

	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Warnf("Can't find instance manager for pod %v, may be deleted", pod.Name)
			return
		}
		utilruntime.HandleError(fmt.Errorf("couldn't get instance manager: %v", err))
		return
	}
	imc.enqueueInstanceManager(im)
}

// cleanupInstanceManager cleans up the Pod that was created by the Instance Manager, stops the monitor, and marks
// any processes that may be on that Instance Manager to Error. This is used when we need to recover from an error
// or prepare for deletion.
func (imc *InstanceManagerController) cleanupInstanceManager(im *longhorn.InstanceManager) error {
	im.Status.IP = ""

	if imc.isMonitoring(im.Name) {
		imc.stopMonitoring(im)
	}

	// Skip updating instance manager if its deletion timestamp is set, so that the controller won't try to add finalizer for the deleting object in the update function.
	// The related instances will become state error after the instance manager is gone hence we don't need to worry about the impacts of this skip.
	if im.DeletionTimestamp == nil {
		// double check the instance map of instance manager.
		// instance manager may be updated when stopping the monitor, we need to get the latest version before modifying it.
		im, err := imc.ds.GetInstanceManager(im.Name)
		if err != nil {
			return err
		}
		for name, instance := range im.Status.Instances {
			instance.Status.State = types.InstanceStateError
			instance.Status.ErrorMsg = "Instance Manager error"
			im.Status.Instances[name] = instance
		}

		// need to update im before deleting pod
		im, err = imc.ds.UpdateInstanceManagerStatus(im)
		if err != nil {
			return err
		}
	}

	pod, err := imc.ds.GetInstanceManagerPod(im.Name)
	if err != nil {
		return err
	}
	if pod != nil {
		if err := imc.ds.DeletePod(pod.Name); err != nil {
			return err
		}
	}

	return nil
}

func (imc *InstanceManagerController) createInstanceManagerPod(im *longhorn.InstanceManager, image *longhorn.EngineImage) error {
	setting, err := imc.ds.GetSetting(types.SettingNameTaintToleration)
	if err != nil {
		return errors.Wrapf(err, "failed to get taint toleration setting before creating instance manager pod")
	}
	tolerations, err := types.UnmarshalTolerations(setting.Value)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal taint toleration setting before creating instance manager pod")
	}

	var podSpec *v1.Pod
	switch im.Spec.Type {
	case types.InstanceManagerTypeEngine:
		podSpec, err = imc.createEngineManagerPodSpec(im, image, tolerations)
	case types.InstanceManagerTypeReplica:
		podSpec, err = imc.createReplicaManagerPodSpec(im, image, tolerations)
	}
	if err != nil {
		return err
	}
	pod, err := imc.ds.CreatePod(podSpec)
	if err != nil {
		return errors.Wrapf(err, "failed to create pod for instance manager %v", im.Name)
	}
	logrus.Infof("Created instance manager pod %v for instance manager %v", pod.Name, im.Name)

	return nil
}

func (imc *InstanceManagerController) createGenericManagerPodSpec(im *longhorn.InstanceManager,
	image *longhorn.EngineImage, tolerations []v1.Toleration) (*v1.Pod, error) {

	privileged := true
	podSpec := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            im.Name,
			Namespace:       imc.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForInstanceManager(im),
		},
		Spec: v1.PodSpec{
			ServiceAccountName: imc.serviceAccount,
			Tolerations:        tolerations,
			Containers: []v1.Container{
				{
					Image: image.Spec.Image,
					LivenessProbe: &v1.Probe{
						Handler: v1.Handler{
							Exec: &v1.ExecAction{
								Command: []string{"/usr/local/bin/grpc_health_probe", "-addr=:8500"},
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
			NodeName:      imc.controllerID,
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	// Apply resource requirements to newly created Instance Manager Pods.
	resourceReq, err := GetGuaranteedResourceRequirement(imc.ds)
	if err != nil {
		return nil, err
	}
	if resourceReq != nil {
		podSpec.Spec.Containers[0].Resources = *resourceReq
	}

	return podSpec, nil
}

func (imc *InstanceManagerController) createEngineManagerPodSpec(im *longhorn.InstanceManager,
	image *longhorn.EngineImage, tolerations []v1.Toleration) (*v1.Pod, error) {

	podSpec, err := imc.createGenericManagerPodSpec(im, image, tolerations)
	if err != nil {
		return nil, err
	}

	podSpec.ObjectMeta.Labels = types.GetInstanceManagerLabels(imc.controllerID, image.Name, types.InstanceManagerTypeEngine)
	podSpec.Spec.Containers[0].Name = "engine-manager"
	podSpec.Spec.Containers[0].Args = []string{
		"engine-manager", "--debug", "daemon", "--listen", "0.0.0.0:8500",
	}
	podSpec.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
		{
			MountPath: "/host/dev",
			Name:      "dev",
		},
		{
			MountPath: "/host/proc",
			Name:      "proc",
		},
		{
			MountPath:        types.EngineBinaryDirectoryInContainer,
			Name:             "engine-binaries",
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
	}
	return podSpec, nil
}

func (imc *InstanceManagerController) createReplicaManagerPodSpec(im *longhorn.InstanceManager,
	image *longhorn.EngineImage, tolerations []v1.Toleration) (*v1.Pod, error) {

	podSpec, err := imc.createGenericManagerPodSpec(im, image, tolerations)
	if err != nil {
		return nil, err
	}

	podSpec.ObjectMeta.Labels = types.GetInstanceManagerLabels(imc.controllerID, image.Name, types.InstanceManagerTypeReplica)
	podSpec.Spec.Containers[0].Name = "replica-manager"
	podSpec.Spec.Containers[0].Args = []string{
		"longhorn-instance-manager", "--debug", "daemon", "--listen", "0.0.0.0:8500",
	}
	podSpec.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
		{
			MountPath:        "/host",
			Name:             "host",
			MountPropagation: &hostToContainer,
		},
	}
	podSpec.Spec.Volumes = []v1.Volume{
		{
			Name: "host",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/",
				},
			},
		},
	}
	return podSpec, nil
}

func NewEngineManagerUpdater(im *longhorn.InstanceManager) *EngineManagerUpdater {
	return &EngineManagerUpdater{
		client: client.NewEngineManagerClient(imutil.GetURL(im.Status.IP, engineapi.InstanceManagerDefaultPort)),
	}
}

func NewReplicaManagerUpdater(im *longhorn.InstanceManager) *ReplicaManagerUpdater {
	return &ReplicaManagerUpdater{
		client: client.NewProcessManagerClient(imutil.GetURL(im.Status.IP, engineapi.InstanceManagerDefaultPort)),
	}
}

func (updater *EngineManagerUpdater) Poll() (map[string]types.InstanceProcess, error) {
	result := map[string]types.InstanceProcess{}

	resp, err := updater.client.EngineList()
	if err != nil {
		return result, err
	}

	for name, instance := range resp {
		result[name] = *engineapi.EngineProcessToInstanceProcess(instance)
	}
	return result, nil
}

func (updater *EngineManagerUpdater) GetNotifier() (InstanceManagerNotifier, error) {
	watch, err := updater.client.EngineWatch()
	if err != nil {
		return nil, err
	}
	return NewEngineManagerNotifier(watch), nil
}

func NewEngineManagerNotifier(stream *api.EngineStream) *EngineManagerNotifier {
	return &EngineManagerNotifier{
		stream: stream,
	}
}

func (notifier *EngineManagerNotifier) Recv() (struct{}, error) {
	if _, err := notifier.stream.Recv(); err != nil {
		return struct{}{}, err
	}

	return struct{}{}, nil
}

func (notifier *EngineManagerNotifier) Close() {
	notifier.stream.Close()
	return
}

func (updater *ReplicaManagerUpdater) Poll() (map[string]types.InstanceProcess, error) {
	result := map[string]types.InstanceProcess{}

	resp, err := updater.client.ProcessList()
	if err != nil {
		return result, err
	}

	for name, instance := range resp {
		result[name] = *engineapi.ReplicaProcessToInstanceProcess(instance)
	}
	return result, nil
}

func (updater *ReplicaManagerUpdater) GetNotifier() (InstanceManagerNotifier, error) {
	watch, err := updater.client.ProcessWatch()
	if err != nil {
		return nil, err
	}
	return NewReplicaManagerNotifier(watch), nil
}

func NewReplicaManagerNotifier(stream *api.ProcessStream) *ReplicaManagerNotifier {
	return &ReplicaManagerNotifier{
		stream: stream,
	}
}

func (notifier *ReplicaManagerNotifier) Recv() (struct{}, error) {
	if _, err := notifier.stream.Recv(); err != nil {
		return struct{}{}, err
	}

	return struct{}{}, nil
}

func (notifier *ReplicaManagerNotifier) Close() {
	notifier.stream.Close()
	return
}

func (imc *InstanceManagerController) monitorUpdater() {
	for imName := range imc.instanceManagerMonitorRemoveCh {
		imc.removeFromInstanceManagerMonitorMap(imName)
	}
}

func (imc *InstanceManagerController) removeFromInstanceManagerMonitorMap(imName string) {
	imc.instanceManagerMonitorMutex.Lock()
	defer imc.instanceManagerMonitorMutex.Unlock()

	stopCh, ok := imc.instanceManagerMonitorMap[imName]
	if ok {
		close(stopCh)
		delete(imc.instanceManagerMonitorMap, imName)
	}
}

func (imc *InstanceManagerController) startMonitoring(im *longhorn.InstanceManager, instanceManagerUpdater InstanceManagerUpdater) {
	if im.Status.IP == "" {
		// IP should be set
		logrus.Errorf("IP of instance manager %v was not set before monitoring", im.Name)
		return
	}

	stopCh := make(chan struct{}, 1)
	monitor := &InstanceManagerMonitor{
		Name:                   im.Name,
		controllerID:           imc.controllerID,
		ds:                     imc.ds,
		instanceManagerUpdater: instanceManagerUpdater,
		lock:                   &sync.Mutex{},
		stopCh:                 stopCh,
		done:                   false,
		// notify monitor to update the instance map
		updateNotification: false,
		monitoringRemoveCh: imc.instanceManagerMonitorRemoveCh,
	}

	imc.instanceManagerMonitorMutex.Lock()
	defer imc.instanceManagerMonitorMutex.Unlock()

	if _, ok := imc.instanceManagerMonitorMap[im.Name]; ok {
		logrus.Warnf("BUG: Monitoring for %v already exists", im.Name)
		return
	}
	imc.instanceManagerMonitorMap[im.Name] = stopCh

	go monitor.Run()
}

func (imc *InstanceManagerController) stopMonitoring(im *longhorn.InstanceManager) {
	imc.instanceManagerMonitorMutex.Lock()

	stopCh, ok := imc.instanceManagerMonitorMap[im.Name]
	if !ok {
		imc.instanceManagerMonitorMutex.Unlock()
		logrus.Warnf("instance manager %v: stop monitoring called when there is no monitoring", im.Name)
		return
	}
	imc.instanceManagerMonitorMutex.Unlock()

	stopCh <- struct{}{}
	return
}

func (imc *InstanceManagerController) isMonitoring(imName string) bool {
	imc.instanceManagerMonitorMutex.RLock()
	defer imc.instanceManagerMonitorMutex.RUnlock()

	_, ok := imc.instanceManagerMonitorMap[imName]
	return ok
}

func (m *InstanceManagerMonitor) Run() {
	logrus.Debugf("Start monitoring instance manager %v", m.Name)
	defer func() {
		m.monitoringRemoveCh <- m.Name
		logrus.Debugf("Stop monitoring instance manager %v", m.Name)
	}()

	notifier, err := m.instanceManagerUpdater.GetNotifier()
	if err != nil {
		logrus.Errorf("failed to start notifier for the monitor of instance manager %v: %v", m.Name, err)
		return
	}

	go func() {
		for {
			if m.done {
				return
			}

			if _, err := notifier.Recv(); err != nil {
				logrus.Errorf("error receiving next item in engine watch: %v", err)
				time.Sleep(MinPollCount * PollInterval)
			} else {
				m.lock.Lock()
				m.updateNotification = true
				m.lock.Unlock()
			}
		}
	}()

	timer := 0
	ticker := time.Tick(MinPollCount * PollInterval)
	for {
		select {
		case <-ticker:
			needUpdate := false

			m.lock.Lock()
			timer++
			if timer >= MaxPollCount || m.updateNotification {
				needUpdate = true
				m.updateNotification = false
				timer = 0
			}
			m.lock.Unlock()

			if needUpdate {
				if err := m.pollAndUpdateInstanceMap(); err != nil {
					logrus.Error(err)
				}
			}
		case <-m.stopCh:
			notifier.Close()
			m.done = true
			if err := m.updateInstanceMapForCleanup(); err != nil {
				logrus.Errorf("failed to mark existing instances to error when stopping instance manager monitor: %v", err)
			}
			return
		}
	}
}

func (m *InstanceManagerMonitor) pollAndUpdateInstanceMap() error {
	resp, err := m.instanceManagerUpdater.Poll()
	if err != nil {
		return fmt.Errorf("failed to poll instance info to update instance manager %v: %v", m.Name, err)
	}

	im, err := m.ds.GetInstanceManager(m.Name)
	if err != nil {
		return fmt.Errorf("failed to list processes to update instance manager %v: %v", m.Name, err)
	}

	if reflect.DeepEqual(im.Status.Instances, resp) {
		return nil
	}
	im.Status.Instances = resp
	if _, err := m.ds.UpdateInstanceManagerStatus(im); err != nil {
		return fmt.Errorf("failed to update instance map for instance manager %v: %v", m.Name, err)
	}

	return nil
}

func (m *InstanceManagerMonitor) updateInstanceMapForCleanup() error {
	im, err := m.ds.GetInstanceManager(m.Name)
	if err != nil {
		return fmt.Errorf("failed to get instance manager %v to cleanup instance map: %v", m.Name, err)
	}

	for name, instance := range im.Status.Instances {
		instance.Status.State = types.InstanceStateError
		instance.Status.ErrorMsg = "Instance Manager errored"
		im.Status.Instances[name] = instance
	}

	if _, err := m.ds.UpdateInstanceManagerStatus(im); err != nil {
		return fmt.Errorf("failed to update instance map for instance manager %v: %v", m.Name, err)
	}

	return nil
}

func (imc *InstanceManagerController) isResponsibleFor(im *longhorn.InstanceManager) bool {
	return isControllerResponsibleFor(imc.controllerID, imc.ds, im.Name, im.Spec.NodeID, im.Status.OwnerID)
}
