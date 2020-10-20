package controller

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-instance-manager/pkg/api"

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
	PollInterval = 1 * time.Second
)

var (
	hostToContainer = v1.MountPropagationHostToContainer
)

type InstanceManagerController struct {
	*baseController

	namespace      string
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	imStoreSynced cache.InformerSynced
	pStoreSynced  cache.InformerSynced
	knStoreSynced cache.InformerSynced

	instanceManagerMonitorMutex *sync.RWMutex
	instanceManagerMonitorMap   map[string]chan struct{}

	// for unit test
	versionUpdater func(*longhorn.InstanceManager) error
}

type InstanceManagerMonitor struct {
	Name         string
	controllerID string

	instanceManagerUpdater  *InstanceManagerUpdater
	instanceManagerNotifier *InstanceManagerNotifier
	ds                      *datastore.DataStore
	lock                    *sync.Mutex
	updateNotification      bool
	stopCh                  chan struct{}
	done                    bool
}

type InstanceManagerUpdater struct {
	client *engineapi.InstanceManagerClient
}

type InstanceManagerNotifier struct {
	stream *api.ProcessStream
}

func updateInstanceManagerVersion(im *longhorn.InstanceManager) error {
	cli, err := engineapi.NewInstanceManagerClient(im)
	if err != nil {
		return err
	}
	apiMinVersion, apiVersion, err := cli.VersionGet()
	if err != nil {
		return err
	}
	im.Status.APIMinVersion = apiMinVersion
	im.Status.APIVersion = apiVersion
	return nil
}

func NewInstanceManagerController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	imInformer lhinformers.InstanceManagerInformer,
	pInformer coreinformers.PodInformer,
	kubeNodeInformer coreinformers.NodeInformer,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string) *InstanceManagerController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-instance-manager-controller"}),

		ds: ds,

		imStoreSynced: imInformer.Informer().HasSynced,
		pStoreSynced:  pInformer.Informer().HasSynced,
		knStoreSynced: kubeNodeInformer.Informer().HasSynced,

		instanceManagerMonitorMutex: &sync.RWMutex{},
		instanceManagerMonitorMap:   map[string]chan struct{}{},

		versionUpdater: updateInstanceManagerVersion,
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

	kubeNodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, cur interface{}) {
			curNode := cur.(*v1.Node)
			imc.enqueueKubernetesNode(curNode)
		},
		DeleteFunc: func(obj interface{}) {
			node := obj.(*v1.Node)
			imc.enqueueKubernetesNode(node)
		},
	})

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

	pod, err := imc.ds.GetInstanceManagerPod(im.Name)
	if err != nil {
		return errors.Wrapf(err, "cannot get pod for instance manager %v", im.Name)
	}

	// We're handling the Instance Manager from a Node that isn't supposed to be responsible for it. This could mean
	// that the Node it belongs to went down, or its newly scheduled.
	if im.Spec.NodeID != imc.controllerID {
		im.Status.CurrentState = types.InstanceManagerStateUnknown
		if err := imc.cleanupInstanceManager(im); err != nil {
			return err
		}
		return nil
	}

	// Sync instance manager status with related pod
	if pod == nil {
		if im.Status.CurrentState != types.InstanceManagerStateError {
			im.Status.CurrentState = types.InstanceManagerStateStopped
		}
	} else if pod.DeletionTimestamp != nil {
		if im.Status.CurrentState != types.InstanceManagerStateError {
			im.Status.CurrentState = types.InstanceManagerStateError
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
			if im.Status.CurrentState != types.InstanceManagerStateStarting {
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
		if im.Status.APIVersion == engineapi.UnknownInstanceManagerAPIVersion {
			if err := imc.versionUpdater(im); err != nil {
				return err
			}
		}
	} else {
		im.Status.APIVersion = engineapi.UnknownInstanceManagerAPIVersion
		im.Status.APIMinVersion = engineapi.UnknownInstanceManagerAPIVersion
	}

	if err := imc.syncInstanceManagerPDB(im); err != nil {
		return err
	}

	if im.Status.CurrentState == types.InstanceManagerStateRunning {
		if err := engineapi.CheckInstanceManagerCompatibilty(im.Status.APIMinVersion, im.Status.APIVersion); err != nil {
			if imc.isMonitoring(im.Name) {
				logrus.Infof("Instance manager controller will stop monitoring the incompatible instance manager %v", im.Name)
				imc.stopMonitoring(im)
			}
		} else {
			if !imc.isMonitoring(im.Name) {
				switch im.Spec.Type {
				case types.InstanceManagerTypeEngine:
					fallthrough
				case types.InstanceManagerTypeReplica:
					imc.startMonitoring(im, NewInstanceManagerUpdater(im))
				default:
					logrus.Errorf("BUG: instance manager %v has invalid type %v", im.Name, im.Spec.Type)
					im.Status.CurrentState = types.InstanceManagerStateError
				}
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
		if err := imc.createInstanceManagerPod(im); err != nil {
			if !types.ErrorAlreadyExists(errors.Cause(err)) {
				return err
			}
			return nil
		}

		im.Status.CurrentState = types.InstanceManagerStateStarting
		return nil
	}

	return nil
}

func (imc *InstanceManagerController) syncInstanceManagerPDB(im *longhorn.InstanceManager) error {
	if im.Status.CurrentState != types.InstanceManagerStateRunning {
		return nil
	}

	unschedulable, err := imc.isNodeUnschedulable()
	if err != nil {
		return err
	}

	imPDB, err := imc.ds.GetPDBRO(imc.getPDBName(im))
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	// When current node is unschedulable, it is a signal that the node is being cordoned/drained.
	if unschedulable {
		if imPDB == nil {
			return nil
		}
		canDelete, err := imc.canDeleteInstanceManagerPDB(im)
		if err != nil {
			return err
		}
		if !canDelete {
			return nil
		}
		return imc.deleteInstanceManagerPDB(im)
	}

	// Make sure that there is a PodDisruptionBudget to protect this instance manager in normal case.
	if imPDB == nil {
		return imc.createInstanceManagerPDB(im)
	}

	return nil
}

func (imc *InstanceManagerController) isNodeUnschedulable() (bool, error) {
	kubeNode, err := imc.ds.GetKubernetesNode(imc.controllerID)
	if err != nil {
		return false, err
	}
	return kubeNode.Spec.Unschedulable, nil
}

func (imc *InstanceManagerController) deleteInstanceManagerPDB(im *longhorn.InstanceManager) error {
	err := imc.ds.DeletePDB(imc.getPDBName(im))
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}
	return nil
}

func (imc *InstanceManagerController) canDeleteInstanceManagerPDB(im *longhorn.InstanceManager) (bool, error) {
	// If there is no engine instance process inside the engine instance manager,
	// it means that all volumes are detached.
	// We can delete the PodDisruptionBudget for the engine instance manager.
	if im.Spec.Type == types.InstanceManagerTypeEngine {
		if len(im.Status.Instances) == 0 {
			return true, nil
		}
		return false, nil
	}

	// Make sure that the instance manager is of type replica
	if im.Spec.Type != types.InstanceManagerTypeReplica {
		return false, fmt.Errorf("the instance manager %v has invalid type: %v ", im.Name, im.Spec.Type)
	}

	// Must wait for all volumes detached from the current node first.
	// This also means that we must wait until the the PDB of engine instance manager on the current node is deleted
	allVolumeDetached, err := imc.areAllVolumesDetachedFromCurrentNode()
	if err != nil {
		return false, err
	}
	if !allVolumeDetached {
		return false, nil
	}

	allowDrainingNodeWithLastReplica, err := imc.ds.GetSettingAsBool(types.SettingNameAllowNodeDrainWithLastHealthyReplica)
	if err != nil {
		return false, err
	}
	if allowDrainingNodeWithLastReplica {
		return true, nil
	}

	replicasOnCurrentNode, err := imc.ds.ListReplicasByNodeRO(imc.controllerID)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return true, nil
		}
		return false, err
	}

	// For each replica process in the current node,
	// find out whether there is a healthy replica of the same volume on another node
	for _, replica := range replicasOnCurrentNode {
		vol, err := imc.ds.GetVolume(replica.Spec.VolumeName)
		if err != nil {
			return false, err
		}

		replicas, err := imc.ds.ListVolumeReplicas(vol.Name)
		if err != nil {
			return false, err
		}

		hasHealthyReplicaOnAnotherNode := false
		for _, r := range replicas {
			if r.Spec.HealthyAt != "" && r.Spec.FailedAt == "" && r.Spec.NodeID != imc.controllerID {
				hasHealthyReplicaOnAnotherNode = true
				break
			}
		}

		if !hasHealthyReplicaOnAnotherNode {
			return false, nil
		}
	}

	return true, nil
}

func (imc *InstanceManagerController) areAllVolumesDetachedFromCurrentNode() (bool, error) {
	engineIMs, err := imc.ds.ListInstanceManagersByNode(imc.controllerID, types.InstanceManagerTypeEngine)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	for _, engineIM := range engineIMs {
		if len(engineIM.Status.Instances) > 0 {
			return false, nil
		}
	}

	return true, nil
}

func (imc *InstanceManagerController) createInstanceManagerPDB(im *longhorn.InstanceManager) error {
	instanceManagerPDB := imc.generateInstanceManagerPDBManifest(im)
	if _, err := imc.ds.CreatePDB(instanceManagerPDB); err != nil {
		if apierrors.IsAlreadyExists(err) {
			imc.logger.Warn("PDB %s is already exists", instanceManagerPDB.GetName())
			return nil
		}
		return err
	}
	return nil
}

func (imc *InstanceManagerController) generateInstanceManagerPDBManifest(im *longhorn.InstanceManager) *policyv1beta1.PodDisruptionBudget {
	return &policyv1beta1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      imc.getPDBName(im),
			Namespace: imc.namespace,
		},
		Spec: policyv1beta1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetInstanceManagerLabels(imc.controllerID, im.Spec.Image, im.Spec.Type),
			},
			MinAvailable: &intstr.IntOrString{IntVal: 1},
		},
	}
}

func (imc *InstanceManagerController) getPDBName(im *longhorn.InstanceManager) string {
	return im.Name
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

func (imc *InstanceManagerController) enqueueKubernetesNode(n *v1.Node) {
	node, err := imc.ds.GetNode(n.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// there is no Longhorn node created for the Kubernetes
			// node (e.g. controller/etcd node). Skip it
			return
		}
		utilruntime.HandleError(fmt.Errorf("Couldn't get node %v: %v ", n.Name, err))
		return
	}

	for _, imType := range []types.InstanceManagerType{types.InstanceManagerTypeEngine, types.InstanceManagerTypeReplica} {
		ims, err := imc.ds.ListInstanceManagersByNode(node.Name, imType)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logrus.Warnf("Can't find instance manager for node %v, may be deleted", node.Name)
				return
			}
			utilruntime.HandleError(fmt.Errorf("couldn't get instance manager: %v", err))
			return
		}

		for _, im := range ims {
			imc.enqueueInstanceManager(im)
		}
	}
}

func (imc *InstanceManagerController) cleanupInstanceManager(im *longhorn.InstanceManager) error {
	im.Status.IP = ""

	if imc.isMonitoring(im.Name) {
		imc.stopMonitoring(im)
	}

	// Need to update the instances before deleting pod.
	if err := imc.updateInstanceMapForCleanup(im.Name); err != nil {
		logrus.Errorf("failed to mark existing instances to error when stopping instance manager monitor: %v", err)
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

func (imc *InstanceManagerController) createInstanceManagerPod(im *longhorn.InstanceManager) error {
	setting, err := imc.ds.GetSetting(types.SettingNameTaintToleration)
	if err != nil {
		return errors.Wrapf(err, "failed to get taint toleration setting before creating instance manager pod")
	}
	tolerations, err := types.UnmarshalTolerations(setting.Value)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal taint toleration setting before creating instance manager pod")
	}

	registrySecretSetting, err := imc.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return errors.Wrapf(err, "failed to get registry secret setting before creating instance manager pod")
	}
	registrySecret := registrySecretSetting.Value

	var podSpec *v1.Pod
	switch im.Spec.Type {
	case types.InstanceManagerTypeEngine:
		podSpec, err = imc.createEngineManagerPodSpec(im, tolerations, registrySecret)
	case types.InstanceManagerTypeReplica:
		podSpec, err = imc.createReplicaManagerPodSpec(im, tolerations, registrySecret)
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

func (imc *InstanceManagerController) createGenericManagerPodSpec(im *longhorn.InstanceManager, tolerations []v1.Toleration, registrySecret string) (*v1.Pod, error) {
	priorityClass, err := imc.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return nil, err
	}

	imagePullPolicy, err := imc.ds.GetSettingImagePullPolicy()
	if err != nil {
		return nil, err
	}

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
			PriorityClassName:  priorityClass.Value,
			Containers: []v1.Container{
				{
					Image:           im.Spec.Image,
					ImagePullPolicy: imagePullPolicy,
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

	if registrySecret != "" {
		podSpec.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
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

func (imc *InstanceManagerController) createEngineManagerPodSpec(im *longhorn.InstanceManager, tolerations []v1.Toleration, registrySecret string) (*v1.Pod, error) {
	podSpec, err := imc.createGenericManagerPodSpec(im, tolerations, registrySecret)
	if err != nil {
		return nil, err
	}

	podSpec.ObjectMeta.Labels = types.GetInstanceManagerLabels(imc.controllerID, im.Spec.Image, types.InstanceManagerTypeEngine)
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

func (imc *InstanceManagerController) createReplicaManagerPodSpec(im *longhorn.InstanceManager, tolerations []v1.Toleration, registrySecret string) (*v1.Pod, error) {
	podSpec, err := imc.createGenericManagerPodSpec(im, tolerations, registrySecret)
	if err != nil {
		return nil, err
	}

	podSpec.ObjectMeta.Labels = types.GetInstanceManagerLabels(imc.controllerID, im.Spec.Image, types.InstanceManagerTypeReplica)
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

func NewInstanceManagerUpdater(im *longhorn.InstanceManager) *InstanceManagerUpdater {
	c, _ := engineapi.NewInstanceManagerClient(im)
	return &InstanceManagerUpdater{
		client: c,
	}
}

func (updater *InstanceManagerUpdater) Poll() (map[string]types.InstanceProcess, error) {
	return updater.client.ProcessList()
}

func (updater *InstanceManagerUpdater) GetNotifier() (*InstanceManagerNotifier, error) {
	watch, err := updater.client.ProcessWatch()
	if err != nil {
		return nil, err
	}
	return NewInstanceManagerNotifier(watch), nil
}

func NewInstanceManagerNotifier(stream *api.ProcessStream) *InstanceManagerNotifier {
	return &InstanceManagerNotifier{
		stream: stream,
	}
}

func (notifier *InstanceManagerNotifier) Recv() (struct{}, error) {
	if _, err := notifier.stream.Recv(); err != nil {
		return struct{}{}, err
	}

	return struct{}{}, nil
}

func (notifier *InstanceManagerNotifier) Close() {
	notifier.stream.Close()
	return
}

func (imc *InstanceManagerController) startMonitoring(im *longhorn.InstanceManager, instanceManagerUpdater *InstanceManagerUpdater) {
	if im.Status.IP == "" {
		// IP should be set
		logrus.Errorf("IP of instance manager %v was not set before monitoring", im.Name)
		return
	}

	// TODO: this function will error out in unit tests. Need to find a way to skip this part for unit tests.
	instanceManagerNotifier, err := instanceManagerUpdater.GetNotifier()
	if err != nil {
		logrus.Errorf("Failed to get the notifier of instance manager %v before monitoring: %v", im.Name, err)
		return
	}

	stopCh := make(chan struct{}, 1)
	monitor := &InstanceManagerMonitor{
		Name:                    im.Name,
		controllerID:            imc.controllerID,
		ds:                      imc.ds,
		instanceManagerUpdater:  instanceManagerUpdater,
		instanceManagerNotifier: instanceManagerNotifier,
		lock:                    &sync.Mutex{},
		stopCh:                  stopCh,
		done:                    false,
		// notify monitor to update the instance map
		updateNotification: false,
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
	defer imc.instanceManagerMonitorMutex.Unlock()

	stopCh, ok := imc.instanceManagerMonitorMap[im.Name]
	if !ok {
		logrus.Warnf("instance manager %v: stop monitoring called when there is no monitoring", im.Name)
		return
	}
	stopCh <- struct{}{}

	delete(imc.instanceManagerMonitorMap, im.Name)

	return
}

func (imc *InstanceManagerController) isMonitoring(imName string) bool {
	imc.instanceManagerMonitorMutex.RLock()
	defer imc.instanceManagerMonitorMutex.RUnlock()

	_, ok := imc.instanceManagerMonitorMap[imName]
	return ok
}

func (imc *InstanceManagerController) updateInstanceMapForCleanup(imName string) error {
	im, err := imc.ds.GetInstanceManager(imName)
	if err != nil {
		return fmt.Errorf("failed to get instance manager %v to cleanup instance map: %v", imName, err)
	}

	for name, instance := range im.Status.Instances {
		instance.Status.State = types.InstanceStateError
		instance.Status.ErrorMsg = "Instance Manager errored"
		im.Status.Instances[name] = instance
	}

	if _, err := imc.ds.UpdateInstanceManagerStatus(im); err != nil {
		return fmt.Errorf("failed to update instance map for instance manager %v: %v", imName, err)
	}

	return nil
}

func (m *InstanceManagerMonitor) Run() {
	logrus.Debugf("Start monitoring instance manager %v", m.Name)
	defer func() {
		logrus.Debugf("Stop monitoring instance manager %v", m.Name)
		m.instanceManagerNotifier.Close()
		m.done = true
		close(m.stopCh)
	}()

	go func() {
		for {
			if m.done {
				return
			}

			if _, err := m.instanceManagerNotifier.Recv(); err != nil {
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
	ticker := time.NewTicker(MinPollCount * PollInterval)
	defer ticker.Stop()
	tick := ticker.C
	for {
		select {
		case <-tick:
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

func (imc *InstanceManagerController) isResponsibleFor(im *longhorn.InstanceManager) bool {
	return isControllerResponsibleFor(imc.controllerID, imc.ds, im.Name, im.Spec.NodeID, im.Status.OwnerID)
}
