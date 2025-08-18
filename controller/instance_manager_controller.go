package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/go-common-libs/multierr"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

var (
	mountPropagationHostToContainer = corev1.MountPropagationHostToContainer
)

type InstanceManagerController struct {
	*baseController

	namespace      string
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	instanceManagerMonitorMutex *sync.Mutex
	instanceManagerMonitorMap   map[string]chan struct{}

	proxyConnCounter util.Counter

	backoff *flowcontrol.Backoff

	// for unit test
	versionUpdater func(*longhorn.InstanceManager) error
}

type InstanceManagerMonitor struct {
	logger logrus.FieldLogger

	Name         string
	imName       string
	controllerID string

	ds                 *datastore.DataStore
	lock               *sync.RWMutex
	updateNotification bool
	stopCh             chan struct{}
	done               bool
	// used to notify the controller that monitoring has stopped
	monitorVoluntaryStopCh chan struct{}

	nodeCallback func(nodeName string)

	client      *engineapi.InstanceManagerClient
	proxyClient engineapi.EngineClientProxy
}

type instanceProcessMap map[string]longhorn.InstanceProcess

func updateInstanceManagerVersion(im *longhorn.InstanceManager) error {
	cli, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return err
	}
	defer func(cli io.Closer) {
		if closeErr := cli.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(cli)
	apiMinVersion, apiVersion, proxyAPIMinVersion, proxyAPIVersion, err := cli.VersionGet()
	if err != nil {
		return err
	}
	im.Status.APIMinVersion = apiMinVersion
	im.Status.APIVersion = apiVersion
	im.Status.ProxyAPIMinVersion = proxyAPIMinVersion
	im.Status.ProxyAPIVersion = proxyAPIVersion
	return nil
}

func NewInstanceManagerController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string, proxyConnCounter util.Counter,
) (*InstanceManagerController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-instance-manager-controller"}),

		ds: ds,

		instanceManagerMonitorMutex: &sync.Mutex{},
		instanceManagerMonitorMap:   map[string]chan struct{}{},

		proxyConnCounter: proxyConnCounter,

		versionUpdater: updateInstanceManagerVersion,

		backoff: newBackoff(context.TODO()),
	}

	var err error
	if _, err = ds.InstanceManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    imc.enqueueInstanceManager,
		UpdateFunc: func(old, cur interface{}) { imc.enqueueInstanceManager(cur) },
		DeleteFunc: imc.enqueueInstanceManager,
	}); err != nil {
		return nil, err
	}
	imc.cacheSyncs = append(imc.cacheSyncs, ds.InstanceManagerInformer.HasSynced)

	if _, err = ds.PodInformer.AddEventHandlerWithResyncPeriod(cache.FilteringResourceEventHandler{
		FilterFunc: isInstanceManagerPod,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    imc.enqueueInstanceManagerPod,
			UpdateFunc: func(old, cur interface{}) { imc.enqueueInstanceManagerPod(cur) },
			DeleteFunc: imc.enqueueInstanceManagerPod,
		},
	}, 0); err != nil {
		return nil, err
	}
	imc.cacheSyncs = append(imc.cacheSyncs, ds.PodInformer.HasSynced)

	if _, err = ds.KubeNodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, cur interface{}) { imc.enqueueKubernetesNode(cur) },
		DeleteFunc: imc.enqueueKubernetesNode,
	}, 0); err != nil {
		return nil, err
	}
	imc.cacheSyncs = append(imc.cacheSyncs, ds.KubeNodeInformer.HasSynced)

	if _, err = ds.OrphanInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc: imc.enqueueInstanceManagerOrphan,
	}, 0); err != nil {
		return nil, err
	}
	imc.cacheSyncs = append(imc.cacheSyncs, ds.OrphanInformer.HasSynced)

	if _, err = ds.SettingInformer.AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: imc.isResponsibleForSetting,
			Handler: cache.ResourceEventHandlerFuncs{
				UpdateFunc: func(old, cur interface{}) { imc.enqueueSettingChange(cur) },
			},
		}, 0); err != nil {
		return nil, err
	}
	imc.cacheSyncs = append(imc.cacheSyncs, ds.SettingInformer.HasSynced)

	return imc, nil
}

func (imc *InstanceManagerController) isResponsibleForSetting(obj interface{}) bool {
	setting, ok := obj.(*longhorn.Setting)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		setting, ok = deletedState.Obj.(*longhorn.Setting)
		if !ok {
			return false
		}
	}

	return types.SettingName(setting.Name) == types.SettingNameKubernetesClusterAutoscalerEnabled ||
		types.SettingName(setting.Name) == types.SettingNameDataEngineCPUMask ||
		types.SettingName(setting.Name) == types.SettingNameOrphanResourceAutoDeletion
}

func isInstanceManagerPod(obj interface{}) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			return false
		}
	}

	for _, container := range pod.Spec.Containers {
		switch container.Name {
		case "engine-manager", "replica-manager", "instance-manager":
			return true
		}
	}
	return false
}

func (imc *InstanceManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer imc.queue.ShutDown()

	imc.logger.Info("Starting Longhorn instance manager controller")
	defer imc.logger.Info("Shut down Longhorn instance manager controller")

	if !cache.WaitForNamedCacheSync("longhorn instance manager", stopCh, imc.cacheSyncs...) {
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

	log := imc.logger.WithField("InstanceManager", key)
	if imc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn instance manager")
		imc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn instance manager out of the queue")
	imc.queue.Forget(key)
}

func getLoggerForInstanceManager(logger logrus.FieldLogger, im *longhorn.InstanceManager) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"instanceManager": im.Name,
			"nodeID":          im.Spec.NodeID,
		},
	)
}

func (imc *InstanceManagerController) syncInstanceManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync instance manager for %v", key)
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
			imc.logger.Warnf("Deleting instance manager pod %v since the instance manager is not found", name)
			return imc.cleanupInstanceManagerPod(name)
		}
		return errors.Wrap(err, "failed to get instance manager")
	}

	log := getLoggerForInstanceManager(imc.logger, im)

	if !imc.isResponsibleFor(im) {
		return nil
	}

	if im.Status.OwnerID != imc.controllerID {
		im.Status.OwnerID = imc.controllerID
		im, err = imc.ds.UpdateInstanceManagerStatus(im)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Instance Manager got new owner %v", imc.controllerID)
	}

	if im.DeletionTimestamp != nil {
		log.Warnf("Deleting instance manager pod %v since the instance manager is being deleted", name)
		// the orphan instance CRs will be also deleted because of owner reference
		return imc.cleanupInstanceManagerPod(im.Name)
	}

	existingIM := im.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingIM.Status, im.Status) {
			_, err = imc.ds.UpdateInstanceManagerStatus(im)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			imc.enqueueInstanceManager(im)
			err = nil
		}
	}()

	if err := imc.syncStatusWithPod(im); err != nil {
		return err
	}

	// An instance manager pod for v2 volume need to consume huge pages, and disks managed by the
	// pod is unable to managed by another pod. Therefore, if an instance manager pod is running on a node,
	// an extra instance manager pod for v2 volume should not be created.
	if types.IsDataEngineV2(im.Spec.DataEngine) {
		syncable, err := imc.canProceedWithInstanceManagerSync(im)
		if err != nil {
			return err
		}
		if !syncable {
			return nil
		}
	}

	if err := imc.syncStatusWithNode(im); err != nil {
		return err
	}

	if err := imc.syncInstanceStatus(im); err != nil {
		return err
	}

	if err := imc.handlePod(im); err != nil {
		return err
	}

	if err := imc.syncInstanceManagerPDB(im); err != nil {
		return err
	}

	if err := imc.syncInstanceManagerAPIVersion(im); err != nil {
		return err
	}

	if err := imc.syncMonitor(im); err != nil {
		return err
	}

	if err := imc.syncOrphans(im); err != nil {
		return err
	}

	return nil
}

func (imc *InstanceManagerController) canProceedWithInstanceManagerSync(currentIm *longhorn.InstanceManager) (bool, error) {
	// If the instance manager is not stopped, proceed with the sync.
	if currentIm.Status.CurrentState != longhorn.InstanceManagerStateStopped {
		return true, nil
	}

	ims, err := imc.ds.ListInstanceManagersByNodeRO(currentIm.Spec.NodeID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return false, err
	}
	// If there is another non-stopped instance manager pod for v2 volume, do not proceed with the sync.
	for _, im := range ims {
		if im.Name == currentIm.Name {
			continue
		}

		if im.Status.CurrentState != longhorn.InstanceManagerStateStopped {
			return false, nil
		}
	}

	defaultInstanceManagerImage, err := imc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		return false, err
	}

	// Only active the sync when the default instance manager image is used.
	return currentIm.Spec.Image == defaultInstanceManagerImage, nil
}

// syncStatusWithPod updates the InstanceManager based on the pod current phase only,
// regardless of the InstanceManager previous status.
func (imc *InstanceManagerController) syncStatusWithPod(im *longhorn.InstanceManager) error {
	log := getLoggerForInstanceManager(imc.logger, im)

	previousState := im.Status.CurrentState
	defer func() {
		if previousState != im.Status.CurrentState {
			log.Infof("Instance manager state is updated from %v to %v after syncing with the pod", previousState, im.Status.CurrentState)
		}
	}()

	pod, err := imc.ds.GetPodRO(imc.namespace, im.Name)
	if err != nil {
		return errors.Wrapf(err, "failed get pod for instance manager %v", im.Name)
	}

	if pod == nil {
		if im.Status.CurrentState == "" || im.Status.CurrentState == longhorn.InstanceManagerStateStopped {
			// This state is for newly created InstanceManagers only.
			im.Status.CurrentState = longhorn.InstanceManagerStateStopped
			return nil
		}
		imc.logger.Warnf("Instance manager pod %v is not found, updating the instance manager state from %s to error", im.Name, im.Status.CurrentState)
		im.Status.CurrentState = longhorn.InstanceManagerStateError
		return nil
	}

	// By design instance manager pods should not be terminated.
	if pod.DeletionTimestamp != nil {
		imc.logger.Warnf("Instance manager pod %v is being deleted, updating the instance manager state from %s to error", im.Name, im.Status.CurrentState)
		im.Status.CurrentState = longhorn.InstanceManagerStateError
		return nil
	}

	// Blindly update the state based on the pod phase.
	switch pod.Status.Phase {
	case corev1.PodPending:
		im.Status.CurrentState = longhorn.InstanceManagerStateStarting
	case corev1.PodRunning:
		isReady := true
		// Make sure readiness probe has passed.
		for _, st := range pod.Status.ContainerStatuses {
			isReady = isReady && st.Ready
		}

		if isReady {
			im.Status.CurrentState = longhorn.InstanceManagerStateRunning
			im.Status.IP = pod.Status.PodIP
		} else {
			im.Status.CurrentState = longhorn.InstanceManagerStateStarting
		}
	default:
		imc.logger.Warnf("Instance manager pod %v is in phase %s, updating the instance manager state from %s to error", im.Name, pod.Status.Phase, im.Status.CurrentState)
		im.Status.CurrentState = longhorn.InstanceManagerStateError
	}

	return nil
}

func (imc *InstanceManagerController) syncStatusWithNode(im *longhorn.InstanceManager) error {
	log := getLoggerForInstanceManager(imc.logger, im).WithField("node", im.Spec.NodeID)

	isDown, err := imc.ds.IsNodeDownOrDeleted(im.Spec.NodeID)
	if err != nil {
		return err
	}
	if isDown {
		if im.Status.CurrentState != longhorn.InstanceManagerStateError && im.Status.CurrentState != longhorn.InstanceManagerStateUnknown {
			im.Status.CurrentState = longhorn.InstanceManagerStateUnknown
			log.Infof("Updated the non-error instance manager to state %v due to node down or deleted", longhorn.InstanceManagerStateUnknown)
		}
	}

	return nil
}

// syncInstanceStatus sets the status of instances in special cases independent of InstanceManagerMonitor (e.g. when
// InstanceManagerMonitor isn't running yet).
func (imc *InstanceManagerController) syncInstanceStatus(im *longhorn.InstanceManager) error {
	if im.Status.CurrentState == longhorn.InstanceManagerStateStopped ||
		im.Status.CurrentState == longhorn.InstanceManagerStateError ||
		im.Status.CurrentState == longhorn.InstanceManagerStateStarting {
		// In these states, instance processes either are not running or will soon not be running.
		// This step prevents other controllers from being confused by stale information.
		// InstanceManagerMonitor will change this when/if it polls.
		im.Status.Instances = nil // nolint: staticcheck
		im.Status.InstanceEngines = nil
		im.Status.InstanceReplicas = nil
		im.Status.BackingImages = nil
	}
	return nil
}

func (imc *InstanceManagerController) isDateEngineCPUMaskApplied(im *longhorn.InstanceManager) (bool, error) {
	if types.IsDataEngineV1(im.Spec.DataEngine) {
		return true, nil
	}

	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return true, nil
	}

	if im.Spec.DataEngineSpec.V2.CPUMask != "" {
		return im.Spec.DataEngineSpec.V2.CPUMask == im.Status.DataEngineStatus.V2.CPUMask, nil
	}

	value, err := imc.ds.GetSettingValueExistedByDataEngine(types.SettingNameDataEngineCPUMask, im.Spec.DataEngine)
	if err != nil {
		return true, errors.Wrapf(err, "failed to get %v setting for updating data engine CPU mask", types.SettingNameDataEngineCPUMask)
	}

	return value == im.Status.DataEngineStatus.V2.CPUMask, nil
}

func (imc *InstanceManagerController) syncLogSettingsToInstanceManagerPod(im *longhorn.InstanceManager) error {
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return nil
	}

	client, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return errors.Wrapf(err, "failed to create instance manager client for %v", im.Name)
	}
	defer func(client io.Closer) {
		if closeErr := client.Close(); closeErr != nil {
			imc.logger.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(client)

	settingNames := []types.SettingName{
		types.SettingNameLogLevel,
		types.SettingNameDataEngineLogLevel,
		types.SettingNameDataEngineLogFlags,
	}

	for _, settingName := range settingNames {
		switch settingName {
		case types.SettingNameLogLevel:
			value, err := imc.ds.GetSettingValueExisted(settingName)
			if err != nil {
				return err
			}
			// We use this to set the instance-manager log level, for either engine type.
			err = client.LogSetLevel("", "", value)
			if err != nil {
				return errors.Wrapf(err, "failed to set instance-manager log level to setting %v value: %v", settingName, value)
			}
		case types.SettingNameDataEngineLogLevel:
			// We use this to set the data engine (such as spdk_tgt for v2 data engine) log level independently of the instance-manager's.
			if types.IsDataEngineV2(im.Spec.DataEngine) {
				value, err := imc.ds.GetSettingValueExistedByDataEngine(settingName, im.Spec.DataEngine)
				if err != nil {
					return err
				}
				if err := client.LogSetLevel(longhorn.DataEngineTypeV2, "", value); err != nil {
					return errors.Wrapf(err, "failed to set data engine log level to setting %v value: %v", settingName, value)
				}
			}
		case types.SettingNameDataEngineLogFlags:
			if types.IsDataEngineV2(im.Spec.DataEngine) {
				value, err := imc.ds.GetSettingValueExistedByDataEngine(settingName, im.Spec.DataEngine)
				if err != nil {
					return err
				}
				if err := client.LogSetFlags(longhorn.DataEngineTypeV2, "spdk_tgt", value); err != nil {
					return errors.Wrapf(err, "failed to set data engine log flags to setting %v value: %v", settingName, value)
				}
			}
		}
	}

	return nil
}

func (imc *InstanceManagerController) handlePod(im *longhorn.InstanceManager) error {
	log := getLoggerForInstanceManager(imc.logger, im)

	err := imc.annotateCASafeToEvict(im)
	if err != nil {
		return err
	}

	err = imc.syncLogSettingsToInstanceManagerPod(im)
	if err != nil {
		log.WithError(err).Warnf("Failed to sync log settings to instance manager pod %v", im.Name)
	}

	dataEngineCPUMaskIsApplied, err := imc.isDateEngineCPUMaskApplied(im)
	if err != nil {
		log.WithError(err).Warnf("Failed to sync date engine CPU mask to instance manager pod %v", im.Name)
	}

	isSettingSynced, isPodDeletedOrNotRunning, areInstancesRunningInPod, err := imc.areDangerZoneSettingsSyncedToIMPod(im)
	if err != nil {
		return err
	}

	isPodDeletionNotRequired := (isSettingSynced && dataEngineCPUMaskIsApplied) || areInstancesRunningInPod || isPodDeletedOrNotRunning
	if im.Status.CurrentState != longhorn.InstanceManagerStateError &&
		im.Status.CurrentState != longhorn.InstanceManagerStateStopped &&
		isPodDeletionNotRequired {
		return nil
	}

	log.Warnf("Deleting instance manager pod %v since one of the following conditions is met: "+
		"setting is not synced (%v) or data engine CPU mask is not applied (%v), instances are running in the pod (%v), "+
		"or the pod is deleted or not running (%v)", im.Name, !isSettingSynced, !dataEngineCPUMaskIsApplied, areInstancesRunningInPod, isPodDeletedOrNotRunning)

	if err := imc.cleanupInstanceManagerPod(im.Name); err != nil {
		return err
	}
	// The instance manager pod should be created on the preferred node only.
	if imc.controllerID != im.Spec.NodeID {
		return nil
	}

	// Since `spec.nodeName` is specified during the pod creation,
	// the node cordon cannot prevent the pod being launched.
	if unscheduled, err := imc.ds.IsKubeNodeUnschedulable(im.Spec.NodeID); unscheduled || err != nil {
		return err
	}

	backoffID := im.Name
	if imc.backoff.IsInBackOffSinceUpdate(backoffID, time.Now()) {
		log.Infof("Skipping pod creation for instance manager %s, will retry after backoff of %s", im.Name, imc.backoff.Get(backoffID))
	} else {
		log.Infof("Creating pod for instance manager %s", im.Name)
		imc.backoff.Next(backoffID, time.Now())

		if err := imc.createInstanceManagerPod(im); err != nil {
			return errors.Wrap(err, "failed to create pod for instance manager")
		}
	}

	return nil
}

func (imc *InstanceManagerController) annotateCASafeToEvict(im *longhorn.InstanceManager) error {
	pod, err := imc.ds.GetPod(im.Name)
	if err != nil {
		return errors.Wrapf(err, "cannot get pod for instance manager %v", im.Name)
	}
	if pod == nil {
		return nil
	}

	clusterAutoscalerEnabled, err := imc.ds.GetSettingAsBool(types.SettingNameKubernetesClusterAutoscalerEnabled)
	if err != nil {
		return err
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	val, exist := pod.Annotations[types.KubernetesClusterAutoscalerSafeToEvictKey]
	updateAnnotation := clusterAutoscalerEnabled && (!exist || val != "true")
	deleteAnnotation := !clusterAutoscalerEnabled && exist
	if updateAnnotation {
		pod.Annotations[types.KubernetesClusterAutoscalerSafeToEvictKey] = "true"
	} else if deleteAnnotation {
		delete(pod.Annotations, types.KubernetesClusterAutoscalerSafeToEvictKey)
	} else {
		return nil
	}

	imc.logger.Infof("Updating annotation %v for pod %v/%v", types.KubernetesClusterAutoscalerSafeToEvictKey, pod.Namespace, pod.Name)
	if _, err := imc.kubeClient.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{}); err != nil {
		return err
	}

	return nil
}

func (imc *InstanceManagerController) areDangerZoneSettingsSyncedToIMPod(im *longhorn.InstanceManager) (isSynced, isPodDeletedOrNotRunning, areInstancesRunningInPod bool, err error) {
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return false, true, false, nil
	}

	// nolint:all
	for _, instance := range types.ConsolidateInstances(im.Status.InstanceEngines, im.Status.InstanceReplicas, im.Status.Instances) {
		if instance.Status.State == longhorn.InstanceStateRunning || instance.Status.State == longhorn.InstanceStateStarting {
			return false, false, true, nil
		}
	}

	pod, err := imc.ds.GetPodRO(im.Namespace, im.Name)
	if err != nil {
		return false, false, false, errors.Wrapf(err, "cannot get pod for instance manager %v", im.Name)
	}
	if pod == nil {
		return false, true, false, nil
	}

	for settingName := range types.GetDangerZoneSettings() {
		isSettingSynced := true
		setting, err := imc.ds.GetSettingWithAutoFillingRO(settingName)
		if err != nil {
			return false, false, false, err
		}
		switch settingName {
		case types.SettingNameTaintToleration:
			isSettingSynced, err = imc.isSettingTaintTolerationSynced(setting, pod)
		case types.SettingNameSystemManagedComponentsNodeSelector:
			isSettingSynced, err = imc.isSettingNodeSelectorSynced(setting, pod)
		case types.SettingNameGuaranteedInstanceManagerCPU:
			isSettingSynced, err = imc.isSettingGuaranteedInstanceManagerCPUSynced(setting, pod)
		case types.SettingNamePriorityClass:
			isSettingSynced, err = imc.isSettingPriorityClassSynced(setting, pod)
		case types.SettingNameStorageNetwork:
			isSettingSynced, err = imc.isSettingStorageNetworkSynced(setting, pod)
		case types.SettingNameV1DataEngine, types.SettingNameV2DataEngine:
			isSettingSynced, err = imc.isSettingDataEngineSynced(settingName, im)
		case types.SettingNameInstanceManagerPodLivenessProbeTimeout:
			isSettingSynced, err = imc.isSettingInstanceManagerPodLivenessProbeTimeoutSynced(setting, pod)
		}
		if err != nil {
			return false, false, false, err
		}
		if !isSettingSynced {
			return false, false, false, nil
		}
	}

	return true, false, false, nil
}

func (imc *InstanceManagerController) isSettingTaintTolerationSynced(setting *longhorn.Setting, pod *corev1.Pod) (bool, error) {
	newTolerationsList, err := types.UnmarshalTolerations(setting.Value)
	if err != nil {
		return false, err
	}
	newTolerationsMap := util.TolerationListToMap(newTolerationsList)
	lastAppliedTolerations, err := getLastAppliedTolerationsList(pod)
	if err != nil {
		return false, err
	}

	return reflect.DeepEqual(util.TolerationListToMap(lastAppliedTolerations), newTolerationsMap), nil
}

func (imc *InstanceManagerController) isSettingNodeSelectorSynced(setting *longhorn.Setting, pod *corev1.Pod) (bool, error) {
	newNodeSelector, err := types.UnmarshalNodeSelector(setting.Value)
	if err != nil {
		return false, err
	}
	if pod.Spec.NodeSelector == nil && len(newNodeSelector) == 0 {
		return true, nil
	}

	return reflect.DeepEqual(pod.Spec.NodeSelector, newNodeSelector), nil
}

func (imc *InstanceManagerController) isSettingGuaranteedInstanceManagerCPUSynced(setting *longhorn.Setting, pod *corev1.Pod) (bool, error) {
	lhNode, err := imc.ds.GetNode(pod.Spec.NodeName)
	if err != nil {
		return false, err
	}
	if types.GetCondition(lhNode.Status.Conditions, longhorn.NodeConditionTypeReady).Status != longhorn.ConditionStatusTrue {
		return true, nil
	}

	resourceReq, err := GetInstanceManagerCPURequirement(imc.ds, pod.Name)
	if err != nil {
		return false, err
	}
	podResourceReq := pod.Spec.Containers[0].Resources
	return IsSameGuaranteedCPURequirement(resourceReq, &podResourceReq), nil
}

func (imc *InstanceManagerController) isSettingPriorityClassSynced(setting *longhorn.Setting, pod *corev1.Pod) (bool, error) {
	return pod.Spec.PriorityClassName == setting.Value, nil
}

func (imc *InstanceManagerController) isSettingInstanceManagerPodLivenessProbeTimeoutSynced(setting *longhorn.Setting, pod *corev1.Pod) (bool, error) {
	if pod.Spec.Containers[0].LivenessProbe == nil {
		// If the liveness probe is not set, we consider it synced.
		return true, nil
	}

	timeoutSeconds, err := strconv.Atoi(setting.Value)
	if err != nil {
		return false, errors.Wrapf(err, "failed to convert %v setting value %v to int",
			types.SettingNameInstanceManagerPodLivenessProbeTimeout, setting.Value)
	}

	return pod.Spec.Containers[0].LivenessProbe.TimeoutSeconds == int32(timeoutSeconds), nil
}

func (imc *InstanceManagerController) isSettingStorageNetworkSynced(setting *longhorn.Setting, pod *corev1.Pod) (bool, error) {
	nadAnnot := string(types.CNIAnnotationNetworks)
	nadAnnotValue := types.CreateCniAnnotationFromSetting(setting)
	return pod.Annotations[nadAnnot] == nadAnnotValue, nil
}

// isSettingDataEngineSynced checks if the data engine setting is synced with the instance manager.
// If the data engine setting is disabled, it checks if the instance manager's data engine type is equal to the disabled data engine type.
// If YES, this instance manager pod should be removed if no running instances are found.
func (imc *InstanceManagerController) isSettingDataEngineSynced(settingName types.SettingName, im *longhorn.InstanceManager) (bool, error) {
	enabled, err := imc.ds.GetSettingAsBool(settingName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get %v setting for checking data engine sync", settingName)
	}
	var dataEngine longhorn.DataEngineType
	switch settingName {
	case types.SettingNameV1DataEngine:
		dataEngine = longhorn.DataEngineTypeV1
	case types.SettingNameV2DataEngine:
		dataEngine = longhorn.DataEngineTypeV2
	}
	if !enabled && im.Spec.DataEngine == dataEngine {
		return false, nil
	}

	return true, nil
}

func (imc *InstanceManagerController) syncInstanceManagerAPIVersion(im *longhorn.InstanceManager) error {
	// Avoid changing API versions when InstanceManagers are state Unknown.
	// Then once required (in the future), the monitor could still talk with the pod and update processes in some corner cases. e.g., kubelet restart.
	// But for now this controller will do nothing for Unknown InstanceManagers.
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning && im.Status.CurrentState != longhorn.InstanceManagerStateUnknown {
		im.Status.APIVersion = engineapi.UnknownInstanceManagerAPIVersion
		im.Status.APIMinVersion = engineapi.UnknownInstanceManagerAPIVersion
		return nil
	}

	shouldUpdateAPIVersion := im.Status.APIVersion == engineapi.UnknownInstanceManagerAPIVersion
	shouldUpdateProxyAPIVersion := im.Status.ProxyAPIVersion == engineapi.UnknownInstanceManagerProxyAPIVersion
	if im.Status.CurrentState == longhorn.InstanceManagerStateRunning && (shouldUpdateAPIVersion || shouldUpdateProxyAPIVersion) {
		if err := imc.versionUpdater(im); err != nil {
			return err
		}
	}
	return nil
}

func (imc *InstanceManagerController) syncMonitor(im *longhorn.InstanceManager) error {
	// For now Longhorn won't actively disable or enable monitoring when the InstanceManager is Unknown.
	if im.Status.CurrentState == longhorn.InstanceManagerStateUnknown {
		return nil
	}

	isMonitorRequired := im.Status.CurrentState == longhorn.InstanceManagerStateRunning &&
		engineapi.CheckInstanceManagerCompatibility(im.Status.APIMinVersion, im.Status.APIVersion) == nil

	// BackingImage monitoring is only required for v2 data engine
	// and it uses proxy client instead of instance manager client.
	// Thus, we use another monitor goroutine for backing image monitoring for better maintenance.
	if isMonitorRequired {
		imc.startMonitoring(im)
		if types.IsDataEngineV2(im.Spec.DataEngine) {
			imc.startBackingImageMonitoring(im)
		}
	} else {
		imc.stopMonitoring(im.Name)
		if types.IsDataEngineV2(im.Spec.DataEngine) {
			imc.stopBackingImageMonitoring(im.Name)
		}
	}

	return nil
}

func (imc *InstanceManagerController) syncInstanceManagerPDB(im *longhorn.InstanceManager) error {
	if err := imc.cleanUpPDBForNonExistingIM(); err != nil {
		return err
	}

	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return nil
	}

	unschedulable, err := imc.ds.IsKubeNodeUnschedulable(im.Spec.NodeID)
	if err != nil {
		return err
	}

	imPDB, err := imc.ds.GetPDBRO(types.GetPDBName(im))
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	// When current node is unschedulable, it is a signal that the node is being
	// cordoned/drained. The replica IM PDB can be delete when there is least one
	// IM PDB on another schedulable node to protect detached volume data.
	//
	// During Cluster Autoscaler scale down, when a node is marked unschedulable
	// means CA already decided that this node is not blocked by any pod PDB limit.
	// Hence there is no need to check when Cluster Autoscaler is enabled.
	if unschedulable {
		if imPDB == nil {
			return nil
		}

		canDeletePDB, msg, err := imc.canDeleteInstanceManagerPDB(im)
		if err != nil {
			return err
		}

		if !canDeletePDB {
			imc.logger.Infof("Node %v is marked unschedulable but removing %v PDB is blocked: %v ", im.Name, imc.controllerID, msg)
			return nil
		}

		imc.logger.Infof("Removing %v PDB since Node %v is marked unschedulable", im.Name, imc.controllerID)
		return imc.deleteInstanceManagerPDB(im)
	}

	// If the setting is enabled, Longhorn needs to retain the least IM PDBs as
	// possible. Each volume will have at least one replica under the protection
	// of an IM PDB while no redundant PDB blocking the Cluster Autoscaler from
	// scale down.
	// CA considers a node is unremovable when there are strict PDB limits
	// protecting the pods on the node.
	//
	// If the setting is disabled, Longhorn will blindly create IM PDBs for all
	// engine and replica IMs.
	clusterAutoscalerEnabled, err := imc.ds.GetSettingAsBool(types.SettingNameKubernetesClusterAutoscalerEnabled)
	if err != nil {
		return err
	}

	if clusterAutoscalerEnabled {
		canDeletePDB, msg, err := imc.canDeleteInstanceManagerPDB(im)
		if err != nil {
			return err
		}

		if !canDeletePDB {
			imc.logger.Debugf("Autoscaler is enabled but removing %v PDB for node %v is blocked: %v", im.Name, imc.controllerID, msg)
			if imPDB == nil {
				return imc.createInstanceManagerPDB(im)
			}
			return nil
		}

		if imPDB != nil {
			return imc.deleteInstanceManagerPDB(im)
		}

		return nil
	}

	// Make sure that there is a PodDisruptionBudget to protect this instance manager in normal case.
	if imPDB == nil {
		return imc.createInstanceManagerPDB(im)
	}

	return nil
}

func (imc *InstanceManagerController) cleanUpPDBForNonExistingIM() error {
	ims, err := imc.ds.ListInstanceManagersRO()
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		ims = make(map[string]*longhorn.InstanceManager)
	}

	imPDBs, err := imc.ds.ListPDBsRO()
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return err
		}
		imPDBs = make(map[string]*policyv1.PodDisruptionBudget)
	}

	for pdbName, pdb := range imPDBs {
		if pdb.Spec.Selector == nil || pdb.Spec.Selector.MatchLabels == nil {
			continue
		}
		labelValue, ok := pdb.Spec.Selector.MatchLabels[types.GetLonghornLabelComponentKey()]
		if !ok {
			continue
		}
		if labelValue != types.LonghornLabelInstanceManager {
			continue
		}
		if _, ok := ims[types.GetIMNameFromPDBName(pdbName)]; ok {
			continue
		}
		if err := imc.ds.DeletePDB(pdbName); err != nil {
			if !datastore.ErrorIsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

func (imc *InstanceManagerController) deleteInstanceManagerPDB(im *longhorn.InstanceManager) error {
	name := types.GetPDBName(im)
	imc.logger.Infof("Deleting %v PDB", name)
	err := imc.ds.DeletePDB(name)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}
	return nil
}

func (imc *InstanceManagerController) syncOrphans(im *longhorn.InstanceManager) error {
	// Instances are live inside the instance manager pod.
	// Remove the instance orphan CRs when the instance manager pod is deleted or not running.
	var isInstanceManagerPodDeletedOrNotRunning bool
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		isInstanceManagerPodDeletedOrNotRunning = true
	} else {
		switch pod, err := imc.ds.GetPodRO(im.Namespace, im.Name); {
		case datastore.ErrorIsNotFound(err):
			isInstanceManagerPodDeletedOrNotRunning = true
		case err != nil:
			return errors.Wrapf(err, "failed to check instance manager %v pod state while sync orphans", im.Name)
		default:
			isInstanceManagerPodDeletedOrNotRunning = pod == nil
		}
	}

	// Once the original owner controller lost the ownership of the instance manager CR, it is no longer able to monitor the instance status.
	// This may be caused by network outage or Longhorn manager failure.
	// Remove the corresponding orphan CRs, and they will be resync after the owner instance manager gets back.
	instanceManagerOwnershipChanged := im.Spec.NodeID != imc.controllerID

	isInstanceManagerTerminating := isInstanceManagerPodDeletedOrNotRunning || instanceManagerOwnershipChanged
	return imc.deleteOrphans(im, isInstanceManagerTerminating)
}

const ITERATE_NAME_LIMIT = 5

func formatInstanceMessage(im *longhorn.InstanceManager) string {
	msg := ""
	ieFormated := false
	if len(im.Status.InstanceEngines) > 0 {
		msg = fmt.Sprintf("InstanceEngines count %v", len(im.Status.InstanceEngines))
		ieFormated = true
		i := 0
		// only fetch the key, format: pvc-546894e9-d41a-4e34-bfa8-a4442f8dce20-e-0
		for k := range im.Status.InstanceEngines {
			msg = msg + " " + k
			if i++; i >= ITERATE_NAME_LIMIT {
				break
			}
		}
	}

	if len(im.Status.Instances) > 0 { // nolint: staticcheck
		if ieFormated {
			msg = fmt.Sprintf("%v Instances count %v", msg, len(im.Status.Instances)) // nolint: staticcheck
		} else {
			msg = fmt.Sprintf("Instances count %v", len(im.Status.Instances)) // nolint: staticcheck
		}
		i := 0
		for k := range im.Status.Instances { // nolint: staticcheck
			msg = msg + " " + k
			if i++; i >= ITERATE_NAME_LIMIT {
				break
			}
		}
	}

	return msg
}

func formatReplicaMessage(rep []*longhorn.Replica) string {
	limit := len(rep)
	msg := fmt.Sprintf("count %v", limit)
	if limit > ITERATE_NAME_LIMIT {
		limit = ITERATE_NAME_LIMIT
	}
	for i := 0; i < limit; i++ {
		msg = msg + " " + rep[i].Name
	}
	return msg
}

func (imc *InstanceManagerController) canDeleteInstanceManagerPDB(im *longhorn.InstanceManager) (bool, string, error) {
	// If there is no engine instance process inside the engine instance manager,
	// it means that all volumes are detached.
	// We can delete the PodDisruptionBudget for the engine instance manager.
	if im.Spec.Type == longhorn.InstanceManagerTypeEngine {
		if len(im.Status.InstanceEngines)+len(im.Status.Instances) == 0 { // nolint: staticcheck
			return true, "", nil
		}
		return false, fmt.Sprintf("some instances are still running %v", formatInstanceMessage(im)), nil
	}

	// Make sure that the instance manager is of type replica
	if im.Spec.Type != longhorn.InstanceManagerTypeReplica && im.Spec.Type != longhorn.InstanceManagerTypeAllInOne {
		return false, "", fmt.Errorf("the instance manager %v has invalid type: %v ", im.Name, im.Spec.Type)
	}

	// Must wait for all volumes detached from the current node first.
	// This also means that we must wait until the PDB of engine instance manager
	// on the current node is deleted
	allVolumeDetached, msg, err := imc.areAllVolumesDetachedFromNode(im.Spec.NodeID)
	if err != nil {
		return false, "", err
	}
	if !allVolumeDetached {
		return false, fmt.Sprintf("some volumes are still attached %v", msg), nil
	}

	nodeDrainingPolicy, err := imc.ds.GetSettingValueExisted(types.SettingNameNodeDrainPolicy)
	if err != nil {
		return false, "", err
	}
	if nodeDrainingPolicy == string(types.NodeDrainPolicyAlwaysAllow) {
		return true, "", nil
	}

	replicasOnCurrentNode, err := imc.ds.ListReplicasByNodeRO(im.Spec.NodeID)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return true, "", nil
		}
		return false, "", err
	}

	if nodeDrainingPolicy == string(types.NodeDrainPolicyBlockForEviction) && len(replicasOnCurrentNode) > 0 {
		// We must wait for ALL replicas to be evicted before removing the PDB.
		return false, fmt.Sprintf("some replicas block eviction %v", formatReplicaMessage(replicasOnCurrentNode)), nil
	}

	targetReplicas := []*longhorn.Replica{}
	if nodeDrainingPolicy == string(types.NodeDrainPolicyAllowIfReplicaIsStopped) {
		for _, replica := range replicasOnCurrentNode {
			if replica.Spec.DesireState != longhorn.InstanceStateStopped || replica.Status.CurrentState != longhorn.InstanceStateStopped {
				targetReplicas = append(targetReplicas, replica)
			}
		}
	} else {
		targetReplicas = replicasOnCurrentNode
	}

	// For each replica in the target replica list, find out whether there is a PDB protected healthy replica of the
	// same volume on another schedulable node.
	for _, replica := range targetReplicas {
		hasPDBOnAnotherNode := false
		isUnusedReplicaOnCurrentNode := false

		pdbProtectedHealthyReplicas, err := imc.ds.ListVolumePDBProtectedHealthyReplicasRO(replica.Spec.VolumeName)
		if err != nil {
			return false, "", err
		}
		for _, pdbProtectedHealthyReplica := range pdbProtectedHealthyReplicas {
			if pdbProtectedHealthyReplica.Spec.NodeID != im.Spec.NodeID {
				hasPDBOnAnotherNode = true
				break
			}
		}

		// If a replica has never been started, there is no data stored in this replica, and retaining it makes no sense
		// for HA. Hence Longhorn doesn't need to block the PDB removal for the replica. This case typically happens on
		// a newly created volume that hasn't been attached to any node.
		// https://github.com/longhorn/longhorn/issues/2673
		isUnusedReplicaOnCurrentNode = replica.Spec.HealthyAt == "" &&
			replica.Spec.FailedAt == "" &&
			replica.Spec.NodeID == im.Spec.NodeID

		if !hasPDBOnAnotherNode && !isUnusedReplicaOnCurrentNode {
			return false, fmt.Sprintf("replica %v has no pdb on another node", replica.Name), nil
		}
	}

	return true, "", nil
}

func (imc *InstanceManagerController) areAllVolumesDetachedFromNode(nodeName string) (bool, string, error) {
	detached, msg, err := imc.areAllInstanceRemovedFromNodeByType(nodeName, longhorn.InstanceManagerTypeEngine)
	if err != nil {
		return false, msg, err
	}
	if !detached {
		return false, msg, nil
	}

	detached, msg, err = imc.areAllInstanceRemovedFromNodeByType(nodeName, longhorn.InstanceManagerTypeAllInOne)
	if err != nil {
		return false, msg, err
	}
	return detached, msg, nil
}

func (imc *InstanceManagerController) areAllInstanceRemovedFromNodeByType(nodeName string, imType longhorn.InstanceManagerType) (bool, string, error) {
	ims, err := imc.ds.ListInstanceManagersByNodeRO(nodeName, imType, "")
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return true, "", nil
		}
		return false, "", err
	}

	for _, im := range ims {
		if len(im.Status.InstanceEngines)+len(im.Status.Instances) > 0 { // nolint: staticcheck
			return false, formatInstanceMessage(im), nil
		}
	}

	return true, "", nil
}

func (imc *InstanceManagerController) createInstanceManagerPDB(im *longhorn.InstanceManager) error {
	instanceManagerPDB := imc.generateInstanceManagerPDBManifest(im)
	imc.logger.Infof("Creating %v PDB", instanceManagerPDB.Name)
	if _, err := imc.ds.CreatePDB(instanceManagerPDB); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	return nil
}

func (imc *InstanceManagerController) generateInstanceManagerPDBManifest(im *longhorn.InstanceManager) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.GetPDBName(im),
			Namespace: imc.namespace,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetInstanceManagerLabels(im.Spec.NodeID, im.Spec.Image, im.Spec.Type, im.Spec.DataEngine),
			},
			MinAvailable: &intstr.IntOrString{IntVal: 1},
		},
	}
}

func (imc *InstanceManagerController) enqueueInstanceManager(instanceManager interface{}) {
	key, err := controller.KeyFunc(instanceManager)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", instanceManager, err))
		return
	}

	imc.queue.Add(key)
}

func (imc *InstanceManagerController) enqueueInstanceManagerPod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	im, err := imc.ds.GetInstanceManagerRO(pod.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		utilruntime.HandleError(fmt.Errorf("failed to get instance manager: %v", err))
		return
	}
	imc.enqueueInstanceManager(im)
}

func (imc *InstanceManagerController) enqueueKubernetesNode(obj interface{}) {
	kubernetesNode, ok := obj.(*corev1.Node)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		kubernetesNode, ok = deletedState.Obj.(*corev1.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	imc.enqueueInstanceManagersForNode(kubernetesNode.Name)
}

func (imc *InstanceManagerController) enqueueInstanceManagerOrphan(obj interface{}) {
	orphan, ok := obj.(*longhorn.Orphan)
	if !ok {
		return
	}

	imc.enqueueInstanceManagersForNode(orphan.Spec.NodeID)
}

func (imc *InstanceManagerController) enqueueInstanceManagersForNode(nodeName string) {
	node, err := imc.ds.GetNodeRO(nodeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// there is no Longhorn node created for the Kubernetes
			// node (e.g. controller/etcd node). Skip it
			return
		}
		utilruntime.HandleError(fmt.Errorf("failed to get node %v: %v ", nodeName, err))
		return
	}

	for _, imType := range []longhorn.InstanceManagerType{longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerTypeAllInOne} {
		ims, err := imc.ds.ListInstanceManagersByNodeRO(node.Name, imType, "")
		if err != nil {
			if apierrors.IsNotFound(err) {
				return
			}
			utilruntime.HandleError(fmt.Errorf("failed to get instance manager: %v", err))
			return
		}

		for _, im := range ims {
			imc.enqueueInstanceManager(im)
		}
	}
}

func (imc *InstanceManagerController) enqueueSettingChange(obj interface{}) {
	imc.enqueueInstanceManagersForNode(imc.controllerID)
}

func (imc *InstanceManagerController) cleanupInstanceManagerPod(imName string) error {
	imc.stopMonitoring(imName)
	imc.stopBackingImageMonitoring(imName)

	pod, err := imc.ds.GetPodRO(imc.namespace, imName)
	if err != nil {
		return err
	}
	if pod != nil && pod.DeletionTimestamp == nil {
		imc.logger.Infof("Deleting instance manager pod %v for instance manager %v", pod.Name, imName)
		if err := imc.ds.DeletePod(pod.Name); err != nil {
			return err
		}
	}

	return nil
}

func (imc *InstanceManagerController) createInstanceManagerPod(im *longhorn.InstanceManager) error {
	log := getLoggerForInstanceManager(imc.logger, im)

	tolerations, err := imc.ds.GetSettingTaintToleration()
	if err != nil {
		return errors.Wrap(err, "failed to get taint toleration setting before creating instance manager pod")
	}

	nodeSelector, err := imc.ds.GetSettingSystemManagedComponentsNodeSelector()
	if err != nil {
		return errors.Wrap(err, "failed to get node selector setting before creating instance manager pod")
	}

	registrySecretSetting, err := imc.ds.GetSettingWithAutoFillingRO(types.SettingNameRegistrySecret)
	if err != nil {
		return errors.Wrap(err, "failed to get registry secret setting before creating instance manager pod")
	}

	registrySecret := registrySecretSetting.Value

	var podSpec *corev1.Pod
	podSpec, err = imc.createInstanceManagerPodSpec(im, tolerations, registrySecret, nodeSelector, im.Spec.DataEngine)
	if err != nil {
		return err
	}

	storageNetwork, err := imc.ds.GetSettingWithAutoFillingRO(types.SettingNameStorageNetwork)
	if err != nil {
		return err
	}

	nadAnnot := string(types.CNIAnnotationNetworks)
	if storageNetwork.Value != types.CniNetworkNone {
		podSpec.Annotations[nadAnnot] = types.CreateCniAnnotationFromSetting(storageNetwork)
	}

	log.Info("Creating instance manager pod")
	if _, err := imc.ds.CreatePod(podSpec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	return nil
}

func (imc *InstanceManagerController) createGenericManagerPodSpec(im *longhorn.InstanceManager, tolerations []corev1.Toleration, registrySecret string, nodeSelector map[string]string) (*corev1.Pod, error) {
	tolerationsByte, err := json.Marshal(tolerations)
	if err != nil {
		return nil, err
	}

	priorityClass, err := imc.ds.GetSettingWithAutoFillingRO(types.SettingNamePriorityClass)
	if err != nil {
		return nil, err
	}

	imagePullPolicy, err := imc.ds.GetSettingImagePullPolicy()
	if err != nil {
		return nil, err
	}

	privileged := true
	podSpec := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            im.Name,
			Namespace:       imc.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForInstanceManager(im),
			Annotations:     map[string]string{types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix): string(tolerationsByte)},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: imc.serviceAccount,
			Tolerations:        util.GetDistinctTolerations(tolerations),
			NodeSelector:       nodeSelector,
			PriorityClassName:  priorityClass.Value,
			Containers: []corev1.Container{
				{
					Image:           im.Spec.Image,
					ImagePullPolicy: imagePullPolicy,
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			NodeName:      im.Spec.NodeID,
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	if registrySecret != "" {
		podSpec.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}

	// Apply resource requirements to newly created Instance Manager Pods.
	cpuResourceReq, err := GetInstanceManagerCPURequirement(imc.ds, im.Name)
	if err != nil {
		return nil, err
	}
	// Do nothing for the CPU requests if the value is 0.
	if cpuResourceReq != nil {
		podSpec.Spec.Containers[0].Resources = *cpuResourceReq
	}

	return podSpec, nil
}

func (imc *InstanceManagerController) createInstanceManagerPodSpec(im *longhorn.InstanceManager, tolerations []corev1.Toleration, registrySecret string, nodeSelector map[string]string, dataEngine longhorn.DataEngineType) (*corev1.Pod, error) {
	podSpec, err := imc.createGenericManagerPodSpec(im, tolerations, registrySecret, nodeSelector)
	if err != nil {
		return nil, err
	}

	secretIsOptional := true
	podSpec.Labels = types.GetInstanceManagerLabels(imc.controllerID, im.Spec.Image, longhorn.InstanceManagerTypeAllInOne, dataEngine)
	podSpec.Spec.Containers[0].Name = "instance-manager"

	if types.IsDataEngineV2(dataEngine) {
		// spdk_tgt doesn't support log level option, so we don't need to pass the log level to the instance manager.
		// The log level will be applied in the reconciliation of instance manager controller.
		logFlagsSetting, err := imc.ds.GetSettingValueExistedByDataEngine(types.SettingNameDataEngineLogFlags, dataEngine)
		if err != nil {
			return nil, err
		}

		logFlags := "all"
		if logFlagsSetting != "" {
			logFlags = strings.ToLower(logFlagsSetting)
		}

		cpuMask := im.Spec.DataEngineSpec.V2.CPUMask
		if cpuMask == "" {
			value, err := imc.ds.GetSettingValueExistedByDataEngine(types.SettingNameDataEngineCPUMask, dataEngine)
			if err != nil {
				return nil, err
			}

			cpuMask = value
			if cpuMask == "" {
				return nil, fmt.Errorf("failed to get CPU mask setting for data engine %v", dataEngine)
			}
		}

		im.Status.DataEngineStatus.V2.CPUMask = cpuMask

		args := []string{
			"instance-manager",
			"--spdk-log", logFlags,
			"--spdk-cpumask", cpuMask,
			"--enable-spdk", "--debug",
			"daemon",
			"--spdk-enabled",
			"--listen", fmt.Sprintf("0.0.0.0:%d", engineapi.InstanceManagerProcessManagerServiceDefaultPort)}

		imc.logger.Infof("Creating instance manager pod %v with args %+v", podSpec.Name, args)

		podSpec.Spec.Containers[0].Args = args

		hugepage, err := imc.ds.GetSettingAsIntByDataEngine(types.SettingNameDataEngineHugepageLimit, im.Spec.DataEngine)
		if err != nil {
			return nil, err
		}

		if podSpec.Spec.Containers[0].Resources.Requests == nil {
			podSpec.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
		}
		podSpec.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("128Mi")

		if podSpec.Spec.Containers[0].Resources.Limits == nil {
			podSpec.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
		}
		podSpec.Spec.Containers[0].Resources.Limits[corev1.ResourceName("hugepages-2Mi")] = resource.MustParse(fmt.Sprintf("%vMi", hugepage))

		podSpec.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"instance-manager-v2-prestop"},
				}},
		}
	} else {
		podSpec.Spec.Containers[0].Args = []string{
			"instance-manager", "--debug", "daemon", "--listen", fmt.Sprintf(":%d", engineapi.InstanceManagerProcessManagerServiceDefaultPort),
		}
	}

	// Create a liveness probe to check if all the required ports and processes are open.
	var livenessProbes []string
	ports := []int{
		engineapi.InstanceManagerProcessManagerServiceDefaultPort,
		engineapi.InstanceManagerProxyServiceDefaultPort,
		engineapi.InstanceManagerDiskServiceDefaultPort,
		engineapi.InstanceManagerInstanceServiceDefaultPort,
	}
	for _, port := range ports {
		livenessProbes = append(livenessProbes, fmt.Sprintf("nc -zv localhost %d > /dev/null 2>&1", port))
	}
	if types.IsDataEngineV2(dataEngine) {
		livenessProbes = append(livenessProbes, fmt.Sprintf("nc -zv localhost %d > /dev/null 2>&1", engineapi.InstanceManagerSpdkServiceDefaultPort))

		processProbe := "[ $(ps aux | grep 'spdk_tgt' | grep -v 'grep' | grep -v 'tee' | wc -l) != 0 ]"
		livenessProbes = append(livenessProbes, processProbe)
	}
	livenessProbeCommand := fmt.Sprintf("test $(%s; echo $?) -eq 0", strings.Join(livenessProbes, " && "))

	podProbeTimeout, err := imc.ds.GetSettingAsInt(types.SettingNameInstanceManagerPodLivenessProbeTimeout)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %v setting", types.SettingNameInstanceManagerPodLivenessProbeTimeout)
	}

	podSpec.Spec.Containers[0].LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"/bin/sh",
					"-c",
					livenessProbeCommand,
				},
			},
		},
		InitialDelaySeconds: datastore.IMPodProbeInitialDelay,
		TimeoutSeconds:      int32(podProbeTimeout),
		PeriodSeconds:       int32(podProbeTimeout + 1),
		FailureThreshold:    datastore.IMPodLivenessProbeFailureThreshold,
	}

	// Set environment variables
	podSpec.Spec.Containers[0].Env = []corev1.EnvVar{
		{
			Name:  "TLS_DIR",
			Value: types.TLSDirectoryInContainer,
		},
		{
			Name: types.EnvPodIP,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
	}
	// Set volume mounts
	podSpec.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{
			MountPath:        "/host",
			Name:             "host",
			MountPropagation: &mountPropagationHostToContainer,
		},
		{
			MountPath:        types.EngineBinaryDirectoryInContainer,
			Name:             "engine-binaries",
			MountPropagation: &mountPropagationHostToContainer,
		},
		{
			MountPath: types.UnixDomainSocketDirectoryInContainer,
			Name:      "unix-domain-socket",
		},
		{
			MountPath: types.TLSDirectoryInContainer,
			Name:      "longhorn-grpc-tls",
		},
	}
	podSpec.Spec.Volumes = []corev1.Volume{
		{
			Name: "host",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/",
				},
			},
		},
		{
			Name: "engine-binaries",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: types.EngineBinaryDirectoryOnHost,
				},
			},
		},
		{
			Name: "unix-domain-socket",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: types.UnixDomainSocketDirectoryOnHost,
				},
			},
		},
		{
			Name: "longhorn-grpc-tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: types.TLSSecretName,
					Optional:   &secretIsOptional,
				},
			},
		},
	}

	if types.IsDataEngineV2(dataEngine) {
		podSpec.Spec.Containers[0].VolumeMounts = append(podSpec.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			MountPath: "/hugepages",
			Name:      "hugepage",
		})

		podSpec.Spec.Volumes = append(podSpec.Spec.Volumes, corev1.Volume{
			Name: "hugepage",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumHugePages,
				},
			},
		})
	}
	types.AddGoCoverDirToPod(podSpec)

	return podSpec, nil
}

// deleteOrphans examines existing instance orphan CRs, and initiates CR deletion if needed.
//
// Orphan CRs will be deleted under any of the following conditions:
//   - The instance manager is terminating or has terminated, which results in the removal of its managed instances.
//   - An instance is missing from the instance manager.
//   - An instance has been rescheduled to the instance manager and is no longer considered orphaned.
//   - Automatic deletion of orphan resources is enabled.
func (imc *InstanceManagerController) deleteOrphans(im *longhorn.InstanceManager, isInstanceManagerTerminating bool) error {
	autoDeletionTypes, err := imc.ds.GetSettingOrphanResourceAutoDeletion()
	if err != nil {
		return errors.Wrapf(err, "failed to get setting %v", types.SettingNameOrphanResourceAutoDeletion)
	}
	autoDeleteEnabled, ok := autoDeletionTypes[types.OrphanResourceTypeInstance]
	if !ok {
		autoDeleteEnabled = false
	}

	autoDeleteGracePeriod, err := imc.ds.GetSettingAsInt(types.SettingNameOrphanResourceAutoDeletionGracePeriod)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v setting", types.SettingNameOrphanResourceAutoDeletionGracePeriod)
	}

	orphanList, err := imc.ds.ListInstanceOrphansByInstanceManagerRO(im.Name)
	if err != nil {
		return err
	}
	errs := multierr.NewMultiError()
	for _, orphan := range orphanList {
		if !orphan.DeletionTimestamp.IsZero() {
			continue
		}

		instanceManager := orphan.Spec.Parameters[longhorn.OrphanInstanceManager]
		instanceName := orphan.Spec.Parameters[longhorn.OrphanInstanceName]
		var instanceExist = false
		var instanceCRScheduledBack = false
		switch orphan.Spec.Type {
		case longhorn.OrphanTypeEngineInstance:
			_, instanceExist = im.Status.InstanceEngines[instanceName]
			instanceCRScheduledBack, err = imc.isEngineOnInstanceManager(instanceManager, instanceName)
		case longhorn.OrphanTypeReplicaInstance:
			_, instanceExist = im.Status.InstanceReplicas[instanceName]
			instanceCRScheduledBack, err = imc.isReplicaOnInstanceManager(instanceManager, instanceName)
		}
		if err != nil {
			errs.Append("errors", errors.Wrapf(err, "failed to check if instance %v is scheduled on instance manager %v", instanceName, instanceManager))
			continue
		}

		if imc.canDeleteOrphan(orphan, isInstanceManagerTerminating, autoDeleteEnabled, instanceExist, instanceCRScheduledBack, autoDeleteGracePeriod) {
			if err := imc.deleteOrphan(orphan); err != nil {
				if datastore.ErrorIsNotFound(err) {
					continue
				}
				errs.Append("errors", errors.Wrapf(err, "failed to delete orphan %v", orphan.Name))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to delete orphans: %v", errs.ErrorByReason("errors"))
	}
	return nil
}

func (imc *InstanceManagerController) deleteOrphan(orphan *longhorn.Orphan) error {
	imc.logger.Infof("Deleting Orphan %v", orphan.Name)
	if err := imc.ds.DeleteOrphan(orphan.Name); err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to delete orphan %q", orphan.Name)
	}
	return nil
}

func (imc *InstanceManagerController) canDeleteOrphan(orphan *longhorn.Orphan, imTerminating, autoDeleteEnabled, instanceExist, instanceCRScheduledBack bool, autoDeleteGracePeriod int64) bool {
	autoDeleteAllowed := false
	if autoDeleteEnabled {
		elapsedTime := time.Since(orphan.CreationTimestamp.Time).Seconds()
		if elapsedTime > float64(autoDeleteGracePeriod) {
			autoDeleteAllowed = true
		}
	}

	canDelete := imTerminating || autoDeleteAllowed || !instanceExist || instanceCRScheduledBack
	if !canDelete {
		imc.logger.Debugf("Orphan %v is not ready to be deleted, imTerminating: %v, autoDeleteAllowed: %v, instanceExist: %v, instanceCRScheduledBack: %v", orphan.Name, imTerminating, autoDeleteAllowed, instanceExist, instanceCRScheduledBack)
	}

	return canDelete
}

func (imc *InstanceManagerController) isEngineOnInstanceManager(instanceManager string, instance string) (bool, error) {
	existEngine, err := imc.ds.GetEngineRO(instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Engine CR not found - instance is orphaned
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to check if engine instance %q is scheduled on instance manager %q", instance, instanceManager)
	}
	return imc.isInstanceOnInstanceManager(instanceManager, &existEngine.ObjectMeta, &existEngine.Spec.InstanceSpec, &existEngine.Status.InstanceStatus), nil
}

func (imc *InstanceManagerController) isReplicaOnInstanceManager(instanceManager string, instance string) (bool, error) {
	existReplica, err := imc.ds.GetReplicaRO(instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Replica CR not found - instance is orphaned
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to check if replica instance %q is scheduled on instance manager %q", instance, instanceManager)
	}
	return imc.isInstanceOnInstanceManager(instanceManager, &existReplica.ObjectMeta, &existReplica.Spec.InstanceSpec, &existReplica.Status.InstanceStatus), nil
}

// isInstanceOnInstanceManager returns true only when it is very certain that an instance is scheduled in a given instance manager
func (imc *InstanceManagerController) isInstanceOnInstanceManager(instanceManager string, meta *metav1.ObjectMeta, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus) bool {
	if !meta.DeletionTimestamp.IsZero() {
		imc.logger.Debugf("Skipping check if Instance %q is scheduled on instance manager %q; instance is marked for deletion", meta.Name, instanceManager)
		return false
	}

	if status.CurrentState != spec.DesireState || status.OwnerID != spec.NodeID {
		imc.logger.WithFields(logrus.Fields{
			"currentState": status.CurrentState,
			"desiredState": spec.DesireState,
			"currentNode":  status.OwnerID,
			"desiredNode":  spec.NodeID,
		}).Debugf("Skipping check if instance %q is scheduled on instance manager %q; instance is in state transition", meta.Name, instanceManager)
		return false
	}

	switch status.CurrentState {
	case longhorn.InstanceStateRunning:
		return status.InstanceManagerName == instanceManager
	case longhorn.InstanceStateStopped:
		// Instance manager is not assigned in the stopped state. Instance is not expected to live in any instance manager.
		return false
	default:
		return false
	}
}

func (imc *InstanceManagerController) startBackingImageMonitoring(im *longhorn.InstanceManager) {
	log := imc.logger.WithField("instance manager", im.Name)

	backingImageMonitorName := types.GetBackingImageMonitorName(im.Name)

	if im.Status.IP == "" {
		log.Errorf("IP is not set before monitoring")
		return
	}
	imc.instanceManagerMonitorMutex.Lock()
	defer imc.instanceManagerMonitorMutex.Unlock()

	if _, ok := imc.instanceManagerMonitorMap[backingImageMonitorName]; ok {
		return
	}

	engineClientProxy, err := engineapi.NewEngineClientProxy(im, log, imc.proxyConnCounter, imc.ds)
	if err != nil {
		log.Errorf("failed to get the engine client proxy for instance manager %v", im.Name)
		return
	}

	stopCh := make(chan struct{}, 1)
	monitorVoluntaryStopCh := make(chan struct{})
	monitor := &InstanceManagerMonitor{
		logger:                 log,
		Name:                   backingImageMonitorName,
		imName:                 im.Name,
		controllerID:           imc.controllerID,
		ds:                     imc.ds,
		lock:                   &sync.RWMutex{},
		stopCh:                 stopCh,
		done:                   false,
		monitorVoluntaryStopCh: monitorVoluntaryStopCh,
		// notify monitor to update the instance map
		updateNotification: true,
		proxyClient:        engineClientProxy,

		nodeCallback: imc.enqueueInstanceManagersForNode,
	}

	imc.instanceManagerMonitorMap[backingImageMonitorName] = stopCh
	go monitor.BackingImageMonitorRun()

	go func() {
		<-monitorVoluntaryStopCh
		engineClientProxy.Close()
		imc.instanceManagerMonitorMutex.Lock()
		delete(imc.instanceManagerMonitorMap, backingImageMonitorName)
		imc.instanceManagerMonitorMutex.Unlock()
	}()
}

func (imc *InstanceManagerController) stopBackingImageMonitoring(imName string) {
	imc.instanceManagerMonitorMutex.Lock()
	defer imc.instanceManagerMonitorMutex.Unlock()

	backingImageMonitorName := types.GetBackingImageMonitorName(imName)
	stopCh, ok := imc.instanceManagerMonitorMap[backingImageMonitorName]
	if !ok {
		return
	}

	select {
	case <-stopCh:
		// stopCh channel is already closed
	default:
		close(stopCh)
	}
}

func (m *InstanceManagerMonitor) BackingImageMonitorRun() {
	m.logger.Infof("Start SPDK backing image monitoring %v", m.Name)

	ctx, cancel := context.WithCancel(context.TODO())
	notifier, err := m.proxyClient.SPDKBackingImageWatch(ctx)
	if err != nil {
		m.logger.WithError(err).Errorf("Failed to get the notifier for monitoring")
		cancel()
		close(m.monitorVoluntaryStopCh)
		return
	}

	defer func() {
		m.logger.Infof("Stop monitoring spdk backing image %v", m.Name)
		cancel()
		m.StopMonitorWithLock()
		close(m.monitorVoluntaryStopCh)
	}()

	go func() {
		continuousFailureCount := 0
		for {
			if continuousFailureCount >= engineapi.MaxMonitorRetryCount {
				m.logger.Errorf("Instance manager SPDK backing image monitor streaming continuously errors receiving items for %v times, will stop the monitor itself", engineapi.MaxMonitorRetryCount)
				m.StopMonitorWithLock()
			}

			if m.CheckMonitorStoppedWithLock() {
				return
			}

			_, err = notifier.Recv()
			if err != nil {
				m.logger.WithError(err).Error("Failed to receive next item in spdk backing image watch")
				continuousFailureCount++
				time.Sleep(engineapi.MinPollCount * engineapi.PollInterval)
			} else {
				m.lock.Lock()
				m.updateNotification = true
				m.lock.Unlock()
				continuousFailureCount = 0
			}
		}
	}()

	timer := 0
	ticker := time.NewTicker(engineapi.MinPollCount * engineapi.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if m.CheckMonitorStoppedWithLock() {
				return
			}

			needUpdate := false

			m.lock.Lock()
			timer++
			if timer >= engineapi.MaxPollCount || m.updateNotification {
				needUpdate = true
				m.updateNotification = false
				timer = 0
			}
			m.lock.Unlock()

			if !needUpdate {
				continue
			}
			if needStop := m.pollAndUpdateV2BackingImageMap(); needStop {
				return
			}
		case <-m.stopCh:
			return
		}
	}
}

func (m *InstanceManagerMonitor) pollAndUpdateV2BackingImageMap() (needStop bool) {
	im, err := m.ds.GetInstanceManager(m.imName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			m.logger.Warn("Stop monitoring because the instance manager no longer exists")
			return true
		}
		utilruntime.HandleError(errors.Wrapf(err, "failed to get instance manager %v for monitoring", m.Name))
		return false
	}

	if im.Status.OwnerID != m.controllerID {
		m.logger.Warnf("Stop monitoring the instance manager on this node (%v) because the instance manager has new ownerID %v", m.controllerID, im.Status.OwnerID)
		return true
	}

	// the key in the resp is in the form of "bi-%s-disk-%s" so we can distinguish the different disks in the same instance manager
	resp, err := m.proxyClient.SPDKBackingImageList()
	if err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to poll spdk backing image info to update instance manager %v", m.Name))
		return false
	}

	if reflect.DeepEqual(im.Status.BackingImages, resp) {
		return false
	}

	im.Status.BackingImages = resp
	if _, err := m.ds.UpdateInstanceManagerStatus(im); err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to update v2 backing image map for instance manager %v", m.Name))
		return false
	}
	return false
}

func (imc *InstanceManagerController) startMonitoring(im *longhorn.InstanceManager) {
	log := imc.logger.WithField("instance manager", im.Name)

	if im.Status.IP == "" {
		log.Errorf("IP is not set before monitoring")
		return
	}

	imc.instanceManagerMonitorMutex.Lock()
	defer imc.instanceManagerMonitorMutex.Unlock()

	if _, ok := imc.instanceManagerMonitorMap[im.Name]; ok {
		return
	}

	// TODO: #2441 refactor this when we do the resource monitoring refactor
	client, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		log.WithError(err).Errorf("Failed to initialize im client to %v before monitoring", im.Name)
		return
	}

	stopCh := make(chan struct{}, 1)
	monitorVoluntaryStopCh := make(chan struct{})
	monitor := &InstanceManagerMonitor{
		logger:                 log,
		Name:                   im.Name,
		controllerID:           imc.controllerID,
		ds:                     imc.ds,
		lock:                   &sync.RWMutex{},
		stopCh:                 stopCh,
		done:                   false,
		monitorVoluntaryStopCh: monitorVoluntaryStopCh,
		// notify monitor to update the instance map
		updateNotification: true,
		client:             client,

		nodeCallback: imc.enqueueInstanceManagersForNode,
	}

	imc.instanceManagerMonitorMap[im.Name] = stopCh

	go monitor.Run()

	go func() {
		<-monitorVoluntaryStopCh
		if closeErr := client.Close(); closeErr != nil {
			imc.logger.WithError(closeErr).Warn("Failed to close instance manager client during stopping instance manager monitor")
		}
		imc.instanceManagerMonitorMutex.Lock()
		delete(imc.instanceManagerMonitorMap, im.Name)
		imc.instanceManagerMonitorMutex.Unlock()
	}()
}

func (imc *InstanceManagerController) stopMonitoring(imName string) {
	imc.instanceManagerMonitorMutex.Lock()
	defer imc.instanceManagerMonitorMutex.Unlock()

	stopCh, ok := imc.instanceManagerMonitorMap[imName]
	if !ok {
		return
	}

	select {
	case <-stopCh:
		// stopCh channel is already closed
	default:
		close(stopCh)
	}

}

func (m *InstanceManagerMonitor) Run() {
	m.logger.Infof("Start monitoring instance manager %v", m.Name)

	// TODO: this function will error out in unit tests. Need to find a way to skip this for unit tests.
	// TODO: #2441 refactor this when we do the resource monitoring refactor
	ctx, cancel := context.WithCancel(context.TODO())
	notifier, err := m.client.InstanceWatch(ctx)
	if err != nil {
		m.logger.WithError(err).Errorf("Failed to get the notifier for monitoring")
		cancel()
		close(m.monitorVoluntaryStopCh)
		return
	}

	defer func() {
		m.logger.Infof("Stop monitoring instance manager %v", m.Name)
		cancel()
		m.StopMonitorWithLock()
		close(m.monitorVoluntaryStopCh)
	}()

	go func() {
		continuousFailureCount := 0
		for {
			if continuousFailureCount >= engineapi.MaxMonitorRetryCount {
				m.logger.Errorf("Instance manager monitor streaming continuously errors receiving items for %v times, will stop the monitor itself", engineapi.MaxMonitorRetryCount)
				m.StopMonitorWithLock()
			}

			if m.CheckMonitorStoppedWithLock() {
				return
			}

			var err error
			if m.client.GetAPIVersion() < 4 {
				_, err = notifier.(*imapi.ProcessStream).Recv()
			} else {
				_, err = notifier.(*imapi.InstanceStream).Recv()
			}
			if err != nil {
				m.logger.WithError(err).Error("Failed to receive next item in instance watch")
				continuousFailureCount++
				time.Sleep(engineapi.MinPollCount * engineapi.PollInterval)
			} else {
				m.lock.Lock()
				m.updateNotification = true
				m.lock.Unlock()
			}
		}
	}()

	timer := 0
	ticker := time.NewTicker(engineapi.MinPollCount * engineapi.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if m.CheckMonitorStoppedWithLock() {
				return
			}

			needUpdate := false

			m.lock.Lock()
			timer++
			if timer >= engineapi.MaxPollCount || m.updateNotification {
				needUpdate = true
				m.updateNotification = false
				timer = 0
			}
			m.lock.Unlock()

			if !needUpdate {
				continue
			}
			instanceManager, instanceMap, needStop := m.pollInstanceMap()
			if needStop {
				return
			}
			if instanceManager != nil && instanceMap != nil {
				m.syncInstances(instanceManager, instanceMap)
				m.syncOrphans(instanceManager, instanceMap)
			}
		case <-m.stopCh:
			return
		}
	}
}

func (m *InstanceManagerMonitor) pollInstanceMap() (im *longhorn.InstanceManager, instanceMap instanceProcessMap, needStop bool) {
	im, err := m.ds.GetInstanceManager(m.Name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			m.logger.Warn("Stop monitoring because the instance manager no longer exists")
			return nil, nil, true
		}
		utilruntime.HandleError(errors.Wrapf(err, "failed to get instance manager %v for monitoring", m.Name))
		return nil, nil, false
	}

	if im.Status.OwnerID != m.controllerID {
		m.logger.Warnf("Stop monitoring the instance manager on this node (%v) because the instance manager has new ownerID %v", m.controllerID, im.Status.OwnerID)
		return nil, nil, true
	}

	resp, err := m.client.InstanceList()
	if err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to poll instance info to update instance manager %v", m.Name))
		return nil, nil, false
	}
	return im, resp, false
}

func (m *InstanceManagerMonitor) syncInstances(im *longhorn.InstanceManager, instanceMap instanceProcessMap) {
	if !m.updateInstanceMap(im, instanceMap) {
		return
	}
	if _, err := m.ds.UpdateInstanceManagerStatus(im); err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to update instance map for instance manager %v", m.Name))
		return
	}

	clusterAutoscalerEnabled, err := m.ds.GetSettingAsBool(types.SettingNameKubernetesClusterAutoscalerEnabled)
	if err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to get %v setting for instance manager %v", types.SettingNameKubernetesClusterAutoscalerEnabled, m.Name))
		return
	}

	// During volume attaching/detaching, it is likely both the engine and replica
	// IMs enqueue at the same time. If the replica IM queued before the engine IM,
	// then the instance in the engine manager possibly still not updated.
	// When ClusterAutoscaler is enabled, Longhorn cannot remove the redundant
	// replica IM PDB in this case. So enqueue the node IMs again to sync replica
	// IM PDB.
	if clusterAutoscalerEnabled && im.Spec.Type == longhorn.InstanceManagerTypeEngine {
		m.nodeCallback(m.controllerID)
	}
}

func (m *InstanceManagerMonitor) updateInstanceMap(im *longhorn.InstanceManager, instanceMap instanceProcessMap) bool {
	switch {
	case im.Status.APIVersion < 4:
		if reflect.DeepEqual(im.Status.Instances, instanceMap) { // nolint: staticcheck
			return false
		}

		im.Status.Instances = instanceMap // nolint: staticcheck
	default:
		engineProcesses, replicaProcesses := m.categorizeProcesses(instanceMap)

		// reflect.DeepEqual treats the two maps `var m1 map[string]process` and `m2 := map[string]process` as different maps.
		// Therefore, to prevent unnecessary updates, we must check both that the length of the maps is zero and that the maps are identical.
		if ((len(im.Status.InstanceEngines) == 0 && len(engineProcesses) == 0) || reflect.DeepEqual(im.Status.InstanceEngines, engineProcesses)) &&
			((len(im.Status.InstanceReplicas) == 0 && len(replicaProcesses) == 0) || reflect.DeepEqual(im.Status.InstanceReplicas, replicaProcesses)) {
			return false
		}

		im.Status.InstanceEngines = engineProcesses
		im.Status.InstanceReplicas = replicaProcesses
	}
	return true
}

func (m *InstanceManagerMonitor) CheckMonitorStoppedWithLock() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.done
}

func (m *InstanceManagerMonitor) StopMonitorWithLock() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.done = true
}

func (m *InstanceManagerMonitor) syncOrphans(im *longhorn.InstanceManager, instanceMap instanceProcessMap) {
	engineProcesses, replicaProcesses := m.categorizeProcesses(instanceMap)
	existOrphansList, err := m.ds.ListInstanceOrphansByInstanceManagerRO(im.Name)
	if err != nil {
		m.logger.WithError(err).Errorf("Failed to list orphans on node %s", im.Spec.NodeID)
		return
	}
	existOrphans := make(map[string]bool)
	for _, orphan := range existOrphansList {
		existOrphans[orphan.Name] = true
	}

	// exam instances and create orphan CRs
	m.createOrphanForInstances(existOrphans, im, engineProcesses, longhorn.OrphanTypeEngineInstance, m.isEngineOrphaned)
	m.createOrphanForInstances(existOrphans, im, replicaProcesses, longhorn.OrphanTypeReplicaInstance, m.isReplicaOrphaned)
}

func (m *InstanceManagerMonitor) isEngineOrphaned(instanceName, instanceManager string) (bool, error) {
	existEngine, err := m.ds.GetEngineRO(instanceName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Engine CR not found - instance is orphaned
			return true, nil
		}
		// Unexpected error - unable to check if engine instance is orphaned or not
		return false, err
	}
	// Engine CR still exists - check the ownership
	return m.isInstanceOrphanedInInstanceManager(&existEngine.ObjectMeta, &existEngine.Spec.InstanceSpec, &existEngine.Status.InstanceStatus, instanceManager), nil
}

func (m *InstanceManagerMonitor) isReplicaOrphaned(instanceName, instanceManager string) (bool, error) {
	existReplica, err := m.ds.GetReplicaRO(instanceName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Replica CR not found - instance is orphaned
			return true, nil
		}
		// Unexpected error - unable to check if replica instance is orphaned or not
		return false, err
	}

	// Replica CR still exists - check the ownership
	return m.isInstanceOrphanedInInstanceManager(&existReplica.ObjectMeta, &existReplica.Spec.InstanceSpec, &existReplica.Status.InstanceStatus, instanceManager), nil
}

// isInstanceOrphanedInInstanceManager returns true only when it is very certain that an instance is scheduled on another instance manager
func (m *InstanceManagerMonitor) isInstanceOrphanedInInstanceManager(meta *metav1.ObjectMeta, spec *longhorn.InstanceSpec, status *longhorn.InstanceStatus, instanceManager string) bool {
	if !meta.DeletionTimestamp.IsZero() {
		m.logger.Debugf("Skipping orphan check; Instance %s is marked for deletion", meta.Name)
		return false
	}

	if status.CurrentState != spec.DesireState || status.OwnerID != spec.NodeID {
		m.logger.WithFields(logrus.Fields{
			"currentState": status.CurrentState,
			"desiredState": spec.DesireState,
			"currentNode":  status.OwnerID,
			"desiredNode":  spec.NodeID,
		}).Debugf("Skipping orphan check; Instance %s is in state transition", meta.Name)
		return false
	}

	switch status.CurrentState {
	case longhorn.InstanceStateRunning:
		return status.InstanceManagerName != instanceManager
	case longhorn.InstanceStateStopped:
		// Instance manager is not assigned in the stopped state. Instance is not expected to live in any instance manager.
		return true
	default:
		return false
	}
}

func (m *InstanceManagerMonitor) createOrphanForInstances(existOrphans map[string]bool, im *longhorn.InstanceManager, instanceMap instanceProcessMap, orphanType longhorn.OrphanType, orphanFilter func(instanceName, instanceManager string) (bool, error)) {
	for instanceName, instance := range instanceMap {
		if instance.Status.State == longhorn.InstanceStateStarting ||
			instance.Status.State == longhorn.InstanceStateStopping ||
			instance.Status.State == longhorn.InstanceStateStopped {
			// Starting: Status transitioning. Will handle this after running.
			// Stopping, Stopped: Terminating. No orphan CR needed, and the orphaned instances will be cleanup by instance manager after stopped.
			continue
		}
		if instance.Spec.DataEngine != longhorn.DataEngineTypeV1 {
			m.logger.Debugf("Skipping orphan creation, instance %s is not data engine v1", instanceName)
			continue
		}
		if instance.Status.UUID == "" {
			// skip the instance without UUID to prevent accidental deletion on processes
			continue
		}

		orphanName := types.GetOrphanChecksumNameForOrphanedInstance(instanceName, instance.Status.UUID, im.Name, string(instance.Spec.DataEngine))
		if _, isExist := existOrphans[orphanName]; isExist {
			continue
		}
		if isOrphaned, err := orphanFilter(instanceName, im.Name); err != nil {
			m.logger.WithError(err).Errorf("Failed to check %v orphan for instance %v", orphanType, instanceName)
		} else if isOrphaned {
			m.logger.WithFields(logrus.Fields{
				"instanceState": instance.Status.State,
				"instanceUUID":  instance.Status.UUID,
			}).Infof("Creating %s Orphan %v for orphaned instance %v", orphanType, orphanName, instanceName)
			newOrphan, err := m.createOrphan(orphanName, im, instanceName, instance.Status.UUID, orphanType, instance.Spec.DataEngine)
			if err != nil {
				m.logger.WithError(err).Errorf("Failed to create %v orphan for instance %v", orphanType, instanceName)
			} else if newOrphan != nil {
				existOrphans[newOrphan.Name] = true
			}
		}
	}
}

func (m *InstanceManagerMonitor) createOrphan(name string, im *longhorn.InstanceManager, instanceName, instanceUUID string, orphanType longhorn.OrphanType, dataEngineType longhorn.DataEngineType) (*longhorn.Orphan, error) {
	if _, err := m.ds.GetOrphanRO(name); err == nil || !apierrors.IsNotFound(err) {
		return nil, err
	}

	// labels will be attached by mutator webhook
	orphan := &longhorn.Orphan{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: longhorn.OrphanSpec{
			NodeID:     m.controllerID,
			Type:       orphanType,
			DataEngine: dataEngineType,
			Parameters: map[string]string{
				longhorn.OrphanInstanceName:    instanceName,
				longhorn.OrphanInstanceUUID:    instanceUUID,
				longhorn.OrphanInstanceManager: im.Name,
			},
		},
	}

	return m.ds.CreateOrphan(orphan)
}

func (m *InstanceManagerMonitor) categorizeProcesses(instanceMap instanceProcessMap) (instanceProcessMap, instanceProcessMap) {
	engineProcesses := make(instanceProcessMap)
	replicaProcesses := make(instanceProcessMap)
	for name, process := range instanceMap {
		switch process.Status.Type {
		case longhorn.InstanceTypeEngine:
			engineProcesses[name] = process
		case longhorn.InstanceTypeReplica:
			replicaProcesses[name] = process
		}
	}
	return engineProcesses, replicaProcesses
}

func (imc *InstanceManagerController) isResponsibleFor(im *longhorn.InstanceManager) bool {
	return isControllerResponsibleFor(imc.controllerID, imc.ds, im.Name, im.Spec.NodeID, im.Status.OwnerID)
}
