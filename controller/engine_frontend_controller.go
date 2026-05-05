package controller

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// EngineFrontendController is responsible for managing the lifecycle of
// EngineFrontend instances (v2 data engine initiators)
type EngineFrontendController struct {
	*baseController

	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	instanceHandler *InstanceHandler

	proxyConnCounter util.Counter

	backoff *flowcontrol.Backoff

	snapshotConcurrentLimiter *SnapshotConcurrentLimiter

	engineFrontendMonitorMutex *sync.RWMutex
	engineFrontendMonitorMap   map[string]chan struct{}
}

type engineFrontendSwitchoverClient interface {
	EngineFrontendSwitchOverTarget(dataEngine longhorn.DataEngineType, name, targetAddress, engineName, switchoverPhase string) error
}

type switchoverFailureType string

const (
	switchoverFailureSwitch switchoverFailureType = "switch"
)

// EngineFrontendMonitor monitors a running EngineFrontend instance
// and periodically collects status information (endpoint, target connection).
type EngineFrontendMonitor struct {
	logger logrus.FieldLogger

	namespace     string
	ds            *datastore.DataStore
	eventRecorder record.EventRecorder

	Name   string
	stopCh chan struct{}
	// used to notify the controller that monitoring has stopped
	monitorVoluntaryStopCh chan struct{}

	controllerID     string
	proxyConnCounter util.Counter

	expansionBackoff *flowcontrol.Backoff
}

// NewEngineFrontendController creates a new EngineFrontendController
func NewEngineFrontendController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string, controllerID string,
	proxyConnCounter util.Counter,
	snapshotConcurrentLimiter *SnapshotConcurrentLimiter,
) (*EngineFrontendController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	efc := &EngineFrontendController{
		baseController: newBaseController("longhorn-engine-frontend", logger),

		ds:        ds,
		namespace: namespace,

		controllerID:  controllerID,
		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-engine-frontend-controller"}),

		proxyConnCounter: proxyConnCounter,

		backoff: flowcontrol.NewBackOff(time.Second*10, time.Minute*5),

		snapshotConcurrentLimiter: snapshotConcurrentLimiter,

		engineFrontendMonitorMutex: &sync.RWMutex{},
		engineFrontendMonitorMap:   map[string]chan struct{}{},
	}
	efc.instanceHandler = NewInstanceHandler(ds, efc, efc.eventRecorder)

	var err error
	if _, err = ds.EngineFrontendInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    efc.enqueueEngineFrontend,
		UpdateFunc: func(old, cur interface{}) { efc.enqueueEngineFrontend(cur) },
		DeleteFunc: efc.enqueueEngineFrontend,
	}); err != nil {
		return nil, err
	}
	efc.cacheSyncs = append(efc.cacheSyncs, ds.EngineFrontendInformer.HasSynced)

	if _, err = ds.InstanceManagerInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    efc.enqueueInstanceManagerChange,
		UpdateFunc: func(old, cur interface{}) { efc.enqueueInstanceManagerChange(cur) },
		DeleteFunc: efc.enqueueInstanceManagerChange,
	}, 0); err != nil {
		return nil, err
	}
	efc.cacheSyncs = append(efc.cacheSyncs, ds.InstanceManagerInformer.HasSynced)

	if _, err = ds.EngineInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    efc.enqueueEngineChange,
		UpdateFunc: func(old, cur interface{}) { efc.enqueueEngineChange(cur) },
		DeleteFunc: efc.enqueueEngineChange,
	}); err != nil {
		return nil, err
	}
	efc.cacheSyncs = append(efc.cacheSyncs, ds.EngineInformer.HasSynced)

	return efc, nil
}

// Run starts the controller
func (efc *EngineFrontendController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer efc.queue.ShutDown()

	efc.logger.Info("Starting Longhorn engine frontend controller")
	defer efc.logger.Info("Shut down Longhorn engine frontend controller")

	if !cache.WaitForNamedCacheSync("longhorn engine frontends", stopCh, efc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(efc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (efc *EngineFrontendController) worker() {
	for efc.processNextWorkItem() {
	}
}

func (efc *EngineFrontendController) processNextWorkItem() bool {
	key, quit := efc.queue.Get()

	if quit {
		return false
	}
	defer efc.queue.Done(key)

	err := efc.syncEngineFrontend(key.(string))
	efc.handleErr(err, key)

	return true
}

func (efc *EngineFrontendController) handleErr(err error, key interface{}) {
	if err == nil {
		efc.queue.Forget(key)
		return
	}

	log := efc.logger.WithField("engineFrontend", key)
	if efc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn engine frontend")
		efc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn engine frontend out of the queue")
	efc.queue.Forget(key)
}

func getLoggerForEngineFrontend(logger logrus.FieldLogger, ef *longhorn.EngineFrontend) *logrus.Entry {
	return logger.WithField("engineFrontend", ef.Name)
}

// Prefer picking the node e.Spec.NodeID if it meet the above requirement.
func (efc *EngineFrontendController) isResponsibleFor(ef *longhorn.EngineFrontend, defaultEngineImage string) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	if isV2DisabledForNode, err := efc.ds.IsV2DataEngineDisabledForNode(efc.controllerID); err != nil || isV2DisabledForNode {
		return false, err
	}

	// If a regular RWX is delinquent, try to switch ownership quickly to the owner node of the share manager CR
	isOwnerNodeDelinquent, err := efc.ds.IsNodeDelinquent(ef.Status.OwnerID, ef.Spec.VolumeName)
	if err != nil {
		return false, err
	}
	isSpecNodeDelinquent, err := efc.ds.IsNodeDelinquent(ef.Spec.NodeID, ef.Spec.VolumeName)
	if err != nil {
		return false, err
	}
	preferredOwnerID := ef.Spec.NodeID
	if isOwnerNodeDelinquent || isSpecNodeDelinquent {
		sm, err := efc.ds.GetShareManager(ef.Spec.VolumeName)
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if sm != nil {
			preferredOwnerID = sm.Status.OwnerID
		}
	}

	isResponsible := isControllerResponsibleFor(efc.controllerID, efc.ds, ef.Name, preferredOwnerID, ef.Status.OwnerID)

	preferredOwnerDataEngineAvailable, err := efc.ds.CheckDataEngineImageReadiness(ef.Status.CurrentImage, ef.Spec.DataEngine, ef.Spec.NodeID)
	if err != nil {
		return false, err
	}
	currentOwnerDataEngineAvailable, err := efc.ds.CheckDataEngineImageReadiness(ef.Status.CurrentImage, ef.Spec.DataEngine, ef.Status.OwnerID)
	if err != nil {
		return false, err
	}
	currentNodeDataEngineAvailable, err := efc.ds.CheckDataEngineImageReadiness(ef.Status.CurrentImage, ef.Spec.DataEngine, efc.controllerID)
	if err != nil {
		return false, err
	}

	isPreferredOwner := currentNodeDataEngineAvailable && isResponsible
	continueToBeOwner := currentNodeDataEngineAvailable && !preferredOwnerDataEngineAvailable && efc.controllerID == ef.Status.OwnerID
	requiresNewOwner := currentNodeDataEngineAvailable && !preferredOwnerDataEngineAvailable && !currentOwnerDataEngineAvailable

	return isPreferredOwner || continueToBeOwner || requiresNewOwner, nil
}

func (efc *EngineFrontendController) syncEngineFrontend(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync engine frontend for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != efc.namespace {
		return nil
	}

	log := efc.logger.WithField("engineFrontend", name)
	ef, err := efc.ds.GetEngineFrontend(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to get engine frontend")
	}

	// EngineFrontend is only for v2 data engine
	if !types.IsDataEngineV2(ef.Spec.DataEngine) {
		log.Warnf("EngineFrontend %v has unexpected data engine type %v, skipping", name, ef.Spec.DataEngine)
		return nil
	}

	// Check if this node is responsible for this EngineFrontend
	defaultEngineImage, err := efc.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return err
	}

	isResponsible, err := efc.isResponsibleFor(ef, defaultEngineImage)
	if err != nil {
		return err
	}
	if !isResponsible {
		return nil
	}

	if ef.Status.OwnerID != efc.controllerID {
		ef.Status.OwnerID = efc.controllerID
		ef, err = efc.ds.UpdateEngineFrontendStatus(ef)
		if err != nil {
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("EngineFrontend got new owner %v", efc.controllerID)
	}

	// Handle deletion
	if ef.DeletionTimestamp != nil {
		if err := efc.DeleteInstance(ef); err != nil {
			return errors.Wrapf(err, "failed to clean up the related engine frontend instance before deleting %v", ef.Name)
		}
		// Reconcile instance state from IM so CurrentState reflects the
		// actual data-plane status after DeleteInstance was sent.
		if err := efc.instanceHandler.ReconcileInstanceState(ef, &ef.Spec.InstanceSpec, &ef.Status.InstanceStatus); err != nil {
			return err
		}
		if _, err := efc.ds.UpdateEngineFrontendStatus(ef); err != nil {
			return err
		}
		// Wait for the data-plane instance (NVMe initiator) to actually stop
		// before removing the finalizer. Without this, the CR can be garbage
		// collected while the NVMe initiator is still active, causing the
		// volume controller to proceed with engine teardown prematurely.
		// Accept Error state as well — it means "instance gone but Started
		// was still true", which is safe for finalizer removal.
		if ef.Status.CurrentState != longhorn.InstanceStateStopped &&
			ef.Status.CurrentState != longhorn.InstanceStateError {
			log.Infof("Waiting for engine frontend instance to stop before removing finalizer (current state: %v)", ef.Status.CurrentState)
			return nil
		}
		return efc.ds.RemoveFinalizerForEngineFrontend(ef)
	}

	existingEF := ef.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingEF.Status, ef.Status) {
			_, err = efc.ds.UpdateEngineFrontendStatus(ef)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debug("Requeue engine frontend due to conflict")
			efc.enqueueEngineFrontend(ef)
			err = nil
		}
	}()

	// Use instance handler to reconcile state
	if err := efc.instanceHandler.ReconcileInstanceState(ef, &ef.Spec.InstanceSpec, &ef.Status.InstanceStatus); err != nil {
		return err
	}

	statusTargetInitialized := isEngineFrontendTargetInitialized(ef.Status.TargetIP, ef.Status.TargetPort)
	specTargetInitialized := isEngineFrontendTargetInitialized(ef.Spec.TargetIP, ef.Spec.TargetPort)
	if !statusTargetInitialized && specTargetInitialized {
		// Initialize target status after successful creation when it is still incomplete.
		ef.Status.TargetIP = ef.Spec.TargetIP
		ef.Status.TargetPort = ef.Spec.TargetPort
		statusTargetInitialized = true
	}

	// Handle switchover if TargetIP changes.
	// Switchover for the NVMe/TCP multipath frontend is driven by a 3-phase
	// state machine via Status.SwitchoverPhase: preparing → switching → promoting.
	// Each phase is a separate reconcile cycle with its own gRPC call.
	// The controller only modifies Status (never Spec), which is persisted
	// by the defer block above.
	//
	// If DesireState is no longer Running (e.g. volume is detaching or being
	// deleted), abort any in-progress switchover immediately. Continuing the
	// switchover while the instance is being stopped can leave the ANA state
	// machine in an intermediate phase (e.g. old target already inaccessible
	// but new target not yet promoted to optimized).
	if ef.Spec.DesireState != longhorn.InstanceStateRunning {
		if ef.Status.SwitchoverPhase != longhorn.EngineFrontendSwitchoverPhaseNone {
			log.Warnf("Clearing in-progress switchover phase %v because DesireState is %v",
				ef.Status.SwitchoverPhase, ef.Spec.DesireState)
			ef.Status.SwitchoverPhase = longhorn.EngineFrontendSwitchoverPhaseNone
		}
	} else if ef.Status.CurrentState == longhorn.InstanceStateRunning && statusTargetInitialized && specTargetInitialized {
		targetChanged := ef.Status.TargetIP != ef.Spec.TargetIP || ef.Status.TargetPort != ef.Spec.TargetPort
		switchoverInProgress := ef.Status.SwitchoverPhase != longhorn.EngineFrontendSwitchoverPhaseNone

		if targetChanged || switchoverInProgress {
			im, err := efc.ds.GetInstanceManager(ef.Status.InstanceManagerName)
			if err != nil {
				return errors.Wrapf(err, "failed to get instance manager %v for target switchover", ef.Status.InstanceManagerName)
			}

			c, err := engineapi.NewInstanceManagerClient(im, false)
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := c.Close(); closeErr != nil {
					efc.logger.WithError(closeErr).Warn("Failed to close instance manager client after switchover")
				}
			}()

			targetAddress := util.BuildTargetAddress(ef.Spec.TargetIP, ef.Spec.TargetPort)

			// Determine which phase to execute based on current status.
			var phase longhorn.EngineFrontendSwitchoverPhase
			switch ef.Status.SwitchoverPhase {
			case longhorn.EngineFrontendSwitchoverPhaseNone:
				phase = longhorn.EngineFrontendSwitchoverPhasePreparing
			case longhorn.EngineFrontendSwitchoverPhasePreparing:
				phase = longhorn.EngineFrontendSwitchoverPhaseSwitching
			case longhorn.EngineFrontendSwitchoverPhaseSwitching:
				phase = longhorn.EngineFrontendSwitchoverPhasePromoting
			default:
				log.Warnf("Unknown switchover phase %v for engine frontend %v, resetting", ef.Status.SwitchoverPhase, ef.Name)
				ef.Status.SwitchoverPhase = longhorn.EngineFrontendSwitchoverPhaseNone
				return nil
			}

			log.Infof("Executing switchover phase %v for engine frontend %v target %v", phase, ef.Name, targetAddress)

			if err := c.EngineFrontendSwitchOverTarget(ef.Spec.DataEngine, ef.Name, targetAddress, ef.Spec.EngineName, string(phase)); err != nil {
				efc.recordEngineFrontendSwitchoverFailureEvent(ef, switchoverFailureSwitch, targetAddress, err)
				return errors.Wrapf(err, "failed to execute switchover phase %v for engine frontend %v", phase, ef.Name)
			}

			// Advance the status phase. The defer block persists status changes.
			switch phase {
			case longhorn.EngineFrontendSwitchoverPhasePreparing:
				ef.Status.SwitchoverPhase = longhorn.EngineFrontendSwitchoverPhasePreparing
				log.Infof("Switchover phase preparing complete for %v, next: switching", ef.Name)
			case longhorn.EngineFrontendSwitchoverPhaseSwitching:
				ef.Status.SwitchoverPhase = longhorn.EngineFrontendSwitchoverPhaseSwitching
				log.Infof("Switchover phase switching complete for %v, next: promoting", ef.Name)
			case longhorn.EngineFrontendSwitchoverPhasePromoting:
				ef.Status.SwitchoverPhase = longhorn.EngineFrontendSwitchoverPhaseNone
				// Update status target to match spec so targetChanged becomes false.
				// The monitor will pick up the actual values from the data plane.
				ef.Status.TargetIP = ef.Spec.TargetIP
				ef.Status.TargetPort = ef.Spec.TargetPort
				log.Infof("Successfully switched over target for engine frontend %v to %v", ef.Name, targetAddress)
				efc.eventRecorder.Eventf(ef, corev1.EventTypeNormal, constant.EventReasonSwitchover, "Successfully switched over target to %v", targetAddress)
			}
		}
	} // end switchover block

	// Start/stop monitoring based on instance state
	if ef.Status.CurrentState == longhorn.InstanceStateRunning {
		if !efc.isMonitoring(ef) {
			efc.startMonitoring(ef)
		}
	} else if efc.isMonitoring(ef) {
		efc.resetAndStopMonitoring(ef)
	}

	return nil
}

// switchEngineFrontendTarget performs a direct multipath target switchover
// using NVMe/TCP ANA state transitions (no suspend/resume of frontend I/O).
func switchEngineFrontendTarget(c engineFrontendSwitchoverClient, ef *longhorn.EngineFrontend, targetAddress, switchoverPhase string) (switchoverFailureType, error) {
	if err := c.EngineFrontendSwitchOverTarget(ef.Spec.DataEngine, ef.Name, targetAddress, ef.Spec.EngineName, switchoverPhase); err != nil {
		return switchoverFailureSwitch, errors.Wrapf(err, "failed to switch over target for engine frontend %v (phase %v)", ef.Name, switchoverPhase)
	}

	return "", nil
}

func getEngineFrontendSwitchoverFailureEventMessage(failureType switchoverFailureType, targetAddress string, err error) string {
	switch failureType {
	default:
		return fmt.Sprintf("Failed to switch over target to %v: %v", targetAddress, err)
	}
}

func recordEngineFrontendSwitchoverFailureEvent(recorder record.EventRecorder, ef *longhorn.EngineFrontend, failureType switchoverFailureType, targetAddress string, err error) {
	recorder.Event(ef, corev1.EventTypeWarning, constant.EventReasonFailedSwitchover,
		getEngineFrontendSwitchoverFailureEventMessage(failureType, targetAddress, err))
}

func (efc *EngineFrontendController) recordEngineFrontendSwitchoverFailureEvent(ef *longhorn.EngineFrontend, failureType switchoverFailureType, targetAddress string, err error) {
	recordEngineFrontendSwitchoverFailureEvent(efc.eventRecorder, ef, failureType, targetAddress, err)
}

func (efc *EngineFrontendController) enqueueEngineFrontend(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	efc.queue.Add(key)
}

func (efc *EngineFrontendController) enqueueInstanceManagerChange(obj interface{}) {
	im, isInstanceManager := obj.(*longhorn.InstanceManager)
	if !isInstanceManager {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		im, ok = deletedState.Obj.(*longhorn.InstanceManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("cannot convert DeletedFinalStateUnknown to InstanceManager object: %#v", deletedState.Obj))
			return
		}
	}

	imType, err := datastore.CheckInstanceManagerType(im)
	if err != nil || (imType != longhorn.InstanceManagerTypeEngine && imType != longhorn.InstanceManagerTypeAllInOne) {
		return
	}

	// List all EngineFrontends and enqueue the ones related to this InstanceManager
	efs, err := efc.ds.ListEngineFrontends()
	if err != nil {
		efc.logger.WithError(err).Warn("Failed to list engine frontends")
		return
	}
	for _, ef := range efs {
		if ef.Spec.NodeID == im.Spec.NodeID || ef.Status.InstanceManagerName == im.Name {
			efc.enqueueEngineFrontend(ef)
		}
	}
}

func (efc *EngineFrontendController) enqueueEngineChange(obj interface{}) {
	engine, isEngine := obj.(*longhorn.Engine)
	if !isEngine {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		engine, ok = deletedState.Obj.(*longhorn.Engine)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("cannot convert DeletedFinalStateUnknown to Engine object: %#v", deletedState.Obj))
			return
		}
	}

	if !types.IsDataEngineV2(engine.Spec.DataEngine) {
		return
	}

	efs, err := efc.ds.ListVolumeEngineFrontends(engine.Spec.VolumeName)
	if err != nil {
		efc.logger.WithError(err).Warn("Failed to list engine frontends for engine change")
		return
	}
	for _, ef := range efs {
		if ef.Spec.EngineName == engine.Name {
			efc.enqueueEngineFrontend(ef)
		}
	}
}

// CreateInstance creates an EngineFrontend instance via Instance Manager
func (efc *EngineFrontendController) CreateInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	ef, ok := obj.(*longhorn.EngineFrontend)
	if !ok {
		return nil, fmt.Errorf("invalid object for engine frontend process creation: %v", obj)
	}
	if ef.Spec.VolumeName == "" || ef.Spec.NodeID == "" {
		return nil, fmt.Errorf("missing parameters for engine frontend instance creation: %v", ef)
	}

	im, err := efc.ds.GetInstanceManagerByInstanceRO(obj)
	if err != nil {
		return nil, err
	}

	if ef.Status.InstanceManagerName == "" {
		ef.Status.InstanceManagerName = im.Name
	}
	if ef.Status.InstanceManagerName != im.Name {
		return nil, fmt.Errorf("found instance manager name conflict %s vs %s during engine frontend instance creation", ef.Status.InstanceManagerName, im.Name)
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, err
	}
	defer func(c io.Closer) {
		if closeErr := c.Close(); closeErr != nil {
			efc.logger.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(c)

	frontend := ef.Spec.Frontend
	if ef.Spec.DisableFrontend {
		frontend = longhorn.VolumeFrontendEmpty
	}

	ublkQueueDepth := ef.Spec.UblkQueueDepth
	ublkNumberOfQueue := ef.Spec.UblkNumberOfQueue

	if frontend == longhorn.VolumeFrontendUblk {
		if ublkQueueDepth == 0 {
			ublkQueueDepthInt64, err := efc.ds.GetSettingAsIntByDataEngine(types.SettingNameDefaultUblkQueueDepth, ef.Spec.DataEngine)
			if err != nil {
				return nil, err
			}
			ublkQueueDepth = int(ublkQueueDepthInt64)
		}
		if ublkNumberOfQueue == 0 {
			ublkNumberOfQueueInt64, err := efc.ds.GetSettingAsIntByDataEngine(types.SettingNameDefaultUblkNumberOfQueue, ef.Spec.DataEngine)
			if err != nil {
				return nil, err
			}
			ublkNumberOfQueue = int(ublkNumberOfQueueInt64)
		}
	}

	ef.Status.Starting = true
	efName := ef.Name
	if ef, err = efc.ds.UpdateEngineFrontendStatus(ef); err != nil {
		return nil, errors.Wrapf(err, "failed to update engine frontend %v status.starting to true", efName)
	}

	// Create initiator instance via Instance Manager
	// Note: This requires Instance Manager to have EngineFrontendInstanceCreate method
	return c.EngineFrontendInstanceCreate(&engineapi.EngineFrontendInstanceCreateRequest{
		EngineFrontend:    ef,
		VolumeFrontend:    frontend,
		UblkQueueDepth:    ublkQueueDepth,
		UblkNumberOfQueue: ublkNumberOfQueue,
		TargetIP:          ef.Spec.TargetIP,
		TargetPort:        ef.Spec.TargetPort,
		EngineName:        ef.Spec.EngineName,
	})
}

// DeleteInstance deletes an EngineFrontend instance via Instance Manager
func (efc *EngineFrontendController) DeleteInstance(obj interface{}) (err error) {
	ef, ok := obj.(*longhorn.EngineFrontend)
	if !ok {
		return fmt.Errorf("invalid object for engine frontend process deletion: %v", obj)
	}

	log := getLoggerForEngineFrontend(efc.logger, ef)
	var im *longhorn.InstanceManager

	if ef.Status.InstanceManagerName == "" {
		if ef.Spec.NodeID == "" {
			log.Warn("EngineFrontend does not set instance manager name and node ID, will skip the actual instance deletion")
			return nil
		}
		im, err = efc.ds.GetInstanceManagerByInstance(obj)
		if err != nil {
			log.WithError(err).Warn("Failed to detect instance manager for engine frontend, will skip the actual instance deletion")
			return nil
		}
		log.Infof("Cleaning up the process for engine frontend in instance manager %v", im.Name)
	} else {
		im, err = efc.ds.GetInstanceManager(ef.Status.InstanceManagerName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			log.Warnf("The engine frontend instance manager %v is gone during the deletion. Will do nothing for the deletion", ef.Status.InstanceManagerName)
			return nil
		}
	}

	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		log.Infof("Skipping deleting engine frontend %v since instance manager is not running", ef.Name)
		return nil
	}

	log.Info("Deleting engine frontend instance")

	c, err := engineapi.NewInstanceManagerClient(im, true)
	if err != nil {
		return err
	}
	defer func(c io.Closer) {
		if closeErr := c.Close(); closeErr != nil {
			efc.logger.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(c)

	// Delete initiator instance via Instance Manager
	err = c.InstanceDelete(ef.Spec.DataEngine, ef.Name, "", string(longhorn.InstanceTypeEngineFrontend), "", true)
	if err != nil && !types.ErrorIsNotFound(err) {
		return err
	}

	return nil
}

// GetInstance gets the EngineFrontend instance status from Instance Manager
func (efc *EngineFrontendController) GetInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	ef, ok := obj.(*longhorn.EngineFrontend)
	if !ok {
		return nil, fmt.Errorf("invalid object for engine frontend instance get: %v", obj)
	}

	var (
		im  *longhorn.InstanceManager
		err error
	)
	if ef.Status.InstanceManagerName == "" {
		im, err = efc.ds.GetInstanceManagerByInstanceRO(obj)
		if err != nil {
			return nil, err
		}
	} else {
		im, err = efc.ds.GetInstanceManagerRO(ef.Status.InstanceManagerName)
		if err != nil {
			return nil, err
		}
	}
	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, err
	}
	defer func(c io.Closer) {
		if closeErr := c.Close(); closeErr != nil {
			efc.logger.WithError(closeErr).Warn("Failed to close instance manager client")
		}
	}(c)

	return c.InstanceGet(ef.Spec.DataEngine, ef.Name, string(longhorn.InstanceTypeEngineFrontend))
}

// LogInstance gets logs for an EngineFrontend instance
func (efc *EngineFrontendController) LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error) {
	ef, ok := obj.(*longhorn.EngineFrontend)
	if !ok {
		return nil, nil, fmt.Errorf("invalid object for engine frontend instance log: %v", obj)
	}

	im, err := efc.ds.GetInstanceManagerRO(ef.Status.InstanceManagerName)
	if err != nil {
		return nil, nil, err
	}

	c, err := engineapi.NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, nil, err
	}

	stream, err := c.InstanceLog(ctx, ef.Spec.DataEngine, ef.Name, string(longhorn.InstanceTypeEngineFrontend))
	return c, stream, err
}

// isMonitoring checks if a monitor goroutine is running for the given EngineFrontend
func (efc *EngineFrontendController) isMonitoring(ef *longhorn.EngineFrontend) bool {
	efc.engineFrontendMonitorMutex.RLock()
	defer efc.engineFrontendMonitorMutex.RUnlock()

	_, ok := efc.engineFrontendMonitorMap[ef.Name]
	return ok
}

// startMonitoring creates and launches a monitor goroutine for the given EngineFrontend
func (efc *EngineFrontendController) startMonitoring(ef *longhorn.EngineFrontend) {
	stopCh := make(chan struct{})
	monitorVoluntaryStopCh := make(chan struct{})
	monitor := &EngineFrontendMonitor{
		logger:                 efc.logger.WithField("engineFrontend", ef.Name),
		Name:                   ef.Name,
		namespace:              ef.Namespace,
		ds:                     efc.ds,
		eventRecorder:          efc.eventRecorder,
		stopCh:                 stopCh,
		monitorVoluntaryStopCh: monitorVoluntaryStopCh,
		controllerID:           efc.controllerID,
		proxyConnCounter:       efc.proxyConnCounter,
		expansionBackoff:       flowcontrol.NewBackOff(time.Second*10, time.Minute*5),
	}

	efc.engineFrontendMonitorMutex.Lock()
	defer efc.engineFrontendMonitorMutex.Unlock()

	if _, ok := efc.engineFrontendMonitorMap[ef.Name]; ok {
		return
	}
	efc.engineFrontendMonitorMap[ef.Name] = stopCh

	go monitor.Run()
	go func() {
		<-monitorVoluntaryStopCh
		efc.engineFrontendMonitorMutex.Lock()
		delete(efc.engineFrontendMonitorMap, ef.Name)
		efc.engineFrontendMonitorMutex.Unlock()
	}()
}

// resetAndStopMonitoring resets monitoring-related status fields and stops the monitor.
// The monitor is always stopped regardless of whether the status reset succeeds,
// to prevent the monitor goroutine from overwriting cleared status fields.
func (efc *EngineFrontendController) resetAndStopMonitoring(ef *longhorn.EngineFrontend) {
	if _, err := efc.ds.ResetMonitoringEngineFrontendStatus(ef); err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to update engine frontend %v to stop monitoring", ef.Name))
	}

	efc.stopMonitoring(ef.Name)
}

// stopMonitoring closes the stop channel for the given EngineFrontend monitor
func (efc *EngineFrontendController) stopMonitoring(efName string) {
	efc.engineFrontendMonitorMutex.Lock()
	defer efc.engineFrontendMonitorMutex.Unlock()

	stopCh, ok := efc.engineFrontendMonitorMap[efName]
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

// Run starts the EngineFrontendMonitor polling loop
func (m *EngineFrontendMonitor) Run() {
	m.logger.Info("Starting monitoring engine frontend")
	defer func() {
		m.logger.Info("Stopping monitoring engine frontend")
		close(m.monitorVoluntaryStopCh)
	}()

	ticker := time.NewTicker(EnginePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if needStop := m.sync(); needStop {
				return
			}
		case <-m.stopCh:
			return
		}
	}
}

// sync fetches the EngineFrontend from the datastore and calls refresh
func (m *EngineFrontendMonitor) sync() bool {
	for count := 0; count < EngineMonitorConflictRetryCount; count++ {
		ef, err := m.ds.GetEngineFrontend(m.Name)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				m.logger.Warn("Stopping monitoring because the engine frontend no longer exists")
				return true
			}
			utilruntime.HandleError(errors.Wrapf(err, "failed to get engine frontend %v for monitoring", m.Name))
			return false
		}

		if ef.Status.OwnerID != m.controllerID {
			m.logger.Warnf("Stopping monitoring the engine frontend on this node (%v) because the engine frontend has new ownerID %v", m.controllerID, ef.Status.OwnerID)
			return true
		}

		// engine frontend is maybe starting
		if ef.Status.CurrentState != longhorn.InstanceStateRunning {
			return false
		}

		if err := m.refresh(ef); err == nil || !apierrors.IsConflict(errors.Cause(err)) {
			utilruntime.HandleError(errors.Wrapf(err, "failed to update status for engine frontend %v", m.Name))
			break
		}
		// Retry if the error is due to conflict
	}

	return false
}

// refresh collects status information from the running EngineFrontend instance
func (m *EngineFrontendMonitor) refresh(ef *longhorn.EngineFrontend) (err error) {
	existingEF := ef.DeepCopy()

	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingEF.Status, ef.Status) {
			if _, updateErr := m.ds.UpdateEngineFrontendStatus(ef); updateErr != nil {
				err = updateErr
			}
		}
	}()

	// Create EngineFrontendClientProxy to communicate with the running initiator
	im, err := m.ds.GetInstanceManagerRO(ef.Status.InstanceManagerName)
	if err != nil {
		return errors.Wrapf(err, "failed to get instance manager %v for engine frontend monitoring", ef.Status.InstanceManagerName)
	}

	efClientProxy, err := engineapi.NewEngineFrontendClientProxy(im, m.logger)
	if err != nil {
		return err
	}
	defer efClientProxy.Close()

	// Get the current instance status
	instance, err := efClientProxy.EngineFrontendGet(ef)
	if err != nil {
		return err
	}

	endpoint, err := engineapi.GetEngineEndpoint(instance.Status.Frontend, instance.Status.Endpoint, "")
	if err != nil {
		return err
	}
	ef.Status.Endpoint = endpoint

	// During a switchover the sync loop is driving TargetIP/TargetPort via
	// the 3-phase state machine. If the monitor overwrites those fields from
	// possibly stale data-plane info, the volume controller may see
	// TargetIP != expected and wait forever. Skip target-related fields here
	// and let the switchover path be the sole writer until it completes.
	if ef.Status.SwitchoverPhase == longhorn.EngineFrontendSwitchoverPhaseNone {
		syncEngineFrontendPathStatus(ef, instance)
	} else {
		m.logger.Debugf("Skipping path/target status sync during switchover phase %v", ef.Status.SwitchoverPhase)
	}
	if isEngineFrontendEndpointRequired(ef) && ef.Status.Endpoint == "" {
		m.logger.WithFields(logrus.Fields{
			"volume":          ef.Spec.VolumeName,
			"frontend":        ef.Spec.Frontend,
			"disableFrontend": ef.Spec.DisableFrontend,
		}).Trace("Skipped engine frontend expansion because endpoint is not ready")
		return nil
	}

	volume, err := m.ds.GetVolumeRO(ef.Spec.VolumeName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to get volume %v for engine frontend monitoring", ef.Spec.VolumeName)
		}
		m.logger.WithField("volume", ef.Spec.VolumeName).Trace("Skip engine frontend expansion because volume no longer exists")
		return nil
	}

	currentEngine, err := m.ds.GetVolumeCurrentEngine(ef.Spec.VolumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get current engine for volume %v during engine frontend monitoring", ef.Spec.VolumeName)
	}

	engineForFrontend := currentEngine
	if ef.Spec.EngineName != "" && ef.Spec.EngineName != currentEngine.Name {
		engineForFrontend, err = m.ds.GetEngineRO(ef.Spec.EngineName)
		if err != nil {
			return errors.Wrapf(err, "failed to get engine %v for engine frontend %v during engine frontend monitoring", ef.Spec.EngineName, ef.Name)
		}
	}

	// Observe the current frontend size via the engine frontend proxy. The proxy's
	// VolumeFrontendGet overlays the EngineFrontend's own size on top of the
	// engine view, so ef.Status.CurrentSize reflects what the frontend device
	// is actually serving, independently of engine.Status.CurrentSize.
	engineClientProxy, err := engineapi.GetCompatibleClient(engineForFrontend, nil, m.ds, m.logger, m.proxyConnCounter)
	if err != nil {
		return errors.Wrapf(err, "failed to get engine client proxy for volume %v during engine frontend monitoring", ef.Spec.VolumeName)
	}
	defer engineClientProxy.Close()

	volumeInfo, err := engineClientProxy.VolumeFrontendGet(engineForFrontend, ef)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume info for volume %v during engine frontend monitoring", ef.Spec.VolumeName)
	}
	ef.Status.CurrentSize = volumeInfo.Size

	if shouldExpandEngineFrontend(ef, volume, engineForFrontend) {
		// Expand only when the volume is actually in expansion flow.
		if m.expansionBackoff.IsInBackOffSinceUpdate(ef.Name, time.Now()) {
			m.logger.Debug("Skipping engine frontend expansion since it is in the back-off window")
		} else {
			engineClientProxy, err := engineapi.GetCompatibleClient(ef, nil, m.ds, m.logger, m.proxyConnCounter)
			if err != nil {
				return errors.Wrapf(err, "failed to get engine client proxy for volume frontend %v", ef.Name)
			}
			defer engineClientProxy.Close()
			cliAPIVersion, err := m.ds.GetDataEngineImageCLIAPIVersion(ef.Status.CurrentImage, ef.Spec.DataEngine)
			if err != nil {
				return err
			}
			expectedExpansionSize, err := util.GetActualBackendSize(ef.Spec.VolumeSize, volume.Spec.Encrypted, cliAPIVersion)
			if err != nil {
				return err
			}
			if err := engineClientProxy.VolumeExpand(ef, expectedExpansionSize); err != nil {
				if isV2ExpansionInProgressError(err) {
					m.logger.WithError(err).Debug("Skipping engine frontend expansion because expansion is already in progress")
					return nil
				}
				m.expansionBackoff.Next(ef.Name, time.Now())
				m.eventRecorder.Eventf(ef, corev1.EventTypeWarning, constant.EventReasonFailedExpansion,
					"Engine frontend failed to expand to size %v: %v", ef.Spec.Size, err)
				return errors.Wrapf(err, "failed to expand volume frontend %v", ef.Name)
			}
			// Expansion succeeded; clear backoff.
			m.expansionBackoff.DeleteEntry(ef.Name)
		}
	} else {
		// Expansion not required or already completed; clear any stale backoff.
		m.expansionBackoff.DeleteEntry(ef.Name)
		m.logger.WithFields(logrus.Fields{
			"volume":            ef.Spec.VolumeName,
			"requestedSize":     ef.Spec.Size,
			"expansionRequired": volume.Status.ExpansionRequired,
		}).Trace("Skip engine frontend expansion because volume expansion is not required")
	}

	return nil
}

func shouldExpandEngineFrontend(ef *longhorn.EngineFrontend, v *longhorn.Volume, e *longhorn.Engine) bool {
	if ef == nil || v == nil || e == nil {
		return false
	}
	if ef.Spec.Size <= 0 {
		return false
	}
	if !v.Status.ExpansionRequired {
		return false
	}
	if e.Status.IsExpanding {
		return false
	}
	if isReplicaRebuildingInProgress(e) {
		return false
	}
	return ef.Status.CurrentSize < ef.Spec.Size
}

func isReplicaRebuildingInProgress(e *longhorn.Engine) bool {
	for _, mode := range e.Status.ReplicaModeMap {
		if mode == longhorn.ReplicaModeWO {
			return true
		}
	}

	for _, status := range e.Status.RebuildStatus {
		if status == nil {
			continue
		}
		// Check both fields defensively: IsRebuilding is the high-level flag,
		// while State may lag behind or be set independently by the engine.
		if status.IsRebuilding || status.State == engineapi.ProcessStateInProgress {
			return true
		}
	}

	return false
}

func isEngineFrontendEndpointRequired(ef *longhorn.EngineFrontend) bool {
	if ef == nil {
		return false
	}
	return !ef.Spec.DisableFrontend && ef.Spec.Frontend != longhorn.VolumeFrontendEmpty
}

func syncEngineFrontendPathStatus(ef *longhorn.EngineFrontend, instance *longhorn.InstanceProcess) {
	if ef == nil || instance == nil {
		return
	}

	ef.Status.ActivePath = instance.Status.ActivePath
	ef.Status.PreferredPath = instance.Status.PreferredPath
	ef.Status.Paths = copyEngineFrontendNvmeTCPPaths(instance.Status.Paths)

	targetIP, targetPort, ok := getEngineFrontendTargetFromPaths(instance.Status.ActivePath, instance.Status.TargetPortStart, ef.Status.Paths)
	if !ok {
		ef.Status.TargetIP = ""
		ef.Status.TargetPort = 0
		return
	}

	ef.Status.TargetIP = targetIP
	ef.Status.TargetPort = targetPort
}

func copyEngineFrontendNvmeTCPPaths(paths []longhorn.EngineFrontendNvmeTCPPath) []longhorn.EngineFrontendNvmeTCPPath {
	if len(paths) == 0 {
		return nil
	}

	res := make([]longhorn.EngineFrontendNvmeTCPPath, 0, len(paths))
	res = append(res, paths...)

	return res
}

func getEngineFrontendTargetFromPaths(activePath string, targetPort int32, paths []longhorn.EngineFrontendNvmeTCPPath) (string, int, bool) {
	for _, path := range paths {
		if fmt.Sprintf("%s:%d", path.TargetIP, path.TargetPort) == activePath && path.TargetIP != "" && path.TargetPort != 0 {
			return path.TargetIP, path.TargetPort, true
		}
	}

	if len(paths) == 1 && paths[0].TargetIP != "" && paths[0].TargetPort != 0 {
		return paths[0].TargetIP, paths[0].TargetPort, true
	}

	for _, path := range paths {
		if path.TargetPort == int(targetPort) && path.TargetIP != "" && path.TargetPort != 0 {
			return path.TargetIP, path.TargetPort, true
		}
	}

	return "", 0, false
}

func isEngineFrontendTargetInitialized(targetIP string, targetPort int) bool {
	return targetIP != "" && targetPort != 0
}

// v2ExpansionInProgressMsg matches the SPDK engine's ErrExpansionInProgress
// error message after it crosses gRPC boundaries (where errors.Is is unavailable).
const v2ExpansionInProgressMsg = "expansion is in progress"

func isV2ExpansionInProgressError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), v2ExpansionInProgressMsg)
}
