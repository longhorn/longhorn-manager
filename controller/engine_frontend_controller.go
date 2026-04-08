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
	EngineFrontendSuspend(dataEngine longhorn.DataEngineType, name string) error
	EngineFrontendSwitchOverTarget(dataEngine longhorn.DataEngineType, name, targetAddress, engineName string) error
	EngineFrontendResume(dataEngine longhorn.DataEngineType, name string) error
}

type switchoverFailureType string

const (
	switchoverFailureSuspend         switchoverFailureType = "suspend"
	switchoverFailureSwitch          switchoverFailureType = "switch"
	switchoverFailureSwitchAndResume switchoverFailureType = "switch-and-resume"
	switchoverFailureResume          switchoverFailureType = "resume"
)

var errV2ReplicaRebuildDeferred = errors.New("v2 replica rebuild deferred")

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

	// Deferred conditions are transient — re-enqueue after a delay without
	// consuming retry budget so that the key is not dropped after maxRetries.
	if errors.Is(err, errV2ReplicaRebuildDeferred) {
		efc.logger.WithField("engineFrontend", key).Debug("Rebuild deferred, re-enqueueing after delay")
		efc.queue.Forget(key)
		efc.queue.AddAfter(key, EnginePollInterval)
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

	if types.IsDataEngineV2(ef.Spec.DataEngine) {
		if isV2DisabledForNode, err := efc.ds.IsV2DataEngineDisabledForNode(efc.controllerID); err != nil || isV2DisabledForNode {
			return false, err
		}
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

	// The engine is not running, the owner node doesn't need to have e.Status.CurrentImage
	// Fall back to the default logic where we pick a running node to be the owner
	if ef.Status.CurrentImage == "" {
		return isResponsible, nil
	}

	if types.IsDataEngineV1(ef.Spec.DataEngine) {
		readyNodesWithEI, err := efc.ds.ListReadyNodesContainingEngineImageRO(ef.Status.CurrentImage)
		if err != nil {
			return false, errors.Wrapf(err, "failed to list ready nodes containing engine image %v", ef.Status.CurrentImage)
		}
		// No node in the system has the ef.Status.CurrentImage,
		// Fall back to the default logic where we pick a running node to be the owner
		if len(readyNodesWithEI) == 0 {
			return isResponsible, nil
		}
	}

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

	// Handle switchover if TargetIP changes.
	// CurrentState == Running: normal case, may need to initiate Phase 1 (suspend).
	// CurrentState == Suspended: mid-switchover, instance monitor reported
	// "suspended" after Phase 1, proceed to Phase 2 (switchover + resume).
	if ef.Status.CurrentState == longhorn.InstanceStateRunning || ef.Status.CurrentState == longhorn.InstanceStateSuspended {
		statusTargetInitialized := isEngineFrontendTargetInitialized(ef.Status.TargetIP, ef.Status.TargetPort)
		specTargetInitialized := isEngineFrontendTargetInitialized(ef.Spec.TargetIP, ef.Spec.TargetPort)
		if !statusTargetInitialized && specTargetInitialized {
			// Initialize target status after successful creation when it is still incomplete.
			ef.Status.TargetIP = ef.Spec.TargetIP
			ef.Status.TargetPort = ef.Spec.TargetPort
		} else if specTargetInitialized {
			targetChanged := ef.Status.TargetIP != ef.Spec.TargetIP || ef.Status.TargetPort != ef.Spec.TargetPort
			targetReverted := !targetChanged

			if targetChanged || ef.Status.CurrentState == longhorn.InstanceStateSuspended {
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

				if ef.Status.CurrentState == longhorn.InstanceStateRunning && targetChanged {
					// Phase 1: Suspend the frontend. The instance monitor will
					// report CurrentState=suspended, which is visible via
					// kubectl and used by the volume controller to stop the old
					// engine while I/O is paused.
					log.Infof("Target IP/Port changed from %v:%v to %v:%v, suspending frontend for switchover",
						ef.Status.TargetIP, ef.Status.TargetPort, ef.Spec.TargetIP, ef.Spec.TargetPort)

					if err := c.EngineFrontendSuspend(ef.Spec.DataEngine, ef.Name); err != nil {
						efc.recordEngineFrontendSwitchoverFailureEvent(ef, switchoverFailureSuspend, targetAddress, err)
						return errors.Wrapf(err, "failed to suspend engine frontend %v before switchover", ef.Name)
					}

					log.Infof("Engine frontend %v suspended, waiting for instance monitor to report suspended state", ef.Name)
					// Return — the instance monitor will update CurrentState to
					// "suspended" and trigger the next reconcile for Phase 2.

				} else if ef.Status.CurrentState == longhorn.InstanceStateSuspended && targetReverted {
					// Switchover was reverted (spec target changed back to
					// match status). Just resume without switchover.
					log.Infof("Switchover reverted for engine frontend %v, resuming without switchover", ef.Name)
					if err := c.EngineFrontendResume(ef.Spec.DataEngine, ef.Name); err != nil {
						return errors.Wrapf(err, "failed to resume engine frontend %v after switchover revert", ef.Name)
					}
					log.Infof("Engine frontend %v resumed after switchover revert", ef.Name)

				} else if ef.Status.CurrentState == longhorn.InstanceStateSuspended && targetChanged {
					// Phase 2: Perform the actual target switchover and resume.
					log.Infof("Switching over target for suspended engine frontend %v to %v", ef.Name, targetAddress)

					if err := c.EngineFrontendSwitchOverTarget(ef.Spec.DataEngine, ef.Name, targetAddress, ef.Spec.EngineName); err != nil {
						efc.logger.WithError(err).Warnf("Failed to switch over target for engine frontend %v, trying to resume it back", ef.Name)
						resumeErr := c.EngineFrontendResume(ef.Spec.DataEngine, ef.Name)
						if resumeErr != nil {
							efc.recordEngineFrontendSwitchoverFailureEvent(ef, switchoverFailureSwitchAndResume, targetAddress, err)
							return errors.Wrapf(err, "failed to switch over target for engine frontend %v, then failed to resume: %v", ef.Name, resumeErr)
						}
						efc.recordEngineFrontendSwitchoverFailureEvent(ef, switchoverFailureSwitch, targetAddress, err)
						return errors.Wrapf(err, "failed to switch over target for engine frontend %v", ef.Name)
					}

					log.Infof("Resuming engine frontend %v after target switchover to %v", ef.Name, targetAddress)
					if err := c.EngineFrontendResume(ef.Spec.DataEngine, ef.Name); err != nil {
						efc.recordEngineFrontendSwitchoverFailureEvent(ef, switchoverFailureResume, targetAddress, err)
						return errors.Wrapf(err, "failed to resume engine frontend %v after switchover", ef.Name)
					}

					log.Infof("Successfully switched over target for engine frontend %v to %v", ef.Name, targetAddress)
					efc.eventRecorder.Eventf(ef, corev1.EventTypeNormal, constant.EventReasonSwitchover, "Successfully switched over target to %v", targetAddress)
					ef.Status.TargetIP = ef.Spec.TargetIP
					ef.Status.TargetPort = ef.Spec.TargetPort
				}
			}
		}
	}

	if err := efc.rebuildNewReplica(ef); err != nil {
		return err
	}

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

// switchEngineFrontendTarget performs the full suspend → switchover → resume
// sequence in a single call. It is no longer used by the phased switchover
// logic in syncEngineFrontend but is kept for unit tests that validate the
// error-handling paths.
func switchEngineFrontendTarget(c engineFrontendSwitchoverClient, ef *longhorn.EngineFrontend, targetAddress string) (switchoverFailureType, error) {
	if err := c.EngineFrontendSuspend(ef.Spec.DataEngine, ef.Name); err != nil {
		return switchoverFailureSuspend, errors.Wrapf(err, "failed to suspend engine frontend %v before switchover", ef.Name)
	}

	if err := c.EngineFrontendSwitchOverTarget(ef.Spec.DataEngine, ef.Name, targetAddress, ef.Spec.EngineName); err != nil {
		resumeErr := c.EngineFrontendResume(ef.Spec.DataEngine, ef.Name)
		if resumeErr != nil {
			return switchoverFailureSwitchAndResume, errors.Wrapf(err, "failed to switch over target for engine frontend %v, then failed to resume: %v", ef.Name, resumeErr)
		}
		return switchoverFailureSwitch, errors.Wrapf(err, "failed to switch over target for engine frontend %v", ef.Name)
	}

	if err := c.EngineFrontendResume(ef.Spec.DataEngine, ef.Name); err != nil {
		return switchoverFailureResume, errors.Wrapf(err, "failed to resume engine frontend %v after switchover", ef.Name)
	}

	return "", nil
}

func getEngineFrontendSwitchoverFailureEventMessage(failureType switchoverFailureType, targetAddress string, err error) string {
	switch failureType {
	case switchoverFailureSuspend:
		return fmt.Sprintf("Failed to suspend engine frontend before switchover to %v: %v", targetAddress, err)
	case switchoverFailureSwitchAndResume:
		return fmt.Sprintf("Failed to switch over target to %v and failed to resume engine frontend: %v", targetAddress, err)
	case switchoverFailureResume:
		return fmt.Sprintf("Switched over target to %v but failed to resume engine frontend: %v", targetAddress, err)
	default:
		return fmt.Sprintf("Failed to switch over target to %v: %v", targetAddress, err)
	}
}

func recordEngineFrontendSwitchoverFailureEvent(recorder record.EventRecorder, ef *longhorn.EngineFrontend, failureType switchoverFailureType, targetAddress string, err error) {
	recorder.Eventf(ef, corev1.EventTypeWarning, constant.EventReasonFailedSwitchover,
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

func (efc *EngineFrontendController) rebuildNewReplica(ef *longhorn.EngineFrontend) error {
	if ef == nil || ef.Spec.EngineName == "" || ef.Status.CurrentState != longhorn.InstanceStateRunning {
		return nil
	}

	engine, err := efc.ds.GetEngineRO(ef.Spec.EngineName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to get engine %v for replica rebuild", ef.Spec.EngineName)
	}
	if !types.IsDataEngineV2(engine.Spec.DataEngine) || engine.Status.CurrentState != longhorn.InstanceStateRunning {
		return nil
	}
	replicaName, replicaAddress, needRebuild := getReplicaRebuildCandidate(engine, efc.logger)
	if !needRebuild {
		return nil
	}

	return efc.startRebuilding(ef, engine, replicaName, replicaAddress)
}

func (efc *EngineFrontendController) startRebuilding(ef *longhorn.EngineFrontend, engine *longhorn.Engine, replicaName, replicaAddress string) error {
	log := efc.logger.WithFields(logrus.Fields{
		"volume":         ef.Spec.VolumeName,
		"engineFrontend": ef.Name,
		"engine":         engine.Name,
		"replica":        replicaName,
	})

	rebuildClientProxy, err := engineapi.GetCompatibleClient(ef, nil, efc.ds, efc.logger, efc.proxyConnCounter)
	if err != nil {
		return err
	}
	rebuildClientProxy.Close()

	// In v2, replica membership is still owned by the engine. We use the
	// engine client for membership checks here even though the add entry point
	// itself is frontend-aware.
	engineMembershipClientProxy, err := engineapi.GetCompatibleClient(engine, nil, efc.ds, efc.logger, efc.proxyConnCounter)
	if err != nil {
		return err
	}
	defer engineMembershipClientProxy.Close()

	alreadyExists, err := doesAddressExistInEngine(engine, replicaAddress, engineMembershipClientProxy)
	if err != nil {
		return err
	}
	if alreadyExists {
		log.Infof("Replica %v address %v has been added to the engine already", replicaName, replicaAddress)
		return nil
	}

	replicaURL := engineapi.GetBackendReplicaURL(replicaAddress)
	startResultCh := make(chan error, 1)
	go func() {
		reportedStart := false
		reportStartResult := func(err error) {
			if reportedStart {
				return
			}
			startResultCh <- err
			reportedStart = true
		}

		autoCleanupSystemGeneratedSnapshot, err := efc.ds.GetSettingAsBool(types.SettingNameAutoCleanupSystemGeneratedSnapshot)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Errorf("Failed to get %v setting", types.SettingNameAutoCleanupSystemGeneratedSnapshot)
			return
		}

		fastReplicaRebuild, err := efc.ds.GetSettingAsBoolByDataEngine(types.SettingNameFastReplicaRebuildEnabled, engine.Spec.DataEngine)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Errorf("Failed to get %v setting for data engine %v", types.SettingNameFastReplicaRebuildEnabled, engine.Spec.DataEngine)
			return
		}

		grpcTimeoutSeconds, err := efc.ds.GetSettingAsInt(types.SettingNameLongGRPCTimeOut)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Errorf("Failed to get %v setting", types.SettingNameLongGRPCTimeOut)
			return
		}

		engineFrontend, err := efc.ds.GetEngineFrontend(ef.Name)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Error("Failed to get engine frontend before rebuilding")
			return
		}
		currentEngine, err := efc.ds.GetEngineRO(engine.Name)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Error("Failed to get engine before rebuilding")
			return
		}
		if engineFrontend.Status.OwnerID != efc.controllerID || engineFrontend.Status.CurrentState != longhorn.InstanceStateRunning ||
			currentEngine.Status.CurrentState != longhorn.InstanceStateRunning {
			reportStartResult(errV2ReplicaRebuildDeferred)
			return
		}
		rebuildClientProxy, err := engineapi.GetCompatibleClient(engineFrontend, nil, efc.ds, efc.logger, efc.proxyConnCounter)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Error("Failed to get engine frontend client proxy for rebuilding")
			return
		}
		defer rebuildClientProxy.Close()

		// Replica cleanup and membership updates are engine operations in v2,
		// so they continue to use the engine client rather than the frontend.
		engineCleanupClientProxy, err := engineapi.GetCompatibleClient(currentEngine, nil, efc.ds, efc.logger, efc.proxyConnCounter)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Error("Failed to get engine client proxy for rebuilding cleanup")
			return
		}
		defer engineCleanupClientProxy.Close()

		// If enabled, call and wait for SnapshotPurge to clean up system generated snapshot before rebuilding.
		if autoCleanupSystemGeneratedSnapshot {
			allowSnapshotPurge, err := efc.snapshotConcurrentLimiter.CanStartSnapshotPurge(engineCleanupClientProxy, currentEngine, efc.ds)
			if err != nil {
				log.WithError(err).Error("Failed to check whether can start snapshot purge before rebuilding")
				reportStartResult(errV2ReplicaRebuildDeferred)
				return
			}

			if !allowSnapshotPurge {
				log.Debugf("Cannot start SnapshotPurge for engine %v before rebuilding because the concurrent limit is reached", currentEngine.Name)
				reportStartResult(errV2ReplicaRebuildDeferred)
				return
			}

			// Unblock the reconcile worker before the potentially long purge
			// wait (up to purgeWaitIntervalInSecond). The outer code will poll
			// for the rebuild address with EnginePollTimeout and requeue on
			// timeout, matching v1 behavior.
			reportStartResult(nil)

			log.Info("Starting snapshot purge before rebuilding")
			dataEngineObj, err := efc.ds.GetDataEngineObject(currentEngine)
			if err != nil {
				reportStartResult(err)
				log.WithError(err).Error("Failed to get data engine object before rebuilding")
				efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeWarning, constant.EventReasonFailedStartingSnapshotPurge,
					"Failed to start snapshot purge for engine %v and volume %v before rebuilding: %v", currentEngine.Name, currentEngine.Spec.VolumeName, err)
				return
			}
			if err := engineCleanupClientProxy.SnapshotPurge(dataEngineObj); err != nil {
				reportStartResult(err)
				log.WithError(err).Error("Failed to start snapshot purge before rebuilding")
				efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeWarning, constant.EventReasonFailedStartingSnapshotPurge,
					"Failed to start snapshot purge for engine %v and volume %v before rebuilding: %v", currentEngine.Name, currentEngine.Spec.VolumeName, err)
				return
			}

			log.Info("Starting snapshot purge before rebuilding, will wait for the purge complete")
			purgeDone := false
			endTime := time.Now().Add(time.Duration(purgeWaitIntervalInSecond) * time.Second)
			ticker := time.NewTicker(2 * EnginePollInterval)
			defer ticker.Stop()
			for !purgeDone && time.Now().Before(endTime) {
				<-ticker.C

				e, err := efc.ds.GetEngineRO(currentEngine.Name)
				if err != nil {
					log.WithError(err).Error("Failed to get engine and wait for the purge before rebuilding")
					reportStartResult(err)
					return
				}
				if !shouldProceedToWaitAndRebuild(e, replicaName, replicaAddress, log) {
					reportStartResult(errV2ReplicaRebuildDeferred)
					return
				}

				purgeDone = true
				for _, purgeStatus := range e.Status.PurgeStatus {
					if purgeStatus.IsPurging {
						purgeDone = false
						break
					}
				}
			}
			if !purgeDone {
				log.Errorf("Timeout waiting for snapshot purge done before rebuilding, wait interval %v second", purgeWaitIntervalInSecond)
				efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeWarning, constant.EventReasonTimeoutSnapshotPurge,
					"Timeout waiting for snapshot purge done before rebuilding volume %v, wait interval %v second",
					currentEngine.Spec.VolumeName, purgeWaitIntervalInSecond)
				reportStartResult(errV2ReplicaRebuildDeferred)
				return
			}
		}

		replica, err := efc.ds.GetReplica(replicaName)
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Errorf("Failed to get replica %v unable to mark failed rebuild", replicaName)
			return
		}

		replica, err = updateReplicaRebuildFailedCondition(efc.ds, replica, "")
		if err != nil {
			reportStartResult(err)
			log.WithError(err).Errorf("Failed to update rebuild status information on replica %v", replicaName)
			return
		}

		efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeNormal, constant.EventReasonRebuilding,
			"Start rebuilding replica %v with Address %v through engine frontend %v for engine %v and volume %v",
			replicaName, replicaAddress, engineFrontend.Name, currentEngine.Name, currentEngine.Spec.VolumeName)
		// TODO: Before calling ReplicaAdd for the v2 frontend path, fetch the
		// latest size/currentSize from currentEngine and pass them through once
		// the proxy API consumes those fields for EngineFrontend-based rebuild.
		err = rebuildClientProxy.ReplicaAdd(engineFrontend, replicaName, replicaURL, false, fastReplicaRebuild, nil, 0, grpcTimeoutSeconds)
		switch {
		case err == nil:
			reportStartResult(nil)
		case isV2ReplicaAddAlreadyInProgressError(err):
			log.WithError(err).Info("Replica add is already in progress, waiting for the current rebuild to complete")
			reportStartResult(nil)
			err = nil
		case isV2ReplicaAddRestoreInProgressError(err):
			reportStartResult(errV2ReplicaRebuildDeferred)
			log.WithError(err).Info("Skipping replica rebuild because restore is in progress")
			return
		default:
			reportStartResult(err)
		}
		if err == nil {
			err = waitForV2EngineRebuild(efc.ds, currentEngine, replicaName, grpcTimeoutSeconds)
		}

		if err != nil {
			replicaRebuildErrMsg := err.Error()

			log.WithError(err).Errorf("Failed to rebuild replica %v", replicaAddress)
			efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeWarning, constant.EventReasonFailedRebuilding,
				"Failed rebuilding replica with Address %v through engine frontend %v: %v", replicaAddress, engineFrontend.Name, err)

			if err := engineCleanupClientProxy.ReplicaRemove(currentEngine, replicaURL, replicaName); err != nil {
				log.WithError(err).Warnf("Failed to remove rebuilding replica %v", replicaAddress)
				efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeWarning, constant.EventReasonFailedDeleting,
					"Failed to remove rebuilding replica %v with address %v for engine %v and volume %v due to rebuilding failure: %v",
					replicaName, replicaAddress, currentEngine.Name, currentEngine.Spec.VolumeName, err)
			}

			// Use backoff to avoid recreating new replicas too quickly on repeated failures.
			if !efc.backoff.IsInBackOffSinceUpdate(currentEngine.Name, time.Now()) {
				replica, err = updateReplicaRebuildFailedCondition(efc.ds, replica, replicaRebuildErrMsg)
				if err != nil {
					log.WithError(err).Errorf("Failed to update rebuild status information on replica %v", replicaName)
					return
				}

				setReplicaFailedAt(replica, util.Now())
				replica.Spec.DesireState = longhorn.InstanceStateStopped
				if _, err := efc.ds.UpdateReplica(replica); err != nil {
					log.WithError(err).Errorf("Unable to mark failed rebuild on replica %v", replicaName)
					return
				}
				efc.backoff.Next(currentEngine.Name, time.Now())
				backoffTime := efc.backoff.Get(currentEngine.Name).Seconds()
				log.Infof("Marked failed rebuild on replica %v, backoff period is now %v seconds", replicaName, backoffTime)
				return
			}
			log.Debugf("Engine is still in backoff for replica %v rebuild failure", replicaName)
			return
		}

		// Replica rebuild succeeded, clear backoff.
		efc.backoff.DeleteEntry(currentEngine.Name)
		efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeNormal, constant.EventReasonRebuilt,
			"Replica %v with Address %v has been rebuilt through engine frontend %v for engine %v and volume %v",
			replicaName, replicaAddress, engineFrontend.Name, currentEngine.Name, currentEngine.Spec.VolumeName)

		// If enabled, call SnapshotPurge to clean up system generated snapshot after rebuilding.
		if autoCleanupSystemGeneratedSnapshot {
			allowSnapshotPurge, err := efc.snapshotConcurrentLimiter.CanStartSnapshotPurge(engineCleanupClientProxy, currentEngine, efc.ds)
			if err != nil {
				log.WithError(err).Error("Failed to check whether can start snapshot purge after rebuilding")
				return
			}

			if !allowSnapshotPurge {
				log.Debug("Cannot start SnapshotPurge for engine after rebuilding because the concurrent limit is reached")
				return
			}

			log.Info("Starting snapshot purge after rebuilding")
			dataEngineObj, err := efc.ds.GetDataEngineObject(currentEngine)
			if err != nil {
				log.WithError(err).Error("Failed to get data engine object after rebuilding")
				efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeWarning, constant.EventReasonFailedStartingSnapshotPurge,
					"Failed to start snapshot purge for engine %v and volume %v after rebuilding: %v", currentEngine.Name, currentEngine.Spec.VolumeName, err)
				return
			}
			if err := engineCleanupClientProxy.SnapshotPurge(dataEngineObj); err != nil {
				log.WithError(err).Error("Failed to start snapshot purge after rebuilding")
				efc.eventRecorder.Eventf(currentEngine, corev1.EventTypeWarning, constant.EventReasonFailedStartingSnapshotPurge,
					"Failed to start snapshot purge for engine %v and volume %v after rebuilding: %v", currentEngine.Name, currentEngine.Spec.VolumeName, err)
				return
			}
		}
	}()

	startErr := <-startResultCh
	if errors.Is(startErr, errV2ReplicaRebuildDeferred) {
		return fmt.Errorf("v2 replica rebuild deferred, will retry: %w", errV2ReplicaRebuildDeferred)
	}
	if startErr != nil {
		return startErr
	}

	// Wait until engine confirmed that rebuild started.
	if err := wait.PollUntilContextTimeout(context.Background(), EnginePollInterval, EnginePollTimeout, true, func(context.Context) (bool, error) {
		return doesAddressExistInEngine(engine, replicaAddress, engineMembershipClientProxy)
	}); err != nil {
		return err
	}

	return nil
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

// resetAndStopMonitoring resets monitoring-related status fields and stops the monitor
func (efc *EngineFrontendController) resetAndStopMonitoring(ef *longhorn.EngineFrontend) {
	if _, err := efc.ds.ResetMonitoringEngineFrontendStatus(ef); err != nil {
		utilruntime.HandleError(errors.Wrapf(err, "failed to update engine frontend %v to stop monitoring", ef.Name))
		return
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
func (m *EngineFrontendMonitor) refresh(ef *longhorn.EngineFrontend) error {
	existingEF := ef.DeepCopy()

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

	// Update target connection info when running
	// ef.Status.TargetIP = instance.Status.TargetIP
	// ef.Status.TargetPort = instance.Status.TargetPort

	volume, err := m.ds.GetVolumeRO(ef.Spec.VolumeName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to get volume %v for engine frontend monitoring", ef.Spec.VolumeName)
		}
		m.logger.WithField("volume", ef.Spec.VolumeName).Trace("Skip engine frontend expansion because volume no longer exists")
	} else {
		currentEngine, err := m.ds.GetVolumeCurrentEngine(ef.Spec.VolumeName)
		if err != nil {
			return errors.Wrapf(err, "failed to get current engine for volume %v during engine frontend monitoring", ef.Spec.VolumeName)
		}

		if shouldExpandEngineFrontend(ef, volume, currentEngine) {
			// Expand only when the volume is actually in expansion flow.
			if m.expansionBackoff.IsInBackOffSinceUpdate(ef.Name, time.Now()) {
				m.logger.Debug("Skipping engine frontend expansion since it is in the back-off window")
			} else {
				engineClientProxy, err := engineapi.GetCompatibleClient(ef, nil, m.ds, m.logger, m.proxyConnCounter)
				if err != nil {
					return errors.Wrapf(err, "failed to get engine client proxy for volume frontend %v", ef.Name)
				}
				defer engineClientProxy.Close()
				if err := engineClientProxy.VolumeExpand(ef); err != nil {
					if isV2ExpansionInProgressError(err) {
						m.logger.WithError(err).Debug("Skipping engine frontend expansion because expansion is already in progress")
						return nil
					}
					m.expansionBackoff.Next(ef.Name, time.Now())
					m.eventRecorder.Eventf(ef, corev1.EventTypeWarning, constant.EventReasonFailedExpansion,
						"Engine frontend failed to expand to size %v: %v", ef.Spec.VolumeSize, err)
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
				"requestedSize":     ef.Spec.VolumeSize,
				"expansionRequired": volume.Status.ExpansionRequired,
			}).Trace("Skip engine frontend expansion because volume expansion is not required")
		}
	}

	// Only update status if something changed
	if !reflect.DeepEqual(existingEF.Status, ef.Status) {
		if _, err := m.ds.UpdateEngineFrontendStatus(ef); err != nil {
			return err
		}
	}

	return nil
}

func shouldExpandEngineFrontend(ef *longhorn.EngineFrontend, v *longhorn.Volume, e *longhorn.Engine) bool {
	if ef == nil || v == nil || e == nil {
		return false
	}
	if ef.Spec.VolumeSize <= 0 {
		return false
	}
	if !v.Status.ExpansionRequired {
		return false
	}
	if e.Status.IsExpanding {
		return false
	}
	return e.Status.CurrentSize < ef.Spec.VolumeSize
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
