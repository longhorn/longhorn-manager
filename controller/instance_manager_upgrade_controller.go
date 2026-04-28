package controller

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	bsutil "github.com/longhorn/backupstore/util"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	// instanceManagerUpgradeRequeueAfter is the duration after which an active upgrade is re-enqueued for processing.
	// This controls how frequently the controller checks for upgrade timeouts and temp node health during the upgrade process.
	instanceManagerUpgradeRequeueAfter = 10 * time.Second
)

// errUpgradePrecondition is returned by buildEngineRelocationPlan when the
// upgrade cannot proceed due to a recoverable precondition (e.g. degraded
// volume, no healthy replica on another node). Callers that want to wait
// rather than propagate should check with errors.Is(err, errUpgradePrecondition).
var errUpgradePrecondition = errors.New("upgrade precondition not met")

type InstanceManagerUpgradeController struct {
	*baseController

	namespace    string
	controllerID string

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder
}

func NewInstanceManagerUpgradeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string,
	controllerID string,
) (*InstanceManagerUpgradeController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	imuc := &InstanceManagerUpgradeController{
		baseController: newBaseController("longhorn-instance-manager-upgrade", logger),

		ds:           ds,
		namespace:    namespace,
		controllerID: controllerID,
		kubeClient:   kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(
			scheme,
			corev1.EventSource{Component: "longhorn-instance-manager-upgrade-controller"},
		),
	}

	var err error

	if _, err = ds.InstanceManagerUpgradeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: imuc.enqueueInstanceManagerUpgrade,
		UpdateFunc: func(oldObj, newObj interface{}) {
			imuc.enqueueInstanceManagerUpgrade(newObj)
		},
		DeleteFunc: imuc.enqueueInstanceManagerUpgrade,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.InstanceManagerUpgradeInformer.HasSynced)

	if _, err = ds.InstanceManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    imuc.enqueueInstanceManagerChange,
		UpdateFunc: func(oldObj, newObj interface{}) { imuc.enqueueInstanceManagerChange(newObj) },
		DeleteFunc: imuc.enqueueInstanceManagerChange,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.InstanceManagerInformer.HasSynced)

	if _, err = ds.VolumeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    imuc.enqueueVolumeChange,
		UpdateFunc: func(oldObj, newObj interface{}) { imuc.enqueueVolumeChange(newObj) },
		DeleteFunc: imuc.enqueueVolumeChange,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.VolumeInformer.HasSynced)

	if _, err = ds.EngineInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    imuc.enqueueEngineChange,
		UpdateFunc: func(oldObj, newObj interface{}) { imuc.enqueueEngineChange(newObj) },
		DeleteFunc: imuc.enqueueEngineChange,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.EngineInformer.HasSynced)

	return imuc, nil
}

func (imuc *InstanceManagerUpgradeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer imuc.queue.ShutDown()

	imuc.logger.Info("Starting Longhorn instance manager upgrade controller")
	defer imuc.logger.Info("Shut down Longhorn instance manager upgrade controller")

	if !cache.WaitForNamedCacheSync("longhorn instance manager upgrades", stopCh, imuc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(imuc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (imuc *InstanceManagerUpgradeController) worker() {
	for imuc.processNextWorkItem() {
	}
}

func (imuc *InstanceManagerUpgradeController) processNextWorkItem() bool {
	key, quit := imuc.queue.Get()
	if quit {
		return false
	}
	defer imuc.queue.Done(key)

	err := imuc.syncInstanceManagerUpgrade(key.(string))
	imuc.handleErr(err, key)

	return true
}

func (imuc *InstanceManagerUpgradeController) handleErr(err error, key interface{}) {
	if err == nil {
		imuc.queue.Forget(key)
		return
	}

	log := imuc.logger.WithField("instanceManagerUpgrade", key)
	if imuc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn instance manager upgrade")
		imuc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn instance manager upgrade out of the queue")
	imuc.queue.Forget(key)
}

func getLoggerForInstanceManagerUpgrade(logger logrus.FieldLogger, imu *longhorn.InstanceManagerUpgrade) *logrus.Entry {
	return logger.WithFields(logrus.Fields{"instanceManagerUpgrade": imu.Name, "node": imu.Spec.NodeID})
}

func (imuc *InstanceManagerUpgradeController) isResponsibleFor(imu *longhorn.InstanceManagerUpgrade) bool {
	return isControllerResponsibleFor(imuc.controllerID, imuc.ds, imu.Name, imu.Spec.NodeID, imu.Status.OwnerID)
}

func (imuc *InstanceManagerUpgradeController) enqueueInstanceManagerUpgrade(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}
	imuc.queue.Add(key)
}

func (imuc *InstanceManagerUpgradeController) enqueueInstanceManagerChange(obj interface{}) {
	im, ok := obj.(*longhorn.InstanceManager)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		im, ok = deletedState.Obj.(*longhorn.InstanceManager)
		if !ok {
			return
		}
	}

	imus, err := imuc.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, imu := range imus {
		if imu.Spec.NodeID == im.Spec.NodeID {
			imuc.enqueueInstanceManagerUpgrade(imu)
		}
	}
}

func (imuc *InstanceManagerUpgradeController) enqueueVolumeChange(obj interface{}) {
	volume, ok := obj.(*longhorn.Volume)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		volume, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			return
		}
	}

	imus, err := imuc.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	for _, imu := range imus {
		if _, ok := imu.Status.Engines[volume.Name]; ok {
			imuc.enqueueInstanceManagerUpgrade(imu)
		}
	}
}

func (imuc *InstanceManagerUpgradeController) enqueueEngineChange(obj interface{}) {
	engine, ok := obj.(*longhorn.Engine)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		engine, ok = deletedState.Obj.(*longhorn.Engine)
		if !ok {
			return
		}
	}

	imus, err := imuc.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, imu := range imus {
		// Enqueue the IMU if this engine belongs to the upgrade's source node
		// or appears in the relocation plan (temporary node case).
		if imu.Spec.NodeID == engine.Spec.NodeID {
			imuc.enqueueInstanceManagerUpgrade(imu)
			continue
		}
		if _, ok := imu.Status.Engines[engine.Spec.VolumeName]; ok {
			imuc.enqueueInstanceManagerUpgrade(imu)
		}
	}
}

func (imuc *InstanceManagerUpgradeController) syncInstanceManagerUpgrade(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != imuc.namespace {
		return nil
	}

	imu, err := imuc.ds.GetInstanceManagerUpgrade(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return err
	}
	if imu == nil {
		return nil
	}

	log := getLoggerForInstanceManagerUpgrade(imuc.logger, imu)

	if !imuc.isResponsibleFor(imu) {
		return nil
	}

	if imu.Status.OwnerID != imuc.controllerID {
		imu.Status.OwnerID = imuc.controllerID
		imu, err = imuc.ds.UpdateInstanceManagerUpgradeStatus(imu)
		if err != nil {
			return err
		}
		log.Infof("InstanceManagerUpgrade got new owner %v", imuc.controllerID)
	}

	imu = imu.DeepCopy()
	existingStatus := imu.Status.DeepCopy()

	defer func() {
		if err == nil && !reflect.DeepEqual(existingStatus, &imu.Status) {
			_, err = imuc.ds.UpdateInstanceManagerUpgradeStatus(imu)
		}
	}()

	if imu.DeletionTimestamp != nil {
		imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete, "Deleting instance manager upgrade %v", imu.Name)
		return imuc.ds.RemoveFinalizerForInstanceManagerUpgrade(imu)
	}

	err = imuc.reconcileStateMachine(imu, log)
	if err != nil {
		return err
	}

	// Re-enqueue periodically to renew the lease and enforce the upgrade timeout
	// while the upgrade is in a transient state.
	if types.IsActiveInstanceManagerUpgradeState(imu.Status.State) {
		imuc.queue.AddAfter(key, instanceManagerUpgradeRequeueAfter)
	}

	return nil
}

func (imuc *InstanceManagerUpgradeController) reconcileStateMachine(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	if shouldStop, err := imuc.enforceUpgradeTimeout(imu, log); err != nil {
		return err
	} else if shouldStop {
		return nil
	}

	switch imu.Status.State {
	case "":
		imu.Status.State = longhorn.InstanceManagerUpgradeStatePending
		fallthrough

	case longhorn.InstanceManagerUpgradeStatePending:
		return imuc.reconcilePending(imu, log)

	case longhorn.InstanceManagerUpgradeStateRelocatingEngines:
		// If an abort is requested, skip relocation and begin restoring engines.
		// Note: We no longer reset StartedAt here. The single global timeout applies
		// to the entire upgrade lifecycle, providing predictable timeout behavior.
		if imuc.shouldAbort(imu) {
			reason := imuc.getIMUAbortReason(imu)
			log.Infof("Abort requested (%s), transitioning to restore engines to original positions", reason)
			imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
			if imu.Status.ErrorMsg == "" {
				imu.Status.ErrorMsg = fmt.Sprintf("upgrade aborted: %s", reason)
			}
			return nil
		}
		return imuc.reconcileRelocatingEngines(imu, log)

	case longhorn.InstanceManagerUpgradeStateWaitingForSourceIM:
		// If an abort is requested, begin restoring engines.
		// Note: We no longer reset StartedAt here. The single global timeout applies
		// to the entire upgrade lifecycle, providing predictable timeout behavior.
		if imuc.shouldAbort(imu) {
			reason := imuc.getIMUAbortReason(imu)
			log.Infof("Abort requested (%s) while waiting for source IM, transitioning to restore engines", reason)
			imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
			if imu.Status.ErrorMsg == "" {
				imu.Status.ErrorMsg = fmt.Sprintf("upgrade aborted: %s", reason)
			}
			return nil
		}
		return imuc.reconcileWaitingForSourceIM(imu, log)

	case longhorn.InstanceManagerUpgradeStateRestoringEngines:
		return imuc.reconcileRestoringEngines(imu, log)

	case longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes:
		return imuc.reconcileWaitingForHealthyVolumes(imu, log)

	case longhorn.InstanceManagerUpgradeStateCompleted, longhorn.InstanceManagerUpgradeStateFailed:
		return nil

	default:
		return fmt.Errorf("unknown instance manager upgrade state %v", imu.Status.State)
	}
}

// shouldAbort checks if the upgrade should be aborted due to controller-detected
// conditions (e.g., timeout, target image change).
func (imuc *InstanceManagerUpgradeController) shouldAbort(imu *longhorn.InstanceManagerUpgrade) bool {
	return imu.Status.AbortRequested
}

func (imuc *InstanceManagerUpgradeController) getIMUAbortReason(imu *longhorn.InstanceManagerUpgrade) string {
	if imu.Status.AbortReason != "" {
		return imu.Status.AbortReason
	}
	return "aborted"
}

func (imuc *InstanceManagerUpgradeController) enforceUpgradeTimeout(
	imu *longhorn.InstanceManagerUpgrade,
	log *logrus.Entry,
) (bool, error) {
	if !types.IsActiveInstanceManagerUpgradeState(imu.Status.State) || imu.Status.StartedAt == "" {
		return false, nil
	}

	// If already aborting (in RestoringEngines state with AbortRequested set),
	// skip timeout enforcement to allow the restore process to complete.
	// Otherwise timeout would block restore progress indefinitely.
	if imu.Status.State == longhorn.InstanceManagerUpgradeStateRestoringEngines && imu.Status.AbortRequested {
		return false, nil
	}

	startedAt, err := util.ParseTime(imu.Status.StartedAt)
	if err != nil {
		log.WithError(err).Warnf("Failed to parse StartedAt %v for IMU %v", imu.Status.StartedAt, imu.Name)
		return false, nil
	}

	// Read the timeout setting (in minutes)
	timeoutMinutes, err := imuc.ds.GetSettingAsInt(types.SettingNameV2InstanceManagerUpgradeTimeout)
	if err != nil {
		log.WithError(err).Warnf("Failed to get %v setting, using default 60 minutes", types.SettingNameV2InstanceManagerUpgradeTimeout)
		timeoutMinutes = 60
	}
	upgradeTimeout := time.Duration(timeoutMinutes) * time.Minute

	if time.Since(startedAt) <= upgradeTimeout {
		return false, nil
	}

	// Timeout detected: transition to RestoringEngines to revert engines back to original nodes.
	// We use a single global timeout - StartedAt is never reset, so this provides predictable
	// timeout behavior regardless of which state the upgrade is stuck in.
	log.Warnf("IMU %v timed out in state %v after %v, transitioning to restore engines", imu.Name, imu.Status.State, upgradeTimeout)
	imu.Status.AbortRequested = true
	imu.Status.AbortReason = "timeout"
	imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
	imu.Status.ErrorMsg = "upgrade timed out; reverting engines to original nodes"
	return true, nil
}

// reconcilePending validates the upgrade request, finds the source instance
// manager on the node, initializes the engine relocation plan, and transitions
// to the appropriate next state.
func (imuc *InstanceManagerUpgradeController) reconcilePending(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	// Validate required spec fields.
	if imu.Spec.TargetImage == "" || imu.Spec.NodeID == "" {
		imu.Status.ErrorMsg = "missing required spec fields"
		imu.Status.State = longhorn.InstanceManagerUpgradeStateFailed
		return nil
	}

	// Find the v2 AllInOne IM on this node that is not yet running the target image.
	sourceIM, err := imuc.findSourceIM(imu)
	if err != nil {
		return err
	}

	if sourceIM == nil {
		// No old IM found; check if the node already has a running IM with the target image.
		converged, err := imuc.isUpgradeAlreadyConverged(imu)
		if err != nil {
			return err
		}
		if converged {
			log.Infof("Node %v already running target image, upgrade converged", imu.Spec.NodeID)
			imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
		} else {
			log.Infof("Source IM not found on node %v, waiting for new IM with target image", imu.Spec.NodeID)
			imuc.markStartedAt(imu)
			imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForSourceIM
		}
		return nil
	}

	// If the source IM is already running the target image, we're done.
	if sourceIM.Spec.Image == imu.Spec.TargetImage && sourceIM.Status.CurrentState == longhorn.InstanceManagerStateRunning {
		log.Infof("Source IM %v already running target image, upgrade converged", sourceIM.Name)
		imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
		return nil
	}

	// Source IM exists but is not yet Running (node down, IM restarting, etc.).
	// Engines are not running either, so we cannot relocate them yet.
	// Wait here in Pending — when the IM recovers we will either find it
	// running the target image (converged) or running the source image and
	// proceed with relocation normally.
	if sourceIM.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		log.Debugf("Source IM %v is not running (state: %v), waiting before building relocation plan",
			sourceIM.Name, sourceIM.Status.CurrentState)
		return nil
	}

	plan, err := imuc.buildEngineRelocationPlan(imu)
	if err != nil {
		if errors.Is(err, errUpgradePrecondition) {
			// Recoverable: wait for preconditions (e.g. degraded volume rebuilding, no temp node yet).
			log.WithError(err).Debug("Engine relocation plan blocked by precondition, waiting")
			return nil
		}
		return err
	}

	imu.Status.Engines = plan
	imuc.markStartedAt(imu)

	if len(plan) == 0 {
		log.Infof("No engines to relocate on node %v, waiting for source IM upgrade", imu.Spec.NodeID)
		imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForSourceIM
		return nil
	}

	log.Infof("Engine relocation plan built with %d engine(s), beginning relocation", len(plan))
	imu.Status.State = longhorn.InstanceManagerUpgradeStateRelocatingEngines
	return nil
}

// findSourceIM returns the v2 AllInOne instance manager on imu.Spec.NodeID
// that is not running the target image, or nil if no such IM exists.
func (imuc *InstanceManagerUpgradeController) findSourceIM(imu *longhorn.InstanceManagerUpgrade) (*longhorn.InstanceManager, error) {
	ims, err := imuc.ds.ListInstanceManagersByNodeRO(imu.Spec.NodeID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return nil, err
	}
	for _, im := range ims {
		if im.Spec.Image != imu.Spec.TargetImage {
			return im, nil
		}
	}
	return nil, nil
}

// reconcileRelocatingEngines moves v2 engines (NVMe-oF targets) away from the
// source node to their assigned temporary nodes. The EngineFrontend (NVMe-oF
// initiator, kernel-level) deliberately stays on the source node throughout —
// it survives the IM pod restart because it runs in the kernel.
// When all engines are Running on their temporary nodes the state advances to
// WaitingForSourceIM.
func (imuc *InstanceManagerUpgradeController) reconcileRelocatingEngines(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	allRelocated := true

	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found during relocation, removing from plan", volumeName)
				imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete,
					"Volume %v deleted during relocation, removing from upgrade plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}

		// Already running on the temporary node — done for this engine.
		if volume.Status.CurrentEngineNodeID == reloc.TemporaryNodeID {
			continue
		}

		allRelocated = false

		// Volume has been directed to the temporary node but switchover is not yet complete.
		// Check whether the temporary node's IM is still healthy. If it is
		// down, re-select a new temporary node rather than waiting for timeout.
		if volume.Spec.EngineNodeID == reloc.TemporaryNodeID {
			imuc.maybeReplanVolume(imu, volumeName, reloc, volume, log)
			// IM is up — switchover is still in progress; wait.
			continue
		}

		// Volume engine has not yet been directed to the temporary node.
		// Ensure a pre-upgrade snapshot (for fast replica rebuild) is ready
		// before relocating so that, if a replica is lost during the upgrade,
		// the rebuild can use the local snapshot rather than transferring the
		// full volume data.
		snapshotReady, err := imuc.ensurePreUpgradeSnapshot(volumeName, imu, log)
		if err != nil {
			return err
		}
		if !snapshotReady {
			continue
		}

		// The EngineFrontend (NVMe-oF initiator) switchover requires that the
		// source IM to be Running — it calls it via gRPC to suspend I/O,
		// switch the target IP, and resume. If the source IM is down we must
		// wait: redirecting the engine spec would start the SPDK target on the
		// temp node but the EF could never reconnect to it, leaving the volume
		// in a worse state.
		sourceIMReady, err := imuc.ds.CheckInstanceManagersReadiness(longhorn.DataEngineTypeV2, imu.Spec.NodeID)
		if err != nil {
			log.WithError(err).Warnf("Failed to check source IM readiness before relocating volume %v, waiting", volumeName)
			continue
		}
		if !sourceIMReady {
			log.Debugf("Source IM on node %v is not ready, waiting before relocating volume %v", imu.Spec.NodeID, volumeName)
			continue
		}

		tempIMReady, err := imuc.ds.CheckInstanceManagersReadiness(longhorn.DataEngineTypeV2, reloc.TemporaryNodeID)
		if err != nil {
			log.WithError(err).Warnf("Failed to check temporary IM readiness on node %v before relocating volume %v, waiting", reloc.TemporaryNodeID, volumeName)
			continue
		}
		if !tempIMReady {
			log.Debugf("Temporary IM on node %v is not ready, waiting before relocating volume %v", reloc.TemporaryNodeID, volumeName)
			continue
		}

		log.Infof("Relocating volume %v from node %v to temporary node %v", volumeName, reloc.OriginalNodeID, reloc.TemporaryNodeID)
		volume.Spec.EngineNodeID = reloc.TemporaryNodeID
		if _, err := imuc.ds.UpdateVolume(volume); err != nil {
			return errors.Wrapf(err, "failed to update volume %v for relocation", volumeName)
		}
		imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
			"Relocating volume %v to temporary node %v", volumeName, reloc.TemporaryNodeID)
	}

	if allRelocated {
		log.Infof("All engines relocated from node %v, waiting for source IM upgrade", imu.Spec.NodeID)
		imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForSourceIM
	}

	return nil
}

// reconcileWaitingForSourceIM waits for the source node to have a running
// instance manager with the target image, then advances to RestoringEngines.
// While waiting it also monitors the temporary nodes: if a temp node's IM goes
// down, the affected engine is re-planned to a new healthy node so that I/O is
// not interrupted for the full timeout period.
func (imuc *InstanceManagerUpgradeController) reconcileWaitingForSourceIM(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	// Check temp node health for each engine. If a temp node/IM has gone down,
	// re-plan the affected volume to a new healthy node.
	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found while waiting for source IM, removing from plan", volumeName)
				imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete,
					"Volume %v deleted while waiting for source IM, removing from upgrade plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}
		imuc.maybeReplanVolume(imu, volumeName, reloc, volume, log)
	}

	ims, err := imuc.ds.ListInstanceManagersByNodeRO(imu.Spec.NodeID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return err
	}

	for _, im := range ims {
		if im.Spec.Image != imu.Spec.TargetImage {
			continue
		}
		if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
			continue
		}

		log.Infof("Source node %v now has IM %v running target image %v", imu.Spec.NodeID, im.Name, imu.Spec.TargetImage)

		if len(imu.Status.Engines) == 0 {
			imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
		} else {
			imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
		}

		return nil
	}

	log.Debugf("Waiting for node %v to have a running IM with target image %v", imu.Spec.NodeID, imu.Spec.TargetImage)
	return nil
}

// reconcileRestoringEngines moves v2 engines back to their original node after
// the source IM has been successfully upgraded. The EngineFrontend (kernel-
// level NVMe-oF initiator) has remained on the source node throughout and will
// reconnect to the engine once it is Running there again.
// Transitions to Completed when all engines are Running on the original node.
func (imuc *InstanceManagerUpgradeController) reconcileRestoringEngines(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	allRestored := true

	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found during restore, removing from plan", volumeName)
				imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete,
					"Volume %v deleted during engine restore, removing from upgrade plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}

		// Already running on the original node — done for this engine.
		// Engine is back on original node. The volume controller will handle
		// replica rebuilding; we'll verify replica health in WaitingForHealthyVolumes.
		if volume.Status.CurrentEngineNodeID == reloc.OriginalNodeID {
			continue
		}

		allRestored = false

		// Kick off restoration by updating spec.EngineNodeID if not yet done.
		if volume.Spec.EngineNodeID != reloc.OriginalNodeID {
			log.Infof("Restoring volume %v from temporary node %v back to original node %v", volumeName, reloc.TemporaryNodeID, reloc.OriginalNodeID)
			volume.Spec.EngineNodeID = reloc.OriginalNodeID
			if _, err := imuc.ds.UpdateVolume(volume); err != nil {
				return errors.Wrapf(err, "failed to update volume %v for restoration", volumeName)
			}
			imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
				"Restoring volume %v back to original node %v", volumeName, reloc.OriginalNodeID)
		}
		// else: restoration is in progress, wait for the volume controller to complete it.
	}

	if allRestored {
		// Check if this restore is due to an abort (user or controller-requested)
		if imuc.shouldAbort(imu) {
			reason := imuc.getIMUAbortReason(imu)
			log.Infof("All engines restored to original nodes after abort (%s), marking upgrade as failed", reason)
			imu.Status.State = longhorn.InstanceManagerUpgradeStateFailed
			if imu.Status.ErrorMsg == "" {
				imu.Status.ErrorMsg = fmt.Sprintf("upgrade aborted: %s", reason)
			}
		} else {
			// Normal restore after successful relocation - verify volume health before completion
			log.Infof("All engines restored to original nodes, waiting for volumes to become healthy")
			imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes
		}
	}

	return nil
}

// reconcileWaitingForHealthyVolumes waits for all relocated volumes to report a
// Robustness status of Healthy on their original node before completing the upgrade.
func (imuc *InstanceManagerUpgradeController) reconcileWaitingForHealthyVolumes(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	allHealthy := true

	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found while waiting for health, removing from plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}

		if volume.Status.CurrentEngineNodeID != reloc.OriginalNodeID {
			log.Debugf("Volume %v engine not yet on original node %v (current: %v), waiting",
				volumeName, reloc.OriginalNodeID, volume.Status.CurrentEngineNodeID)
			allHealthy = false
			break
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
			log.Debugf("Volume %v not yet healthy (robustness: %v), waiting",
				volumeName, volume.Status.Robustness)
			allHealthy = false
			break
		}

	}

	if allHealthy {
		log.Infof("All volumes are healthy on original nodes, upgrade completed")
		imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
		imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
			"Instance manager upgrade for node %v completed successfully", imu.Spec.NodeID)
	}

	return nil
}

// isUpgradeAlreadyConverged returns true when the source node already has a
// running v2 AllInOne instance manager with the target image.
func (imuc *InstanceManagerUpgradeController) isUpgradeAlreadyConverged(imu *longhorn.InstanceManagerUpgrade) (bool, error) {
	ims, err := imuc.ds.ListInstanceManagersByNodeRO(imu.Spec.NodeID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
	if err != nil {
		return false, err
	}

	for _, im := range ims {
		if im.Status.CurrentState == longhorn.InstanceManagerStateRunning && im.Spec.Image == imu.Spec.TargetImage {
			return true, nil
		}
	}
	return false, nil
}

// markStartedAt stamps the current time into StartedAt if it has not been set
// yet. This records when the upgrade transitioned out of Pending so the
// timeout can be measured from actual start rather than creation time.
func (imuc *InstanceManagerUpgradeController) markStartedAt(imu *longhorn.InstanceManagerUpgrade) {
	if imu.Status.StartedAt == "" {
		imu.Status.StartedAt = util.Now()
	}
}

// ---------------------------------------------------------------------------
// Relocation plan helpers
// ---------------------------------------------------------------------------

// buildEngineRelocationPlan scans the source node for running v2 engines
// (NVMe-oF targets) and assigns each a temporary relocation node that:
//   - is not the source node,
//   - has a healthy running replica for the volume, and
//   - has a running v2 AllInOne instance manager.
//
// The EngineFrontend (NVMe-oF initiator, kernel-level) is intentionally not
// moved — it stays on the source node and survives the IM pod restart.
//
// Returns an error if any engine is not running (upgrade pre-conditions not
// met) or if no suitable temporary node can be found for any engine.
func (imuc *InstanceManagerUpgradeController) buildEngineRelocationPlan(imu *longhorn.InstanceManagerUpgrade) (map[string]longhorn.EngineRelocation, error) {
	plan := map[string]longhorn.EngineRelocation{}

	engines, err := imuc.ds.ListEnginesByNodeRO(imu.Spec.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list engines")
	}

	for _, engine := range engines {
		if !types.IsDataEngineV2(engine.Spec.DataEngine) {
			continue
		}

		// Pre-condition: engine must be running before we can safely relocate
		// it. A non-running engine indicates a degraded volume that blocks the
		// upgrade.
		if engine.Status.CurrentState != longhorn.InstanceStateRunning {
			return nil, fmt.Errorf("%w: engine %v is not running (state: %v)",
				errUpgradePrecondition, engine.Name, engine.Status.CurrentState)
		}

		tempNode, err := imuc.selectTemporaryNode(imu.Spec.NodeID, engine.Spec.VolumeName, plan)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot find temporary node for engine %v (volume %v): %v",
				errUpgradePrecondition, engine.Name, engine.Spec.VolumeName, err)
		}

		// Use volume.Name as the key since engine names change during safe v2 switchover migrations
		plan[engine.Spec.VolumeName] = longhorn.EngineRelocation{
			OriginalNodeID:  engine.Spec.NodeID,
			TemporaryNodeID: tempNode,
		}
	}

	return plan, nil
}

// maybeReplanVolume checks whether the temporary node assigned to a volume is
// still healthy. If the temp node's IM is down, it selects a new healthy temp
// node and updates the relocation plan. The volume's Spec.EngineNodeID is updated on
// the next reconcile when the normal relocation branch detects the mismatch.
// If no new candidate is available, the volume stays on the current plan and
// waits (timeout will eventually fire).
func (imuc *InstanceManagerUpgradeController) maybeReplanVolume(
	imu *longhorn.InstanceManagerUpgrade,
	volumeName string,
	reloc longhorn.EngineRelocation,
	volume *longhorn.Volume,
	log *logrus.Entry,
) {
	imReady, err := imuc.ds.CheckInstanceManagersReadiness(longhorn.DataEngineTypeV2, reloc.TemporaryNodeID)
	if err != nil {
		log.WithError(err).Warnf("Failed to check IM readiness on temporary node %v for volume %v", reloc.TemporaryNodeID, volumeName)
		return
	}
	if imReady {
		return
	}

	newTempNode, err := imuc.selectTemporaryNode(imu.Spec.NodeID, volumeName, imu.Status.Engines)
	if err != nil || newTempNode == reloc.TemporaryNodeID {
		// No alternative temp node — revert the volume back to the original node
		// immediately rather than waiting for the configured upgrade timeout.
		if volume.Spec.EngineNodeID != reloc.OriginalNodeID {
			log.WithError(err).Warnf("No new temporary node for volume %v, reverting to original node %v", volumeName, reloc.OriginalNodeID)
			volume.Spec.EngineNodeID = reloc.OriginalNodeID
			if _, updateErr := imuc.ds.UpdateVolume(volume); updateErr != nil {
				log.WithError(updateErr).Warnf("Failed to revert volume %v to original node %v", volumeName, reloc.OriginalNodeID)
				return
			}
			imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonUpdate,
				"No temporary node available for volume %v, reverting to original node %v", volumeName, reloc.OriginalNodeID)
		}
		return
	}

	updatedReloc := reloc
	updatedReloc.TemporaryNodeID = newTempNode
	imu.Status.Engines[volumeName] = updatedReloc
	log.Infof("Re-planning volume %v: temp node %v is down, new temp node %v", volumeName, reloc.TemporaryNodeID, newTempNode)
	imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
		"Re-planning volume %v to new temporary node %v", volumeName, newTempNode)
}

// selectTemporaryNode picks a relocation target for one engine frontend. The
// chosen node must:
//  1. Have a healthy running replica for volumeName (so the frontend has local
//     or near-local replica access after the move), and
//  2. Have a running v2 AllInOne instance manager (so the frontend process can
//     actually be hosted there).
//
// Returns an error if no suitable node is found — in which case the live
// upgrade cannot proceed and the IMU transitions to Failed.
func (imuc *InstanceManagerUpgradeController) selectTemporaryNode(sourceNodeID, volumeName string, currentPlan map[string]longhorn.EngineRelocation) (string, error) {
	replicas, err := imuc.ds.ListVolumeReplicasRO(volumeName)
	if err != nil {
		return "", errors.Wrapf(err, "failed to list replicas for volume %v", volumeName)
	}

	// Collect nodes that have a healthy and currently running replica, excluding
	// the source node. Historical replica health alone is not sufficient for
	// live upgrade safety if the replica process is no longer running.
	healthyReplicaNodes := map[string]struct{}{}
	for _, r := range replicas {
		if r.Spec.NodeID == sourceNodeID || r.Spec.NodeID == "" {
			continue
		}
		if r.Status.CurrentState != longhorn.InstanceStateRunning {
			continue
		}
		if r.Spec.FailedAt != "" || r.Spec.HealthyAt == "" {
			continue
		}
		healthyReplicaNodes[r.Spec.NodeID] = struct{}{}
	}

	if len(healthyReplicaNodes) == 0 {
		return "", fmt.Errorf("no healthy replica found on nodes other than source node %v for volume %v; "+
			"single-replica or all-same-node volumes are not supported for live upgrade", sourceNodeID, volumeName)
	}

	var candidates []string
	// Among those replica nodes, find one with a running v2 AllInOne IM.
	for nodeID := range healthyReplicaNodes {
		ims, err := imuc.ds.ListInstanceManagersByNodeRO(nodeID, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)
		if err != nil {
			continue
		}
		for _, im := range ims {
			if im.Status.CurrentState == longhorn.InstanceManagerStateRunning {
				candidates = append(candidates, nodeID)
				break
			}
		}
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no temporary node with a running v2 IM found among healthy replica nodes for volume %v", volumeName)
	}

	return imuc.chooseBestTemporaryNode(candidates, currentPlan), nil
}

// chooseBestTemporaryNode selects the best node among the candidates based on the current usage
// (to balance the load evenly across all available temporary nodes).
func (imuc *InstanceManagerUpgradeController) chooseBestTemporaryNode(candidates []string, currentPlan map[string]longhorn.EngineRelocation) string {
	usage := make(map[string]int)
	for _, nodeID := range candidates {
		usage[nodeID] = 0
		// Start with the number of engines currently running on this node
		// (which represents the number of attached volumes).
		count := 0
		if engines, err := imuc.ds.ListEnginesByNodeRO(nodeID); err == nil {
			for _, e := range engines {
				if types.IsDataEngineV2(e.Spec.DataEngine) {
					count++
				}
			}
		} else {
			imuc.logger.WithError(err).Warnf("Failed to list engines for node %v when choosing best temporary node", nodeID)
		}
		usage[nodeID] = count
	}

	// Add the volumes that are already planned to be relocated to these nodes
	for _, reloc := range currentPlan {
		if _, ok := usage[reloc.TemporaryNodeID]; ok {
			usage[reloc.TemporaryNodeID]++
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if usage[candidates[i]] == usage[candidates[j]] {
			// tie-breaker: sort by node ID to ensure determinism
			return candidates[i] < candidates[j]
		}
		return usage[candidates[i]] < usage[candidates[j]]
	})

	return candidates[0]
}

// ensurePreUpgradeSnapshot creates a pre-upgrade snapshot for the volume when
// both FastReplicaRebuildEnabled and TakeSnapshotBeforeV2DataEngineUpgrade
// settings are enabled and snapshot data integrity checking is enabled for the
// volume. It returns (true, nil) when the snapshot is ready or not required,
// and (false, nil) when the snapshot has been created but its checksum has not
// yet been computed. The caller should re-check on the next reconcile cycle.
func (imuc *InstanceManagerUpgradeController) ensurePreUpgradeSnapshot(volumeName string, imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) (bool, error) {
	engine, err := imuc.ds.GetVolumeCurrentEngine(volumeName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get current engine for volume %v", volumeName)
	}

	fastReplicaRebuild, err := imuc.ds.GetSettingAsBoolByDataEngine(types.SettingNameFastReplicaRebuildEnabled, engine.Spec.DataEngine)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get %v setting for data engine %v", types.SettingNameFastReplicaRebuildEnabled, engine.Spec.DataEngine)
	}
	if !fastReplicaRebuild {
		return true, nil
	}

	takeSnapshot, err := imuc.ds.GetSettingAsBoolByDataEngine(types.SettingNameTakeSnapshotBeforeV2DataEngineUpgrade, engine.Spec.DataEngine)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get %v setting for data engine %v", types.SettingNameTakeSnapshotBeforeV2DataEngineUpgrade, engine.Spec.DataEngine)
	}
	if !takeSnapshot {
		return true, nil
	}

	dataIntegrity, err := imuc.ds.GetVolumeSnapshotDataIntegrity(volumeName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get snapshot data integrity setting for volume %v", volumeName)
	}
	if dataIntegrity != longhorn.SnapshotDataIntegrityEnabled {
		return true, nil
	}

	reloc := imu.Status.Engines[volumeName]

	// If a snapshot was already created, check if its checksum is ready.
	if reloc.SnapshotName != "" {
		snapshot, err := imuc.ds.GetSnapshot(reloc.SnapshotName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				// Snapshot was deleted externally; clear the name so we create a new one below.
				reloc.SnapshotName = ""
				imu.Status.Engines[volumeName] = reloc
			} else {
				return false, errors.Wrapf(err, "failed to get pre-upgrade snapshot %v for volume %v", reloc.SnapshotName, volumeName)
			}
		} else {
			if snapshot.Status.ReadyToUse && snapshot.Status.Checksum != "" {
				return true, nil
			}
			log.Debugf("Waiting for pre-upgrade snapshot %v of volume %v (checksum computation in progress)", reloc.SnapshotName, volumeName)
			return false, nil
		}
	}

	// No snapshot yet; create one non-blocking. The status update (SnapshotName
	// persisted in imu.Status.Engines) is written by the deferred status save in
	// syncInstanceManagerUpgrade.
	snapshotName := bsutil.GenerateName(fmt.Sprintf("upgrade-snapshot-%s", volumeName))
	snapshotCR := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotName,
		},
		Spec: longhorn.SnapshotSpec{
			Volume:         volumeName,
			CreateSnapshot: true,
		},
	}
	if _, err := imuc.ds.CreateSnapshot(snapshotCR); err != nil {
		return false, errors.Wrapf(err, "failed to create pre-upgrade snapshot for volume %v", volumeName)
	}
	reloc.SnapshotName = snapshotName
	imu.Status.Engines[volumeName] = reloc
	log.Infof("Created pre-upgrade snapshot %v for volume %v, waiting for checksum computation", snapshotName, volumeName)
	return false, nil
}
