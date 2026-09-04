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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	// imucRequeueAfter is how often the controller re-checks while a node upgrade is active.
	imucRequeueAfter = 10 * time.Second
	// imucMaxNodeRetries is the number of retry attempts after the initial node upgrade fails.
	imucMaxNodeRetries = 5
)

// InstanceManagerUpgradeControlController reconciles the singleton
// InstanceManagerUpgradeControl CR. It orchestrates a rolling live upgrade of
// v2 instance managers node by node — one node at a time.
type InstanceManagerUpgradeControlController struct {
	*baseController

	namespace    string
	controllerID string

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder
}

func NewInstanceManagerUpgradeControlController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string,
	controllerID string,
) (*InstanceManagerUpgradeControlController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	c := &InstanceManagerUpgradeControlController{
		baseController: newBaseController("longhorn-instance-manager-upgrade-control", logger),

		ds:           ds,
		namespace:    namespace,
		controllerID: controllerID,
		kubeClient:   kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(
			scheme,
			corev1.EventSource{Component: "longhorn-instance-manager-upgrade-control-controller"},
		),
	}

	var err error

	if _, err = ds.InstanceManagerUpgradeControlInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueIMUC,
		UpdateFunc: func(_, obj interface{}) { c.enqueueIMUC(obj) },
		DeleteFunc: c.enqueueIMUC,
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.InstanceManagerUpgradeControlInformer.HasSynced)

	// Re-enqueue whenever any IMU changes state.
	if _, err = ds.InstanceManagerUpgradeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueIMUCForIMU,
		UpdateFunc: func(_, obj interface{}) { c.enqueueIMUCForIMU(obj) },
		DeleteFunc: c.enqueueIMUCForIMU,
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.InstanceManagerUpgradeInformer.HasSynced)

	// Re-enqueue when an instance manager changes (e.g. new IM with target image appears).
	if _, err = ds.InstanceManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueIMUCForIM,
		UpdateFunc: func(_, obj interface{}) { c.enqueueIMUCForIM(obj) },
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.InstanceManagerInformer.HasSynced)
	c.cacheSyncs = append(c.cacheSyncs, ds.PodInformer.HasSynced)

	if _, err = ds.SettingInformer.AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: c.isResponsibleForSetting,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    c.enqueueSettingChange,
				UpdateFunc: func(_, obj interface{}) { c.enqueueSettingChange(obj) },
				DeleteFunc: c.enqueueSettingChange,
			},
		}, 0); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.SettingInformer.HasSynced)

	return c, nil
}

func (c *InstanceManagerUpgradeControlController) isResponsibleForSetting(obj interface{}) bool {
	setting, ok := obj.(*longhorn.Setting)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}
		setting, ok = deletedState.Obj.(*longhorn.Setting)
		if !ok {
			return false
		}
	}

	return types.SettingName(setting.Name) == types.SettingNameAllowV2InstanceManagerAutomaticUpgrade ||
		types.SettingName(setting.Name) == types.SettingNameV2InstanceManagerUpgradeStartTime
}

func (c *InstanceManagerUpgradeControlController) enqueueSettingChange(obj interface{}) {
	c.queue.Add(c.namespace + "/" + types.InstanceManagerUpgradeControlName)
}

func (c *InstanceManagerUpgradeControlController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting Longhorn instance manager upgrade control controller")
	defer c.logger.Info("Shut down Longhorn instance manager upgrade control controller")

	if !cache.WaitForNamedCacheSync("longhorn instance manager upgrade control", stopCh, c.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *InstanceManagerUpgradeControlController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *InstanceManagerUpgradeControlController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncIMUC(key.(string))
	c.handleErr(err, key)
	return true
}

func (c *InstanceManagerUpgradeControlController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}
	log := c.logger.WithField("instanceManagerUpgradeControl", key)
	if c.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn instance manager upgrade control")
		c.queue.AddRateLimited(key)
		return
	}
	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn instance manager upgrade control out of the queue")
	c.queue.Forget(key)
}

func (c *InstanceManagerUpgradeControlController) enqueueIMUC(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}
	c.queue.Add(key)
}

func (c *InstanceManagerUpgradeControlController) enqueueIMUCForIMU(obj interface{}) {
	if _, ok := obj.(*longhorn.InstanceManagerUpgrade); !ok {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			if _, ok = d.Obj.(*longhorn.InstanceManagerUpgrade); !ok {
				return
			}
		} else {
			return
		}
	}
	c.queue.Add(c.namespace + "/" + types.InstanceManagerUpgradeControlName)
}

func (c *InstanceManagerUpgradeControlController) enqueueIMUCForIM(obj interface{}) {
	if _, ok := obj.(*longhorn.InstanceManager); ok {
		c.queue.Add(c.namespace + "/" + types.InstanceManagerUpgradeControlName)
		return
	}
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if _, ok = d.Obj.(*longhorn.InstanceManager); ok {
			c.queue.Add(c.namespace + "/" + types.InstanceManagerUpgradeControlName)
		}
	}
}

func (c *InstanceManagerUpgradeControlController) isResponsibleFor(imuc *longhorn.InstanceManagerUpgradeControl) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, imuc.Name, "", imuc.Status.OwnerID)
}

func (c *InstanceManagerUpgradeControlController) syncIMUC(key string) (err error) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace || name != types.InstanceManagerUpgradeControlName {
		return nil
	}

	imuc, err := c.ds.GetInstanceManagerUpgradeControl(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return err
	}

	log := c.logger.WithField("instanceManagerUpgradeControl", imuc.Name)

	if !c.isResponsibleFor(imuc) {
		return nil
	}

	if imuc.Status.OwnerID != c.controllerID {
		imuc.Status.OwnerID = c.controllerID
		imuc, err = c.ds.UpdateInstanceManagerUpgradeControlStatus(imuc)
		if err != nil {
			return err
		}
		log.Infof("InstanceManagerUpgradeControl got new owner %v", c.controllerID)
	}

	imuc = imuc.DeepCopy()
	existingStatus := imuc.Status.DeepCopy()

	defer func() {
		if err == nil && !reflect.DeepEqual(existingStatus, &imuc.Status) {
			if _, updateErr := c.ds.UpdateInstanceManagerUpgradeControlStatus(imuc); updateErr != nil {
				log.WithError(updateErr).Warn("Failed to update InstanceManagerUpgradeControl status in deferred update")
				err = updateErr
			}
		}
	}()

	active, err := c.reconcile(imuc, log)
	if err != nil {
		return err
	}

	if active {
		c.queue.AddAfter(key, imucRequeueAfter)
	}

	return nil
}

// reconcile is the flat, stateless reconcile function. It returns true when
// a node upgrade is actively in progress (so the caller requeues periodically).
func (c *InstanceManagerUpgradeControlController) reconcile(imuc *longhorn.InstanceManagerUpgradeControl, log *logrus.Entry) (bool, error) {
	if imuc.Status.Nodes == nil {
		imuc.Status.Nodes = make(map[string]longhorn.NodeUpgradeInfo)
	}
	var startAt time.Time
	if imuc.Spec.StartAt != "" {
		var err error
		startAt, err = util.ParseTime(imuc.Spec.StartAt)
		if err != nil {
			return false, errors.Wrapf(err, "failed to parse startAt %v", imuc.Spec.StartAt)
		}
	}

	repairedCurrentNode, err := c.repairIMUCInvariants(imuc, log)
	if err != nil {
		return false, err
	}
	if repairedCurrentNode {
		// Preserve strict one-node-at-a-time semantics by stopping after repairing
		// the active-node bookkeeping. The next reconcile can safely resume.
		return false, nil
	}

	// --- Step 1: process the current node if one is set ---
	var active bool
	if imuc.Status.CurrentNode != "" {
		var err error
		active, err = c.processCurrentNode(imuc, log)
		if err != nil {
			return false, err
		}
	}

	// --- Step 1.5: detect and fix orphaned in-progress nodes ---
	// A node can be stuck in "in-progress" state without being the currentNode
	// if its IMU was deleted externally or if the currentNode was cleared unexpectedly.
	recoveringOrphan, err := c.recoverOrphanedNodes(imuc, log)
	if err != nil {
		return false, err
	}
	if recoveringOrphan {
		return true, nil
	}

	// --- Step 2: check for a target image change mid-cycle ---
	// Always run, whether or not a node upgrade is in progress.
	if err := c.handleTargetImageChange(imuc, log); err != nil {
		return false, err
	}

	if active {
		// Current node is still in progress; wait for the next reconcile.
		return true, nil
	}

	allowed, err := c.ds.GetSettingAsBool(types.SettingNameAllowV2InstanceManagerAutomaticUpgrade)
	if err != nil {
		return false, err
	}
	if !allowed {
		log.Debugf("Skipping next V2 instance manager upgrade because %v is disabled", types.SettingNameAllowV2InstanceManagerAutomaticUpgrade)
		return false, nil
	}

	// --- Step 3: pick the next pending node ---
	nextNode := c.pickNextPendingNode(imuc)

	// Pick nodes before checking the schedule so a new target image can reset
	// terminal state from a previous cycle.
	if imuc.Spec.StartAt != "" && !hasStartedNodeUpgrade(imuc) {
		if time.Now().Before(startAt) {
			log.Debugf("Upgrade scheduled for %v, waiting", imuc.Spec.StartAt)
			c.queue.AddAfter(c.namespace+"/"+imuc.Name, time.Until(startAt))
			return false, nil
		}
	}

	if nextNode == "" {
		return c.retryFailedNode(imuc, log)
	}

	if err := c.startNodeUpgrade(imuc, nextNode, log); err != nil {
		return false, err
	}
	return true, nil
}

// retryFailedNode starts one retry only after every pending node has had its
// initial attempt. It resumes the failed IMU so its persisted relocation and
// replica-detachment plans remain the source of truth.
func (c *InstanceManagerUpgradeControlController) retryFailedNode(
	imuc *longhorn.InstanceManagerUpgradeControl,
	log *logrus.Entry,
) (bool, error) {
	var failedNodes []string
	for nodeID, info := range imuc.Status.Nodes {
		if info.State == longhorn.NodeUpgradeStateFailed && info.RetryCount < imucMaxNodeRetries {
			failedNodes = append(failedNodes, nodeID)
		}
	}
	sort.Strings(failedNodes)

	for _, nodeID := range failedNodes {
		info := imuc.Status.Nodes[nodeID]
		if info.IMUName == "" {
			continue
		}

		imu, err := c.ds.GetInstanceManagerUpgrade(info.IMUName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Cannot retry node %v: failed IMU %v no longer exists", nodeID, info.IMUName)
				info.RetryCount = imucMaxNodeRetries
				imuc.Status.Nodes[nodeID] = info
				continue
			}
			return false, err
		}
		if imu.Status.AbortReason == instanceManagerUpgradeAbortReasonSourceNodeDeleted {
			// A deleted source node cannot host a retry. Preserve the terminal
			// failure instead of waiting for a node that will not return.
			info.RetryCount = imucMaxNodeRetries
			imuc.Status.Nodes[nodeID] = info
			continue
		}
		if imu.Spec.TargetImage != imuc.Spec.TargetImage {
			continue
		}

		imu = imu.DeepCopy()
		imu.Status.AbortRequested = false
		imu.Status.AbortReason = ""
		imu.Status.ErrorMsg = ""
		imu.Status.StartedAt = util.Now()
		if len(imu.Status.Engines) > 0 {
			// Re-enter relocation so already-relocated engines and planned replica
			// detachment are reconciled from the persisted plan.
			imu.Status.State = longhorn.InstanceManagerUpgradeStateRelocatingEngines
		} else {
			imu.Status.State = longhorn.InstanceManagerUpgradeStatePending
		}
		if _, err := c.ds.UpdateInstanceManagerUpgradeStatus(imu); err != nil {
			return false, errors.Wrapf(err, "failed to resume IMU %v for node %v", imu.Name, nodeID)
		}

		info.RetryCount++
		imuc.Status.Nodes[nodeID] = info
		if err := c.startNodeInProgress(imuc, nodeID, imu.Name); err != nil {
			return false, err
		}

		log.Warnf("Retrying node %v upgrade with IMU %v (retry %d/%d)", nodeID, imu.Name, info.RetryCount, imucMaxNodeRetries)
		c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonUpdate,
			"Retrying node %v upgrade (%d/%d)", nodeID, info.RetryCount, imucMaxNodeRetries)
		return true, nil
	}

	return false, nil
}

func (c *InstanceManagerUpgradeControlController) repairIMUCInvariants(
	imuc *longhorn.InstanceManagerUpgradeControl,
	log *logrus.Entry,
) (bool, error) {
	repairedCurrentNode := false

	if imuc.Status.CurrentNode != "" {
		currentInfo, exists := imuc.Status.Nodes[imuc.Status.CurrentNode]
		if !exists {
			log.Warnf("Current node %v is missing from upgrade status, marking it failed and clearing current assignment", imuc.Status.CurrentNode)
			imuc.Status.Nodes[imuc.Status.CurrentNode] = longhorn.NodeUpgradeInfo{
				State:       longhorn.NodeUpgradeStateFailed,
				CompletedAt: util.Now(),
				ErrorMsg:    "current node is missing from instance manager upgrade status",
			}
			imuc.Status.CurrentNode = ""
			repairedCurrentNode = true
		} else if currentInfo.State != longhorn.NodeUpgradeStateInProgress {
			log.Warnf("Current node %v has state %v instead of in-progress, clearing current assignment", imuc.Status.CurrentNode, currentInfo.State)
			imuc.Status.CurrentNode = ""
			repairedCurrentNode = true
		}
	}

	for nodeID, info := range imuc.Status.Nodes {
		if info.State != longhorn.NodeUpgradeStateInProgress {
			continue
		}
		if info.IMUName == "" {
			log.Warnf("Node %v is in-progress without an IMU name, marking it failed", nodeID)
			if err := c.markNodeFailed(imuc, nodeID, "in-progress node has no instance manager upgrade"); err != nil {
				return false, err
			}
			repairedCurrentNode = true
			continue
		}
		if nodeID != imuc.Status.CurrentNode && imuc.Status.CurrentNode != "" {
			// Keep this entry intact so recoverOrphanedNodes can mark its IMU failed.
			log.Warnf("Node %v is in-progress while current node is %v, deferring orphan recovery", nodeID, imuc.Status.CurrentNode)
			continue
		}
	}

	return repairedCurrentNode, nil
}

// resetNodeToPending clears transient attempt state and optionally preserves retries.
func (c *InstanceManagerUpgradeControlController) resetNodeToPending(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
	retryCount int,
) error {
	info, exists := imuc.Status.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("BUG: node %v not found in upgrade control status", nodeID)
	}
	c.resetNodeToPendingWithInfo(imuc, nodeID, info, retryCount)
	return nil
}

func (c *InstanceManagerUpgradeControlController) resetNodeToPendingWithInfo(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
	info longhorn.NodeUpgradeInfo,
	retryCount int,
) {
	info.State = longhorn.NodeUpgradeStatePending
	info.IMUName = ""
	info.StartedAt = ""
	info.CompletedAt = ""
	info.ErrorMsg = ""
	info.RetryCount = retryCount
	if imuc.Status.CurrentNode == nodeID {
		imuc.Status.CurrentNode = ""
	}
	imuc.Status.Nodes[nodeID] = info
}

// startNodeInProgress marks nodeID as the sole active upgrade target.
func (c *InstanceManagerUpgradeControlController) startNodeInProgress(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
	imuName string,
) error {
	info, exists := imuc.Status.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("BUG: node %v not found in upgrade control status", nodeID)
	}
	info.State = longhorn.NodeUpgradeStateInProgress
	info.IMUName = imuName
	info.StartedAt = util.Now()
	info.CompletedAt = ""
	info.ErrorMsg = ""
	imuc.Status.CurrentNode = nodeID
	imuc.Status.Nodes[nodeID] = info
	return nil
}

// markNodeCompleted marks a node as successfully completed.
func (c *InstanceManagerUpgradeControlController) markNodeCompleted(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
) error {
	info, exists := imuc.Status.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("BUG: node %v not found in upgrade control status", nodeID)
	}
	info.State = longhorn.NodeUpgradeStateCompleted
	info.CompletedAt = util.Now()
	if imuc.Status.CurrentNode == nodeID {
		imuc.Status.CurrentNode = ""
	}
	imuc.Status.Nodes[nodeID] = info
	return nil
}

// markNodeFailed marks a node as permanently failed with an error message.
func (c *InstanceManagerUpgradeControlController) markNodeFailed(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
	errorMsg string,
) error {
	info, exists := imuc.Status.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("BUG: node %v not found in upgrade control status", nodeID)
	}
	info.State = longhorn.NodeUpgradeStateFailed
	info.CompletedAt = util.Now()
	info.ErrorMsg = errorMsg
	if imuc.Status.CurrentNode == nodeID {
		imuc.Status.CurrentNode = ""
	}
	imuc.Status.Nodes[nodeID] = info
	return nil
}

// processCurrentNode manages the active IMU for imuc.Status.CurrentNode.
// Clears CurrentNode when the node reaches a terminal outcome; leaves it set while waiting.
// Returns active=true when the caller should requeue periodically.
func (c *InstanceManagerUpgradeControlController) processCurrentNode(imuc *longhorn.InstanceManagerUpgradeControl, log *logrus.Entry,
) (active bool, err error) {
	nodeID := imuc.Status.CurrentNode
	nodeInfo := imuc.Status.Nodes[nodeID]

	if nodeInfo.IMUName == "" {
		// CurrentNode is set but IMUName is empty — inconsistent state, clear it.
		log.Warnf("Node %v is set as current but has no IMU name, clearing", nodeID)
		imuc.Status.CurrentNode = ""
		return false, nil
	}

	imu, err := c.ds.GetInstanceManagerUpgrade(nodeInfo.IMUName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			log.Warnf("IMU %v for node %v was deleted externally, counting it as an upgrade failure", nodeInfo.IMUName, nodeID)
			return false, c.handleNodeFailure(imuc, nodeID, fmt.Sprintf("IMU %v was deleted externally", nodeInfo.IMUName), log)
		}
		return false, err
	}

	// Check terminal states first to ensure they are processed even after
	// timeout or target-image-change triggered an abort.
	switch imu.Status.State {
	case longhorn.InstanceManagerUpgradeStateCompleted:
		if err := c.markNodeCompleted(imuc, nodeID); err != nil {
			return false, err
		}
		log.Infof("Node %v upgrade completed", nodeID)
		c.eventRecorder.Eventf(imuc, corev1.EventTypeNormal, constant.EventReasonUpdate,
			"Node %v upgrade completed", nodeID)
		return false, nil

	case longhorn.InstanceManagerUpgradeStateFailed:
		log.Warnf("Node %v upgrade failed: %v", nodeID, imu.Status.ErrorMsg)
		if err := c.markNodeFailed(imuc, nodeID, imu.Status.ErrorMsg); err != nil {
			return false, err
		}
		c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonFailed,
			"Node %v upgrade failed: %v", nodeID, imu.Status.ErrorMsg)
		return false, nil
	}

	// For non-terminal states, only enforce target-image changes here. The IMU
	// controller owns timeout detection so there is a single writer for timeout
	// abort transitions.
	if imu.Spec.TargetImage != imuc.Spec.TargetImage {
		if !imu.Status.AbortRequested {
			log.Infof("Target image changed (%v → %v), aborting IMU %v",
				imu.Spec.TargetImage, imuc.Spec.TargetImage, imu.Name)
			imu.Status.AbortRequested = true
			imu.Status.AbortReason = "target-image-changed"
			if _, err := c.ds.UpdateInstanceManagerUpgradeStatus(imu); err != nil {
				return false, errors.Wrapf(err, "failed to set AbortRequested on IMU %v", imu.Name)
			}
		}
		// Wait for IMU to reach Failed (after restoring engines).
		return true, nil
	}

	// Still in progress.
	return true, nil
}

// handleNodeFailure marks a node as failed. Failed nodes are terminal for the
// current rolling-upgrade cycle and are not retried.
func (c *InstanceManagerUpgradeControlController) handleNodeFailure(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
	errorMsg string,
	log *logrus.Entry,
) error {
	_, exists := imuc.Status.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("BUG: node %v not found in upgrade control status", nodeID)
	}
	log.Errorf("Node %v upgrade failed: %v", nodeID, errorMsg)
	if err := c.markNodeFailed(imuc, nodeID, errorMsg); err != nil {
		return err
	}

	c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonFailed,
		"Node %v upgrade failed: %v", nodeID, errorMsg)
	return nil
}

// recoverOrphanedNodes marks nodes stuck in "in-progress" state without being
// the current node as failed. This can happen if:
// - An IMU was deleted externally while the node was being upgraded
// - The currentNode was cleared but the node state wasn't reset
// - A controller restart interrupted state transitions
func (c *InstanceManagerUpgradeControlController) recoverOrphanedNodes(
	imuc *longhorn.InstanceManagerUpgradeControl,
	log *logrus.Entry,
) (bool, error) {
	for nodeID, nodeInfo := range imuc.Status.Nodes {
		// Skip the current node (it's being actively processed)
		if nodeID == imuc.Status.CurrentNode {
			continue
		}

		// Skip nodes in terminal or pending states
		if nodeInfo.State != longhorn.NodeUpgradeStateInProgress {
			continue
		}

		// An orphaned IMU has lost its ownership relationship with the IMUC.
		// Do not attempt recovery; fail this node for the current upgrade cycle.
		log.Warnf("Node %v is in-progress but not the current node (IMU: %v), failing orphaned IMU",
			nodeID, nodeInfo.IMUName)

		if nodeInfo.IMUName == "" {
			if err := c.markNodeFailed(imuc, nodeID, "orphaned node has no instance manager upgrade"); err != nil {
				return false, err
			}
			continue
		}

		imu, err := c.ds.GetInstanceManagerUpgrade(nodeInfo.IMUName)
		if err != nil {
			if !datastore.ErrorIsNotFound(err) {
				return false, errors.Wrapf(err, "failed to get orphaned IMU %v for node %v", nodeInfo.IMUName, nodeID)
			}
			if err := c.markNodeFailed(imuc, nodeID, fmt.Sprintf("orphaned IMU %v no longer exists", nodeInfo.IMUName)); err != nil {
				return false, err
			}
			continue
		}

		if imu.Status.State == longhorn.InstanceManagerUpgradeStateCompleted {
			if err := c.markNodeCompleted(imuc, nodeID); err != nil {
				return false, err
			}
			continue
		}

		if imu.Status.State != longhorn.InstanceManagerUpgradeStateFailed {
			imu.Status.State = longhorn.InstanceManagerUpgradeStateFailed
			imu.Status.ErrorMsg = "instance manager upgrade became orphaned"
			if _, err := c.ds.UpdateInstanceManagerUpgradeStatus(imu); err != nil {
				return false, errors.Wrapf(err, "failed to mark orphaned IMU %v as failed", imu.Name)
			}
		}
		if err := c.markNodeFailed(imuc, nodeID, imu.Status.ErrorMsg); err != nil {
			return false, err
		}
		c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonUpdate,
			"Marked orphaned IMU %v for node %v as failed", imu.Name, nodeID)
	}
	return false, nil
}

// handleTargetImageChange resets pending nodes whose tracked IMU (if any) was
// for a different target image. Active-node abort is handled in processCurrentNode.
func (c *InstanceManagerUpgradeControlController) handleTargetImageChange(
	imuc *longhorn.InstanceManagerUpgradeControl,
	log *logrus.Entry,
) error {
	for nodeID, nodeInfo := range imuc.Status.Nodes {
		// Terminal nodes are now re-evaluated dynamically in pickNextPendingNode
		// by checking their actual Instance Manager.
		if nodeInfo.State == longhorn.NodeUpgradeStateCompleted || nodeInfo.State == longhorn.NodeUpgradeStateFailed {
			continue
		}
		if nodeInfo.State != longhorn.NodeUpgradeStatePending {
			continue
		}
		if nodeInfo.IMUName == "" {
			continue
		}
		imu, err := c.ds.GetInstanceManagerUpgrade(nodeInfo.IMUName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				continue
			}
			return err
		}
		if imu.Spec.TargetImage != imuc.Spec.TargetImage {
			log.Infof("Resetting pending node %v: IMU %v has stale target image %v",
				nodeID, imu.Name, imu.Spec.TargetImage)
			nodeInfo.IMUName = ""
			imuc.Status.Nodes[nodeID] = nodeInfo
		}
	}
	return nil
}

// startNodeUpgrade creates an IMU for nodeID and updates the control status.
func (c *InstanceManagerUpgradeControlController) startNodeUpgrade(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
	log *logrus.Entry,
) error {
	imuName, err := c.ensureIMUForNode(imuc.Spec.TargetImage, nodeID)
	if err != nil {
		return err
	}

	if err := c.startNodeInProgress(imuc, nodeID, imuName); err != nil {
		return err
	}

	log.Infof("Started upgrade for node %v (IMU: %v)", nodeID, imuName)
	c.eventRecorder.Eventf(imuc, corev1.EventTypeNormal, constant.EventReasonUpdate,
		"Started upgrade for node %v", nodeID)
	return nil
}

// ensureIMUForNode returns the name of an existing IMU for the node/targetImage
// pair, creating one if it does not exist.
func (c *InstanceManagerUpgradeControlController) ensureIMUForNode(targetImage, nodeID string) (string, error) {
	imus, err := c.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		return "", err
	}
	for _, imu := range imus {
		if imu.DeletionTimestamp != nil {
			continue
		}
		if imu.Spec.NodeID == nodeID && imu.Spec.TargetImage == targetImage &&
			imu.Status.State != longhorn.InstanceManagerUpgradeStateFailed &&
			imu.Status.State != longhorn.InstanceManagerUpgradeStateCompleted {
			return imu.Name, nil
		}
	}

	for attempt := 0; attempt < 3; attempt++ {
		imuName := generateIMUName(nodeID)
		imu := &longhorn.InstanceManagerUpgrade{
			ObjectMeta: metav1.ObjectMeta{
				Name:       imuName,
				Finalizers: []string{longhornFinalizerKey},
			},
			Spec: longhorn.InstanceManagerUpgradeSpec{
				NodeID:      nodeID,
				TargetImage: targetImage,
			},
		}
		if _, err := c.ds.CreateInstanceManagerUpgrade(imu); err == nil {
			return imuName, nil
		} else if apierrors.IsAlreadyExists(err) {
			existing, getErr := c.ds.GetInstanceManagerUpgrade(imuName)
			if getErr == nil && existing.DeletionTimestamp == nil &&
				existing.Spec.NodeID == nodeID && existing.Spec.TargetImage == targetImage &&
				existing.Status.State != longhorn.InstanceManagerUpgradeStateFailed &&
				existing.Status.State != longhorn.InstanceManagerUpgradeStateCompleted {
				return imuName, nil
			}
			continue
		} else {
			return "", errors.Wrapf(err, "failed to create IMU for node %v", nodeID)
		}
	}
	return "", errors.Errorf("failed to generate a unique IMU name for node %v", nodeID)
}

// pickNextPendingNode prioritizes nodes that host v2 engines, then selects
// lexicographically among remaining replica-only nodes. It discovers nodes
// from IM CRs, then checks convergence using the active pod-backed IM.
func (c *InstanceManagerUpgradeControlController) pickNextPendingNode(imuc *longhorn.InstanceManagerUpgradeControl) string {
	// Register any nodes we haven't seen yet.
	discoveredNodes := map[string]struct{}{}
	// Exclude nodes whose active IM already uses the requested image. Keep their
	// existing status unchanged so terminal upgrade history is retained.
	excludedNodes := map[string]struct{}{}
	ims, err := c.ds.ListInstanceManagersRO()
	if err != nil {
		c.logger.WithError(err).Warn("Failed to list instance managers while picking next node")
	} else {
		for _, im := range ims {
			if !types.IsDataEngineV2(im.Spec.DataEngine) || im.Spec.Type != longhorn.InstanceManagerTypeAllInOne {
				continue
			}
			discoveredNodes[im.Spec.NodeID] = struct{}{}
		}

		for nodeID := range discoveredNodes {
			activeIM, err := c.ds.GetNodeV2InstanceManagerRO(nodeID)
			if err != nil {
				if datastore.ErrorIsNotFound(err) || types.ErrorIsNotFound(err) {
					continue
				}
				c.logger.WithError(err).Warnf("Failed to get active instance manager on node %v", nodeID)
				continue
			}
			if activeIM.Spec.Image == imuc.Spec.TargetImage {
				excludedNodes[nodeID] = struct{}{}
				continue
			}

			info, tracked := imuc.Status.Nodes[nodeID]
			if !tracked {
				imuc.Status.Nodes[nodeID] = longhorn.NodeUpgradeInfo{
					State: longhorn.NodeUpgradeStatePending,
				}
			} else if info.State == longhorn.NodeUpgradeStateCompleted {
				// The active IM image is authoritative. Completed IMUs may have been
				// deleted, so do not require their historical target image to start a
				// new upgrade cycle.
				c.logger.Infof("Node %v is in state %v but its active IM is not on target image %v, resetting to pending", nodeID, info.State, imuc.Spec.TargetImage)
				if err := c.resetNodeToPending(imuc, nodeID, 0); err != nil {
					c.logger.WithError(err).Warnf("Failed to reset node %v to pending", nodeID)
					continue
				}
			} else if info.State == longhorn.NodeUpgradeStateFailed {
				// A failed IMU is retained as the retry record. When it exists for
				// the same target image, retryFailedNode must resume it and enforce
				// the retry limit. A missing record cannot be resumed, so start a
				// fresh upgrade; a different target does the same below.
				failedIMU, err := c.ds.GetInstanceManagerUpgradeRO(info.IMUName)
				if err != nil {
					if datastore.ErrorIsNotFound(err) || types.ErrorIsNotFound(err) {
						c.logger.Warnf("Failed IMU retry record %v for node %v no longer exists, resetting the node for a new upgrade", info.IMUName, nodeID)
						if err := c.resetNodeToPending(imuc, nodeID, 0); err != nil {
							c.logger.WithError(err).Warnf("Failed to reset node %v to pending", nodeID)
						}
					} else {
						c.logger.WithError(err).Warnf("Failed to get failed IMU %v for node %v", info.IMUName, nodeID)
					}
					continue
				}
				if failedIMU.Spec.TargetImage == imuc.Spec.TargetImage {
					continue
				}

				c.logger.Infof("Node %v failed for target image %v but requested target is %v, resetting to pending", nodeID, failedIMU.Spec.TargetImage, imuc.Spec.TargetImage)
				if err := c.resetNodeToPending(imuc, nodeID, 0); err != nil {
					c.logger.WithError(err).Warnf("Failed to reset node %v to pending", nodeID)
					continue
				}
			}
		}
	}

	engineNodes := map[string]struct{}{}
	engines, err := c.ds.ListEnginesRO()
	if err != nil {
		c.logger.WithError(err).Warn("Failed to list engines while prioritizing pending nodes")
	} else {
		for _, engine := range engines {
			if engine.DeletionTimestamp == nil && types.IsDataEngineV2(engine.Spec.DataEngine) && engine.Spec.NodeID != "" {
				engineNodes[engine.Spec.NodeID] = struct{}{}
			}
		}
	}

	var pendingEngineNodes, pendingReplicaOnlyNodes []string
	for nodeID, info := range imuc.Status.Nodes {
		if info.State == longhorn.NodeUpgradeStatePending {
			if _, discovered := discoveredNodes[nodeID]; !discovered {
				continue
			}
			// A Pending entry can predate image convergence. Do not create another
			// IMU for a node that already runs the requested image.
			if _, excluded := excludedNodes[nodeID]; excluded {
				continue
			}
			if _, hasEngine := engineNodes[nodeID]; hasEngine {
				pendingEngineNodes = append(pendingEngineNodes, nodeID)
			} else {
				pendingReplicaOnlyNodes = append(pendingReplicaOnlyNodes, nodeID)
			}
		}
	}
	if len(pendingEngineNodes) > 0 {
		sort.Strings(pendingEngineNodes)
		return pendingEngineNodes[0]
	}
	if len(pendingReplicaOnlyNodes) > 0 {
		sort.Strings(pendingReplicaOnlyNodes)
		return pendingReplicaOnlyNodes[0]
	}
	return ""
}

func generateIMUName(nodeID string) string {
	return util.AutoCorrectName(fmt.Sprintf("upgrade-%s-%d", nodeID, time.Now().UTC().UnixNano()), datastore.NameMaximumLength)
}

func hasStartedNodeUpgrade(imuc *longhorn.InstanceManagerUpgradeControl) bool {
	if imuc.Status.CurrentNode != "" {
		return true
	}
	for _, info := range imuc.Status.Nodes {
		if info.StartedAt != "" {
			return true
		}
	}
	return false
}
