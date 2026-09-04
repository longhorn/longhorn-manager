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

	// imucMaxNodeRetries is the maximum number of automatic retries for a single
	// node's upgrade before the entire control CR stops attempting that node.
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

	// Respect the scheduled start time only before any node upgrade has started.
	if imuc.Spec.StartAt != "" && !hasStartedNodeUpgrade(imuc) {
		startAt, err := util.ParseTime(imuc.Spec.StartAt)
		if err != nil {
			return false, errors.Wrapf(err, "failed to parse startAt %v", imuc.Spec.StartAt)
		}
		if time.Now().Before(startAt) {
			log.Debugf("Upgrade scheduled for %v, waiting", imuc.Spec.StartAt)
			c.queue.AddAfter(c.namespace+"/"+imuc.Name, time.Until(startAt))
			return false, nil
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
	if nextNode == "" {
		// No pending nodes left — all done (some may have failed).
		return false, nil
	}

	if err := c.startNodeUpgrade(imuc, nextNode, log); err != nil {
		return false, err
	}
	return true, nil
}

func (c *InstanceManagerUpgradeControlController) repairIMUCInvariants(
	imuc *longhorn.InstanceManagerUpgradeControl,
	log *logrus.Entry,
) (bool, error) {
	repairedCurrentNode := false

	if imuc.Status.CurrentNode != "" {
		currentInfo, exists := imuc.Status.Nodes[imuc.Status.CurrentNode]
		if !exists {
			log.Warnf("Current node %v is missing from upgrade status, registering it as pending and clearing current assignment", imuc.Status.CurrentNode)
			imuc.Status.Nodes[imuc.Status.CurrentNode] = longhorn.NodeUpgradeInfo{
				State: longhorn.NodeUpgradeStatePending,
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
			log.Warnf("Node %v is in-progress without an IMU name, resetting to pending", nodeID)
			c.resetNodeToPendingWithInfo(imuc, nodeID, info, info.RetryCount)
			continue
		}
		if nodeID != imuc.Status.CurrentNode && imuc.Status.CurrentNode != "" {
			// Keep this entry intact so recoverOrphanedNodes can abort its IMU before resetting it.
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

func (c *InstanceManagerUpgradeControlController) cleanupFailedIMUsForNode(nodeID, keepIMUName string) error {
	imus, err := c.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		return err
	}

	for _, imu := range imus {
		if imu.Spec.NodeID != nodeID || imu.Name == keepIMUName {
			continue
		}
		if imu.Status.State != longhorn.InstanceManagerUpgradeStateFailed {
			continue
		}

		if err := c.ds.DeleteInstanceManagerUpgrade(imu.Name); err != nil && !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to delete failed IMU %v for node %v", imu.Name, nodeID)
		}
	}

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
		if err := c.cleanupFailedIMUsForNode(nodeID, imu.Name); err != nil {
			return false, err
		}
		log.Infof("Node %v upgrade completed", nodeID)
		c.eventRecorder.Eventf(imuc, corev1.EventTypeNormal, constant.EventReasonUpdate,
			"Node %v upgrade completed", nodeID)
		return false, nil

	case longhorn.InstanceManagerUpgradeStateFailed:
		if imu.Status.AbortReason == instanceManagerUpgradeAbortReasonSourceNodeDeleted {
			log.Warnf("Node %v source was deleted while restoring engines; skipping retry", nodeID)
			if err := c.markNodeFailed(imuc, nodeID, imu.Status.ErrorMsg); err != nil {
				return false, err
			}
			c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonFailed,
				"Node %v source was deleted while restoring engines; skipping retry", nodeID)
			return false, nil
		}

		// If the IMU failed because we explicitly aborted it (target image changed),
		// reset the node to pending without consuming a retry — the failure was
		// intentional and a new IMU with the updated target image will be created.
		if imu.Status.AbortRequested && imu.Spec.TargetImage != imuc.Spec.TargetImage {
			log.Infof("Node %v IMU %v failed due to abort (target image change), resetting without retry", nodeID, imu.Name)
			if err := c.resetNodeToPending(imuc, nodeID, 0); err != nil {
				return false, err
			}
			return false, nil
		}
		return false, c.handleNodeFailure(imuc, nodeID, imu.Status.ErrorMsg, log)
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

// handleNodeFailure decides whether to retry or mark the node as permanently failed.
// Always clears CurrentNode; the reconcile loop picks up the next action.
func (c *InstanceManagerUpgradeControlController) handleNodeFailure(
	imuc *longhorn.InstanceManagerUpgradeControl,
	nodeID string,
	errorMsg string,
	log *logrus.Entry,
) error {
	nodeInfo, exists := imuc.Status.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("BUG: node %v not found in upgrade control status", nodeID)
	}
	if nodeInfo.RetryCount < imucMaxNodeRetries {
		newRetryCount := nodeInfo.RetryCount + 1
		log.Warnf("Node %v upgrade failed with error: %v (retry %d/%d)", nodeID, errorMsg, newRetryCount, imucMaxNodeRetries)
		if err := c.resetNodeToPending(imuc, nodeID, newRetryCount); err != nil {
			return err
		}
		return nil
	}

	// Retries exhausted.
	log.Errorf("Node %v upgrade failed after %d retries: %v", nodeID, imucMaxNodeRetries, errorMsg)
	if err := c.markNodeFailed(imuc, nodeID, errorMsg); err != nil {
		return err
	}

	c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonFailed,
		"Node %v upgrade failed after %d retries: %v", nodeID, imucMaxNodeRetries, errorMsg)
	return nil
}

// recoverOrphanedNodes detects and fixes nodes stuck in "in-progress" state
// without being the currentNode. This can happen if:
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

		// Found an orphaned in-progress node; abort it before selecting another node.
		log.Warnf("Node %v is in-progress but not the current node (IMU: %v), aborting orphaned IMU before continuing",
			nodeID, nodeInfo.IMUName)

		// Try to abort the orphaned IMU to ensure clean state
		if nodeInfo.IMUName != "" {
			imu, err := c.ds.GetInstanceManagerUpgrade(nodeInfo.IMUName)
			if err != nil {
				if !datastore.ErrorIsNotFound(err) {
					return false, errors.Wrapf(err, "failed to get orphaned IMU %v for node %v", nodeInfo.IMUName, nodeID)
				}
			} else if imu.Status.State != longhorn.InstanceManagerUpgradeStateCompleted &&
				imu.Status.State != longhorn.InstanceManagerUpgradeStateFailed {
				log.Infof("Aborting orphaned IMU %v for node %v", imu.Name, nodeID)
				imu.Status.AbortRequested = true
				imu.Status.AbortReason = "orphaned-imu"
				if _, err := c.ds.UpdateInstanceManagerUpgradeStatus(imu); err != nil {
					return false, errors.Wrapf(err, "failed to abort orphaned IMU %v", imu.Name)
				}
				// Keep the aborting IMU as the current recovery operation until it is terminal.
				imuc.Status.CurrentNode = nodeID
				c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonUpdate,
					"Recovered orphaned in-progress node %v, aborting IMU %v before continuing", nodeID, nodeInfo.IMUName)
				return true, nil
			}
		}

		// Preserve retry count so we don't retry infinitely if there's a persistent issue.
		if err := c.resetNodeToPending(imuc, nodeID, nodeInfo.RetryCount); err != nil {
			return false, err
		}

		c.eventRecorder.Eventf(imuc, corev1.EventTypeWarning, constant.EventReasonUpdate,
			"Recovered orphaned in-progress node %v, resetting to pending", nodeID)
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
			} else if info.State == longhorn.NodeUpgradeStateCompleted || info.State == longhorn.NodeUpgradeStateFailed {
				// Keep failed nodes terminal unless the tracked IMU proves a new target cycle.
				if info.IMUName == "" {
					continue
				}
				trackedIMU, err := c.ds.GetInstanceManagerUpgrade(info.IMUName)
				if err != nil {
					continue
				}
				if trackedIMU.Spec.TargetImage == imuc.Spec.TargetImage {
					continue
				}
				c.logger.Infof("Node %v is in state %v but has a new target image %v, resetting to pending", nodeID, info.State, imuc.Spec.TargetImage)
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
			// If the user manually patched a failed node back to Pending,
			// reset its retry count so it actually gets attempted again.
			if info.RetryCount >= imucMaxNodeRetries {
				if err := c.resetNodeToPending(imuc, nodeID, 0); err != nil {
					c.logger.WithError(err).Warnf("Failed to reset retry count for node %v", nodeID)
					continue
				}
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
