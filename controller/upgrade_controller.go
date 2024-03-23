package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	upgradeControllerResyncPeriod = 5 * time.Second
)

type UpgradeController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewUpgradeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*UpgradeController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	uc := &UpgradeController{
		baseController: newBaseController("longhorn-upgrade", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-upgrade-controller"}),
	}

	ds.UpgradeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueUpgrade,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueUpgrade(cur) },
		DeleteFunc: uc.enqueueUpgrade,
	}, upgradeControllerResyncPeriod)
	uc.cacheSyncs = append(uc.cacheSyncs, ds.UpgradeInformer.HasSynced)

	return uc, nil
}

func (uc *UpgradeController) enqueueUpgrade(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	uc.queue.Add(key)
}

func (uc *UpgradeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer uc.queue.ShutDown()

	uc.logger.Info("Starting Longhorn Upgrade controller")
	defer uc.logger.Info("Shut down Longhorn Upgrade controller")

	if !cache.WaitForNamedCacheSync(uc.name, stopCh, uc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(uc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (uc *UpgradeController) worker() {
	for uc.processNextWorkItem() {
	}
}

func (uc *UpgradeController) processNextWorkItem() bool {
	key, quit := uc.queue.Get()
	if quit {
		return false
	}
	defer uc.queue.Done(key)
	err := uc.syncUpgrade(key.(string))
	uc.handleErr(err, key)
	return true
}

func (uc *UpgradeController) handleErr(err error, key interface{}) {
	if err == nil {
		uc.queue.Forget(key)
		return
	}

	log := uc.logger.WithField("upgrade", key)
	if uc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn upgrade")
		uc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn upgrade out of the queue")
	uc.queue.Forget(key)
}

func getLoggerForUpgrade(logger logrus.FieldLogger, upgrade *longhorn.Upgrade) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"upgrade": upgrade.Name,
		},
	)
}

func (uc *UpgradeController) isResponsibleFor(upgrade *longhorn.Upgrade) bool {
	return isControllerResponsibleFor(uc.controllerID, uc.ds, upgrade.Name, "", upgrade.Status.OwnerID)
}

func (uc *UpgradeController) syncUpgrade(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync upgrade %v", key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != uc.namespace {
		return nil
	}

	return uc.reconcile(name)
}

func (uc *UpgradeController) reconcile(upgradeName string) (err error) {
	upgrade, err := uc.ds.GetUpgrade(upgradeName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForUpgrade(uc.logger, upgrade)

	if !uc.isResponsibleFor(upgrade) {
		return nil
	}

	if upgrade.Status.OwnerID != uc.controllerID {
		upgrade.Status.OwnerID = uc.controllerID
		upgrade, err = uc.ds.UpdateUpgradeStatus(upgrade)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Upgrade %v got new owner %v", upgrade.Name, uc.controllerID)
	}

	if !upgrade.DeletionTimestamp.IsZero() {
		return uc.ds.RemoveFinalizerForUpgrade(upgrade)
	}

	existingUpgrade := upgrade.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingUpgrade.Status, upgrade.Status) {
			if _, err := uc.ds.UpdateUpgradeStatus(upgrade); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to conflict", upgradeName)
				uc.enqueueUpgrade(upgrade)
			}
		}
	}()

	return uc.upgrade(upgrade)
}

func (uc *UpgradeController) upgrade(upgrade *longhorn.Upgrade) error {
	if types.IsDataEngineV2(upgrade.Spec.DataEngine) {
		return uc.upgradeNodes(upgrade)
	}

	return fmt.Errorf("unsupported data engine %v for upgrade resource %v", upgrade.Spec.DataEngine, upgrade.Name)
}

func (uc *UpgradeController) pickNode(u *longhorn.Upgrade) (*longhorn.Node, error) {
	for _, nodeID := range u.Spec.Nodes {
		if _, ok := u.Status.UpgradedNodes[nodeID]; !ok {
			return uc.ds.GetNode(nodeID)
		}
	}

	return nil, nil
}

func (uc *UpgradeController) upgradeNodes(upgrade *longhorn.Upgrade) (err error) {
	if upgrade.Status.CurrentState == longhorn.UpgradeStateCompleted ||
		upgrade.Status.CurrentState == longhorn.UpgradeStateError {
		return nil
	}

	log := getLoggerForUpgrade(uc.logger, upgrade)

	defer func() {
		if err != nil {
			upgrade.Status.CurrentState = longhorn.UpgradeStateError
		}
	}()

	if len(upgrade.Spec.Nodes) == 0 {
		return fmt.Errorf("nodes are not specified in upgrade resource %v", upgrade.Name)
	}

	if len(upgrade.Status.UpgradingNodes) > 0 {
		return uc.upgradeNode(upgrade)
	}

	switch upgrade.Status.CurrentState {
	case longhorn.UpgradeStateUndefined:
		defaultInstanceManagerImage, err := uc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
		if err != nil {
			return errors.Wrapf(err, "failed to get setting %v", types.SettingNameDefaultInstanceManagerImage)
		}

		upgrade.Status.InstanceManagerImage = defaultInstanceManagerImage
		upgrade.Status.CurrentState = longhorn.UpgradeStateUpgrading
		return nil

	case longhorn.UpgradeStateUpgrading:
		node, err := uc.pickNode(upgrade)
		if err != nil {
			if apierrors.IsNotFound(err) {
				upgrade.Status.UpgradedNodes[node.Name] = &longhorn.NodeUpgradeStatus{
					State: longhorn.UpgradeStateError,
					// TODO: add error message
				}
				return nil
			}
			return errors.Wrapf(err, "failed to pick node for upgrade %v", upgrade.Name)
		}
		if node == nil {
			log.Info("No node needs to upgrade")
			upgrade.Status.CurrentState = longhorn.UpgradeStateCompleted
			return nil
		}

		if upgrade.Status.UpgradingNodes == nil {
			upgrade.Status.UpgradingNodes = map[string]*longhorn.NodeUpgradeStatus{}
		}
		if upgrade.Status.UpgradedNodes == nil {
			upgrade.Status.UpgradedNodes = map[string]*longhorn.NodeUpgradeStatus{}
		}

		upgrade.Status.UpgradingNodes[node.Name] = &longhorn.NodeUpgradeStatus{
			State:   longhorn.UpgradeStateInitializing,
			Volumes: map[string]*longhorn.VolumeUpgradeStatus{},
		}
	default:
		err = fmt.Errorf("unknown state %v for upgrade resource %v", upgrade.Status.CurrentState, upgrade.Name)
	}

	return err
}

func (uc *UpgradeController) upgradeNode(upgrade *longhorn.Upgrade) (err error) {
	// Get nodeID from the first node in the map
	nodeID := ""
	var upgradingNode *longhorn.NodeUpgradeStatus
	for nodeID = range upgrade.Status.UpgradingNodes {
		upgradingNode = upgrade.Status.UpgradingNodes[nodeID]
		break
	}

	if nodeID == "" {
		return nil
	}

	log := uc.logger.WithFields(logrus.Fields{"nodeID": nodeID})

	defer func() {
		if upgradingNode.State == longhorn.UpgradeStateCompleted ||
			upgradingNode.State == longhorn.UpgradeStateError {
			upgrade.Status.UpgradedNodes[nodeID] = upgradingNode
			delete(upgrade.Status.UpgradingNodes, nodeID)
		}
	}()

	// Check if the node is existing and ready
	node, err := uc.ds.GetNode(nodeID)
	if err != nil {
		state := longhorn.UpgradeStateError
		if apierrors.IsNotFound(err) {
			state = longhorn.UpgradeStateCompleted
		}
		upgrade.Status.UpgradedNodes[node.Name] = &longhorn.NodeUpgradeStatus{
			State:   state,
			Message: err.Error(),
		}
		return nil
	}

	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	if condition.Status != longhorn.ConditionStatusTrue {
		upgrade.Status.UpgradedNodes[node.Name] = &longhorn.NodeUpgradeStatus{
			State:   longhorn.UpgradeStateError,
			Message: fmt.Sprintf("node %v is not ready", node.Name),
		}
		return nil
	}

	// Mark the node as upgrade requested
	if !node.Spec.UpgradeRequested {
		node.Spec.UpgradeRequested = true

		node, err = uc.ds.UpdateNode(node)
		if err != nil {
			upgrade.Status.UpgradedNodes[node.Name] = &longhorn.NodeUpgradeStatus{
				State:   longhorn.UpgradeStateError,
				Message: errors.Wrapf(err, "failed to update node %v for upgrade", node.Name).Error(),
			}
			return nil
		}
	}

	switch upgradingNode.State {
	case longhorn.UpgradeStateUndefined:
		log.Infof("Switching state to %v", longhorn.UpgradeStateInitializing)

		upgradingNode.State = longhorn.UpgradeStateInitializing
		return nil

	case longhorn.UpgradeStateInitializing:
		log.Info("Collecting volumes for upgrading node")

		volumes, err := uc.collectNodeVolumes(nodeID)
		if err != nil {
			// TODO: max retry
			return nil
		}

		upgradingNode.Volumes = volumes
		upgradingNode.State = longhorn.UpgradeStateUpgrading
		return nil

	case longhorn.UpgradeStateUpgrading:
		completed, err := uc.upgradeNodeVolumes(nodeID, upgrade.Status.InstanceManagerImage, upgradingNode)
		if !completed {
			if err != nil {
				return errors.Wrapf(err, "failed to upgrade node %v volumes", nodeID)
			}
			return nil
		}

		log.Infof("Switching node to %v state", longhorn.UpgradeStateCompleted)
		upgradingNode.State = longhorn.UpgradeStateCompleted
		return nil

	case longhorn.UpgradeStateCompleted:
		return nil

	default:
		return fmt.Errorf("unknown state %v for node %v", upgradingNode.State, nodeID)
	}
}

func (uc *UpgradeController) collectNodeVolumes(nodeID string) (map[string]*longhorn.VolumeUpgradeStatus, error) {
	// Retrieve engines associated with the NodeID specified in the upgrade
	engines, err := uc.ds.ListEnginesByNodeRO(nodeID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list engines for node %v", nodeID)
	}

	// Initialize the map to store upgrade volumes
	volumes := make(map[string]*longhorn.VolumeUpgradeStatus)
	for _, engine := range engines {
		volume, err := uc.prepareVolumeForUpgrade(engine)
		if err != nil {
			return nil, err
		}
		volumes[volume.Name] = &longhorn.VolumeUpgradeStatus{
			NodeID: volume.Status.OwnerID,
		}
	}

	return volumes, nil
}

func (uc *UpgradeController) prepareVolumeForUpgrade(engine *longhorn.Engine) (*longhorn.Volume, error) {
	volume, err := uc.ds.GetVolumeRO(engine.Spec.VolumeName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get volume %v for engine %v", engine.Spec.VolumeName, engine.Name)
	}

	if volume.Status.State != longhorn.VolumeStateAttached &&
		volume.Status.State != longhorn.VolumeStateDetached {
		return nil, fmt.Errorf("volume %v is not in attached or detached", volume.Name)
	}

	return volume, nil
}

func (uc *UpgradeController) upgradeNodeVolumes(nodeID, instanceManagerImage string, upgradingNode *longhorn.NodeUpgradeStatus) (upgradeCompleted bool, err error) {
	log := uc.logger.WithFields(logrus.Fields{"nodeID": nodeID})

	err = uc.updateVolumesImage(upgradingNode, instanceManagerImage)
	if err != nil {
		// TODO: Should we retry?
		return false, err
	}

	allUpgraded := uc.checkVolumesUpgraded(upgradingNode, instanceManagerImage)
	if !allUpgraded {
		return false, nil
	}

	node, err := uc.ds.GetNode(nodeID)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Warnf("Node %v not found, so mark the upgrade as completed", nodeID)
			return true, nil
		}
		return true, errors.Wrapf(err, "failed to get node %v", nodeID)
	}

	if node.Spec.UpgradeRequested {
		node.Spec.UpgradeRequested = false

		_, err = uc.ds.UpdateNode(node)
		if err != nil {
			return true, errors.Wrapf(err, "failed to update node %v for updating upgrade requested to true", nodeID)
		}
	}

	return true, nil
}

func (uc *UpgradeController) updateVolumesImage(upgradingNode *longhorn.NodeUpgradeStatus, image string) error {
	allUpdated := true

	for name := range upgradingNode.Volumes {
		if err := uc.updateVolumeImage(name, image); err != nil {
			uc.logger.WithError(err).Warnf("Failed to update volume %v", name)
			allUpdated = false
		}
	}

	if !allUpdated {
		return fmt.Errorf("failed to request all volumes to upgrade")
	}
	return nil
}

func (uc *UpgradeController) updateVolumeImage(volumeName, image string) error {
	volume, err := uc.ds.GetVolume(volumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume %v for upgrade", volumeName)
	}

	if volume.Spec.Image != image {
		volume.Spec.Image = image
		_, err := uc.ds.UpdateVolume(volume)
		if err != nil {
			return errors.Wrapf(err, "failed to update volume %v for upgrade", volumeName)
		}
	}
	return nil
}

func (uc *UpgradeController) checkVolumesUpgraded(upgradingNode *longhorn.NodeUpgradeStatus, defaultImage string) bool {
	allUpgraded := true

	for name := range upgradingNode.Volumes {
		volume, err := uc.ds.GetVolume(name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				upgradingNode.Volumes[name].State = longhorn.UpgradeStateCompleted
			} else {
				upgradingNode.Volumes[name].State = longhorn.UpgradeStateError
			}
			upgradingNode.Volumes[name].Message = err.Error()
			continue
		}

		if volume.Status.CurrentImage == defaultImage {
			upgradingNode.Volumes[name].State = longhorn.UpgradeStateCompleted
		} else {
			upgradingNode.Volumes[name].State = longhorn.UpgradeStateUpgrading
			allUpgraded = false
		}
	}
	return allUpgraded
}
