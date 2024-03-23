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

	ds.UpgradeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueUpgrade,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueUpgrade(cur) },
		DeleteFunc: uc.enqueueUpgrade,
	})
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
		if reflect.DeepEqual(existingUpgrade.Status, upgrade.Status) {
			return
		}
		if _, err := uc.ds.UpdateUpgradeStatus(upgrade); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", upgradeName)
			uc.enqueueUpgrade(upgrade)
		}
	}()

	return uc.upgrade(upgrade)
}

func (uc *UpgradeController) upgrade(upgrade *longhorn.Upgrade) error {
	if types.IsDataEngineV2(upgrade.Spec.DataEngine) {
		return uc.handleInstanceManagerImageUpgrade(upgrade)
	}

	return fmt.Errorf("unsupported data engine %v for upgrade resource %v", upgrade.Spec.DataEngine, upgrade.Name)
}

func (uc *UpgradeController) handleInstanceManagerImageUpgrade(u *longhorn.Upgrade) (err error) {
	if u.Status.State == longhorn.UpgradeStateCompleted ||
		u.Status.State == longhorn.UpgradeStateError {
		return nil
	}

	defer func() {
		if err != nil {
			u.Status.State = longhorn.UpgradeStateError
		}
	}()

	if u.Spec.NodeID == "" {
		u.Status.State = longhorn.UpgradeStateError
		return fmt.Errorf("NodeID is not assigned for upgrade resource %v", u.Name)
	}

	node, err := uc.ds.GetNode(u.Spec.NodeID)
	if err != nil {
		return errors.Wrapf(err, "failed to get node %v", u.Spec.NodeID)
	}

	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	if condition.Status != longhorn.ConditionStatusTrue {
		return fmt.Errorf("node %v is not ready", node.Name)
	}

	// Mark the node as upgrade requested
	if !node.Spec.UpgradeRequested {
		node.Spec.UpgradeRequested = true

		node, err = uc.ds.UpdateNode(node)
		if err != nil {
			return errors.Wrapf(err, "failed to update node %v for upgrade %v", node.Name, u.Name)
		}
	}

	log := uc.logger.WithFields(logrus.Fields{"nodeID": u.Spec.NodeID})

	if u.Status.Volumes == nil {
		u.Status.Volumes = map[string]*longhorn.UpgradedVolume{}
	}

	switch u.Status.State {
	case longhorn.UpgradeStateUndefined:
		log.Infof("Switching upgrade %v to %v state", u.Name, longhorn.UpgradeStateInitializing)
		u.Status.State = longhorn.UpgradeStateInitializing
	case longhorn.UpgradeStateInitializing:
		err = uc.updateVolumesForUpgrade(u)
	case longhorn.UpgradeStateUpgrading:
		err = uc.handleVolumeSuspension(u)
	}

	return err
}

func (uc *UpgradeController) updateVolumesForUpgrade(u *longhorn.Upgrade) error {
	log := uc.logger.WithFields(logrus.Fields{"nodeID": u.Spec.NodeID})

	// Retrieve engines associated with the NodeID specified in the upgrade
	engines, err := uc.ds.ListEnginesByNodeRO(u.Spec.NodeID)
	if err != nil {
		return errors.Wrapf(err, "failed to list engines for node %v", u.Spec.NodeID)
	}

	// Initialize the map to store upgrade volumes
	upgradeVolumes := make(map[string]*longhorn.UpgradedVolume)
	for _, engine := range engines {
		volume, err := uc.prepareVolumeForUpgrade(engine)
		if err != nil {
			return err
		}
		upgradeVolumes[volume.Name] = &longhorn.UpgradedVolume{NodeID: volume.Status.OwnerID}
	}

	// Update upgrade status
	log.Infof("Initiating upgrade %v to %v state", u.Name, longhorn.UpgradeStateUpgrading)
	u.Status.Volumes = upgradeVolumes
	u.Status.State = longhorn.UpgradeStateUpgrading

	return nil
}

func (uc *UpgradeController) prepareVolumeForUpgrade(engine *longhorn.Engine) (*longhorn.Volume, error) {
	volume, err := uc.ds.GetVolumeRO(engine.Spec.VolumeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume %v for engine %v: %w", engine.Spec.VolumeName, engine.Name, err)
	}

	if volume.Status.State != longhorn.VolumeStateAttached &&
		volume.Status.State != longhorn.VolumeStateDetached {
		return nil, fmt.Errorf("volume %v is not in a valid state (attached or detached)", volume.Name)
	}

	return volume, nil
}

func (uc *UpgradeController) handleVolumeSuspension(u *longhorn.Upgrade) error {
	log := uc.logger.WithFields(logrus.Fields{"nodeID": u.Spec.NodeID})

	defaultInstanceManagerImage, err := uc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		return errors.Wrapf(err, "failed to get setting %v", types.SettingNameDefaultInstanceManagerImage)
	}

	if err := uc.updateVolumesToDefaultImage(u, defaultInstanceManagerImage); err != nil {
		// Early return on failure to update any volume
		return err
	}

	if err := uc.ensureVolumesUpgraded(u, defaultInstanceManagerImage); err != nil {
		// Early return if any volume hasn't been upgraded
		return err
	}

	node, err := uc.ds.GetNode(u.Spec.NodeID)
	if err != nil {
		return errors.Wrapf(err, "failed to get node %v", u.Spec.NodeID)
	}

	if node.Spec.UpgradeRequested {
		node.Spec.UpgradeRequested = false

		node, err = uc.ds.UpdateNode(node)
		if err != nil {
			return errors.Wrapf(err, "failed to update node %v for upgrade %v", node.Name, u.Name)
		}
	}

	log.Infof("Switching upgrade %v to %v state", u.Name, longhorn.UpgradeStateCompleted)
	u.Status.State = longhorn.UpgradeStateCompleted

	return nil
}

func (uc *UpgradeController) updateVolumesToDefaultImage(u *longhorn.Upgrade, defaultImage string) error {
	allVolumesUpdated := true
	for name := range u.Status.Volumes {
		if err := uc.updateVolumeImage(name, u.Name, defaultImage); err != nil {
			uc.logger.WithError(err).Warnf("Failed to update volume %v", name)
			allVolumesUpdated = false
		}
	}

	if !allVolumesUpdated {
		return fmt.Errorf("failed to request all volumes to upgrade")
	}
	return nil
}

func (uc *UpgradeController) updateVolumeImage(volumeName, upgradeName, defaultImage string) error {
	volume, err := uc.ds.GetVolume(volumeName)
	if err != nil {
		return fmt.Errorf("failed to get volume %v for upgrade %v: %w", volumeName, upgradeName, err)
	}

	if volume.Spec.Image != defaultImage {
		volume.Spec.Image = defaultImage
		_, err := uc.ds.UpdateVolume(volume)
		if err != nil {
			return fmt.Errorf("failed to update volume %v: %w", volumeName, err)
		}
	}
	return nil
}

func (uc *UpgradeController) ensureVolumesUpgraded(u *longhorn.Upgrade, defaultImage string) error {
	for name := range u.Status.Volumes {
		volume, err := uc.ds.GetVolume(name)
		if err != nil {
			return fmt.Errorf("failed to get volume %v for upgrade %v: %w", name, u.Name, err)
		}

		if volume.Status.CurrentImage != defaultImage {
			return fmt.Errorf("volume %v is still upgrading", volume.Name)
		}
	}
	return nil
}
