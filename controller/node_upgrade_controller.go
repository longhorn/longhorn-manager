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

	"github.com/longhorn/longhorn-manager/controller/monitor"
	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type NodeDataEngineUpgradeController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	nodeDataEngineUpgradeMonitor monitor.Monitor

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewNodeDataEngineUpgradeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*NodeDataEngineUpgradeController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	uc := &NodeDataEngineUpgradeController{
		baseController: newBaseController("longhorn-node-data-engine-upgrade", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-node-data-engine-upgrade-controller"}),
	}

	if _, err := ds.NodeDataEngineUpgradeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueNodeDataEngineUpgrade,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueNodeDataEngineUpgrade(cur) },
		DeleteFunc: uc.enqueueNodeDataEngineUpgrade,
	}); err != nil {
		return nil, err
	}
	uc.cacheSyncs = append(uc.cacheSyncs, ds.NodeDataEngineUpgradeInformer.HasSynced)

	return uc, nil
}

func (uc *NodeDataEngineUpgradeController) enqueueNodeDataEngineUpgrade(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	uc.queue.Add(key)
}

func (uc *NodeDataEngineUpgradeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer uc.queue.ShutDown()

	uc.logger.Info("Starting Longhorn NodeDataEngineUpgrade controller")
	defer uc.logger.Info("Shut down Longhorn NodeDataEngineUpgrade controller")

	if !cache.WaitForNamedCacheSync(uc.name, stopCh, uc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(uc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (uc *NodeDataEngineUpgradeController) worker() {
	for uc.processNextWorkItem() {
	}
}

func (uc *NodeDataEngineUpgradeController) processNextWorkItem() bool {
	key, quit := uc.queue.Get()
	if quit {
		return false
	}
	defer uc.queue.Done(key)
	err := uc.syncNodeDataEngineUpgrade(key.(string))
	uc.handleErr(err, key)
	return true
}

func (uc *NodeDataEngineUpgradeController) handleErr(err error, key interface{}) {
	if err == nil {
		uc.queue.Forget(key)
		return
	}

	log := uc.logger.WithField("nodeDataEngineUpgrade", key)
	if uc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn nodeDataEngineUpgrade resource")
		uc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn nodeDataEngineUpgrade out of the queue")
	uc.queue.Forget(key)
}

func getLoggerForNodeDataEngineUpgrade(logger logrus.FieldLogger, nodeUpgrade *longhorn.NodeDataEngineUpgrade) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"nodeDataEngineUpgrade": nodeUpgrade.Name,
		},
	)
}

func (uc *NodeDataEngineUpgradeController) isResponsibleFor(upgrade *longhorn.NodeDataEngineUpgrade) bool {
	preferredOwnerID := upgrade.Spec.NodeID

	return isControllerResponsibleFor(uc.controllerID, uc.ds, upgrade.Name, preferredOwnerID, upgrade.Status.OwnerID)
}

func (uc *NodeDataEngineUpgradeController) syncNodeDataEngineUpgrade(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync nodeDataEngineUpgrade %v", key)
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

func (uc *NodeDataEngineUpgradeController) reconcile(upgradeName string) (err error) {
	nodeUpgrade, err := uc.ds.GetNodeDataEngineUpgrade(upgradeName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForNodeDataEngineUpgrade(uc.logger, nodeUpgrade)

	if !uc.isResponsibleFor(nodeUpgrade) {
		return nil
	}

	if nodeUpgrade.Status.OwnerID != uc.controllerID {
		nodeUpgrade.Status.OwnerID = uc.controllerID
		nodeUpgrade, err = uc.ds.UpdateNodeDataEngineUpgradeStatus(nodeUpgrade)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("NodeDataEngineUpgrade resource %v got new owner %v", nodeUpgrade.Name, uc.controllerID)
	}

	if !nodeUpgrade.DeletionTimestamp.IsZero() {
		if uc.nodeDataEngineUpgradeMonitor != nil {
			uc.nodeDataEngineUpgradeMonitor.Close()
			uc.nodeDataEngineUpgradeMonitor = nil
		}

		return uc.ds.RemoveFinalizerForNodeDataEngineUpgrade(nodeUpgrade)
	}

	if nodeUpgrade.Status.State == longhorn.UpgradeStateCompleted ||
		nodeUpgrade.Status.State == longhorn.UpgradeStateError {
		return nil
	}

	existingNodeUpgrade := nodeUpgrade.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingNodeUpgrade.Status, nodeUpgrade.Status) {
			if _, err := uc.ds.UpdateNodeDataEngineUpgradeStatus(nodeUpgrade); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to conflict", upgradeName)
				uc.enqueueNodeDataEngineUpgrade(upgradeName)
			}
		}
	}()

	if _, err := uc.createNodeDataEngineUpgradeMonitor(nodeUpgrade); err != nil {
		return err
	}

	if uc.nodeDataEngineUpgradeMonitor != nil {
		uc.updateNodeDataEngineUpgradeStatus(nodeUpgrade)
	}

	if nodeUpgrade.Status.State == longhorn.UpgradeStateCompleted ||
		nodeUpgrade.Status.State == longhorn.UpgradeStateError {
		uc.updateNodeDataEngineUpgradeStatus(nodeUpgrade)
		if uc.nodeDataEngineUpgradeMonitor != nil {
			uc.nodeDataEngineUpgradeMonitor.Close()
			uc.nodeDataEngineUpgradeMonitor = nil
		}
	}

	return nil
}

func (uc *NodeDataEngineUpgradeController) updateNodeDataEngineUpgradeStatus(nodeUpgrade *longhorn.NodeDataEngineUpgrade) {
	log := getLoggerForNodeDataEngineUpgrade(uc.logger, nodeUpgrade)

	data, err := uc.nodeDataEngineUpgradeMonitor.GetCollectedData()
	if err != nil {
		log.WithError(err).Error("Failed to get collected data from nodeDataEngineUpgrade monitor")
		return
	}
	status, ok := data.(*longhorn.NodeDataEngineUpgradeStatus)
	if !ok {
		log.Errorf("Failed to assert value from nodeDataEngineUpgrade monitor: %v", data)
		return
	}

	nodeUpgrade.Status.State = status.State
	nodeUpgrade.Status.Message = status.Message
	nodeUpgrade.Status.Volumes = make(map[string]*longhorn.VolumeUpgradeStatus)
	for k, v := range status.Volumes {
		nodeUpgrade.Status.Volumes[k] = &longhorn.VolumeUpgradeStatus{
			State:   v.State,
			Message: v.Message,
		}
	}
}

func (uc *NodeDataEngineUpgradeController) createNodeDataEngineUpgradeMonitor(nodeUpgrade *longhorn.NodeDataEngineUpgrade) (monitor.Monitor, error) {
	if uc.nodeDataEngineUpgradeMonitor != nil {
		return uc.nodeDataEngineUpgradeMonitor, nil
	}

	nodeUpgradeMonitor, err := monitor.NewNodeDataEngineUpgradeMonitor(uc.logger, uc.ds, nodeUpgrade.Name, nodeUpgrade.Status.OwnerID, uc.enqueueNodeDataEngineUpgradeForMonitor)
	if err != nil {
		return nil, err
	}

	uc.nodeDataEngineUpgradeMonitor = nodeUpgradeMonitor

	return nodeUpgradeMonitor, nil
}

func (uc *NodeDataEngineUpgradeController) enqueueNodeDataEngineUpgradeForMonitor(key string) {
	uc.queue.Add(key)
}
