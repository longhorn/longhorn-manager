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

type DataEngineUpgradeManagerController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	dataEngineUpgradeManagerMonitor monitor.Monitor

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewDataEngineUpgradeManagerController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*DataEngineUpgradeManagerController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	uc := &DataEngineUpgradeManagerController{
		baseController: newBaseController("longhorn-data-engine-upgrade-manager", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-data-engine-upgrade-manager-controller"}),
	}

	if _, err := ds.DataEngineUpgradeManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    uc.enqueueDataEngineUpgradeManager,
		UpdateFunc: func(old, cur interface{}) { uc.enqueueDataEngineUpgradeManager(cur) },
		DeleteFunc: uc.enqueueDataEngineUpgradeManager,
	}); err != nil {
		return nil, err
	}
	uc.cacheSyncs = append(uc.cacheSyncs, ds.DataEngineUpgradeManagerInformer.HasSynced)

	return uc, nil
}

func (uc *DataEngineUpgradeManagerController) enqueueDataEngineUpgradeManager(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	uc.queue.Add(key)
}

func (uc *DataEngineUpgradeManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer uc.queue.ShutDown()

	uc.logger.Info("Starting Longhorn DataEngineUpgradeManager controller")
	defer uc.logger.Info("Shut down Longhorn DataEngineUpgradeManager controller")

	if !cache.WaitForNamedCacheSync(uc.name, stopCh, uc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(uc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (uc *DataEngineUpgradeManagerController) worker() {
	for uc.processNextWorkItem() {
	}
}

func (uc *DataEngineUpgradeManagerController) processNextWorkItem() bool {
	key, quit := uc.queue.Get()
	if quit {
		return false
	}
	defer uc.queue.Done(key)
	err := uc.syncDataEngineUpgradeManager(key.(string))
	uc.handleErr(err, key)
	return true
}

func (uc *DataEngineUpgradeManagerController) handleErr(err error, key interface{}) {
	if err == nil {
		uc.queue.Forget(key)
		return
	}

	log := uc.logger.WithField("dataEngineUpgradeManager", key)
	if uc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn DataEngineUpgradeManager resource")
		uc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn dataEngineUpgradeManager out of the queue")
	uc.queue.Forget(key)
}

func getLoggerForDataEngineUpgradeManager(logger logrus.FieldLogger, upgradeManager *longhorn.DataEngineUpgradeManager) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"dataEngineUpgradeManager": upgradeManager.Name,
		},
	)
}

func (uc *DataEngineUpgradeManagerController) isResponsibleFor(upgrade *longhorn.DataEngineUpgradeManager) bool {
	return isControllerResponsibleFor(uc.controllerID, uc.ds, upgrade.Name, "", upgrade.Status.OwnerID)
}

func (uc *DataEngineUpgradeManagerController) syncDataEngineUpgradeManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync dataEngineUpgradeManager %v", key)
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

func (uc *DataEngineUpgradeManagerController) reconcile(upgradeManagerName string) (err error) {
	upgradeManager, err := uc.ds.GetDataEngineUpgradeManager(upgradeManagerName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForDataEngineUpgradeManager(uc.logger, upgradeManager)

	if !uc.isResponsibleFor(upgradeManager) {
		return nil
	}

	if upgradeManager.Status.OwnerID != uc.controllerID {
		upgradeManager.Status.OwnerID = uc.controllerID
		upgradeManager, err = uc.ds.UpdateDataEngineUpgradeManagerStatus(upgradeManager)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("DataEngineUpgradeManager resource %v got new owner %v", upgradeManager.Name, uc.controllerID)
	}

	if !upgradeManager.DeletionTimestamp.IsZero() {
		if uc.dataEngineUpgradeManagerMonitor != nil {
			uc.dataEngineUpgradeManagerMonitor.Close()
			uc.dataEngineUpgradeManagerMonitor = nil
		}

		return uc.ds.RemoveFinalizerForDataEngineUpgradeManager(upgradeManager)
	}

	if upgradeManager.Status.State == longhorn.UpgradeStateCompleted ||
		upgradeManager.Status.State == longhorn.UpgradeStateError {
		return nil
	}

	existingUpgradeManager := upgradeManager.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingUpgradeManager.Status, upgradeManager.Status) {
			if _, err := uc.ds.UpdateDataEngineUpgradeManagerStatus(upgradeManager); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to conflict", upgradeManagerName)
				uc.enqueueDataEngineUpgradeManager(upgradeManager)
			}
		}
	}()

	if _, err := uc.createDataEngineUpgradeManagerMonitor(upgradeManager); err != nil {
		return err
	}

	if uc.dataEngineUpgradeManagerMonitor != nil {
		data, err := uc.dataEngineUpgradeManagerMonitor.GetCollectedData()
		if err != nil {
			log.WithError(err).Error("Failed to get data from dataEngineUpgradeManager monitor")
		} else {
			status, ok := data.(*longhorn.DataEngineUpgradeManagerStatus)
			if !ok {
				log.Errorf("Failed to assert value from dataEngineUpgradeManager monitor: %v", data)
			} else {
				upgradeManager.Status.InstanceManagerImage = status.InstanceManagerImage
				upgradeManager.Status.State = status.State
				upgradeManager.Status.Message = status.Message
				upgradeManager.Status.UpgradingNode = status.UpgradingNode
				upgradeManager.Status.UpgradeNodes = make(map[string]*longhorn.UpgradeNodeStatus)
				for k, v := range status.UpgradeNodes {
					upgradeManager.Status.UpgradeNodes[k] = &longhorn.UpgradeNodeStatus{
						State:   v.State,
						Message: v.Message,
					}
				}
			}
		}
	}

	if upgradeManager.Status.State == longhorn.UpgradeStateCompleted ||
		upgradeManager.Status.State == longhorn.UpgradeStateError {
		uc.dataEngineUpgradeManagerMonitor.Close()
		uc.dataEngineUpgradeManagerMonitor = nil
	}

	return nil
}

func (uc *DataEngineUpgradeManagerController) createDataEngineUpgradeManagerMonitor(upgradeManager *longhorn.DataEngineUpgradeManager) (monitor.Monitor, error) {
	if uc.dataEngineUpgradeManagerMonitor != nil {
		return uc.dataEngineUpgradeManagerMonitor, nil
	}

	monitor, err := monitor.NewDataEngineUpgradeManagerMonitor(uc.logger, uc.ds, upgradeManager.Name, upgradeManager.Status.OwnerID, uc.enqueueDataEngineUpgradeManagerForMonitor)
	if err != nil {
		return nil, err
	}

	uc.dataEngineUpgradeManagerMonitor = monitor

	return monitor, nil
}

func (uc *DataEngineUpgradeManagerController) enqueueDataEngineUpgradeManagerForMonitor(key string) {
	uc.queue.Add(key)
}
