package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
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

	"github.com/longhorn/go-common-libs/multierr"

	systembackupstore "github.com/longhorn/backupstore/systembackup"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackupTargetController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	// backup store timer map is responsible for updating the backupTarget.spec.syncRequestAt
	bsTimerMap     map[string]*BackupStoreTimer
	bsTimerMapLock *sync.RWMutex

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	proxyConnCounter util.Counter
}

type BackupStoreTimer struct {
	logger       logrus.FieldLogger
	controllerID string
	ds           *datastore.DataStore

	btName       string
	pollInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewBackupTargetController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
	proxyConnCounter util.Counter) (*BackupTargetController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	btc := &BackupTargetController{
		baseController: newBaseController("longhorn-backup-target", logger),

		namespace:    namespace,
		controllerID: controllerID,

		bsTimerMap:     map[string]*BackupStoreTimer{},
		bsTimerMapLock: &sync.RWMutex{},

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-backup-target-controller"}),

		proxyConnCounter: proxyConnCounter,
	}

	var err error
	if _, err = ds.BackupTargetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    btc.enqueueBackupTarget,
		UpdateFunc: func(old, cur interface{}) { btc.enqueueBackupTarget(cur) },
		DeleteFunc: btc.enqueueBackupTarget,
	}); err != nil {
		return nil, err
	}
	btc.cacheSyncs = append(btc.cacheSyncs, ds.BackupTargetInformer.HasSynced)

	if _, err = ds.EngineImageInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, cur interface{}) {
			oldEI := old.(*longhorn.EngineImage)
			curEI := cur.(*longhorn.EngineImage)
			if curEI.ResourceVersion == oldEI.ResourceVersion {
				// Periodic resync will send update events for all known secrets.
				// Two different versions of the same secret will always have different RVs.
				// Ref to https://github.com/kubernetes/kubernetes/blob/c8ebc8ab75a9c36453cf6fa30990fd0a277d856d/pkg/controller/deployment/deployment_controller.go#L256-L263
				return
			}
			btc.enqueueEngineImage(cur)
		},
	}, 0); err != nil {
		return nil, err
	}
	btc.cacheSyncs = append(btc.cacheSyncs, ds.EngineImageInformer.HasSynced)

	return btc, nil
}

func (btc *BackupTargetController) enqueueBackupTarget(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	btc.queue.Add(key)
}

func (btc *BackupTargetController) enqueueEngineImage(obj interface{}) {
	ei, ok := obj.(*longhorn.EngineImage)
	if !ok {
		return
	}

	defaultEngineImage, err := btc.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	// Enqueue the backup target only when the default engine image becomes ready
	if err != nil || ei.Spec.Image != defaultEngineImage || ei.Status.State != longhorn.EngineImageStateDeployed {
		return
	}

	backupTargetMap, err := btc.ds.ListBackupTargetsRO()
	if err != nil {
		return
	}
	for backupTargetName, backupTarget := range backupTargetMap {
		// Enqueue the backup target only when the backup target is not started.
		if backupTarget.Spec.PollInterval.Duration == time.Duration(0) {
			btc.queue.Add(ei.Namespace + "/" + backupTargetName)
		}
	}
}

func (btc *BackupTargetController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer btc.queue.ShutDown()

	btc.logger.Info("Starting Longhorn Backup Target controller")
	defer btc.logger.Info("Shut down Longhorn Backup Target controller")

	if !cache.WaitForNamedCacheSync(btc.name, stopCh, btc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(btc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (btc *BackupTargetController) worker() {
	for btc.processNextWorkItem() {
	}
}

func (btc *BackupTargetController) processNextWorkItem() bool {
	key, quit := btc.queue.Get()
	if quit {
		return false
	}
	defer btc.queue.Done(key)
	err := btc.syncHandler(key.(string))
	btc.handleErr(err, key)
	return true
}

func (btc *BackupTargetController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync %v", btc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != btc.namespace {
		// Not ours, skip it
		return nil
	}
	return btc.reconcile(name)
}

func (btc *BackupTargetController) handleErr(err error, key interface{}) {
	if err == nil {
		btc.queue.Forget(key)
		return
	}

	log := btc.logger.WithField("BackupTarget", key)
	if btc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn backup target")
		btc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn backup target out of the queue")
	btc.queue.Forget(key)
}

func getLoggerForBackupTarget(logger logrus.FieldLogger, backupTarget *longhorn.BackupTarget) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"url":      backupTarget.Spec.BackupTargetURL,
			"cred":     backupTarget.Spec.CredentialSecret,
			"interval": backupTarget.Spec.PollInterval.Duration,
			"name":     backupTarget.Name,
		},
	)
}

func getBackupTarget(nodeID string, backupTarget *longhorn.BackupTarget, ds *datastore.DataStore, log logrus.FieldLogger, proxyConnCounter util.Counter) (engineClientProxy engineapi.EngineClientProxy, backupTargetClient *engineapi.BackupTargetClient, err error) {
	var instanceManager *longhorn.InstanceManager
	errs := multierr.NewMultiError()
	dataEngines := ds.GetDataEngines()
	for dataEngine := range dataEngines {
		instanceManager, err = ds.GetRunningInstanceManagerByNodeRO(nodeID, dataEngine)
		if err == nil {
			break
		}
		errs.Append("errors", errors.Wrapf(err, "failed to get running instance manager for node %v and data engine %v", nodeID, dataEngine))
	}
	if instanceManager == nil {
		return nil, nil, fmt.Errorf("failed to find a running instance manager for node %v: %v", nodeID, errs.Error())
	}

	engineClientProxy, err = engineapi.NewEngineClientProxy(instanceManager, log, proxyConnCounter, ds)
	if err != nil {
		return nil, nil, err
	}

	backupTargetClient, err = newBackupTargetClientFromDefaultEngineImage(ds, backupTarget)
	if err != nil {
		engineClientProxy.Close()
		return nil, nil, err
	}

	return engineClientProxy, backupTargetClient, nil
}

func newBackupTargetClient(ds *datastore.DataStore, backupTarget *longhorn.BackupTarget, engineImage string) (backupTargetClient *engineapi.BackupTargetClient, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to get %v backup target client on %v", backupTarget.Name, engineImage)
	}()

	backupType, err := util.CheckBackupType(backupTarget.Spec.BackupTargetURL)
	if err != nil {
		return nil, err
	}

	var credential map[string]string
	if types.BackupStoreRequireCredential(backupType) {
		if backupTarget.Spec.CredentialSecret == "" {
			return nil, fmt.Errorf("could not access %s without credential secret", backupType)
		}
		credential, err = ds.GetCredentialFromSecret(backupTarget.Spec.CredentialSecret)
		if err != nil {
			return nil, err
		}
	}

	executeTimeout, err := ds.GetSettingAsInt(types.SettingNameBackupExecutionTimeout)
	if err != nil {
		return nil, err
	}
	timeout := time.Duration(executeTimeout) * time.Minute

	return engineapi.NewBackupTargetClient(engineImage, backupTarget.Spec.BackupTargetURL, credential, timeout), nil
}

func newBackupTargetClientFromDefaultEngineImage(ds *datastore.DataStore, backupTarget *longhorn.BackupTarget) (*engineapi.BackupTargetClient, error) {
	defaultEngineImage, err := ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return nil, err
	}

	return newBackupTargetClient(ds, backupTarget, defaultEngineImage)
}

func (btc *BackupTargetController) reconcile(name string) (err error) {
	backupTarget, err := btc.ds.GetBackupTarget(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForBackupTarget(btc.logger, backupTarget)

	// Every controller should do the clean up even it is not responsible for the CR
	if backupTarget.Spec.BackupTargetURL == "" {
		if err := btc.cleanUpAllMounts(backupTarget); err != nil {
			log.WithError(err).Warn("Failed to clean up all mount points")
		}
	}

	// Check the responsible node
	defaultEngineImage, err := btc.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return err
	}
	isResponsible, err := btc.isResponsibleFor(backupTarget, defaultEngineImage)
	if err != nil {
		return nil
	}
	if !isResponsible {
		return nil
	}
	if backupTarget.Status.OwnerID != btc.controllerID {
		backupTarget.Status.OwnerID = btc.controllerID
		backupTarget, err = btc.ds.UpdateBackupTargetStatus(backupTarget)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
	}

	// start a BackupStoreTimer and keep it in a map[string]*BackupStoreTimer
	stopTimer := func(backupTargetName string) {
		_, exists := btc.bsTimerMap[backupTargetName]
		if exists {
			btc.bsTimerMap[backupTargetName].Stop()
			delete(btc.bsTimerMap, backupTargetName)
		}
	}

	if !backupTarget.DeletionTimestamp.IsZero() {
		btc.bsTimerMapLock.Lock()
		defer btc.bsTimerMapLock.Unlock()

		stopTimer(backupTarget.Name)

		if err := btc.cleanUpAllBackupRelatedResources(backupTarget.Name); err != nil {
			return err
		}
		return btc.ds.RemoveFinalizerForBackupTarget(backupTarget)
	}

	btc.bsTimerMapLock.Lock()
	if backupTarget.Spec.PollInterval.Duration == time.Duration(0) ||
		(btc.bsTimerMap[name] != nil && btc.bsTimerMap[name].pollInterval != backupTarget.Spec.PollInterval.Duration) {
		stopTimer(backupTarget.Name)
	}
	if btc.bsTimerMap[name] == nil && backupTarget.Spec.PollInterval.Duration != time.Duration(0) && backupTarget.Spec.BackupTargetURL != "" {
		// Start backup store sync timer
		ctx, cancel := context.WithCancel(context.Background())
		btc.bsTimerMap[name] = &BackupStoreTimer{
			logger:       log.WithField("component", "backup-store-timer"),
			controllerID: btc.controllerID,
			ds:           btc.ds,

			btName:       name,
			pollInterval: backupTarget.Spec.PollInterval.Duration,
			ctx:          ctx,
			cancel:       cancel,
		}
		go btc.bsTimerMap[name].Start()
	}
	btc.bsTimerMapLock.Unlock()

	// Check the controller should run synchronization
	if !backupTarget.Status.LastSyncedAt.IsZero() &&
		!backupTarget.Spec.SyncRequestedAt.After(backupTarget.Status.LastSyncedAt.Time) {
		return nil
	}

	existingBackupTarget := backupTarget.DeepCopy()

	syncTime := metav1.Time{Time: time.Now().UTC()}
	syncTimeRequired := false

	defer func() {
		if err != nil {
			return
		}
		if syncTimeRequired {
			// If there is something wrong with the backup target config and Longhorn cannot launch the client,
			// lacking the credential; for example, Longhorn won't even try to connect with the remote backupstore.
			// In this case, the controller should not update `Status.LastSyncedAt`.
			backupTarget.Status.LastSyncedAt = syncTime
		}
		if reflect.DeepEqual(existingBackupTarget.Status, backupTarget.Status) {
			return
		}
		if _, err := btc.ds.UpdateBackupTargetStatus(backupTarget); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", name)
			btc.enqueueBackupTarget(backupTarget)
		}
	}()

	// clean up invalid BackupVolumes that are created during split-brain
	// https://github.com/longhorn/longhorn/issues/11154
	clusterVolumeBVMap, duplicatedBackupVolumeSet, err := btc.getClusterBVsDuplicatedBVs(backupTarget)
	if err != nil {
		return err
	}
	if err := btc.cleanupDuplicateBackupVolumeForBackupTarget(backupTarget, duplicatedBackupVolumeSet); err != nil {
		return err
	}

	if backupTarget.Spec.BackupTargetURL == "" {
		stopTimer(backupTarget.Name)

		backupTarget.Status.Available = false
		backupTarget.Status.Conditions = types.SetCondition(backupTarget.Status.Conditions,
			longhorn.BackupTargetConditionTypeUnavailable, longhorn.ConditionStatusTrue,
			longhorn.BackupTargetConditionReasonUnavailable, "backup target URL is empty")

		if err := btc.cleanUpAllBackupRelatedResources(backupTarget.Name); err != nil {
			return err
		}
		return nil
	}

	info, err := btc.getInfoFromBackupStore(backupTarget)
	if err != nil {
		backupTarget.Status.Available = false
		backupTarget.Status.Conditions = types.SetCondition(backupTarget.Status.Conditions,
			longhorn.BackupTargetConditionTypeUnavailable, longhorn.ConditionStatusTrue,
			longhorn.BackupTargetConditionReasonUnavailable, err.Error())
		log.WithError(err).Error("Failed to get info from backup store")
		return nil // Ignore error to allow status update as well as preventing enqueue
	}

	if !backupTarget.Status.Available {
		backupTarget.Status.Available = true
		backupTarget.Status.Conditions = types.SetCondition(backupTarget.Status.Conditions,
			longhorn.BackupTargetConditionTypeUnavailable, longhorn.ConditionStatusFalse,
			"", "")
		// If the controller can communicate with the remote backup target while "backupTarget.Status.Available" is "false",
		// Longhorn should update the field to "true" first rather than continuing to fetch info from the target.
		// related issue: https://github.com/longhorn/longhorn/issues/11337
		return nil
	}
	syncTimeRequired = true // Errors beyond this point are NOT backup target related.

	if err = btc.syncBackupVolume(backupTarget, info.backupStoreBackupVolumeNames, clusterVolumeBVMap, syncTime, log); err != nil {
		return err
	}

	if err = btc.syncBackupBackingImage(backupTarget, info.backupStoreBackingImageNames, syncTime, log); err != nil {
		return err
	}

	if backupTarget.Name == types.DefaultBackupTargetName {
		if err = btc.syncSystemBackup(backupTarget, info.backupStoreSystemBackups, log); err != nil {
			return err
		}
	}
	return nil
}

func (btc *BackupTargetController) cleanUpAllBackupRelatedResources(backupTargetName string) error {
	if err := btc.cleanupBackupVolumes(backupTargetName); err != nil {
		return errors.Wrap(err, "failed to clean up BackupVolumes")
	}

	if err := btc.cleanupBackupBackingImages(backupTargetName); err != nil {
		return errors.Wrap(err, "failed to clean up BackupBackingImages")
	}

	if backupTargetName == types.DefaultBackupTargetName {
		if err := btc.cleanupSystemBackups(); err != nil {
			return errors.Wrap(err, "failed to clean up SystemBackups")
		}
	}

	return nil
}

func (btc *BackupTargetController) cleanUpAllMounts(backupTarget *longhorn.BackupTarget) (err error) {
	log := getLoggerForBackupTarget(btc.logger, backupTarget)
	engineClientProxy, backupTargetClient, err := getBackupTarget(btc.controllerID, backupTarget, btc.ds, log, btc.proxyConnCounter)
	if err != nil {
		return err
	}
	defer engineClientProxy.Close()
	// cleanup mount points in instance-manager
	if err := engineClientProxy.CleanupBackupMountPoints(); err != nil {
		return err
	}
	// clean mount points in longhorn-manager
	err = backupTargetClient.BackupCleanUpAllMounts()
	return err
}

type backupStoreInfo struct {
	backupStoreBackupVolumeNames []string
	backupStoreBackingImageNames []string
	backupStoreSystemBackups     systembackupstore.SystemBackups
}

func (btc *BackupTargetController) getInfoFromBackupStore(backupTarget *longhorn.BackupTarget) (info backupStoreInfo, err error) {
	log := getLoggerForBackupTarget(btc.logger, backupTarget)

	// Initialize a backup target client
	engineClientProxy, backupTargetClient, err := getBackupTarget(btc.controllerID, backupTarget, btc.ds, log, btc.proxyConnCounter)
	if err != nil {
		return backupStoreInfo{}, errors.Wrap(err, "failed to init backup target clients")
	}
	defer engineClientProxy.Close()

	// Get required information using backup target client.
	// Get SystemBackups first to update the backup target to `available` while minimizing requests to S3.
	info.backupStoreSystemBackups, err = backupTargetClient.ListSystemBackup()
	if err != nil {
		return backupStoreInfo{}, errors.Wrapf(err, "failed to list system backups in %v", backupTargetClient.URL)
	}
	if !backupTarget.Status.Available {
		return info, nil
	}
	info.backupStoreBackupVolumeNames, err = backupTargetClient.BackupVolumeNameList()
	if err != nil {
		return backupStoreInfo{}, errors.Wrapf(err, "failed to list BackupVolumes in %v", backupTargetClient.URL)
	}
	info.backupStoreBackingImageNames, err = backupTargetClient.BackupBackingImageNameList()
	if err != nil {
		return backupStoreInfo{}, errors.Wrapf(err, "failed to list backup backing images in %v", backupTargetClient.URL)
	}

	return info, nil
}

func (btc *BackupTargetController) getClusterBVsDuplicatedBVs(backupTarget *longhorn.BackupTarget) (map[string]*longhorn.BackupVolume, sets.Set[string], error) {
	log := getLoggerForBackupTarget(btc.logger, backupTarget)
	backupTargetName := backupTarget.Name

	// Get a list of the BackupVolumes of the backup target that exist as custom resources in the cluster
	backupVolumeList, err := btc.ds.ListBackupVolumesWithBackupTargetNameRO(backupTargetName)
	if err != nil {
		return nil, nil, err
	}

	duplicateBackupVolumeSet := sets.New[string]()
	volumeBVMap := make(map[string]*longhorn.BackupVolume, len(backupVolumeList))
	for _, bv := range backupVolumeList {
		if bv.Spec.BackupTargetName == "" {
			log.WithField("backupVolume", bv.Name).Debug("spec.backupTargetName is empty")
			duplicateBackupVolumeSet.Insert(bv.Name)
			continue
		}
		if bv.Spec.VolumeName == "" {
			log.WithField("backupVolume", bv.Name).Debug("spec.volumeName is empty")
			duplicateBackupVolumeSet.Insert(bv.Name)
			continue
		}
		if bv.Spec.BackupTargetName != backupTargetName {
			log.WithField("backupVolume", bv.Name).Debugf("spec.backupTargetName %v is different from label backup-target", bv.Spec.BackupTargetName)
			duplicateBackupVolumeSet.Insert(bv.Name)
			continue
		}
		if existingBV, exists := volumeBVMap[bv.Spec.VolumeName]; exists {
			if existingBV.CreationTimestamp.Before(&bv.CreationTimestamp) {
				log.WithField("backupVolume", bv.Name).Warnf("Found duplicated BackupVolume with volume name %s", bv.Spec.VolumeName)
				duplicateBackupVolumeSet.Insert(bv.Name)
				continue
			}
			log.WithField("backupVolume", existingBV.Name).Warnf("Found duplicated BackupVolume with volume name %s", existingBV.Spec.VolumeName)
			duplicateBackupVolumeSet.Insert(existingBV.Name)
		}
		volumeBVMap[bv.Spec.VolumeName] = bv
	}

	return volumeBVMap, duplicateBackupVolumeSet, nil
}

func (btc *BackupTargetController) syncBackupVolume(backupTarget *longhorn.BackupTarget, backupStoreBackupVolumeNames []string, clusterVolumeBVMap map[string]*longhorn.BackupVolume, syncTime metav1.Time, log logrus.FieldLogger) error {
	backupStoreBackupVolumes := sets.New[string](backupStoreBackupVolumeNames...)
	clusterBackupVolumesSet := sets.New[string]()
	for _, bv := range clusterVolumeBVMap {
		clusterBackupVolumesSet.Insert(bv.Spec.VolumeName)
	}

	// TODO: add a unit test
	// Get a list of BackupVolumes that *are* in the backup target and *aren't* in the cluster
	// and create the BackupVolume CR in the cluster
	if err := btc.pullBackupVolumeFromBackupTarget(backupTarget, backupStoreBackupVolumes, clusterBackupVolumesSet, log); err != nil {
		log.WithError(err).Error("Failed to pull BackupVolumes that do not exist in the cluster")
		return err
	}

	// TODO: add a unit test
	// Get a list of BackupVolumes that *are* in the cluster and *aren't* in the backup target
	// and delete the BackupVolume CR in the cluster
	if err := btc.cleanupBackupVolumeNotExistOnBackupTarget(clusterVolumeBVMap, backupStoreBackupVolumes, clusterBackupVolumesSet, log); err != nil {
		log.WithError(err).Error("Failed to clean up BackupVolumes that do not exist on the backup target server")
		return err
	}

	// Update the BackupVolume CR spec.syncRequestAt to request the
	// backup_volume_controller to reconcile the BackupVolume CR
	errs := multierr.NewMultiError()
	for volumeName, backupVolume := range clusterVolumeBVMap {
		if !backupStoreBackupVolumes.Has(volumeName) {
			continue
		}
		backupVolume.Spec.SyncRequestedAt = syncTime
		if _, err := btc.ds.UpdateBackupVolume(backupVolume); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
			errs.Append("errors", errors.Wrapf(err, "failed to update BackupVolume %v", backupVolume.Name))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to update syncRequestedAt for BackupVolumes: %v", errs.ErrorByReason("errors"))
	}

	return nil
}

func (btc *BackupTargetController) pullBackupVolumeFromBackupTarget(backupTarget *longhorn.BackupTarget, backupStoreBackupVolumes, clusterBackupVolumesSet sets.Set[string], log logrus.FieldLogger) (err error) {
	backupVolumesToPull := backupStoreBackupVolumes.Difference(clusterBackupVolumesSet)
	if count := backupVolumesToPull.Len(); count > 0 {
		log.Infof("Found %d BackupVolumes in the backup target that do not exist in the cluster and need to be pulled", count)
	}
	for remoteVolumeName := range backupVolumesToPull {
		backupVolumeName := types.GetBackupVolumeNameFromVolumeName(remoteVolumeName, backupTarget.Name)
		backupVolume := &longhorn.BackupVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: backupVolumeName,
				Labels: map[string]string{
					types.LonghornLabelBackupTarget: backupTarget.Name,
					types.LonghornLabelBackupVolume: remoteVolumeName,
				},
				OwnerReferences: datastore.GetOwnerReferencesForBackupTarget(backupTarget),
			},
			Spec: longhorn.BackupVolumeSpec{
				BackupTargetName: backupTarget.Name,
				VolumeName:       remoteVolumeName,
			},
		}
		if _, err = btc.ds.CreateBackupVolume(backupVolume); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create BackupVolume %s/%s in the cluster from backup target %s", backupVolumeName, remoteVolumeName, backupTarget.Name)
		}
	}
	return nil
}

func (btc *BackupTargetController) cleanupBackupVolumeNotExistOnBackupTarget(clusterVolumeBVMap map[string]*longhorn.BackupVolume, backupStoreBackupVolumes, clusterBackupVolumesSet sets.Set[string], log logrus.FieldLogger) (err error) {
	backupVolumesToDelete := clusterBackupVolumesSet.Difference(backupStoreBackupVolumes)
	if count := backupVolumesToDelete.Len(); count > 0 {
		log.Infof("Found %d BackupVolumes in the backup target that do not exist in the cluster and need to be deleted from the cluster", count)
	}

	errs := multierr.NewMultiError()
	for volumeName := range backupVolumesToDelete {
		bv, exists := clusterVolumeBVMap[volumeName]
		if !exists {
			log.WithField("volume", volumeName).Warnf("Failed to find the BackupVolume CR in the cluster BackupVolume map")
			continue
		}

		backupVolumeName := bv.Name
		log.WithField("backupVolume", backupVolumeName).Info("Deleting BackupVolume not exist in backupstore")
		if err := btc.deleteBackupVolumeCROnly(backupVolumeName, log); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			errs.Append("errors", errors.Wrapf(err, "failed to only delete BackupVolume CR %s", backupVolumeName))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to delete BackupVolumes not exist in backupstore: %v", errs.ErrorByReason("errors"))
	}
	return nil
}

func (btc *BackupTargetController) deleteBackupVolumeCROnly(backupVolumeName string, log logrus.FieldLogger) error {
	if err := datastore.AddBackupVolumeDeleteCustomResourceOnlyLabel(btc.ds, backupVolumeName); err != nil {
		return errors.Wrapf(err, "failed to add label delete-custom-resource-only to BackupVolume %s", backupVolumeName)
	}
	if err := btc.ds.DeleteBackupVolume(backupVolumeName); err != nil {
		return errors.Wrapf(err, "failed to delete BackupVolume %s", backupVolumeName)
	}
	return nil
}

func (btc *BackupTargetController) cleanupDuplicateBackupVolumeForBackupTarget(backupTarget *longhorn.BackupTarget, duplicateBackupVolumesSet sets.Set[string]) (err error) {
	log := getLoggerForBackupTarget(btc.logger, backupTarget)
	if count := duplicateBackupVolumesSet.Len(); count > 0 {
		log.Infof("Found %d duplicated BackupVolume CRs for the backup target and need to be deleted from the cluster", count)
	}

	errs := multierr.NewMultiError()
	for bvName := range duplicateBackupVolumesSet {
		log.WithField("backupVolume", bvName).Info("Deleting BackupVolume that has duplicate volume name in cluster")
		if err := btc.deleteBackupVolumeCROnly(bvName, log); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			errs.Append("errors", errors.Wrapf(err, "failed to only delete BackupVolume CR %s", bvName))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to delete duplicated BackupVolumes from cluster: %v", errs.ErrorByReason("errors"))
	}
	return nil
}

func (btc *BackupTargetController) syncBackupBackingImage(backupTarget *longhorn.BackupTarget, backupStoreBackingImageNames []string, syncTime metav1.Time, log logrus.FieldLogger) error {
	backupStoreBackupBackingImages := sets.New[string](backupStoreBackingImageNames...)

	// Get a list of all the BackupVolumes that exist as custom resources in the cluster
	clusterBackupBackingImages, err := btc.ds.ListBackupBackingImagesWithBackupTargetNameRO(backupTarget.Name)
	if err != nil {
		return err
	}

	clusterBackingImageBBIMap := make(map[string]*longhorn.BackupBackingImage, len(clusterBackupBackingImages))
	clusterBackupBackingImagesSet := sets.New[string]()
	for _, bbi := range clusterBackupBackingImages {
		clusterBackupBackingImagesSet.Insert(bbi.Spec.BackingImage)
		clusterBackingImageBBIMap[bbi.Spec.BackingImage] = bbi
	}

	backupBackingImagesToPull := backupStoreBackupBackingImages.Difference(clusterBackupBackingImagesSet)
	if count := backupBackingImagesToPull.Len(); count > 0 {
		log.Infof("Found %d backup backing images in the backup target that do not exist in the cluster and need to be pulled", count)
	}
	for canonicalBackingImageName := range backupBackingImagesToPull {
		backupBackingImageName := types.GetBackupBackingImageNameFromBIName(canonicalBackingImageName)
		backupBackingImage := &longhorn.BackupBackingImage{
			ObjectMeta: metav1.ObjectMeta{
				Name: backupBackingImageName,
				Labels: map[string]string{
					types.LonghornLabelBackupTarget: backupTarget.Name,
					types.LonghornLabelBackingImage: canonicalBackingImageName,
				},
				OwnerReferences: datastore.GetOwnerReferencesForBackupTarget(backupTarget),
			},
			Spec: longhorn.BackupBackingImageSpec{
				UserCreated:      false,
				BackupTargetName: backupTarget.Name,
				BackingImage:     canonicalBackingImageName,
			},
		}
		if _, err = btc.ds.CreateBackupBackingImage(backupBackingImage); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create backup backing image %s/%s from backup target %s in the cluster", backupBackingImageName, canonicalBackingImageName, backupTarget.Name)
		}
	}

	backupBackingImagesToDelete := clusterBackupBackingImagesSet.Difference(backupStoreBackupBackingImages)
	if count := backupBackingImagesToDelete.Len(); count > 0 {
		log.Infof("Found %d backup backing images in the cluster that do not exist in the backup target and need to be deleted", count)
	}
	for backingImageName := range backupBackingImagesToDelete {
		bbi, exists := clusterBackingImageBBIMap[backingImageName]
		if !exists {
			log.WithField("backing image", backingImageName).Warnf("Failed to find the BackupBackingImage CR in the cluster backup backing image map")
			continue
		}

		backupBackingImageName := bbi.Name
		log.WithField("backupBackingImage", backupBackingImageName).Info("Deleting BackupBackingImage not exist in backupstore")
		if err = datastore.AddBackupBackingImageDeleteCustomResourceOnlyLabel(btc.ds, backupBackingImageName); err != nil {
			return errors.Wrapf(err, "failed to add label delete-custom-resource-only to BackupBackingImage %s", backupBackingImageName)
		}
		if err = btc.ds.DeleteBackupBackingImage(backupBackingImageName); err != nil {
			return errors.Wrapf(err, "failed to delete BackupBackingImage %s not exist in backupstore", backupBackingImageName)
		}
	}

	return nil
}

func (btc *BackupTargetController) syncSystemBackup(backupTarget *longhorn.BackupTarget, backupStoreSystemBackups systembackupstore.SystemBackups, log logrus.FieldLogger) error {
	clusterSystemBackups, err := btc.ds.ListSystemBackups()
	if err != nil {
		return errors.Wrap(err, "failed to list SystemBackups")
	}

	clusterReadySystemBackupNames := sets.New[string]()
	for _, systemBackup := range clusterSystemBackups {
		if systemBackup.Status.State != longhorn.SystemBackupStateReady {
			continue
		}
		clusterReadySystemBackupNames.Insert(systemBackup.Name)
	}

	backupstoreSystemBackupNames := sets.New[string](util.GetSortedKeysFromMap(backupStoreSystemBackups)...)

	// Create SystemBackup from the system backups in the backup store if not already exist in the cluster.
	addSystemBackupsToCluster := backupstoreSystemBackupNames.Difference(clusterReadySystemBackupNames)
	for name := range addSystemBackupsToCluster {
		systemBackupURI := backupStoreSystemBackups[systembackupstore.Name(name)]
		longhornVersion, _, err := parseSystemBackupURI(string(systemBackupURI))
		if err != nil {
			return errors.Wrapf(err, "failed to parse system backup URI: %v", systemBackupURI)
		}

		log.WithField("systemBackup", name).Info("Creating SystemBackup from remote backup target")
		systemBackup := &longhorn.SystemBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					// Label with the version to be used by the system-backup controller
					// to get the config from the backup target.
					types.GetVersionLabelKey(): longhornVersion,
				},
				OwnerReferences: datastore.GetOwnerReferencesForBackupTarget(backupTarget),
			},
		}
		_, err = btc.ds.CreateSystemBackup(systemBackup)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create SystemBackup %v from remote backup target", name)
		}

	}

	// Delete ready SystemBackup that doesn't exist in the backup store.
	delSystemBackupsInCluster := clusterReadySystemBackupNames.Difference(backupstoreSystemBackupNames)
	for name := range delSystemBackupsInCluster {
		log.WithField("systemBackup", name).Info("Deleting SystemBackup not exist in backupstore")
		if err = datastore.AddSystemBackupDeleteCustomResourceOnlyLabel(btc.ds, name); err != nil {
			return errors.Wrapf(err, "failed to add label delete-custom-resource-only to SystemBackup %v", name)
		}
		if err = btc.ds.DeleteSystemBackup(name); err != nil {
			return errors.Wrapf(err, "failed to delete SystemBackup %v not exist in backupstore", name)
		}
	}

	return nil
}

func (btc *BackupTargetController) isResponsibleFor(bt *longhorn.BackupTarget, defaultEngineImage string) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	isResponsible := isControllerResponsibleFor(btc.controllerID, btc.ds, bt.Name, "", bt.Status.OwnerID)

	currentOwnerEngineAvailable, err := btc.ds.CheckEngineImageReadiness(defaultEngineImage, bt.Status.OwnerID)
	if err != nil {
		return false, err
	}
	currentNodeEngineAvailable, err := btc.ds.CheckEngineImageReadiness(defaultEngineImage, btc.controllerID)
	if err != nil {
		return false, err
	}

	// Skip instance manager readiness check when the BackupTarget is being deleted.
	// During cluster uninstallation or when data engines are disabled,
	// instance-manager pods on this node might already have been removed.
	// Ref: https://github.com/longhorn/longhorn/issues/11934
	if bt.DeletionTimestamp.IsZero() {
		instanceManager, err := btc.ds.GetRunningInstanceManagerByNodeRO(btc.controllerID, "")
		if err != nil {
			return false, err
		}
		if instanceManager == nil {
			return false, errors.New("failed to get running instance manager")
		}
	}

	isPreferredOwner := currentNodeEngineAvailable && isResponsible
	continueToBeOwner := currentNodeEngineAvailable && btc.controllerID == bt.Status.OwnerID
	requiresNewOwner := currentNodeEngineAvailable && !currentOwnerEngineAvailable

	return isPreferredOwner || continueToBeOwner || requiresNewOwner, nil
}

// cleanupBackupVolumes deletes all BackupVolume CRs
func (btc *BackupTargetController) cleanupBackupVolumes(backupTargetName string) error {
	clusterBackupVolumes, err := btc.ds.ListBackupVolumesWithBackupTargetNameRO(backupTargetName)
	if err != nil {
		return err
	}

	var errs []string
	for backupVolumeName := range clusterBackupVolumes {
		if err = datastore.AddBackupVolumeDeleteCustomResourceOnlyLabel(btc.ds, backupVolumeName); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if err = btc.ds.DeleteBackupVolume(backupVolumeName); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err.Error())
			continue
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ","))
	}
	return nil
}

// cleanupBackupBackingImages deletes all BackupBackingImage CRs
func (btc *BackupTargetController) cleanupBackupBackingImages(backupTargetName string) error {
	clusterBackupBackingImages, err := btc.ds.ListBackupBackingImagesWithBackupTargetNameRO(backupTargetName)
	if err != nil {
		return err
	}

	var errs []string
	for backupBackingImageName := range clusterBackupBackingImages {
		if err = datastore.AddBackupBackingImageDeleteCustomResourceOnlyLabel(btc.ds, backupBackingImageName); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if err = btc.ds.DeleteBackupBackingImage(backupBackingImageName); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err.Error())
			continue
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ","))
	}
	return nil
}

// cleanupSystemBackups deletes all SystemBackup CRs
func (btc *BackupTargetController) cleanupSystemBackups() error {
	systemBackups, err := btc.ds.ListSystemBackups()
	if err != nil {
		return err
	}

	var errs []string
	for systemBackup := range systemBackups {
		if err = datastore.AddSystemBackupDeleteCustomResourceOnlyLabel(btc.ds, systemBackup); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if err = btc.ds.DeleteSystemBackup(systemBackup); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err.Error())
			continue
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ","))
	}
	return nil
}

// parseSystemBackupURI and return version and name.
// Ex: v1.4.0, sample-system-backup, nil = parseSystemBackupURI("backupstore/system-backups/v1.4.0/sample-system-backup")
func parseSystemBackupURI(uri string) (version, name string, err error) {
	split := strings.Split(uri, "/")
	if len(split) < 2 {
		return "", "", errors.Errorf("invalid system-backup URI: %v", uri)
	}

	return split[len(split)-2], split[len(split)-1], nil
}

func (bst *BackupStoreTimer) Start() {
	log := bst.logger.WithFields(logrus.Fields{
		"interval": bst.pollInterval,
	})
	log.Info("Starting backup store timer")

	if err := wait.PollUntilContextCancel(bst.ctx, bst.pollInterval, false, func(context.Context) (done bool, err error) {
		backupTarget, err := bst.ds.GetBackupTarget(bst.btName)
		if err != nil {
			bst.logger.WithError(err).Errorf("Failed to get %s backup target", bst.btName)
			return false, err
		}

		bst.logger.Debug("Triggering sync backup target")
		backupTarget.Spec.SyncRequestedAt = metav1.Time{Time: time.Now().UTC()}
		if _, err = bst.ds.UpdateBackupTarget(backupTarget); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
			bst.logger.WithError(err).Warn("Failed to update backup target")
		}
		return false, nil
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			log.WithError(err).Warnf("Backup store timer for backup target %v is stopped", bst.btName)
		} else {
			log.WithError(err).Errorf("Failed to sync backup target %v", bst.btName)
		}
	}

	bst.logger.Info("Stopped backup store timer")
}

func (bst *BackupStoreTimer) Stop() {
	bst.cancel()
}
