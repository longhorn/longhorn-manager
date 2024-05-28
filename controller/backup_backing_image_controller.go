package controller

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/lasso/pkg/log"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/backupstore/backupbackingimage"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackupBackingImageController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	monitors    map[string]*engineapi.BackupBackingImageMonitor
	monitorLock sync.RWMutex

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	proxyConnCounter util.Counter
}

func NewBackupBackingImageController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
	proxyConnCounter util.Counter,
) (*BackupBackingImageController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	bc := &BackupBackingImageController{
		baseController: newBaseController("longhorn-backup-backing-image", logger),

		namespace:    namespace,
		controllerID: controllerID,

		monitors:    map[string]*engineapi.BackupBackingImageMonitor{},
		monitorLock: sync.RWMutex{},

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-backing-image-controller"}),

		proxyConnCounter: proxyConnCounter,
	}

	var err error
	if _, err = ds.BackupBackingImageInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck
		AddFunc:    bc.enqueueBackupBackingImage,
		UpdateFunc: func(old, cur interface{}) { bc.enqueueBackupBackingImage(cur) },
		DeleteFunc: bc.enqueueBackupBackingImage,
	}); err != nil {
		return nil, err
	}
	bc.cacheSyncs = append(bc.cacheSyncs, ds.BackupBackingImageInformer.HasSynced)

	return bc, nil
}

func (bc *BackupBackingImageController) enqueueBackupBackingImage(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bc.queue.Add(key)
}

func (bc *BackupBackingImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bc.queue.ShutDown()

	bc.logger.Info("Starting Longhorn Backup Backing Image controller")
	defer bc.logger.Info("Shut down Longhorn Backup Backing Image controller")

	if !cache.WaitForNamedCacheSync(bc.name, stopCh, bc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(bc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (bc *BackupBackingImageController) worker() {
	for bc.processNextWorkItem() {
	}
}

func (bc *BackupBackingImageController) processNextWorkItem() bool {
	key, quit := bc.queue.Get()
	if quit {
		return false
	}
	defer bc.queue.Done(key)
	err := bc.syncHandler(key.(string))
	bc.handleErr(err, key)
	return true
}

func (bc *BackupBackingImageController) handleErr(err error, key interface{}) {
	if err == nil {
		bc.queue.Forget(key)
		return
	}

	bc.logger.WithError(err).Errorf("Failed to sync Longhorn backup backing image %v", key)
	bc.queue.AddRateLimited(key)
}

func (bc *BackupBackingImageController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync backup backing image %v", bc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bc.namespace {
		return nil
	}
	return bc.reconcile(name)
}

func getLoggerForBackupBackingImage(logger logrus.FieldLogger, backupbackingimage *longhorn.BackupBackingImage) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"backupbackingimage": backupbackingimage.Name,
		},
	)
}

func (bc *BackupBackingImageController) reconcile(backupBackingImageName string) (err error) {
	bbi, err := bc.ds.GetBackupBackingImage(backupBackingImageName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !bc.isResponsibleFor(bbi) {
		return nil
	}
	if bbi.Status.OwnerID != bc.controllerID {
		bbi.Status.OwnerID = bc.controllerID
		bbi, err = bc.ds.UpdateBackupBackingImageStatus(bbi)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Backup backing image got new owner %v", bc.controllerID)
	}

	log := getLoggerForBackupBackingImage(bc.logger, bbi)

	// Get default backup target
	backupTarget, err := bc.ds.GetBackupTargetRO(types.DefaultBackupTargetName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to get the backup target %v", types.DefaultBackupTargetName)
	}

	// Examine DeletionTimestamp to determine if object is under deletion
	if !bbi.DeletionTimestamp.IsZero() {
		if backupTarget.Spec.BackupTargetURL != "" {
			backupTargetClient, err := newBackupTargetClientFromDefaultEngineImage(bc.ds, backupTarget)
			if err != nil {
				log.WithError(err).Warn("Failed to init backup target clients")
				return nil // Ignore error to prevent enqueue
			}

			backupURL := backupbackingimage.EncodeBackupBackingImageURL(bbi.Name, backupTargetClient.URL)

			log.Infof("Deleting backup backing image %v", bbi.Name)
			if err := backupTargetClient.BackupBackingImageDelete(backupURL); err != nil {
				return errors.Wrap(err, "failed to delete remote backup backing image")
			}
		}

		// Disable monitor regardless of backup state
		bc.disableBackupMonitor(bbi.Name)

		if bbi.Status.State == longhorn.BackupStateError || bbi.Status.State == longhorn.BackupStateUnknown {
			bc.eventRecorder.Eventf(bbi, corev1.EventTypeWarning, string(bbi.Status.State), "Failed backup backing image %s has been deleted: %s", bbi.Name, bbi.Status.Error)
		}

		return bc.ds.RemoveFinalizerForBackupBackingImage(bbi)
	}

	syncTime := metav1.Time{Time: time.Now().UTC()}
	existingBackupBackingImage := bbi.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingBackupBackingImage.Status, bbi.Status) {
			return
		}
		if _, err := bc.ds.UpdateBackupBackingImageStatus(bbi); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", backupBackingImageName)
			bc.enqueueBackupBackingImage(bbi)
			return
		}
	}()

	if bbi.Status.LastSyncedAt.IsZero() && bbi.Spec.UserCreated && bc.backupNotInFinalState(bbi) {
		backingImage, err := bc.ds.GetBackingImage(backupBackingImageName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			err = fmt.Errorf("Cannot find the corresponding backing image: %v", err)
			log.WithError(err).Error()
			bbi.Status.Error = err.Error()
			bbi.Status.State = longhorn.BackupStateError
			bbi.Status.LastSyncedAt = syncTime
			return nil // Ignore error to prevent enqueue
		}

		bbi.Status.BackingImage = backingImage.Name
		bbi.Status.Size = backingImage.Status.Size
		bbi.Status.Checksum = backingImage.Status.Checksum

		monitor, err := bc.checkMonitor(bbi, backingImage, backupTarget)
		if err != nil {
			if bbi.Status.State == longhorn.BackupStateError {
				log.WithError(err).Warnf("Failed to enable the backup monitor for backup backing image %v", bbi.Name)
				return nil
			}
			return err
		}

		if err = bc.syncWithMonitor(bbi, backingImage, monitor); err != nil {
			return err
		}

		switch bbi.Status.State {
		case longhorn.BackupStateNew, longhorn.BackupStatePending, longhorn.BackupStateInProgress:
			return nil
		case longhorn.BackupStateCompleted:
			bc.disableBackupMonitor(bbi.Name)
		case longhorn.BackupStateError, longhorn.BackupStateUnknown:
			bbi.Status.LastSyncedAt = syncTime
			bc.disableBackupMonitor(bbi.Name)
			return nil
		}
	}

	// The backup config had synced
	if !bbi.Status.LastSyncedAt.IsZero() &&
		!bbi.Spec.SyncRequestedAt.After(bbi.Status.LastSyncedAt.Time) {
		return nil
	}

	// The backup creation is complete, then the source of truth becomes the remote backup target
	backupTargetClient, err := newBackupTargetClientFromDefaultEngineImage(bc.ds, backupTarget)
	if err != nil {
		log.WithError(err).Error("Error init backup target clients")
		return nil // Ignore error to prevent enqueue
	}

	backupURL := backupbackingimage.EncodeBackupBackingImageURL(backupBackingImageName, backupTargetClient.URL)
	backupBackingImageInfo, err := backupTargetClient.BackupBackingImageGet(backupURL)
	if err != nil {
		if !strings.Contains(err.Error(), "in progress") {
			log.WithError(err).Error("Error inspecting backup config")
		}
		return nil // Ignore error to prevent enqueue
	}
	if backupBackingImageInfo == nil {
		log.Warn("Backup backing image info is nil")
		return nil
	}

	// Update BackupBackingImage CR status
	bbi.Status.State = longhorn.BackupStateCompleted
	bbi.Status.BackingImage = backupBackingImageInfo.Name
	bbi.Status.URL = backupBackingImageInfo.URL
	bbi.Status.BackupCreatedAt = backupBackingImageInfo.CompleteAt
	bbi.Status.Checksum = backupBackingImageInfo.Checksum
	bbi.Status.Size = backupBackingImageInfo.Size
	bbi.Status.Labels = backupBackingImageInfo.Labels
	bbi.Status.CompressionMethod = longhorn.BackupCompressionMethod(backupBackingImageInfo.CompressionMethod)
	bbi.Status.LastSyncedAt = syncTime
	return nil
}

func (bc *BackupBackingImageController) checkMonitor(bbi *longhorn.BackupBackingImage, backingImage *longhorn.BackingImage, backupTarget *longhorn.BackupTarget) (*engineapi.BackupBackingImageMonitor, error) {
	if bbi == nil || backupTarget == nil || backingImage == nil {
		return nil, nil
	}

	// There is a monitor already
	if monitor := bc.hasMonitor(bbi.Name); monitor != nil {
		return monitor, nil
	}

	concurrentLimit, err := bc.ds.GetSettingAsInt(types.SettingNameBackupConcurrentLimit)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to assert %v value", types.SettingNameBackupConcurrentLimit)
	}

	backupTargetClient, err := newBackupTargetClientFromDefaultEngineImage(bc.ds, backupTarget)
	if err != nil {
		return nil, err
	}

	// pick one backing image manager to do backup
	var targetBim *longhorn.BackingImageManager
	for diskUUID := range backingImage.Spec.DiskFileSpecMap {
		bimMap, err := bc.ds.ListBackingImageManagersByDiskUUID(diskUUID)
		if err != nil {
			return nil, err
		}
		for _, bim := range bimMap {
			if bim.DeletionTimestamp == nil {
				if uuidInManager, exists := bim.Status.BackingImageFileMap[backingImage.Name]; exists && uuidInManager.UUID == backingImage.Status.UUID {
					targetBim = bim
					break
				}
			}
		}
		if targetBim != nil {
			break
		}
	}
	if targetBim == nil {
		return nil, fmt.Errorf("failed to find backing image manager to backup backing image %v", bbi.Name)
	}

	bimClient, err := engineapi.NewBackingImageManagerClient(targetBim)
	if err != nil {
		return nil, err
	}

	compressionMethod, _ := bc.ds.GetSettingValueExisted(types.SettingNameBackupCompressionMethod)
	if compressionMethod == "" {
		return nil, fmt.Errorf("failed to get setting %v", types.SettingNameBackupCompressionMethod)
	}

	monitor, err := bc.enableBackupBackingImageMonitor(bbi, backingImage, backupTargetClient, longhorn.BackupCompressionMethod(compressionMethod), int(concurrentLimit), bimClient)
	if err != nil {
		bbi.Status.Error = err.Error()
		bbi.Status.State = longhorn.BackupStateError
		bbi.Status.LastSyncedAt = metav1.Time{Time: time.Now().UTC()}
		return nil, err
	}
	return monitor, nil
}

// syncWithMonitor syncs the backup state/progress from the replica monitor
func (bc *BackupBackingImageController) syncWithMonitor(bbi *longhorn.BackupBackingImage, bi *longhorn.BackingImage, monitor *engineapi.BackupBackingImageMonitor) error {
	if bbi == nil || monitor == nil {
		return nil
	}

	existingBackupBackingImageState := bbi.Status.State

	backupStatus := monitor.GetBackupBackingImageStatus()
	bbi.Status.Progress = backupStatus.Progress
	bbi.Status.URL = backupStatus.URL
	bbi.Status.Error = backupStatus.Error
	bbi.Status.State = backupStatus.State

	if existingBackupBackingImageState == bbi.Status.State {
		return nil
	}

	if bbi.Status.Error != "" {
		bc.eventRecorder.Eventf(bi, corev1.EventTypeWarning, string(bbi.Status.State),
			"BackingIamge %s backup %s label %v: %s", bi.Name, bbi.Name, bbi.Spec.Labels, bbi.Status.Error)
		return nil
	}
	bc.eventRecorder.Eventf(bi, corev1.EventTypeNormal, string(bbi.Status.State),
		"BackingIamge %s backup %s label %v", bi.Name, bbi.Name, bbi.Spec.Labels)

	return nil
}

func (bc *BackupBackingImageController) hasMonitor(backupBackingImageName string) *engineapi.BackupBackingImageMonitor {
	bc.monitorLock.RLock()
	defer bc.monitorLock.RUnlock()
	return bc.monitors[backupBackingImageName]
}

func (bc *BackupBackingImageController) enableBackupBackingImageMonitor(bbi *longhorn.BackupBackingImage, backingImage *longhorn.BackingImage, backupTargetClient *engineapi.BackupTargetClient,
	compressionMethod longhorn.BackupCompressionMethod, concurrentLimit int, bimClient *engineapi.BackingImageManagerClient) (*engineapi.BackupBackingImageMonitor, error) {
	monitor := bc.hasMonitor(bbi.Name)
	if monitor != nil {
		return monitor, nil
	}

	bc.monitorLock.Lock()
	defer bc.monitorLock.Unlock()

	monitor, err := engineapi.NewBackupBackingImageMonitor(bc.logger, bc.ds, bbi, backingImage, backupTargetClient,
		compressionMethod, concurrentLimit, bimClient, bc.enqueueBackupBackingImageForMonitor)
	if err != nil {
		return nil, err
	}
	bc.monitors[bbi.Name] = monitor
	return monitor, nil
}

func (bc *BackupBackingImageController) disableBackupMonitor(backupName string) {
	monitor := bc.hasMonitor(backupName)
	if monitor == nil {
		return
	}

	bc.monitorLock.Lock()
	defer bc.monitorLock.Unlock()
	delete(bc.monitors, backupName)
	monitor.Close()
}

func (bc *BackupBackingImageController) enqueueBackupBackingImageForMonitor(key string) {
	bc.queue.Add(key)
}

func (bc *BackupBackingImageController) isResponsibleFor(bbi *longhorn.BackupBackingImage) bool {
	return isControllerResponsibleFor(bc.controllerID, bc.ds, bbi.Name, "", bbi.Status.OwnerID)
}

func (bc *BackupBackingImageController) backupNotInFinalState(bbi *longhorn.BackupBackingImage) bool {
	return bbi.Status.State != longhorn.BackupStateCompleted &&
		bbi.Status.State != longhorn.BackupStateError &&
		bbi.Status.State != longhorn.BackupStateUnknown
}
