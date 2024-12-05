package controller

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	MessageTypeReconcileInfo  = "info"
	MessageTypeReconcileError = "error"
)

const (
	WaitForSnapshotMessage                 = "Waiting for the snapshot %v to be ready"
	WaitForEngineMessage                   = "Waiting for the engine %v to be ready"
	WaitForBackupDeletionIsCompleteMessage = "Wait for backup %v to be deleted"
	FailedToGetSnapshotMessage             = "Failed to get the Snapshot %v"
	FailedToDeleteBackupMessage            = "Failed to delete the backup %v in the backupstore, err %v"
	NoDeletionInProgressRecordMessage      = "No deletion in progress record, retry the deletion command"
)

const (
	DeletionMinInterval = time.Minute * 1
	DeletionMaxInterval = time.Hour * 24
)

type DeletingStatus struct {
	State        longhorn.BackupState
	ErrorMessage string
}

type BackupController struct {
	*baseController

	// Which namespace controller is running with
	namespace string
	// Use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	monitors    map[string]*engineapi.BackupMonitor
	monitorLock sync.RWMutex

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	proxyConnCounter util.Counter

	// Use to track the result of the deletion command.
	// Also used to track if controller crashes after the deletion command is triggered.
	deletingMapLock       sync.Mutex
	inProgressDeletingMap map[string]*DeletingStatus

	deletingBackoff *flowcontrol.Backoff
}

func NewBackupController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
	proxyConnCounter util.Counter,
) (*BackupController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	bc := &BackupController{
		baseController: newBaseController("longhorn-backup", logger),

		namespace:    namespace,
		controllerID: controllerID,

		monitors:    map[string]*engineapi.BackupMonitor{},
		monitorLock: sync.RWMutex{},

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-backup-controller"}),

		proxyConnCounter: proxyConnCounter,

		deletingMapLock:       sync.Mutex{},
		inProgressDeletingMap: map[string]*DeletingStatus{},

		deletingBackoff: flowcontrol.NewBackOff(DeletionMinInterval, DeletionMaxInterval),
	}

	var err error
	if _, err = ds.BackupInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bc.enqueueBackup,
		UpdateFunc: func(old, cur interface{}) { bc.enqueueBackup(cur) },
		DeleteFunc: bc.enqueueBackup,
	}); err != nil {
		return nil, err
	}
	bc.cacheSyncs = append(bc.cacheSyncs, ds.BackupInformer.HasSynced)

	return bc, nil
}

func (bc *BackupController) enqueueBackup(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bc.queue.Add(key)
}

func (bc *BackupController) enqueueBackupForMonitor(key string) {
	bc.queue.Add(key)
}

func (bc *BackupController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bc.queue.ShutDown()

	bc.logger.Info("Starting Longhorn Backup controller")
	defer bc.logger.Info("Shut down Longhorn Backup controller")

	if !cache.WaitForNamedCacheSync(bc.name, stopCh, bc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(bc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (bc *BackupController) worker() {
	for bc.processNextWorkItem() {
	}
}

func (bc *BackupController) processNextWorkItem() bool {
	key, quit := bc.queue.Get()
	if quit {
		return false
	}
	defer bc.queue.Done(key)
	err := bc.syncHandler(key.(string))
	bc.handleErr(err, key)
	return true
}

func (bc *BackupController) handleErr(err error, key interface{}) {
	if err == nil {
		bc.queue.Forget(key)
		return
	}

	log := bc.logger.WithField("Backup", key)
	handleReconcileErrorLogging(log, err, "Failed to sync Longhorn backup")
	bc.queue.AddRateLimited(key)
}

func (bc *BackupController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync backup %v", bc.name, key)
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

func getLoggerForBackup(logger logrus.FieldLogger, backup *longhorn.Backup) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"backup": backup.Name,
		},
	)
}

func (bc *BackupController) isBackupNotBeingUsedForVolumeRestore(backupName, backupVolumeName string) (bool, error) {
	volumes, err := bc.ds.ListVolumesByBackupVolumeRO(backupVolumeName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to list volumes for backup volume %v for checking restore status", backupVolumeName)
	}
	for _, v := range volumes {
		if !v.Status.RestoreRequired {
			continue
		}
		engines, err := bc.ds.ListVolumeEnginesRO(v.Name)
		if err != nil {
			return false, errors.Wrapf(err, "failed to list engines for volume %v for checking restore status", v.Name)
		}
		for _, e := range engines {
			for _, status := range e.Status.RestoreStatus {
				if status.IsRestoring {
					return false, fmt.Errorf("backup %v cannot be deleted due to the ongoing volume %v restoration", backupName, v.Name)
				}
			}
		}
	}
	return true, nil
}

func (bc *BackupController) reconcile(backupName string) (err error) {
	// Get Backup CR
	backup, err := bc.ds.GetBackup(backupName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	// Check the responsible node
	defaultEngineImage, err := bc.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		return err
	}
	isResponsible, err := bc.isResponsibleFor(backup, defaultEngineImage)
	if err != nil {
		return nil
	}
	if !isResponsible {
		return nil
	}
	if backup.Status.OwnerID != bc.controllerID {
		backup.Status.OwnerID = bc.controllerID
		backup, err = bc.ds.UpdateBackupStatus(backup)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
	}

	log := getLoggerForBackup(bc.logger, backup)

	// Get default backup target
	backupTarget, err := bc.ds.GetBackupTargetRO(types.DefaultBackupTargetName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to get the backup target %v", types.DefaultBackupTargetName)
	}

	// Find the backup volume name from label
	backupVolumeName, err := bc.getBackupVolumeName(backup)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			return nil // Ignore error to prevent enqueue
		}
		return errors.Wrap(err, "failed to get backup volume name")
	}

	if backup.Status.Messages == nil {
		backup.Status.Messages = map[string]string{}
	}

	// Examine DeletionTimestamp to determine if object is under deletion
	if !backup.DeletionTimestamp.IsZero() {
		if err := bc.handleAttachmentTicketDeletion(backup, backupVolumeName); err != nil {
			return err
		}

		backupVolume, err := bc.ds.GetBackupVolume(backupVolumeName)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}

		needsCleanupRemoteData, err := checkIfRemoteDataCleanupIsNeeded(backup, backupTarget)
		if err != nil {
			return errors.Wrap(err, "failed to check if it needs to delete remote backup data")
		}

		if needsCleanupRemoteData && backupVolume != nil && backupVolume.DeletionTimestamp == nil {
			backupTargetClient, err := newBackupTargetClientFromDefaultEngineImage(bc.ds, backupTarget)
			if err != nil {
				log.WithError(err).Warn("Failed to init backup target clients")
				return nil // Ignore error to prevent enqueue
			}
			backupURL := backupstore.EncodeBackupURL(backup.Name, backupVolumeName, backupTargetClient.URL)

			backupDeleted := bc.handleBackupDeletionInBackupStore(backup, backupVolumeName, backupURL, backupTargetClient)
			if !backupDeleted {
				return nil
			}
		}

		// Request backup_volume_controller to reconcile BackupVolume immediately if it's the last backup
		if backupVolume != nil && backupVolume.Status.LastBackupName == backup.Name {
			backupVolume.Spec.SyncRequestedAt = metav1.Time{Time: time.Now().UTC()}
			if _, err = bc.ds.UpdateBackupVolume(backupVolume); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Errorf("Failed to update backup volume %s spec", backupVolumeName)
				// Do not return err to enqueue since backup_controller is responsible to
				// reconcile Backup CR spec, waits the backup_volume_controller next reconcile time
				// to update it's BackupVolume CR status
			}
		}

		// Disable monitor regardless of backup state
		bc.disableBackupMonitor(backup.Name)

		if backup.Status.State == longhorn.BackupStateError || backup.Status.State == longhorn.BackupStateUnknown {
			bc.eventRecorder.Eventf(backup, corev1.EventTypeWarning, string(backup.Status.State), "Failed backup %s has been deleted: %s", backup.Name, backup.Status.Error)
		}

		autocleanup, err := bc.ds.GetSettingAsBool(types.SettingNameAutoCleanupSnapshotWhenDeleteBackup)
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"name": types.SettingNameAutoCleanupSnapshotWhenDeleteBackup,
			}).Warn("Failed to get the setting")
		}
		if autocleanup {
			// do the best effort to delete the snapshot
			if err = bc.ds.DeleteSnapshot(backup.Spec.SnapshotName); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"backup":   backup.Name,
					"snapshot": backup.Spec.SnapshotName,
				}).Warn("Failed to delete snapshot")
			}
		}
		return bc.ds.RemoveFinalizerForBackup(backup)
	}

	syncTime := metav1.Time{Time: time.Now().UTC()}
	existingBackup := backup.DeepCopy()
	existingBackupState := backup.Status.State
	defer func() {
		if err != nil {
			return
		}

		if bc.backupInFinalState(backup) && (!backup.Status.LastSyncedAt.IsZero() || backup.Spec.SnapshotName == "") {
			err = bc.handleAttachmentTicketDeletion(backup, backupVolumeName)
		}
		if reflect.DeepEqual(existingBackup.Status, backup.Status) {
			return
		}
		if _, err := bc.ds.UpdateBackupStatus(backup); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", backupName)
			bc.enqueueBackup(backup)
			err = nil // nolint: ineffassign
			return
		}
		if backup.Status.State == longhorn.BackupStateCompleted && existingBackupState != backup.Status.State {
			if err := bc.syncBackupVolume(backupVolumeName); err != nil {
				log.Warnf("failed to sync Backup Volume: %v", backupVolumeName)
				return
			}
		}
	}()

	// Perform backup snapshot to the remote backup target
	// If the Backup CR is created by the user/API layer (spec.snapshotName != ""), has not been synced (status.lastSyncedAt == "")
	// and is not in final state, it means creating a backup from a volume snapshot is required.
	// Hence the source of truth is the engine/replica and the controller needs to sync the status with it.
	// Otherwise, the Backup CR is created by the backup volume controller, which means the backup already
	// exists in the remote backup target before the CR creation.
	// What the controller needs to do for this case is retrieve the info from the remote backup target.
	if backup.Status.LastSyncedAt.IsZero() && backup.Spec.SnapshotName != "" && !bc.backupInFinalState(backup) {
		volume, err := bc.ds.GetVolume(backupVolumeName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
			err = fmt.Errorf("Cannot find the corresponding volume: %v", err)
			log.WithError(err).Error()
			backup.Status.Error = err.Error()
			backup.Status.State = longhorn.BackupStateError
			backup.Status.LastSyncedAt = syncTime
			return nil // Ignore error to prevent enqueue
		}

		if err := bc.handleAttachmentTicketCreation(backup, backupVolumeName); err != nil {
			return err
		}

		if backup.Status.SnapshotCreatedAt == "" || backup.Status.VolumeSize == "" {
			bc.syncBackupStatusWithSnapshotCreationTimeAndVolumeSize(volume, backup)
		}

		if err := bc.backupBackingImage(volume); err != nil {
			return err
		}

		monitor, err := bc.checkMonitor(backup, volume, backupTarget)
		if err != nil {
			if backup.Status.State == longhorn.BackupStateError {
				log.WithError(err).Warnf("Failed to enable the backup monitor for backup %v", backup.Name)
				return nil
			}
			if backup.Status.State == longhorn.BackupStatePending {
				log.WithError(err).Warnf("Waiting to enable the backup monitor for backup %v", backup.Name)
				return nil
			}
			return err
		}

		if err = bc.syncWithMonitor(backup, volume, monitor); err != nil {
			return err
		}

		switch backup.Status.State {
		case longhorn.BackupStateNew, longhorn.BackupStatePending, longhorn.BackupStateInProgress:
			return nil
		case longhorn.BackupStateCompleted:
			bc.disableBackupMonitor(backup.Name)
		case longhorn.BackupStateError, longhorn.BackupStateUnknown:
			backup.Status.LastSyncedAt = syncTime
			bc.disableBackupMonitor(backup.Name)
			return nil
		}
	}

	// The backup config had synced
	if !backup.Status.LastSyncedAt.IsZero() &&
		!backup.Spec.SyncRequestedAt.After(backup.Status.LastSyncedAt.Time) {
		return nil
	}

	// The backup creation is complete, then the source of truth becomes the remote backup target
	backupTargetClient, err := newBackupTargetClientFromDefaultEngineImage(bc.ds, backupTarget)
	if err != nil {
		log.WithError(err).Error("Error init backup target clients")
		return nil // Ignore error to prevent enqueue
	}

	backupURL := backupstore.EncodeBackupURL(backup.Name, backupVolumeName, backupTargetClient.URL)
	backupInfo, err := backupTargetClient.BackupGet(backupURL, backupTargetClient.Credential)
	if err != nil && !types.ErrorIsNotFound(err) {
		if !types.ErrorIsInProgress(err) {
			log.WithError(err).Error("Error inspecting backup config")
		}
		return nil // Ignore error to prevent enqueue
	}
	if backupInfo == nil {
		log.Warn("Backup info is nil")
		return nil
	}

	// Remove the Backup Volume recurring jobs/groups information.
	// Only record the latest recurring jobs/groups information in backup volume CR and volume.cfg on remote backup target.
	delete(backupInfo.Labels, types.VolumeRecurringJobInfoLabel)

	// Update Backup CR status
	backup.Status.State = longhorn.BackupStateCompleted
	backup.Status.URL = backupInfo.URL
	backup.Status.SnapshotName = backupInfo.SnapshotName
	backup.Status.SnapshotCreatedAt = backupInfo.SnapshotCreated
	backup.Status.BackupCreatedAt = backupInfo.Created
	backup.Status.Size = backupInfo.Size
	backup.Status.Labels = backupInfo.Labels
	backup.Status.Messages = backupInfo.Messages
	backup.Status.VolumeName = backupInfo.VolumeName
	backup.Status.VolumeSize = backupInfo.VolumeSize
	backup.Status.VolumeCreated = backupInfo.VolumeCreated
	backup.Status.VolumeBackingImageName = backupInfo.VolumeBackingImageName
	backup.Status.CompressionMethod = longhorn.BackupCompressionMethod(backupInfo.CompressionMethod)
	backup.Status.LastSyncedAt = syncTime
	backup.Status.NewlyUploadedDataSize = backupInfo.NewlyUploadedDataSize
	backup.Status.ReUploadedDataSize = backupInfo.ReUploadedDataSize
	return nil
}

// handleAttachmentTicketDeletion check and delete attachment so that the source volume is detached if needed
func (bc *BackupController) handleAttachmentTicketDeletion(backup *longhorn.Backup, volumeName string) (err error) {
	defer func() {
		err = errors.Wrap(err, "handleAttachmentTicketDeletion: failed to clean up attachment")
	}()

	va, err := bc.ds.GetLHVolumeAttachmentByVolumeName(volumeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	attachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeBackupController, backup.Name)

	if _, ok := va.Spec.AttachmentTickets[attachmentTicketID]; ok {
		delete(va.Spec.AttachmentTickets, attachmentTicketID)
		if _, err = bc.ds.UpdateLHVolumeAttachment(va); err != nil {
			return err
		}
	}

	return nil
}

// handleAttachmentTicketCreation check and create attachment so that the source volume is attached if needed
func (bc *BackupController) handleAttachmentTicketCreation(backup *longhorn.Backup, volumeName string) (err error) {
	defer func() {
		err = errors.Wrap(err, "handleAttachmentTicketCreation: failed to create/update attachment")
	}()

	vol, err := bc.ds.GetVolumeRO(volumeName)
	if err != nil {
		return err
	}

	va, err := bc.ds.GetLHVolumeAttachmentByVolumeName(vol.Name)
	if err != nil {
		return err
	}

	existingVA := va.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingVA.Spec, va.Spec) {
			return
		}

		if _, err = bc.ds.UpdateLHVolumeAttachment(va); err != nil {
			return
		}
	}()

	attachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeBackupController, backup.Name)
	createOrUpdateAttachmentTicket(va, attachmentTicketID, vol.Status.OwnerID, longhorn.AnyValue, longhorn.AttacherTypeBackupController)

	return nil
}

// VerifyAttachment check the volume attachment ticket for this backup is satisfied
func (bc *BackupController) VerifyAttachment(backup *longhorn.Backup, volumeName string) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "VerifyAttachment: failed to verify attachment")
	}()

	vol, err := bc.ds.GetVolumeRO(volumeName)
	if err != nil {
		return false, err
	}

	va, err := bc.ds.GetLHVolumeAttachmentByVolumeName(vol.Name)
	if err != nil {
		return false, err
	}

	attachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeBackupController, backup.Name)
	isVolumeStillDesiredToAttachToSameNodeAsAttachmentTicket := vol.Spec.NodeID == longhorn.GetNodeIdOfAttachmentTicket(attachmentTicketID, va)

	return isVolumeStillDesiredToAttachToSameNodeAsAttachmentTicket && longhorn.IsAttachmentTicketSatisfied(attachmentTicketID, va), nil
}

func (bc *BackupController) isResponsibleFor(b *longhorn.Backup, defaultEngineImage string) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	node, err := bc.ds.GetNodeRO(bc.controllerID)
	if err != nil {
		return false, err
	}

	if node.Spec.DataEngineUpgradeRequested {
		return false, nil
	}

	isResponsible := isControllerResponsibleFor(bc.controllerID, bc.ds, b.Name, "", b.Status.OwnerID)

	currentOwnerEngineAvailable, err := bc.ds.CheckEngineImageReadiness(defaultEngineImage, b.Status.OwnerID)
	if err != nil {
		return false, err
	}
	currentNodeEngineAvailable, err := bc.ds.CheckEngineImageReadiness(defaultEngineImage, bc.controllerID)
	if err != nil {
		return false, err
	}

	isPreferredOwner := currentNodeEngineAvailable && isResponsible
	continueToBeOwner := currentNodeEngineAvailable && bc.controllerID == b.Status.OwnerID
	requiresNewOwner := currentNodeEngineAvailable && !currentOwnerEngineAvailable

	return isPreferredOwner || continueToBeOwner || requiresNewOwner, nil
}

func (bc *BackupController) getBackupVolumeName(backup *longhorn.Backup) (string, error) {
	backupVolumeName, ok := backup.Labels[types.LonghornLabelBackupVolume]
	if !ok {
		return "", fmt.Errorf("cannot find the backup volume label")
	}
	return backupVolumeName, nil
}

func (bc *BackupController) getEngineBinaryClient(volumeName string) (*engineapi.EngineBinary, error) {
	engine, err := bc.ds.GetVolumeCurrentEngine(volumeName)
	if err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, fmt.Errorf("cannot get the client since the engine is nil")
	}
	return GetBinaryClientForEngine(engine, &engineapi.EngineCollection{}, engine.Status.CurrentImage)
}

// validateBackingImageChecksum validates backing image checksum
func (bc *BackupController) validateBackingImageChecksum(volName, biName string) (string, error) {
	if biName == "" {
		return "", nil
	}

	bi, err := bc.ds.GetBackingImageRO(biName)
	if err != nil {
		return "", err
	}

	bv, err := bc.ds.GetBackupVolumeRO(volName)
	if err != nil && !apierrors.IsNotFound(err) {
		return "", err
	}

	if bv != nil &&
		bv.Status.BackingImageChecksum != "" && bi.Status.Checksum != "" &&
		bv.Status.BackingImageChecksum != bi.Status.Checksum {
		return "", fmt.Errorf("the backing image %v checksum %v in the backup volume doesn't match the current checksum %v",
			biName, bv.Status.BackingImageChecksum, bi.Status.Checksum)
	}
	return bi.Status.Checksum, nil
}

func (bc *BackupController) backupBackingImage(volume *longhorn.Volume) error {
	if volume == nil {
		return nil
	}

	// volume is not using backing image, there is no need to backup
	biName := volume.Spec.BackingImage
	if biName == "" {
		return nil
	}

	_, err := bc.ds.GetBackingImage(biName)
	if err != nil {
		return errors.Wrapf(err, "failed to get backing image %v", biName)
	}

	bbi, err := bc.ds.GetBackupBackingImage(biName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to get backup backing image %v", biName)
		}
	}

	if bbi == nil {
		backupBackingImage := &longhorn.BackupBackingImage{
			ObjectMeta: metav1.ObjectMeta{
				Name: biName,
			},
			Spec: longhorn.BackupBackingImageSpec{
				UserCreated: true,
			},
		}
		if _, err = bc.ds.CreateBackupBackingImage(backupBackingImage); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create backup backing image %s in the cluster", biName)
		}
	}

	return nil
}

// checkMonitor checks if the replica monitor existed.
// If yes, returns the replica monitor. Otherwise, create a new replica monitor.
func (bc *BackupController) checkMonitor(backup *longhorn.Backup, volume *longhorn.Volume, backupTarget *longhorn.BackupTarget) (*engineapi.BackupMonitor, error) {
	if backup == nil || volume == nil || backupTarget == nil {
		return nil, nil
	}

	// There is a monitor already
	if monitor := bc.hasMonitor(backup.Name); monitor != nil {
		return monitor, nil
	}

	// Backing image checksum validation
	biChecksum, err := bc.validateBackingImageChecksum(volume.Name, volume.Spec.BackingImage)
	if err != nil {
		return nil, err
	}

	concurrentLimit, err := bc.ds.GetSettingAsInt(types.SettingNameBackupConcurrentLimit)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to assert %v value", types.SettingNameBackupConcurrentLimit)
	}
	// check if my ticket is satisfied
	ok, err := bc.VerifyAttachment(backup, volume.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("waiting for attachment %v to be attached before enabling backup monitor", longhorn.GetAttachmentTicketID(longhorn.AttacherTypeBackupController, backup.Name))
	}

	engineClientProxy, backupTargetClient, err := getBackupTarget(bc.controllerID, backupTarget, bc.ds, bc.logger, bc.proxyConnCounter)
	if err != nil {
		return nil, err
	}

	// get storage class of the pvc binding with the volume
	kubernetesStatus := &volume.Status.KubernetesStatus
	storageClassName := ""
	if kubernetesStatus.PVCName != "" && kubernetesStatus.LastPVCRefAt == "" {
		pvc, _ := bc.ds.GetPersistentVolumeClaim(kubernetesStatus.Namespace, kubernetesStatus.PVCName)
		if pvc != nil {
			if pvc.Spec.StorageClassName != nil {
				storageClassName = *pvc.Spec.StorageClassName
			}
			if storageClassName == "" {
				if v, exist := pvc.Annotations[corev1.BetaStorageClassAnnotation]; exist {
					storageClassName = v
				}
			}
			if storageClassName == "" {
				bc.logger.Warnf("Failed to find the StorageClassName from the pvc %v", pvc.Name)
			}
		}
	}

	engine, err := bc.ds.GetVolumeCurrentEngine(volume.Name)
	if err != nil {
		return nil, err
	}

	if engine.Status.CurrentState != longhorn.InstanceStateRunning ||
		engine.Spec.DesireState != longhorn.InstanceStateRunning ||
		volume.Status.State != longhorn.VolumeStateAttached {
		backup.Status.State = longhorn.BackupStatePending
		backup.Status.Messages[MessageTypeReconcileInfo] = fmt.Sprintf(WaitForEngineMessage, engine.Name)
		return nil, fmt.Errorf("waiting for the engine %v to be running before enabling backup monitor", engine.Name)
	}

	snapshot, err := bc.ds.GetSnapshotRO(backup.Spec.SnapshotName)
	if err != nil {
		backup.Status.State = longhorn.BackupStatePending
		backup.Status.Messages[MessageTypeReconcileInfo] = fmt.Sprintf(FailedToGetSnapshotMessage, backup.Spec.SnapshotName)
		return nil, errors.Wrapf(err, "failed to get the snapshot %v before enabling backup monitor", backup.Spec.SnapshotName)
	}
	if snapshot != nil {
		if !snapshot.Status.ReadyToUse {
			backup.Status.State = longhorn.BackupStatePending
			backup.Status.Messages[MessageTypeReconcileInfo] = fmt.Sprintf(WaitForSnapshotMessage, backup.Spec.SnapshotName)
			return nil, fmt.Errorf("waiting for the snapshot %v to be ready before enabling backup monitor", backup.Spec.SnapshotName)
		}
	}

	clusterBackups, err := bc.ds.ListBackupsWithBackupVolumeName(volume.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list backups in the cluster")
	}
	for _, b := range clusterBackups {
		if b.Status.State == longhorn.BackupStateDeleting {
			backup.Status.State = longhorn.BackupStatePending
			backup.Status.Messages[MessageTypeReconcileInfo] = fmt.Sprintf(WaitForBackupDeletionIsCompleteMessage, b.Name)
			return nil, fmt.Errorf("waiting for the backup %v to be deleted before enabling backup monitor", b.Name)
		}
	}

	// Enable the backup monitor
	monitor, err := bc.enableBackupMonitor(backup, volume, backupTargetClient, biChecksum,
		volume.Spec.BackupCompressionMethod, int(concurrentLimit), storageClassName, engineClientProxy)
	if err != nil {
		backup.Status.Error = err.Error()
		backup.Status.State = longhorn.BackupStateError
		backup.Status.LastSyncedAt = metav1.Time{Time: time.Now().UTC()}
		return nil, err
	}
	return monitor, nil
}

// syncWithMonitor syncs the backup state/progress from the replica monitor
func (bc *BackupController) syncWithMonitor(backup *longhorn.Backup, volume *longhorn.Volume, monitor *engineapi.BackupMonitor) error {
	if backup == nil || volume == nil || monitor == nil {
		return nil
	}

	existingBackupState := backup.Status.State

	backupStatus := monitor.GetBackupStatus()
	backup.Status.Progress = backupStatus.Progress
	backup.Status.URL = backupStatus.URL
	backup.Status.Error = backupStatus.Error
	backup.Status.SnapshotName = backupStatus.SnapshotName
	backup.Status.ReplicaAddress = backupStatus.ReplicaAddress
	backup.Status.State = backupStatus.State

	if existingBackupState == backup.Status.State {
		return nil
	}

	if backup.Status.Error != "" {
		bc.eventRecorder.Eventf(volume, corev1.EventTypeWarning, string(backup.Status.State),
			"Snapshot %s backup %s label %v: %s", backup.Spec.SnapshotName, backup.Name, backup.Spec.Labels, backup.Status.Error)
		return nil
	}
	bc.eventRecorder.Eventf(volume, corev1.EventTypeNormal, string(backup.Status.State),
		"Snapshot %s backup %s label %v", backup.Spec.SnapshotName, backup.Name, backup.Spec.Labels)

	return nil
}

// syncBackupVolume triggers the backup_volume_controller/backup_target_controller
// to run reconcile immediately
func (bc *BackupController) syncBackupVolume(volumeName string) error {
	syncTime := metav1.Time{Time: time.Now().UTC()}
	backupVolume, err := bc.ds.GetBackupVolume(volumeName)
	if err == nil {
		// Request backup_volume_controller to reconcile BackupVolume immediately.
		backupVolume.Spec.SyncRequestedAt = syncTime
		if _, err = bc.ds.UpdateBackupVolume(backupVolume); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
			bc.logger.WithError(err).Warnf("Failed to update backup volume %v spec", volumeName)
		}
	} else if err != nil && apierrors.IsNotFound(err) {
		// Request backup_target_controller to reconcile BackupTarget immediately.
		backupTarget, err := bc.ds.GetBackupTarget(types.DefaultBackupTargetName)
		if err != nil {
			return errors.Wrap(err, "failed to get backup target")
		}
		backupTarget.Spec.SyncRequestedAt = syncTime
		if _, err = bc.ds.UpdateBackupTarget(backupTarget); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
			bc.logger.WithError(err).Warn("Failed to update backup target")
		}
	}
	return nil
}

func (bc *BackupController) hasMonitor(backupName string) *engineapi.BackupMonitor {
	bc.monitorLock.RLock()
	defer bc.monitorLock.RUnlock()
	return bc.monitors[backupName]
}

func (bc *BackupController) enableBackupMonitor(backup *longhorn.Backup, volume *longhorn.Volume, backupTargetClient *engineapi.BackupTargetClient,
	biChecksum string, compressionMethod longhorn.BackupCompressionMethod, concurrentLimit int, storageClassName string,
	engineClientProxy engineapi.EngineClientProxy) (*engineapi.BackupMonitor, error) {
	monitor := bc.hasMonitor(backup.Name)
	if monitor != nil {
		return monitor, nil
	}

	bc.monitorLock.Lock()
	defer bc.monitorLock.Unlock()

	engine, err := bc.ds.GetVolumeCurrentEngine(volume.Name)
	if err != nil {
		return nil, err
	}

	monitor, err = engineapi.NewBackupMonitor(bc.logger, bc.ds, backup, volume, backupTargetClient,
		biChecksum, compressionMethod, concurrentLimit, storageClassName, engine, engineClientProxy, bc.enqueueBackupForMonitor)
	if err != nil {
		return nil, err
	}
	bc.monitors[backup.Name] = monitor
	return monitor, nil
}

func (bc *BackupController) disableBackupMonitor(backupName string) {
	monitor := bc.hasMonitor(backupName)
	if monitor == nil {
		return
	}

	bc.monitorLock.Lock()
	defer bc.monitorLock.Unlock()
	delete(bc.monitors, backupName)
	monitor.Close()
}

func (bc *BackupController) syncBackupStatusWithSnapshotCreationTimeAndVolumeSize(volume *longhorn.Volume, backup *longhorn.Backup) {
	backup.Status.VolumeSize = strconv.FormatInt(volume.Spec.Size, 10)
	engineCliClient, err := bc.getEngineBinaryClient(volume.Name)
	if err != nil {
		bc.logger.WithError(err).Warn("Failed to get engine client when syncing backup status")
		return
	}

	e, err := bc.ds.GetVolumeCurrentEngine(volume.Name)
	if err != nil {
		bc.logger.WithError(err).Warn("Failed to get engine when syncing backup status")
		return
	}

	engineClientProxy, err := engineapi.GetCompatibleClient(e, engineCliClient, bc.ds, bc.logger, bc.proxyConnCounter)
	if err != nil {
		bc.logger.WithError(err).Warn("Failed to get proxy when syncing backup status")
		return
	}
	defer engineClientProxy.Close()

	snap, err := engineClientProxy.SnapshotGet(e, backup.Spec.SnapshotName)
	if err != nil {
		bc.logger.WithError(err).Warnf("Failed to get snapshot %v when syncing backup status", backup.Spec.SnapshotName)
		return
	}

	if snap == nil {
		bc.logger.WithError(err).Warnf("Failed to get the snapshot %v in volume %v when syncing backup status", backup.Spec.SnapshotName, volume.Name)
		return
	}

	backup.Status.SnapshotCreatedAt = snap.Created
}

func (bc *BackupController) backupInFinalState(backup *longhorn.Backup) bool {
	return backup.Status.State == longhorn.BackupStateCompleted ||
		backup.Status.State == longhorn.BackupStateError ||
		backup.Status.State == longhorn.BackupStateUnknown ||
		backup.Status.State == longhorn.BackupStateDeleting
}

func (bc *BackupController) startDeletingBackupInBackupStore(backupURL string, backupTargetClient *engineapi.BackupTargetClient) {
	bc.deletingMapLock.Lock()
	bc.inProgressDeletingMap[backupURL] = &DeletingStatus{
		State:        longhorn.BackupStateDeleting,
		ErrorMessage: "",
	}
	bc.deletingMapLock.Unlock()

	// The backup deletion will be executed asynchronously.
	// After triggering the backup deletion, update the state to deleting.
	// Then keep checking if the backup is deleted in the backupstore before removing the finalizer.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				bc.setInprogressDeletionMap(backupURL, longhorn.BackupStateError, fmt.Sprintf("Recovered from panic: %v", r))
			}
		}()
		err := backupTargetClient.BackupDelete(backupURL, backupTargetClient.Credential)
		// If the deletion command fails, update the in-memory map to inform the controller.
		if err != nil {
			bc.setInprogressDeletionMap(backupURL, longhorn.BackupStateError, fmt.Sprintf(FailedToDeleteBackupMessage, backupURL, err.Error()))
		}
	}()
}

func (bc *BackupController) handleBackupDeletionInBackupStore(backup *longhorn.Backup, backupVolumeName string, backupURL string, backupTargetClient *engineapi.BackupTargetClient) bool {
	log := getLoggerForBackup(bc.logger, backup)
	existingBackup := backup.DeepCopy()

	if backup.Status.State != longhorn.BackupStateDeleting {
		if bc.deletingBackoff.IsInBackOffSinceUpdate(backup.Name, time.Now()) {
			return false
		}

		if unused, err := bc.isBackupNotBeingUsedForVolumeRestore(backup.Name, backupVolumeName); !unused {
			log.WithError(err).Warn("Failed to delete backup in backupstore")
			return false
		}
		bc.startDeletingBackupInBackupStore(backupURL, backupTargetClient)

		// After triggering the asynchronous deletion,
		// update the status to Deleting and requeue the backup to check the backupstore in the following reconciliations.
		// Controller won't execute this code block if the status is deleting.
		backup.Status.State = longhorn.BackupStateDeleting
		backup.Status.Messages = map[string]string{}
		if _, err := bc.ds.UpdateBackupStatus(backup); err != nil {
			log.WithError(err).Debugf("Backup %v update status error", backup.Name)
		}
		bc.deletingBackoff.Next(backup.Name, time.Now())
		bc.enqueueBackup(backup)
		return false
	}

	// For the in progress backup, the inspect command returns nil backupInfo with in progress error.
	// We should consider the backup exists even the backupInfo is nil when it is in progress.
	backupInfo, err := backupTargetClient.BackupGet(backupURL, backupTargetClient.Credential)
	if err != nil && !types.ErrorIsNotFound(err) && !types.ErrorIsInProgress(err) {
		log.WithError(err).Debugf("Failed to check backup %v in the backupstore", backup.Name)
		bc.enqueueBackup(backup)
		return false
	}
	if backupInfo != nil || types.ErrorIsInProgress(err) {
		bc.deletingMapLock.Lock()
		defer bc.deletingMapLock.Unlock()
		// Controller could have crashed if there is no record in the map and the backup still exists in the backupstore.
		// We update the status to Error so the controller can retry the deletion command again.
		if bc.inProgressDeletingMap[backupURL] == nil {
			backup.Status.State = longhorn.BackupStateError
			backup.Status.Messages[MessageTypeReconcileError] = NoDeletionInProgressRecordMessage
		} else {
			// If the state is Deleting, we early return to check the file in the backupstore in the next reconciliation
			// If the state is Error, we update the state and message to inform users.
			// With the state being Error, other pending backups can start without considering the deletion lock in the backupstore.
			// Controller can also retry the deletion command after the status is Error.
			if bc.inProgressDeletingMap[backupURL].State == longhorn.BackupStateDeleting {
				return false
			}
			log.Warnf("Failed to delete backup in the backupstore, %v", bc.inProgressDeletingMap[backupURL].ErrorMessage)
			backup.Status.State = longhorn.BackupStateError
			backup.Status.Messages[MessageTypeReconcileError] = fmt.Sprintf(FailedToDeleteBackupMessage, backupURL, bc.inProgressDeletingMap[backupURL].ErrorMessage)
		}
		if reflect.DeepEqual(existingBackup.Status, backup.Status) {
			return false
		}
		if _, err := bc.ds.UpdateBackupStatus(backup); err != nil {
			log.WithError(err).Errorf("Backup %v update status error", backup.Name)
		}
		return false
	}

	// Clean up the deleting backoff
	bc.deletingBackoff.DeleteEntry(backup.Name)
	bc.deletingMapLock.Lock()
	delete(bc.inProgressDeletingMap, backupURL)
	bc.deletingMapLock.Unlock()
	return true
}

func (bc *BackupController) setInprogressDeletionMap(backupURL string, state longhorn.BackupState, errMsg string) {
	bc.deletingMapLock.Lock()
	defer bc.deletingMapLock.Unlock()

	bc.inProgressDeletingMap[backupURL].State = state
	bc.inProgressDeletingMap[backupURL].ErrorMessage = errMsg
}
