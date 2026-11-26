package controller

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/go-common-libs/multierr"

	systembackupstore "github.com/longhorn/backupstore/systembackup"
	bsutil "github.com/longhorn/backupstore/util"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/meta"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	SystemBackupControllerName = "longhorn-system-backup"

	SystemBackupTempDir = "/tmp"

	SystemBackupErrArchive         = "failed to archive system backup file"
	SystemBackupErrDelete          = "failed to delete system backup in backup target"
	SystemBackupErrGenerate        = "failed to generate system backup file"
	SystemBackupErrGenerateYAML    = "failed to generate resource YAMLs"
	SystemBackupErrGetFmt          = "failed to get %v"
	SystemBackupErrGetConfig       = "failed to get system backup config"
	SystemBackupErrGetBackupTarget = "failed to get backup target"
	SystemBackupErrMkdir           = "failed to create system backup file directory"
	SystemBackupErrRemoveAll       = "failed to remove system backup directory"
	SystemBackupErrRemove          = "failed to remove system backup file"
	SystemBackupErrOSStat          = "failed to compute system backup file size"
	SystemBackupErrSync            = "failed to sync from backup target"
	SystemBackupErrTimeoutSnapshot = "timeout taking volume snapshot for system backup"
	SystemBackupErrTimeoutUpload   = "timeout uploading system backup"
	SystemBackupErrUpload          = "failed to upload system backup file"
	SystemBackupErrVolumeBackup    = "failed to backup volumes"

	SystemBackupMsgCreatedArchieveFmt  = "Created system backup file: %v"
	SystemBackupMsgDeletingRemote      = "Deleting system backup in backup target"
	SystemBackupMsgDeleted             = "Deleted system backup"
	SystemBackupMsgRequeueNextPhaseFmt = "Requeue %v for next phase: %v"
	SystemBackupMsgStarting            = "Starting system backup"
	SystemBackupMsgSyncedBackupTarget  = "Synced system backup from backup target"
	SystemBackupMsgSyncingBackupTarget = "Syncing system backup from backup target"
	SystemBackupMsgUploadBackupTarget  = "Uploaded system backup to backup target"
)

type systemBackupRecordType string

const (
	systemBackupRecordTypeError  = systemBackupRecordType("error")
	systemBackupRecordTypeNone   = systemBackupRecordType("")
	systemBackupRecordTypeNormal = systemBackupRecordType("normal")
)

type systemBackupRecord struct {
	nextState longhorn.SystemBackupState

	recordType systemBackupRecordType
	message    string
	reason     string
}

var errSystemBackupOwnedObjectDeleting = errors.New("is being deleted")

type SystemBackupController struct {
	*baseController

	// which namespace controller is running with
	namespace string

	// use as the OwnerID of the controller
	controllerID string

	// the running manager image
	managerImage string

	kubeClient clientset.Interface

	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	// generatingSystemBackups holds the SystemBackups whose archive generation
	// is running in this process.
	generatingSystemBackups sync.Map
	// uploadingSystemBackups holds the SystemBackups whose archive upload is
	// running in this process.
	uploadingSystemBackups sync.Map

	archiveBackupNamesLock sync.Mutex
	// archiveBackupNames caches the volume backups referenced by the archives
	// in the backup target, keyed by the backup target URL and archive URI.
	archiveBackupNames map[string]archiveBackupNamesEntry
}

// archiveBackupNamesEntry is the cached last backups of an archive, valid as
// long as the archive checksum in the backup target is unchanged.
type archiveBackupNamesEntry struct {
	checksum    string
	backupNames []string
}

func NewSystemBackupController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string,
	controllerID string,
	managerImage string) (*SystemBackupController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	c := &SystemBackupController{
		baseController: newBaseController(SystemBackupControllerName, logger),

		namespace:    namespace,
		controllerID: controllerID,
		managerImage: managerImage,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: SystemBackupControllerName + "-controller"}),

		archiveBackupNames: map[string]archiveBackupNamesEntry{},
	}

	var err error
	if _, err = ds.SystemBackupInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueue,
		UpdateFunc: func(old, cur interface{}) { c.enqueue(cur) },
		DeleteFunc: c.enqueue,
	}, 0); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.SystemBackupInformer.HasSynced)

	return c, nil
}

func (c *SystemBackupController) enqueue(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

func (c *SystemBackupController) enqueueAfter(obj interface{}, duration time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.AddAfter(key, duration)
}

func (c *SystemBackupController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting Longhorn SystemBackup controller")
	defer c.logger.Info("Shut down Longhorn SystemBackup controller")

	if !cache.WaitForNamedCacheSync(c.name, stopCh, c.cacheSyncs...) {
		return
	}
	c.initReconcileMetrics(workers)
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (c *SystemBackupController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *SystemBackupController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncWithMetrics(func() error { return c.syncSystemBackup(key.(string)) })
	c.handleErr(err, key)

	return true
}

func (c *SystemBackupController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	log := c.logger.WithField("SystemBackup", key)

	if c.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn SystemBackup")
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping SystemBackup out of the queue")
	c.queue.Forget(key)
}

func (c *SystemBackupController) handleStatusUpdate(record *systemBackupRecord, systemBackup *longhorn.SystemBackup, existingSystemBackup *longhorn.SystemBackup, err error, log logrus.FieldLogger) { // nolint: staticcheck
	switch record.recordType {
	case systemBackupRecordTypeError:
		c.recordErrorState(record, systemBackup)
	case systemBackupRecordTypeNormal:
		c.recordNormalState(record, systemBackup)
	default:
		return
	}

	if !reflect.DeepEqual(existingSystemBackup.Status, systemBackup.Status) {
		systemBackup.Status.LastSyncedAt = metav1.Time{Time: time.Now().UTC()}
		if _, err = c.ds.UpdateSystemBackupStatus(systemBackup); err != nil { // nolint: staticcheck
			log.WithError(err).Debugf("Requeue %v due to error", systemBackup.Name)
			c.enqueue(systemBackup)
			err = nil // nolint: ineffassign
			return
		}
	}

	switch record.recordType {
	case systemBackupRecordTypeError:
		c.LogErrorState(record, systemBackup, log)
	case systemBackupRecordTypeNormal:
		c.LogNormalState(record, systemBackup, log)
	default:
		return
	}

	if systemBackup.Status.State != existingSystemBackup.Status.State {
		log.Infof(SystemBackupMsgRequeueNextPhaseFmt, systemBackup.Name, systemBackup.Status.State)
	}
}

func (c *SystemBackupController) isResponsibleFor(systemBackup *longhorn.SystemBackup) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, systemBackup.Name, "", systemBackup.Status.OwnerID)
}

func (c *SystemBackupController) syncSystemBackup(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync SystemBackup %v", c.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	if namespace != c.namespace {
		return nil
	}

	backupTarget, err := c.ds.GetDefaultBackupTargetRO()
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	var backupTargetClient *engineapi.BackupTargetClient
	if backupTarget != nil {
		backupTargetClient, err = newBackupTargetClientFromDefaultEngineImage(c.ds, backupTarget)
		if err != nil {
			return err
		}
	}

	return c.reconcile(name, backupTargetClient, backupTarget)
}

func getLoggerForSystemBackup(logger logrus.FieldLogger, systemBackup *longhorn.SystemBackup) logrus.FieldLogger {
	logger = logger.WithField("systemBackup", systemBackup.Name)
	if systemBackup.Spec.VolumeBackupPolicy != "" {
		logger = logger.WithField("volumeBackupPolicy", systemBackup.Spec.VolumeBackupPolicy)
	}
	return logger
}

func (c *SystemBackupController) LogErrorState(record *systemBackupRecord, systemBackup *longhorn.SystemBackup, log logrus.FieldLogger) {
	log.Error(record.message)
	c.eventRecorder.Event(systemBackup, corev1.EventTypeWarning, constant.EventReasonFailed, util.CapitalizeFirstLetter(record.message))
}

func (c *SystemBackupController) LogNormalState(record *systemBackupRecord, systemBackup *longhorn.SystemBackup, log logrus.FieldLogger) {
	log.Info(record.message)
	c.eventRecorder.Event(systemBackup, corev1.EventTypeNormal, record.reason, record.message)
}

func (c *SystemBackupController) recordErrorState(record *systemBackupRecord, systemBackup *longhorn.SystemBackup) {
	systemBackup.Status.State = longhorn.SystemBackupStateError
	systemBackup.Status.Conditions = types.SetCondition(
		systemBackup.Status.Conditions,
		longhorn.SystemBackupConditionTypeError,
		longhorn.ConditionStatusTrue,
		record.reason,
		util.SanitizeVolatileErrorContent(record.message),
	)
}

func (c *SystemBackupController) recordNormalState(record *systemBackupRecord, systemBackup *longhorn.SystemBackup) {
	systemBackup.Status.State = record.nextState
}

func (c *SystemBackupController) updateSystemBackupRecord(record *systemBackupRecord, recordType systemBackupRecordType, nextState longhorn.SystemBackupState, reason, message string) {
	record.recordType = recordType
	record.nextState = nextState
	record.reason = reason
	record.message = message
}

func (c *SystemBackupController) reconcile(name string, backupTargetClient engineapi.SystemBackupOperationInterface, backupTarget *longhorn.BackupTarget) (err error) {
	systemBackup, err := c.ds.GetSystemBackup(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !c.isResponsibleFor(systemBackup) {
		return nil
	}

	log := getLoggerForSystemBackup(c.logger, systemBackup)

	if systemBackup.Status.OwnerID != c.controllerID {
		systemBackup.Status.OwnerID = c.controllerID
		systemBackup, err = c.ds.UpdateSystemBackupStatus(systemBackup)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}

		log.Infof("System backup got new owner %v", c.controllerID)
	}

	record := &systemBackupRecord{}
	existingSystemBackup := systemBackup.DeepCopy()
	defer c.handleStatusUpdate(record, systemBackup, existingSystemBackup, err, log)

	if !systemBackup.DeletionTimestamp.IsZero() &&
		systemBackup.Status.State != longhorn.SystemBackupStateDeleting {
		c.updateSystemBackupRecord(record,
			systemBackupRecordTypeNormal, longhorn.SystemBackupStateDeleting,
			constant.EventReasonDeleting, SystemBackupMsgDeletingRemote,
		)
		return
	}

	// If the system backup is in the final state, we don't need to do anything for the backup target not found.
	if backupTarget == nil && !systemBackupInFinalState(systemBackup) {
		c.updateSystemBackupRecord(record,
			systemBackupRecordTypeError, longhorn.SystemBackupStateError,
			constant.EventReasonFailedStarting, SystemBackupErrGetBackupTarget,
		)
		return fmt.Errorf(string(SystemBackupErrGetBackupTarget)+": %v", types.DefaultBackupTargetName)
	}

	tempBackupArchivePath := filepath.Join(SystemBackupTempDir, systemBackup.Name+types.SystemBackupExtension)
	tempBackupDir := filepath.Join(SystemBackupTempDir, systemBackup.Name)

	switch systemBackup.Status.State {
	case longhorn.SystemBackupStateSyncing:
		err = syncSystemBackupFromBackupTarget(systemBackup, backupTargetClient)
		if err != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				longhorn.SystemBackupConditionReasonSync, SystemBackupErrSync,
			)
			return err
		}

		c.updateSystemBackupRecord(record,
			systemBackupRecordTypeNormal, longhorn.SystemBackupStateReady,
			constant.EventReasonSynced, SystemBackupMsgSyncedBackupTarget,
		)

	case longhorn.SystemBackupStateNone:
		// Sync the ready SystemBackups that are not in the current cluster.
		if isSystemBackupFromRemoteBackupTarget(systemBackup) {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeNormal, longhorn.SystemBackupStateSyncing,
				constant.EventReasonSyncing, SystemBackupMsgSyncingBackupTarget,
			)

			return
		}

		var longhornVersion string
		longhornVersion, err = getSystemBackupVersionExistInRemoteBackupTarget(systemBackup, backupTargetClient)
		if err != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				constant.EventReasonStart, err.Error(),
			)
			return nil
		}

		if longhornVersion != "" {
			if err := datastore.LabelSystemBackupVersion(longhornVersion, systemBackup); err != nil {
				return err
			}

			_, err = c.ds.UpdateSystemBackup(systemBackup)
			if err != nil {
				return err
			}
			return
		}

		err = c.InitSystemBackup(systemBackup, log)
		if err != nil {
			return err
		}

		c.updateSystemBackupRecord(record,
			systemBackupRecordTypeNormal, longhorn.SystemBackupStateVolumeBackup,
			constant.EventReasonStart, SystemBackupMsgStarting,
		)

	case longhorn.SystemBackupStateVolumeBackup:
		backups, err := c.BackupVolumes(systemBackup)
		if err != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				constant.EventReasonStart, err.Error(),
			)
			return nil
		}

		// TODO: handle error check
		go c.WaitForVolumeBackupToComplete(backups, systemBackup) // nolint: errcheck

	case longhorn.SystemBackupStateBackingImageBackup:
		backupBackingImages, err := c.BackupBackingImage()
		if err != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				constant.EventReasonStart, err.Error(),
			)
			return nil
		}

		go c.WaitForBackingImageBackupToComplete(backupBackingImages, systemBackup)

	case longhorn.SystemBackupStateGenerating:
		if _, running := c.generatingSystemBackups.LoadOrStore(systemBackup.Name, struct{}{}); !running {
			go c.GenerateSystemBackup(systemBackup, tempBackupArchivePath, tempBackupDir)
		}

	case longhorn.SystemBackupStateUploading:
		if _, running := c.uploadingSystemBackups.LoadOrStore(systemBackup.Name, struct{}{}); !running {
			go c.UploadSystemBackup(systemBackup, tempBackupArchivePath, tempBackupDir, backupTargetClient)
		}

	case longhorn.SystemBackupStateReady, longhorn.SystemBackupStateError:
		cleanupLocalSystemBackupFiles(tempBackupArchivePath, tempBackupDir, log)

	case longhorn.SystemBackupStateDeleting:
		// Generation adds the SystemBackup to the owners of the last backups it
		// archives, and an upload publishes the archive referencing them, so
		// wait for both to stop before releasing them.
		if _, running := c.generatingSystemBackups.Load(systemBackup.Name); running {
			log.Info("Waiting for system backup generation to stop before releasing its volume backups and snapshots")
			c.enqueueAfter(systemBackup, time.Second)
			return nil
		}
		if _, running := c.uploadingSystemBackups.Load(systemBackup.Name); running {
			log.Info("Waiting for system backup upload to stop before releasing its volume backups and snapshots")
			c.enqueueAfter(systemBackup, time.Second)
			return nil
		}
		// An upload can still be running on the manager that owned this
		// SystemBackup before ownership moved. Wait for that upload to stop
		// so it cannot publish the archive after its backups are released.
		uploading, err := c.systemBackupUploadInProgress(systemBackup.Name)
		if err != nil || uploading {
			if err != nil {
				log.WithError(err).Warn("Failed to check the system backup upload")
			} else {
				log.Info("Waiting for system backup upload on another manager to stop before releasing its volume backups and snapshots")
			}
			c.enqueueAfter(systemBackup, time.Second)
			return err
		}
		// Delete the archive before releasing the volume backups it references,
		// so a surviving archive never references deleted backups.
		if err := cleanupRemoteSystemBackupFiles(systemBackup, backupTargetClient, backupTarget, log); err != nil {
			c.enqueueAfter(systemBackup, time.Second)
			return err
		}
		if err := c.cleanupAssociatedBackupAndSnapshotCRs(systemBackup, backupTargetClient, backupTarget); err != nil {
			c.enqueueAfter(systemBackup, time.Second)
			return err
		}

		cleanupLocalSystemBackupFiles(tempBackupArchivePath, tempBackupDir, log)

		err = c.ds.RemoveFinalizerForSystemBackup(systemBackup)
		if err != nil {
			return err
		}
	}

	return nil
}

// cleanupAssociatedBackupAndSnapshotCRs removes the SystemBackup from the owners
// of the Backup and Snapshot CRs it references, and deletes the ones it was the
// last owner of. A Backup reused by a newer SystemBackup with the if-not-present
// policy is kept until that SystemBackup is deleted as well.
func (c *SystemBackupController) cleanupAssociatedBackupAndSnapshotCRs(systemBackup *longhorn.SystemBackup, backupTargetClient engineapi.SystemBackupOperationInterface, backupTarget *longhorn.BackupTarget) error {
	// A CR-only deletion usually leaves the system backup in the backup store,
	// so the volume backups it references must stay restorable. The backup
	// target controller also uses a CR-only deletion when the archive has
	// disappeared from the backup store, and then the references are released.
	deleteCROnly, err := datastore.IsLabelLonghornDeleteCustomResourceOnlyExisting(systemBackup)
	if err != nil {
		return err
	}
	if !isBackupTargetConfigured(backupTarget) {
		// The archives cannot be checked. Keep the children and remove the
		// finalizer; waiting for them would block deletion forever.
		c.logger.Warnf("Keeping the backups and snapshots of system backup %v since the default backup target is not configured", systemBackup.Name)
		return nil
	}
	if deleteCROnly {
		// A listing failure is returned so the presence check is retried
		// before the finalizer is removed.
		archiveExists, err := isSystemBackupArchivePresent(systemBackup.Name, backupTargetClient)
		if err != nil {
			return err
		}
		if archiveExists {
			return nil
		}
	}

	selector := labels.SelectorFromSet(types.GetSystemBackupLabels())
	errs := multierr.NewMultiError()

	// Delete backups before snapshots: a backup that is still in progress reads
	// from its snapshot. Stop on backup-cleanup errors so a failed Backup DELETE
	// cannot leave snapshot cleanup free to remove the source snapshot.
	c.releaseAssociatedBackups(systemBackup.Name, selector, backupTargetClient, backupTarget, errs)
	if len(errs) > 0 {
		return fmt.Errorf("failed to clean up backups of system backup %v: %v", systemBackup.Name, errs.Error())
	}
	// An accepted Backup DELETE can still leave the CR and its finalizer while
	// the backup is in progress. Wait until those last-owner backups disappear
	// before deleting their snapshots. Shared children with other owners are
	// retained and are not waited on.
	pending, err := c.hasLastOwnerBackups(systemBackup.Name, selector)
	if err != nil {
		return err
	}
	if pending {
		return fmt.Errorf("waiting for last-owner backups of system backup %v to be deleted", systemBackup.Name)
	}

	c.releaseAssociatedSnapshots(systemBackup.Name, selector, errs)
	if len(errs) > 0 {
		return fmt.Errorf("failed to clean up snapshots of system backup %v: %v", systemBackup.Name, errs.Error())
	}
	pending, err = c.hasLastOwnerSnapshots(systemBackup.Name, selector)
	if err != nil {
		return err
	}
	if pending {
		return fmt.Errorf("waiting for last-owner snapshots of system backup %v to be deleted", systemBackup.Name)
	}
	return nil
}

// releaseAssociatedBackups deletes a backup only when no system backup archive
// in the backup target references it. Archives that this cluster does not track
// with owners, such as those of another cluster sharing the backup target or
// those kept by a CR-only deletion, become the owners of the backups they
// reference, so the backups are released when those archives are deleted.
func (c *SystemBackupController) releaseAssociatedBackups(systemBackupName string, selector labels.Selector, backupTargetClient engineapi.SystemBackupOperationInterface, backupTarget *longhorn.BackupTarget, errs multierr.MultiError) {
	backups, err := c.ds.ListBackupsWithSelectorRO(selector)
	if err != nil {
		errs.Append("backups", err)
		return
	}

	var archiveReferences map[string][]string
	for _, backup := range backups {
		currentOwners := getSystemBackupOwners(backup.Annotations, backup.Status.Owners)
		if backup.DeletionTimestamp != nil || !slices.Contains(currentOwners, systemBackupName) {
			continue
		}

		owners := removeOwner(currentOwners, systemBackupName)
		if len(owners) == 0 {
			if !isBackupTargetConfigured(backupTarget) {
				// The archives cannot be checked, so the backup is kept.
				c.logger.Warnf("Keeping backup %v of system backup %v since the default backup target is not configured", backup.Name, systemBackupName)
				continue
			}
			if archiveReferences == nil {
				archiveReferences, err = c.getUntrackedArchiveBackupReferences(backupTargetClient, backupTarget, systemBackupName)
				if err != nil {
					errs.Append("backups", err)
					return
				}
			}
			owners = archiveReferences[backup.Name]
		}
		if len(owners) == 0 {
			err = c.ds.DeleteBackupWithResourceVersionPrecondition(backup.Name, backup.ResourceVersion)
		} else {
			backup = backup.DeepCopy()
			backup.Status.Owners = owners
			_, err = c.ds.UpdateBackupStatus(backup)
		}
		if err != nil && !apierrors.IsNotFound(err) {
			errs.Append("backups", err)
		}
	}
}

func (c *SystemBackupController) releaseAssociatedSnapshots(systemBackupName string, selector labels.Selector, errs multierr.MultiError) {
	snapshots, err := c.ds.ListSnapshotsRO(selector)
	if err != nil {
		errs.Append("snapshots", err)
		return
	}
	for _, snapshot := range snapshots {
		currentOwners := getSystemBackupOwners(snapshot.Annotations, snapshot.Status.Owners)
		if snapshot.DeletionTimestamp != nil || !slices.Contains(currentOwners, systemBackupName) {
			continue
		}

		owners := removeOwner(currentOwners, systemBackupName)
		if len(owners) == 0 {
			err = c.ds.DeleteSnapshotWithResourceVersionPrecondition(snapshot.Name, snapshot.ResourceVersion)
		} else {
			snapshot = snapshot.DeepCopy()
			snapshot.Status.Owners = owners
			_, err = c.ds.UpdateSnapshotStatus(snapshot)
		}
		if err != nil && !apierrors.IsNotFound(err) {
			errs.Append("snapshots", err)
		}
	}
}

// hasLastOwnerBackups reports whether a Backup that has this SystemBackup as its
// sole remaining owner still exists. One uncached list is used so a deletion
// pass does not read every Backup individually. Shared children are
// deliberately retained and ignored here.
func (c *SystemBackupController) hasLastOwnerBackups(systemBackupName string, selector labels.Selector) (bool, error) {
	backups, err := c.ds.ListLonghornBackupsWithSelectorUncached(selector)
	if err != nil {
		return false, err
	}
	for i := range backups {
		backup := &backups[i]
		if isLastOwner(getSystemBackupOwners(backup.Annotations, backup.Status.Owners), systemBackupName) {
			return true, nil
		}
	}
	return false, nil
}

// hasLastOwnerSnapshots reports whether a Snapshot that has this SystemBackup
// as its sole remaining owner is still in use. One uncached list is used.
// Snapshots that are already being deleted are ignored: SnapshotController can
// retain them while purge is disabled or the snapshot is the parent of
// volume-head, and that finalization is independent of this SystemBackup.
// Shared children are deliberately retained and ignored here.
func (c *SystemBackupController) hasLastOwnerSnapshots(systemBackupName string, selector labels.Selector) (bool, error) {
	snapshots, err := c.ds.ListLonghornSnapshotsWithSelectorUncached(selector)
	if err != nil {
		return false, err
	}
	for i := range snapshots {
		snapshot := &snapshots[i]
		if snapshot.DeletionTimestamp != nil {
			continue
		}
		if isLastOwner(getSystemBackupOwners(snapshot.Annotations, snapshot.Status.Owners), systemBackupName) {
			return true, nil
		}
	}
	return false, nil
}

func isLastOwner(owners []string, systemBackupName string) bool {
	return slices.Contains(owners, systemBackupName) && len(removeOwner(owners, systemBackupName)) == 0
}

// isBackupTargetConfigured returns whether the backup target can still hold
// archives. Unlike isBackupTargetAvailable, a temporarily unavailable backup
// target is considered configured, so the callers retry instead of giving up.
func isBackupTargetConfigured(backupTarget *longhorn.BackupTarget) bool {
	return backupTarget != nil &&
		backupTarget.DeletionTimestamp == nil &&
		backupTarget.Spec.BackupTargetURL != ""
}

// isSystemBackupArchivePresent returns whether the archive of the SystemBackup
// exists in the default backup target.
func isSystemBackupArchivePresent(systemBackupName string, backupTargetClient engineapi.SystemBackupOperationInterface) (bool, error) {
	archives, err := backupTargetClient.ListSystemBackup()
	if err != nil {
		return false, errors.Wrap(err, "failed to list system backups in backup target")
	}
	for name := range archives {
		if string(name) == systemBackupName {
			return true, nil
		}
	}
	return false, nil
}

func removeOwner(owners []string, owner string) []string {
	return slices.DeleteFunc(slices.Clone(owners), func(o string) bool { return o == owner })
}

// getUntrackedArchiveBackupReferences returns the archives in the backup target
// that reference each backup, for the archives without a SystemBackup created by
// this cluster. The archive of the given SystemBackup must have been deleted,
// otherwise an error is returned so its volume backups are kept.
func (c *SystemBackupController) getUntrackedArchiveBackupReferences(backupTargetClient engineapi.SystemBackupOperationInterface, backupTarget *longhorn.BackupTarget, systemBackupName string) (map[string][]string, error) {
	archives, err := backupTargetClient.ListSystemBackup()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list system backups in backup target")
	}

	references := map[string][]string{}
	listedKeys := map[string]bool{}
	for name, uri := range archives {
		archiveName := string(name)
		cacheKey := getArchiveBackupNamesCacheKey(backupTarget, string(uri))
		listedKeys[cacheKey] = true
		if archiveName == systemBackupName {
			return nil, fmt.Errorf("system backup %v still exists in the backup target", systemBackupName)
		}
		systemBackup, err := c.ds.GetSystemBackupRO(archiveName)
		if err == nil && !isSystemBackupFromRemoteBackupTarget(systemBackup) {
			continue
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}

		backupNames, err := c.getArchiveBackupNames(backupTargetClient, archiveName, string(uri), cacheKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read the volume backups referenced by system backup %v in backup target", archiveName)
		}
		for _, backupName := range backupNames {
			references[backupName] = append(references[backupName], archiveName)
		}
	}
	for _, archiveNames := range references {
		slices.Sort(archiveNames)
	}

	c.archiveBackupNamesLock.Lock()
	defer c.archiveBackupNamesLock.Unlock()
	// Entries of archives no longer listed, or of a previous backup target URL,
	// are pruned.
	for key := range c.archiveBackupNames {
		if !listedKeys[key] {
			delete(c.archiveBackupNames, key)
		}
	}
	return references, nil
}

// getArchiveBackupNamesCacheKey identifies an archive by the backup target URL
// and its URI, since the URI is relative to the backup target.
func getArchiveBackupNamesCacheKey(backupTarget *longhorn.BackupTarget, uri string) string {
	return backupTarget.Spec.BackupTargetURL + "\x00" + uri
}

// getArchiveBackupNames returns the last backups of the volumes in an archive,
// which system restore restores the volumes from. The result is cached by the
// backup target URL and its URI, and validated against the archive checksum
// since an archive can be recreated with the same name and version.
func (c *SystemBackupController) getArchiveBackupNames(backupTargetClient engineapi.SystemBackupOperationInterface, archiveName, uri, cacheKey string) ([]string, error) {
	version, _, err := parseSystemBackupURI(uri)
	if err != nil {
		return nil, err
	}

	cfg, err := backupTargetClient.GetSystemBackupConfig(archiveName, version)
	if err != nil {
		return nil, err
	}

	c.archiveBackupNamesLock.Lock()
	entry, cached := c.archiveBackupNames[cacheKey]
	c.archiveBackupNamesLock.Unlock()
	if cached && cfg.Checksum != "" && entry.checksum == cfg.Checksum {
		return entry.backupNames, nil
	}

	tempDir, err := os.MkdirTemp(SystemBackupTempDir, "system-backup-archive-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			c.logger.WithError(err).Warnf("Failed to remove %v", tempDir)
		}
	}()

	archivePath := filepath.Join(tempDir, archiveName+types.SystemBackupExtension)
	if err := backupTargetClient.DownloadSystemBackup(archiveName, version, archivePath); err != nil {
		return nil, err
	}
	backupNames, err := readArchiveVolumeLastBackups(archivePath, archiveName)
	if err != nil {
		return nil, err
	}

	// The checksum is read before the download, so a concurrent recreation
	// makes the next lookup download the archive again.
	c.archiveBackupNamesLock.Lock()
	c.archiveBackupNames[cacheKey] = archiveBackupNamesEntry{checksum: cfg.Checksum, backupNames: backupNames}
	c.archiveBackupNamesLock.Unlock()
	return backupNames, nil
}

func readArchiveVolumeLastBackups(archivePath, archiveName string) ([]string, error) {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()

	volumesPath := filepath.ToSlash(filepath.Join(archiveName, types.SystemBackupSubDirYaml, types.SystemBackupSubDirLonghorn, "volumes.yaml"))
	file, err := reader.Open(volumesPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()
	contents, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err := longhorn.AddToScheme(scheme); err != nil {
		return nil, err
	}
	obj, _, err := serializer.NewCodecFactory(scheme).UniversalDeserializer().Decode(contents, nil, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode %v", volumesPath)
	}
	volumeList, ok := obj.(*longhorn.VolumeList)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T in %v", obj, volumesPath)
	}

	backupNames := []string{}
	seen := map[string]bool{}
	for i := range volumeList.Items {
		backupName, err := getVolumeRestoreSourceBackupName(&volumeList.Items[i])
		if err != nil {
			return nil, err
		}
		if backupName == "" || seen[backupName] {
			continue
		}
		seen[backupName] = true
		backupNames = append(backupNames, backupName)
	}
	return backupNames, nil
}

func systemBackupInFinalState(sb *longhorn.SystemBackup) bool {
	// The state SystemBackupStateDeleting, as a final state, will clean up the system backup files locally and remotely if necessary,
	// and the system backup will be deleted once the finalizer is removed.
	return sb.Status.State == longhorn.SystemBackupStateDeleting ||
		sb.Status.State == longhorn.SystemBackupStateReady ||
		sb.Status.State == longhorn.SystemBackupStateError
}

func getSystemBackupVersionExistInRemoteBackupTarget(systemBackup *longhorn.SystemBackup, backupTargetClient engineapi.SystemBackupOperationInterface) (string, error) {
	systemBackupsInBackupstore, err := backupTargetClient.ListSystemBackup()
	if err != nil {
		return "", errors.Wrap(err, "failed to list system backups in backup target")
	}

	for name, uri := range systemBackupsInBackupstore {
		if string(name) != systemBackup.Name {
			continue
		}

		longhornVersion, _, err := parseSystemBackupURI(string(uri))
		if err != nil {
			return "", errors.Wrapf(err, "failed to parse system backup URI: %v", uri)
		}

		return longhornVersion, nil
	}
	return "", nil
}

func isSystemBackupFromRemoteBackupTarget(systemBackup *longhorn.SystemBackup) bool {
	return systemBackup.Labels != nil && systemBackup.Labels[types.GetVersionLabelKey()] != ""
}

func syncSystemBackupFromBackupTarget(systemBackup *longhorn.SystemBackup, backupTargetClient engineapi.SystemBackupOperationInterface) error {
	if systemBackup.Labels == nil {
		err := fmt.Errorf("missing %v label", types.GetVersionLabelKey())
		return errors.Wrapf(err, SystemBackupErrSync)
	}

	longhornVersion := systemBackup.Labels[types.GetVersionLabelKey()]
	if longhornVersion == "" {
		err := fmt.Errorf("missing %v label value", types.GetVersionLabelKey())
		return errors.Wrapf(err, SystemBackupErrSync)
	}

	systemBackupCfg, err := backupTargetClient.GetSystemBackupConfig(systemBackup.Name, longhornVersion)
	if err != nil {
		return err
	}

	systemBackup.Status.Version = systemBackupCfg.LonghornVersion
	systemBackup.Status.GitCommit = systemBackupCfg.LonghornGitCommit
	systemBackup.Status.ManagerImage = systemBackupCfg.ManagerImage
	systemBackup.Status.CreatedAt = metav1.Time{Time: systemBackupCfg.CreatedAt}
	return nil
}

func (c *SystemBackupController) InitSystemBackup(systemBackup *longhorn.SystemBackup, log logrus.FieldLogger) error {
	systemBackup.Status.Version = meta.Version
	systemBackup.Status.GitCommit = meta.GitCommit
	return nil
}

func (c *SystemBackupController) UploadSystemBackup(systemBackup *longhorn.SystemBackup, archievePath, tempDir string, backupTargetClient engineapi.SystemBackupOperationInterface) {
	log := getLoggerForSystemBackup(c.logger, systemBackup)

	var recordErr error
	var deleting bool
	existingSystemBackup := systemBackup.DeepCopy()
	// Handle the CR status update here because this method is called by a separate goroutine.
	defer func() {
		// Mark upload as stopped before the status update, so the reconcile
		// it triggers can start upload again or release the references.
		c.uploadingSystemBackups.Delete(systemBackup.Name)
		if clearErr := c.clearSystemBackupUploading(systemBackup.Name); clearErr != nil {
			log.WithError(clearErr).Warn("Failed to clear the system backup uploading annotation")
		}

		if deleting {
			// The SystemBackup is in the Deleting state now.
			return
		}

		record := &systemBackupRecord{}
		if recordErr != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				longhorn.SystemBackupConditionReasonUpload, recordErr.Error(),
			)
		} else {
			systemBackup.Status.ManagerImage = c.managerImage
			systemBackup.Status.CreatedAt = metav1.Time{Time: time.Now().UTC()}

			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeNormal, longhorn.SystemBackupStateReady,
				longhorn.SystemBackupConditionReasonUpload, SystemBackupMsgUploadBackupTarget,
			)
		}

		c.handleStatusUpdate(record, systemBackup, existingSystemBackup, recordErr, log)
	}()

	defaultEngineImage, err := c.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if err != nil {
		recordErr = errors.Wrapf(err, SystemBackupErrGetFmt, "default engine image")
		return
	}

	timer := time.NewTimer(datastore.SystemBackupTimeout)
	defer timer.Stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var systemBackupCfg *systembackupstore.Config
	for {
		select {
		case <-timer.C:
			recordErr = errors.Wrap(err, SystemBackupErrTimeoutUpload)
			return
		case <-ticker.C:
			// Do not publish the archive once deletion begins, and remove an
			// archive published by an upload that raced with deletion.
			current, getErr := c.ds.GetLonghornSystemBackupUncached(systemBackup.Name)
			if apierrors.IsNotFound(getErr) || (getErr == nil && current.DeletionTimestamp != nil) {
				if _, delErr := backupTargetClient.DeleteSystemBackup(systemBackup); delErr != nil && !types.ErrorIsNotFound(delErr) {
					log.WithError(delErr).Warn("Failed to delete the system backup published while it was being deleted")
				}
				log.Info("Stopping system backup upload since the system backup is being deleted")
				deleting = true
				return
			}
			if getErr != nil {
				log.WithError(getErr).Warn("Failed to confirm the system backup is not being deleted")
				continue
			}

			systemBackupCfg, err = backupTargetClient.GetSystemBackupConfig(systemBackup.Name, systemBackup.Status.Version)
			if err != nil && !types.ErrorIsNotFound(err) {
				err = errors.Wrap(err, SystemBackupErrGetConfig)
				continue
			}
			if systemBackupCfg != nil {
				return
			}

			// Refresh the uploading annotation before the upload so another
			// manager keeps waiting until this attempt finishes.
			if err := c.markSystemBackupUploading(systemBackup.Name); err != nil {
				log.WithError(err).Warn("Failed to record the system backup upload")
				continue
			}

			_, err = backupTargetClient.UploadSystemBackup(systemBackup.Name, archievePath, systemBackup.Status.Version, systemBackup.Status.GitCommit, c.managerImage, defaultEngineImage)
			if err != nil {
				if types.ErrorAlreadyExists(err) {
					err = nil
				}

				log.WithError(err).Warnf("Failed to upload %v to backup target", archievePath)
			}
		}
	}
}

// markSystemBackupUploading records that this manager is uploading the
// SystemBackup. The annotation is cleared when the upload stops.
func (c *SystemBackupController) markSystemBackupUploading(name string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		systemBackup, err := c.ds.GetLonghornSystemBackupUncached(name)
		if err != nil {
			return err
		}
		if systemBackup.Annotations == nil {
			systemBackup.Annotations = map[string]string{}
		}
		systemBackup.Annotations[types.SystemBackupAnnotationUploading] = systemBackupUploadingAnnotationValue(c.controllerID, time.Now())
		_, err = c.ds.UpdateSystemBackup(systemBackup)
		return err
	})
}

// clearSystemBackupUploading removes this manager's uploading annotation.
func (c *SystemBackupController) clearSystemBackupUploading(name string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		systemBackup, err := c.ds.GetLonghornSystemBackupUncached(name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		owner, _, ok := parseSystemBackupUploadingAnnotation(systemBackup.Annotations)
		if !ok || owner != c.controllerID {
			return nil
		}
		delete(systemBackup.Annotations, types.SystemBackupAnnotationUploading)
		_, err = c.ds.UpdateSystemBackup(systemBackup)
		return err
	})
}

// systemBackupUploadInProgress reports whether an upload is still running on
// this or another manager. A record older than the upload timeout is stale:
// that manager's upload loop has exited or its process is gone.
func (c *SystemBackupController) systemBackupUploadInProgress(name string) (bool, error) {
	systemBackup, err := c.ds.GetLonghornSystemBackupUncached(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	owner, at, ok := parseSystemBackupUploadingAnnotation(systemBackup.Annotations)
	if !ok {
		return false, nil
	}
	if time.Since(at) > datastore.SystemBackupTimeout {
		return false, nil
	}
	if owner == c.controllerID {
		_, running := c.uploadingSystemBackups.Load(name)
		return running, nil
	}
	return true, nil
}

func systemBackupUploadingAnnotationValue(controllerID string, at time.Time) string {
	return controllerID + "|" + at.UTC().Format(time.RFC3339Nano)
}

func parseSystemBackupUploadingAnnotation(annotations map[string]string) (controllerID string, at time.Time, ok bool) {
	owner, stamp, found := strings.Cut(annotations[types.SystemBackupAnnotationUploading], "|")
	if !found || owner == "" {
		return "", time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return "", time.Time{}, false
	}
	return owner, at, true
}

// cleanupRemoteSystemBackupFiles deletes the archive in the backup target and
// confirms it is gone. An error is returned so the deletion is retried before
// the volume backups it references are released.
func cleanupRemoteSystemBackupFiles(systemBackup *longhorn.SystemBackup, backupTargetClient engineapi.SystemBackupOperationInterface, backupTarget *longhorn.BackupTarget, log logrus.FieldLogger) error {
	if systemBackup.Status.Version == "" {
		// The backup store sync might not have finished
		return nil
	}
	needsCleanupRemoteData, err := checkIfRemoteDataCleanupIsNeeded(systemBackup, backupTarget)
	if err != nil {
		return errors.Wrap(err, "failed to check if it needs to delete remote system backup data")
	}
	if !needsCleanupRemoteData {
		deleteCROnly, err := datastore.IsLabelLonghornDeleteCustomResourceOnlyExisting(systemBackup)
		if err != nil {
			return errors.Wrap(err, "failed to check if it needs to delete remote system backup data")
		}
		// A configured but unavailable target cannot be listed reliably. Normal
		// deletion retries instead of releasing backups that the surviving
		// archive still references. A CR-only deletion keeps the remote data.
		if !deleteCROnly && isBackupTargetConfigured(backupTarget) {
			return fmt.Errorf("waiting for the default backup target to become available before deleting system backup %v", systemBackup.Name)
		}
		return nil
	}

	systemBackupsFromBackupTarget, err := backupTargetClient.ListSystemBackup()
	if err != nil {
		return errors.Wrap(err, "failed to list system backups in backup target")
	}

	if _, exist := systemBackupsFromBackupTarget[systembackupstore.Name(systemBackup.Name)]; !exist {
		return nil
	}

	_, err = backupTargetClient.DeleteSystemBackup(systemBackup)
	if err != nil && !types.ErrorIsNotFound(err) {
		return errors.Wrapf(err, "failed to delete system backup %v in backup target", systemBackup.Name)
	}

	systemBackupCfg, err := backupTargetClient.GetSystemBackupConfig(systemBackup.Name, systemBackup.Status.Version)
	if err != nil && !types.ErrorIsNotFound(err) {
		return errors.Wrap(err, SystemBackupErrGetConfig)
	}

	if systemBackupCfg != nil {
		return fmt.Errorf("system backup %v is still being deleted in backup target", systemBackup.Name)
	}

	log.Info("Deleted system backup in backup target")
	return nil
}

type systemBackupMeta struct {
	LonghornVersion       string      `json:"longhornVersion"`
	LonghornGitCommit     string      `json:"longhornGitCommit"`
	KubernetesVersion     string      `json:"kubernetesVersion"`
	LonghornNamespaceUUID string      `json:"longhornNamspaceUUID"`
	SystemBackupCreatedAt metav1.Time `json:"systemBackupCreatedAt"`
	ManagerImage          string      `json:"managerImage"`
}

func (c *SystemBackupController) newSystemBackupMeta(systemBackup *longhorn.SystemBackup) (*systemBackupMeta, error) {
	namespace, err := c.ds.GetNamespace(systemBackup.Namespace)
	if err != nil {
		return nil, err
	}

	kubeVersion, err := c.ds.GetKubernetesVersion()
	if err != nil {
		return nil, err
	}

	return &systemBackupMeta{
		LonghornVersion:       meta.Version,
		LonghornGitCommit:     meta.GitCommit,
		KubernetesVersion:     kubeVersion.GitVersion,
		LonghornNamespaceUUID: string(namespace.UID),
		SystemBackupCreatedAt: systemBackup.Status.CreatedAt,
		ManagerImage:          c.managerImage,
	}, nil
}

func (c *SystemBackupController) GenerateSystemBackup(systemBackup *longhorn.SystemBackup, archievePath, tempDir string) {
	log := getLoggerForSystemBackup(c.logger, systemBackup)

	var err error
	var errMessage string
	existingSystemBackup := systemBackup.DeepCopy()
	defer func() {
		// Mark generation as stopped before the status update, so the reconcile
		// it triggers can start generation again or release the references.
		c.generatingSystemBackups.Delete(systemBackup.Name)

		record := &systemBackupRecord{}
		if err != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				longhorn.SystemBackupConditionReasonGenerate, errMessage,
			)
		} else {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeNormal, longhorn.SystemBackupStateUploading,
				longhorn.SystemBackupConditionReasonGenerate, fmt.Sprintf(SystemBackupMsgCreatedArchieveFmt, archievePath),
			)
		}

		c.handleStatusUpdate(record, systemBackup, existingSystemBackup, err, log)
	}()

	systemBackupMeta, err := c.newSystemBackupMeta(systemBackup)
	if err != nil {
		errMessage = fmt.Sprint(errors.Wrapf(err, SystemBackupErrGetFmt, "system backup meta"))
	}

	err = os.MkdirAll(tempDir, os.FileMode(0755))
	if err != nil {
		errMessage = fmt.Sprint(errors.Wrap(err, SystemBackupErrMkdir))
		return
	}

	err = c.generateSystemBackup(systemBackup.Name, systemBackupMeta, tempDir)
	if err != nil {
		errMessage = fmt.Sprint(errors.Wrap(err, SystemBackupErrGenerate))
		return
	}

	cmd := exec.Command("zip", "-r", filepath.Base(archievePath), systemBackup.Name)
	cmd.Dir = filepath.Dir(tempDir)
	err = cmd.Run()
	if err != nil {
		errMessage = fmt.Sprint(errors.Wrap(err, SystemBackupErrArchive))
		return
	}

	_, err = os.Stat(archievePath)
	if err != nil {
		errMessage = fmt.Sprint(errors.Wrap(err, SystemBackupErrOSStat))
		return
	}
}

func (c *SystemBackupController) BackupVolumes(systemBackup *longhorn.SystemBackup) (map[string]*longhorn.Backup, error) {
	switch systemBackup.Spec.VolumeBackupPolicy {
	case longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent:
		return c.backupVolumesIfNotPresent(systemBackup)
	case longhorn.SystemBackupCreateVolumeBackupPolicyAlways:
		return c.backupVolumesAlways(systemBackup)
	}
	return nil, nil
}

func (c *SystemBackupController) backupVolumesAlways(systemBackup *longhorn.SystemBackup) (map[string]*longhorn.Backup, error) {
	volumes, err := c.ds.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	volumeBackups := make(map[string]*longhorn.Backup, len(volumes))
	for _, volume := range volumes {
		// Don't need to create volume data backup for DR volumes since it will
		// be restored from the source volume's backup.
		if volume.Status.IsStandby {
			c.logger.Infof("Skip backup for standby volume %v", volume.Name)
			continue
		}

		if volume.Spec.CloneMode == longhorn.CloneModeLinkedClone {
			c.logger.Infof("Skip backup for linked-clone volume %v", volume.Name)
			continue
		}

		volumeBackupName := bsutil.GenerateName("system-backup")

		snapshot, err := c.createVolumeSnapshot(ctx, volume, volumeBackupName, systemBackup.Name)
		if err != nil {
			return nil, err
		}

		backup, err := c.createVolumeBackupFromSnapshot(volume, snapshot, systemBackup.Name)
		if err != nil {
			return nil, err
		}

		volumeBackups[backup.Name] = backup
	}

	return volumeBackups, nil
}

func (c *SystemBackupController) backupVolumesIfNotPresent(systemBackup *longhorn.SystemBackup) (map[string]*longhorn.Backup, error) {
	volumes, err := c.ds.ListVolumesRO()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	volumeBackups := make(map[string]*longhorn.Backup, len(volumes))
	for _, volume := range volumes {
		// Don't need to create volume data backup for DR volumes since it will
		// be restored from the source volume's backup.
		if volume.Status.IsStandby {
			c.logger.Infof("Skip backup for standby volume %v", volume.Name)
			continue
		}

		if volume.Spec.CloneMode == longhorn.CloneModeLinkedClone {
			c.logger.Infof("Skip backup for linked-clone volume %v", volume.Name)
			continue
		}

		volumeBackupName := bsutil.GenerateName("system-backup")

		snapshot, err := c.createVolumeSnapshot(ctx, volume, volumeBackupName, systemBackup.Name)
		if err != nil {
			return nil, err
		}

		isUpToDate, err := c.isVolumeBackupUpToDate(volume, systemBackup)
		if err != nil {
			return nil, err
		}

		if isUpToDate {
			if err := c.addOwnerToVolumeLastBackup(volume, systemBackup.Name); err != nil {
				return nil, errors.Wrapf(err, "failed to add system backup %v to the owners of the last backup %v of volume %v", systemBackup.Name, volume.Status.LastBackup, volume.Name)
			}

			// When the setting is disabled, the snapshot is kept until this
			// SystemBackup is deleted.
			if c.isAutoCleanupSnapshotEnabled() {
				if err := c.ds.DeleteSnapshot(snapshot.Name); err != nil && !apierrors.IsNotFound(err) {
					c.logger.WithError(err).Warnf("Failed to clean up intermediate snapshot %v for volume %v", snapshot.Name, volume.Name)
				}
			}
			continue
		}

		backup, err := c.createVolumeBackupFromSnapshot(volume, snapshot, systemBackup.Name)
		if err != nil {
			return nil, err
		}

		volumeBackups[backup.Name] = backup
	}

	return volumeBackups, nil
}

func (c *SystemBackupController) WaitForVolumeBackupToComplete(backups map[string]*longhorn.Backup, systemBackup *longhorn.SystemBackup) (err error) {
	log := getLoggerForSystemBackup(c.logger, systemBackup)

	existingSystemBackup := systemBackup.DeepCopy()
	defer func() {
		record := &systemBackupRecord{}
		if err != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				constant.EventReasonStart, err.Error(),
			)
		} else {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeNormal, longhorn.SystemBackupStateBackingImageBackup,
				constant.EventReasonStart, SystemBackupMsgStarting,
			)
		}

		c.handleStatusUpdate(record, systemBackup, existingSystemBackup, err, log)
	}()

	startTime := time.Now()
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for range ticker.C {
		for name := range backups {
			// Retrieve the latest backup
			var backup *longhorn.Backup
			backup, err = c.ds.GetBackupRO(name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Warnf("Backup %v not found when checking the volume backup status in system backup", name)
					continue
				}
				break
			}

			switch backup.Status.State {
			case longhorn.BackupStateCompleted:
				if !c.isVolumeLastBackupSynced(backup) {
					continue
				}

				if c.isAutoCleanupSnapshotEnabled() {
					c.deleteOutdatedSystemBackupSnapshots(backup, log)
				}

				delete(backups, name)
			case longhorn.BackupStateError:
				return errors.Wrapf(fmt.Errorf("%s", backup.Status.Error), "failed creating Volume backup %v", name)
			}
		}

		if len(backups) == 0 {
			return
		}

		// Return error when Volume backup exceeds timeout
		if time.Since(startTime) > datastore.VolumeBackupTimeout {
			return fmt.Errorf("timed out waiting for Volume backups to complete")
		}
	}
	// This should never be reached, return this error just in case.
	return fmt.Errorf("unexpected error: stopped waiting for Volume backups without completing, failing or timing out")
}

// isAutoCleanupSnapshotEnabled falls back to disabled, which keeps the
// snapshots until the SystemBackups that own them are deleted.
func (c *SystemBackupController) isAutoCleanupSnapshotEnabled() bool {
	enabled, err := c.ds.GetSettingAsBool(types.SettingNameAutoCleanupRecurringJobBackupSnapshot)
	if err != nil {
		c.logger.WithError(err).Warnf("Failed to get setting %v, fallback to disabled", types.SettingNameAutoCleanupRecurringJobBackupSnapshot)
		return false
	}
	return enabled
}

// deleteOutdatedSystemBackupSnapshots deletes the snapshots that SystemBackups
// created for the volume before the snapshot of the completed backup. That
// snapshot is kept because isVolumeBackupUpToDate compares the volume against
// it to decide whether the if-not-present policy can reuse the backup.
func (c *SystemBackupController) deleteOutdatedSystemBackupSnapshots(backup *longhorn.Backup, log logrus.FieldLogger) {
	volumeName := getBackupVolumeName(backup)
	snapshots, err := c.ds.ListVolumeSnapshotsRO(volumeName)
	if err != nil {
		log.WithError(err).Warnf("Failed to list snapshots of volume %v", volumeName)
		return
	}

	latestSnapshot, exists := snapshots[backup.Status.SnapshotName]
	if !exists {
		return
	}
	latestCreationTime, err := time.Parse(time.RFC3339, latestSnapshot.Status.CreationTime)
	if err != nil {
		log.WithError(err).Warnf("Failed to parse creation time %q of snapshot %v", latestSnapshot.Status.CreationTime, latestSnapshot.Name)
		return
	}

	for _, snapshot := range snapshots {
		if snapshot.Name == latestSnapshot.Name || snapshot.DeletionTimestamp != nil || !types.IsCreatedBySystemBackup(snapshot.Labels) {
			continue
		}
		creationTime, err := time.Parse(time.RFC3339, snapshot.Status.CreationTime)
		if err != nil || !creationTime.Before(latestCreationTime) {
			continue
		}
		// Delete only after the backup of this snapshot has finished. A missing
		// backup may not be created yet, because another SystemBackup is waiting
		// for the snapshot to become ready, or may not be observed by the cache
		// yet. The snapshot is still owned and is deleted with its SystemBackups.
		snapshotBackup, err := c.ds.GetBackupRO(snapshot.Name)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.WithError(err).Warnf("Failed to get backup of snapshot %v", snapshot.Name)
			}
			continue
		}
		if snapshotBackup.Status.State != longhorn.BackupStateCompleted && snapshotBackup.Status.State != longhorn.BackupStateError {
			continue
		}
		if err := c.ds.DeleteSnapshot(snapshot.Name); err != nil && !apierrors.IsNotFound(err) {
			log.WithError(err).Warnf("Failed to clean up snapshot %v of volume %v", snapshot.Name, volumeName)
		}
	}
}

func getBackupVolumeName(backup *longhorn.Backup) string {
	if backup.Status.VolumeName != "" {
		return backup.Status.VolumeName
	}
	return backup.Labels[types.LonghornLabelBackupVolume]
}

// isVolumeLastBackupSynced reports whether the volume status has caught up
// with a completed volume backup created by this system backup.
//
// The backup store records whichever backup finished uploading most recently
// as the volume's LastBackup. LastBackupAt is not an upload time: it is the
// SnapshotCreatedAt of that LastBackup. When several backups of one volume
// run close together, another backup can take that slot instead of the
// system backup's volume backup. The status still counts as caught up if
// that other backup carries the same or newer data, or if it finished
// uploading after the system backup's volume backup.
func (c *SystemBackupController) isVolumeLastBackupSynced(backup *longhorn.Backup) bool {
	synced, err := c.checkVolumeLastBackupSynced(backup)
	if err != nil {
		c.logger.WithError(err).Warnf("Failed to check volume status for backup %v", backup.Name)
		return false
	}
	return synced
}

// checkVolumeLastBackupSynced returns an error when it cannot decide, such as
// a missing volume or an unparsable timestamp. The caller logs it, treats the
// backup as not synced, and checks again on the next tick.
func (c *SystemBackupController) checkVolumeLastBackupSynced(backup *longhorn.Backup) (bool, error) {
	volumeName := getBackupVolumeName(backup)
	if volumeName == "" {
		return false, fmt.Errorf("backup %v has no volume name in status or labels", backup.Name)
	}
	volume, err := c.ds.GetVolumeRO(volumeName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get volume %v of backup %v", volumeName, backup.Name)
	}

	if volume.Status.LastBackup == backup.Name {
		return true, nil
	}
	if volume.Status.LastBackupAt == "" {
		// Volume status has not been synced from the backup target yet.
		return false, nil
	}

	lastBackupAt, err := parseBackupTimestamp("volume "+volume.Name, "lastBackupAt", volume.Status.LastBackupAt)
	if err != nil {
		return false, err
	}
	snapshotCreatedAt, err := parseBackupTimestamp("backup "+backup.Name, "snapshotCreatedAt", backup.Status.SnapshotCreatedAt)
	if err != nil {
		return false, err
	}
	if !lastBackupAt.Before(snapshotCreatedAt) {
		// A different backup is last, but its snapshot is the same or newer,
		// so it carries this backup's data.
		return true, nil
	}

	// The last backup has an older snapshot than this backup. If it finished
	// before this backup, the volume status is stale and the next sync will
	// name this backup, so keep waiting. If it finished after this backup,
	// the volume status will not change again, so accept it.
	completedAfter, err := c.lastBackupCompletedAfter(volume, backup)
	if err != nil || !completedAfter {
		return false, err
	}
	c.logger.Warnf("Accepting volume %v last backup %v as synced: it finished after backup %v but holds an older snapshot, so a restore from the volume's last backup will carry older data than this system backup",
		volume.Name, volume.Status.LastBackup, backup.Name)
	return true, nil
}

// lastBackupCompletedAfter reports whether the volume's current LastBackup
// finished uploading after the system backup's volume backup. A LastBackup
// that is missing or still uploading is not an error: it returns false and
// the caller keeps waiting.
func (c *SystemBackupController) lastBackupCompletedAfter(volume *longhorn.Volume, backup *longhorn.Backup) (bool, error) {
	lastBackup, err := c.ds.GetBackupRO(volume.Status.LastBackup)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to get last backup %v of volume %v", volume.Status.LastBackup, volume.Name)
	}
	if lastBackup.Status.State != longhorn.BackupStateCompleted || lastBackup.Status.BackupCreatedAt == "" {
		return false, nil
	}

	lastBackupCreatedAt, err := parseBackupTimestamp("backup "+lastBackup.Name, "backupCreatedAt", lastBackup.Status.BackupCreatedAt)
	if err != nil {
		return false, err
	}
	backupCreatedAt, err := parseBackupTimestamp("backup "+backup.Name, "backupCreatedAt", backup.Status.BackupCreatedAt)
	if err != nil {
		return false, err
	}
	return !lastBackupCreatedAt.Before(backupCreatedAt), nil
}

// parseBackupTimestamp wraps util.ParseTimeZ with the owning object and field
// so a parse failure in the wait loop identifies the bad value.
func parseBackupTimestamp(owner, field, value string) (time.Time, error) {
	t, err := util.ParseTimeZ(value)
	return t, errors.Wrapf(err, "failed to parse %s %q of %s", field, value, owner)
}

func (c *SystemBackupController) isVolumeBackupUpToDate(volume *longhorn.Volume, systemBackup *longhorn.SystemBackup) (bool, error) {
	log := getLoggerForSystemBackup(c.logger, systemBackup)
	log = log.WithField("volume", volume.Name)

	// Return early if there is no recorded last backup.
	if volume.Status.LastBackup == "" {
		return false, nil
	}

	// Retrieve last backup and its snapshot.
	lastBackup, err := c.ds.GetBackup(volume.Status.LastBackup)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Warnf("Last Backup %v not found for volume %v", volume.Status.LastBackup, volume.Name)
			return false, nil
		}
		return false, err
	}
	if lastBackup.DeletionTimestamp != nil {
		log.Infof("Last backup %v is being deleted; backup is not up-to-date", lastBackup.Name)
		return false, nil
	}

	lastBackupSnapshot, err := c.ds.GetSnapshot(lastBackup.Status.SnapshotName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Warnf("Snapshot %v not found for backup %v", lastBackup.Status.SnapshotName, lastBackup.Name)
			return false, nil
		}
		return false, err
	}
	if lastBackupSnapshot.DeletionTimestamp != nil {
		log.Infof("Snapshot %v of last backup %v is being deleted; backup is not up-to-date", lastBackupSnapshot.Name, lastBackup.Name)
		return false, nil
	}

	lastBackupTime, err := time.Parse(time.RFC3339, lastBackupSnapshot.Status.CreationTime)
	if err != nil {
		log.WithError(err).Warnf("Failed to parse creation time %q for snapshot %v", lastBackupSnapshot.Status.CreationTime, lastBackupSnapshot.Name)
		return false, nil
	}

	// Identify snapshots created after the last backup.
	snapshots, err := c.ds.ListVolumeSnapshotsRO(volume.Name)
	if err != nil {
		return false, err
	}

	// The last backup contains the data of its snapshot and all earlier snapshots
	// in the same chain. Skip any of these snapshots whose creation time cannot be
	// parsed, such as the first snapshot of a volume restored or cloned on the
	// V1 Data Engine.
	backedUpAncestors := map[string]struct{}{}
	for name := lastBackupSnapshot.Name; name != ""; {
		if _, visited := backedUpAncestors[name]; visited {
			break
		}
		backedUpAncestors[name] = struct{}{}

		ancestor, exists := snapshots[name]
		if !exists {
			break
		}
		name = ancestor.Status.Parent
	}

	for _, snapshot := range snapshots {
		if snapshot.Status.Size == 0 {
			continue
		}

		snapshotTime, err := time.Parse(time.RFC3339, snapshot.Status.CreationTime)
		if err != nil {
			if _, isAncestor := backedUpAncestors[snapshot.Name]; isAncestor {
				log.WithError(err).Debugf("Failed to parse creation time %q for snapshot %v, ignoring it because it is an ancestor of the last backup snapshot %v", snapshot.Status.CreationTime, snapshot.Name, lastBackupSnapshot.Name)
				continue
			}
			log.WithError(err).Warnf("Failed to parse creation time %q for snapshot %v, assuming the volume backup is not up-to-date", snapshot.Status.CreationTime, snapshot.Name)
			return false, nil
		}

		if snapshotTime.After(lastBackupTime) {
			log.WithField("snapshot", snapshot.Name).Infof("Detected new data in snapshot %v after last backup %v", snapshot.Name, lastBackup.Name)
			return false, nil // Backup is not up-to-date
		}
	}

	log.Infof("No snapshots with new data after last backup %v; backup is up-to-date", lastBackup.Name)
	return true, nil // Backup is up-to-date
}

func (c *SystemBackupController) createVolumeBackupFromSnapshot(volume *longhorn.Volume, snapshot *longhorn.Snapshot, systemBackupName string) (backup *longhorn.Backup, err error) {
	backupTargetName := volume.Spec.BackupTargetName
	backupLabels := types.GetSystemBackupLabels()
	backupLabels[types.LonghornLabelBackupTarget] = backupTargetName
	backup = &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        snapshot.Name,
			Labels:      backupLabels,
			Annotations: map[string]string{types.SystemBackupAnnotationCreator: systemBackupName},
		},
		Spec: longhorn.BackupSpec{
			SnapshotName: snapshot.Name,
		},
	}
	backup, err = c.ds.CreateBackup(backup, volume.Name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Volume %v backup %s", volume.Name, snapshot.Name)
	}

	if err := c.addBackupOwner(backup.DeepCopy(), systemBackupName); err != nil {
		return nil, errors.Wrapf(err, "failed to add system backup %v to the owners of backup %v", systemBackupName, backup.Name)
	}
	return backup, nil
}

// addBackupOwner adds the SystemBackup to backup.Status.Owners. A backup that is
// being deleted is rejected, since its remote data may already be removed.
func (c *SystemBackupController) addBackupOwner(backup *longhorn.Backup, systemBackupName string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if backup.DeletionTimestamp != nil {
			return fmt.Errorf("backup %v %w", backup.Name, errSystemBackupOwnedObjectDeleting)
		}
		if slices.Contains(backup.Status.Owners, systemBackupName) {
			return nil
		}
		updated := backup.DeepCopy()
		updated.Status.Owners = addOwner(getSystemBackupOwners(backup.Annotations, backup.Status.Owners), systemBackupName)
		_, err := c.ds.UpdateBackupStatus(updated)
		if !apierrors.IsConflict(err) {
			return err
		}
		// Always reload from the API server after a conflict. getBackup may
		// return the same stale cached object that the API server just rejected.
		latest, getErr := c.ds.GetLonghornBackupUncached(backup.Name)
		if getErr != nil {
			return getErr
		}
		backup = latest
		return err
	})
}

// addSnapshotOwner adds the SystemBackup to snapshot.Status.Owners. A snapshot
// that is being deleted is rejected.
func (c *SystemBackupController) addSnapshotOwner(snapshot *longhorn.Snapshot, systemBackupName string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if snapshot.DeletionTimestamp != nil {
			return fmt.Errorf("snapshot %v %w", snapshot.Name, errSystemBackupOwnedObjectDeleting)
		}
		if slices.Contains(snapshot.Status.Owners, systemBackupName) {
			return nil
		}
		updated := snapshot.DeepCopy()
		updated.Status.Owners = addOwner(getSystemBackupOwners(snapshot.Annotations, snapshot.Status.Owners), systemBackupName)
		_, err := c.ds.UpdateSnapshotStatus(updated)
		if !apierrors.IsConflict(err) {
			return err
		}
		latest, getErr := c.ds.GetLonghornSnapshotUncached(snapshot.Name)
		if getErr != nil {
			return getErr
		}
		snapshot = latest
		return err
	})
}

// getSystemBackupOwners returns the SystemBackups that own a Snapshot or Backup
// CR. Status.Owners is never written empty, because the CR is deleted when its
// last owner is removed, so an empty list means the creator recorded in the
// annotation has not been written to it yet.
func getSystemBackupOwners(annotations map[string]string, statusOwners []string) []string {
	if len(statusOwners) > 0 {
		return statusOwners
	}
	if creator := annotations[types.SystemBackupAnnotationCreator]; creator != "" {
		return []string{creator}
	}
	return nil
}

func addOwner(owners []string, owner string) []string {
	if slices.Contains(owners, owner) {
		return owners
	}
	return append(slices.Clone(owners), owner)
}

// getVolumesAndAddLastBackupOwners returns the volumes to archive. System
// restore restores each volume from its restore-source backup, so the
// SystemBackup is added to the owners of those backups, whatever the volume
// backup policy.
func (c *SystemBackupController) getVolumesAndAddLastBackupOwners(systemBackupName string) (runtime.Object, error) {
	obj, err := c.ds.GetAllLonghornVolumes()
	if err != nil {
		return nil, err
	}
	volumeList, ok := obj.(*longhorn.VolumeList)
	if !ok {
		return nil, fmt.Errorf("BUG: unexpected type %T for the volume list", obj)
	}
	for i := range volumeList.Items {
		volume := &volumeList.Items[i]
		backupName, err := getVolumeRestoreSourceBackupName(volume)
		if err != nil {
			return nil, err
		}
		if backupName == "" {
			continue
		}
		systemBackup, err := c.ds.GetSystemBackupRO(systemBackupName)
		if err != nil {
			return nil, err
		}
		if systemBackup.DeletionTimestamp != nil {
			return nil, fmt.Errorf("system backup %v is being deleted", systemBackupName)
		}
		if err := c.addOwnerToVolumeRestoreSourceBackup(backupName, systemBackupName); err != nil {
			return nil, errors.Wrapf(err, "failed to add system backup %v to the owners of the restore-source backup %v of volume %v", systemBackupName, backupName, volume.Name)
		}
	}
	return obj, nil
}

// getVolumeRestoreSourceBackupName returns the backup System Restore uses for
// the volume. Restore overwrites Spec.FromBackup from Status.LastBackup only
// when LastBackup is set and the volume is not standby; otherwise it preserves
// Spec.FromBackup.
func getVolumeRestoreSourceBackupName(volume *longhorn.Volume) (string, error) {
	if volume.Status.LastBackup != "" && !volume.Status.IsStandby {
		return volume.Status.LastBackup, nil
	}
	if volume.Spec.FromBackup == "" {
		return "", nil
	}
	backupName, _, _, err := backupstore.DecodeBackupURL(volume.Spec.FromBackup)
	if err != nil {
		return "", errors.Wrapf(err, "failed to decode FromBackup of volume %v", volume.Name)
	}
	return backupName, nil
}

// getBackup falls back to the API server when the cache has not observed the
// backup yet, since the volume list being archived is read from the API server.
func (c *SystemBackupController) getBackup(name string) (*longhorn.Backup, error) {
	backup, err := c.ds.GetBackup(name)
	if apierrors.IsNotFound(err) {
		return c.ds.GetLonghornBackupUncached(name)
	}
	return backup, err
}

// addOwnerToVolumeLastBackup adds the SystemBackup to the owners of the
// volume's last backup and its snapshot when the if-not-present policy reuses
// them.
func (c *SystemBackupController) addOwnerToVolumeLastBackup(volume *longhorn.Volume, systemBackupName string) error {
	if volume.Status.LastBackup == "" {
		return nil
	}
	return c.addOwnerToVolumeRestoreSourceBackup(volume.Status.LastBackup, systemBackupName)
}

// addOwnerToVolumeRestoreSourceBackup adds the SystemBackup to the owners of
// the restore-source backup and its snapshot, so deleting the SystemBackup that
// created them does not delete them while this SystemBackup references them.
// A missing or deleting backup is an error. Restore reads only the backup, so
// the snapshot is optional: it may already be removed, for example by
// auto-cleanup-snapshot-after-on-demand-backup-completed.
func (c *SystemBackupController) addOwnerToVolumeRestoreSourceBackup(backupName, systemBackupName string) error {
	backup, err := c.getBackup(backupName)
	if err != nil {
		return err
	}
	if !types.IsCreatedBySystemBackup(backup.Labels) {
		return nil
	}
	if err := c.addBackupOwner(backup, systemBackupName); err != nil {
		return err
	}

	snapshot, err := c.ds.GetSnapshot(backup.Status.SnapshotName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !types.IsCreatedBySystemBackup(snapshot.Labels) || snapshot.DeletionTimestamp != nil {
		return nil
	}
	err = c.addSnapshotOwner(snapshot, systemBackupName)
	if apierrors.IsNotFound(err) || errors.Is(err, errSystemBackupOwnedObjectDeleting) {
		return nil
	}
	return err
}

func (c *SystemBackupController) createVolumeSnapshot(ctx context.Context, volume *longhorn.Volume, volumeSnapshotName string, systemBackupName string) (snapshot *longhorn.Snapshot, err error) {
	snapshotCR := &longhorn.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        volumeSnapshotName,
			Labels:      types.GetSystemBackupLabels(),
			Annotations: map[string]string{types.SystemBackupAnnotationCreator: systemBackupName},
		},
		Spec: longhorn.SnapshotSpec{
			Volume:         volume.Name,
			CreateSnapshot: true,
		},
	}

	snapshot, err = c.ds.CreateSnapshot(snapshotCR)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Volume %v snapshot %s", volume.Name, volumeSnapshotName)
	}

	if err := c.addSnapshotOwner(snapshot.DeepCopy(), systemBackupName); err != nil {
		return nil, errors.Wrapf(err, "failed to add system backup %v to the owners of snapshot %v", systemBackupName, snapshot.Name)
	}

	if datastore.SkipListerCheck {
		return snapshot, nil
	}

	// Poll until snapshot is ready or timeout exceeded.
	timeout := time.After(datastore.SystemBackupTimeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, errors.New(SystemBackupErrTimeoutSnapshot)
		case <-ticker.C:
			snapshot, err = c.ds.GetSnapshot(snapshot.Name)
			if err != nil {
				return nil, err
			}

			if snapshot.Status.ReadyToUse {
				return snapshot, nil
			}
		}
	}
}

func (c *SystemBackupController) BackupBackingImage() (map[string]*longhorn.BackupBackingImage, error) {
	backingImages, err := c.ds.ListBackingImagesRO()
	if err != nil {
		return nil, err
	}

	backingImageBackups := make(map[string]*longhorn.BackupBackingImage, len(backingImages))
	for _, backingImage := range backingImages {
		// TODO: support backup backing image v2
		if types.IsDataEngineV2(backingImage.Spec.DataEngine) {
			continue
		}
		backupBackingImage, err := c.createBackingImageBackup(backingImage)
		if err != nil {
			return nil, err
		}

		backingImageBackups[backupBackingImage.Name] = backupBackingImage
	}

	return backingImageBackups, nil
}

func (c *SystemBackupController) WaitForBackingImageBackupToComplete(backupBackingImages map[string]*longhorn.BackupBackingImage, systemBackup *longhorn.SystemBackup) {
	var err error
	log := getLoggerForSystemBackup(c.logger, systemBackup)

	existingSystemBackup := systemBackup.DeepCopy()
	defer func() {
		record := &systemBackupRecord{}
		if err != nil {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeError, longhorn.SystemBackupStateError,
				constant.EventReasonStart, err.Error(),
			)
		} else {
			c.updateSystemBackupRecord(record,
				systemBackupRecordTypeNormal, longhorn.SystemBackupStateGenerating,
				constant.EventReasonStart, SystemBackupMsgStarting,
			)
		}

		c.handleStatusUpdate(record, systemBackup, existingSystemBackup, err, log)
	}()

	startTime := time.Now()
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for range ticker.C {
		for name := range backupBackingImages {
			// Retrieve the latest backup
			var backupBackingImage *longhorn.BackupBackingImage
			backupBackingImage, err = c.ds.GetBackupBackingImageRO(name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Warnf("Backup backing image %v not found when checking the backing image backup status in system backup", name)
					continue
				}
				break
			}

			switch backupBackingImage.Status.State {
			case longhorn.BackupStateCompleted:
				delete(backupBackingImages, name)
			case longhorn.BackupStateError:
				log.Warnf("Failed to create BackingImage backup %v: %v", name, backupBackingImage.Status.Error)
				return
			}
		}

		if len(backupBackingImages) == 0 {
			return
		}

		// Return when BackingImage backup exceeds timeout
		if time.Since(startTime) > datastore.BackingImageBackupTimeout {
			log.Warn("Timed out waiting for BackingImage backups to complete")
			return
		}
	}
	// This should never be reached.
	log.Warn("Unexpected error: stopped waiting for BackingImage backups without completing, failing or timing out")
}

func (c *SystemBackupController) createBackingImageBackup(backingImage *longhorn.BackingImage) (backupBackingImage *longhorn.BackupBackingImage, err error) {
	backupTargetName := types.DefaultBackupTargetName
	backingImageName := backingImage.Name
	backupBackingImage, err = c.ds.GetBackupBackingImagesWithBackupTargetNameRO(backupTargetName, backingImageName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to get backup backing image %v", backingImageName)
		}
	}

	if backupBackingImage == nil {
		backupBackingImageName := types.GetBackupBackingImageNameFromBIName(backingImageName)
		backupBackingImage = &longhorn.BackupBackingImage{
			ObjectMeta: metav1.ObjectMeta{
				Name: backupBackingImageName,
			},
			Spec: longhorn.BackupBackingImageSpec{
				UserCreated:      true,
				BackingImage:     backingImageName,
				BackupTargetName: backupTargetName,
			},
		}
		backupBackingImage, err = c.ds.CreateBackupBackingImage(backupBackingImage)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, errors.Wrapf(err, "failed to create backup backing image %s", backingImageName)
		}
	}
	return backupBackingImage, nil
}

func (c *SystemBackupController) generateSystemBackup(systemBackupName string, systemBackupMeta *systemBackupMeta, tempDir string) (err error) {
	metaFile := filepath.Join(tempDir, "metadata.yaml")
	err = util.EncodeToYAMLFile(systemBackupMeta, metaFile)
	if err != nil {
		return err
	}

	return c.generateSystemBackupYAMLs(systemBackupName, filepath.Join(tempDir, "yamls"))
}

func (c *SystemBackupController) generateSystemBackupYAMLs(systemBackupName, yamlsDir string) (err error) {
	defer func() {
		err = errors.Wrap(err, SystemBackupErrGenerateYAML)
	}()

	schemeGenerateFns := map[string]func(string) error{
		filepath.Join(yamlsDir, "apiextensions"): c.generateSystemBackupYAMLsForAPIExtensions,
		filepath.Join(yamlsDir, "kubernetes"):    c.generateSystemBackupYAMLsForKubernetes,
		filepath.Join(yamlsDir, "longhorn"): func(dir string) error {
			return c.generateSystemBackupYAMLsForLonghorn(systemBackupName, dir)
		},
	}
	for scheme, fn := range schemeGenerateFns {
		err = fn(scheme)
		if err != nil {
			return
		}
	}
	return
}

func (c *SystemBackupController) generateSystemBackupYAMLsForLonghorn(systemBackupName, dir string) (err error) {
	scheme := runtime.NewScheme()
	err = longhorn.AddToScheme(scheme)
	if err != nil {
		return errors.Wrap(err, "failed to add Longhorn to scheme")
	}

	// TODO: handle BackingImage in https://github.com/longhorn/longhorn/issues/4165
	resourceGetFns := map[string]func() (runtime.Object, error){
		"setting":      c.ds.GetAllLonghornSettings,
		"engineimages": c.ds.GetAllLonghornEngineImages,
		"volumes": func() (runtime.Object, error) {
			return c.getVolumesAndAddLastBackupOwners(systemBackupName)
		},
		"recurringjobs": c.ds.GetAllLonghornRecurringJobs,
		"backingimages": c.ds.GetAllLonghornBackingImages,
		"backuptargets": c.ds.GetAllLonghornBackupTargets,
	}

	for name, fn := range resourceGetFns {
		err = getObjectsAndPrintToYAML(dir, name, fn, scheme)
		if err != nil {
			return
		}
	}

	return nil
}

func (c *SystemBackupController) generateSystemBackupYAMLsForKubernetes(dir string) (err error) {
	scheme := kubernetesscheme.Scheme

	err = c.generateSystemBackupYAMLsForServiceAccount(dir, "serviceaccounts", "clusterroles", "clusterrolebindings", scheme)
	if err != nil {
		return
	}

	err = c.generateSystemBackupYAMLsForRoles(dir, "roles", "rolebindings", scheme)
	if err != nil {
		return
	}

	err = getObjectsAndPrintToYAML(dir, "daemonsets", c.ds.GetAllDaemonSetsList, scheme)
	if err != nil {
		return
	}

	err = getObjectsAndPrintToYAML(dir, "deployments", c.ds.GetAllDeploymentsList, scheme)
	if err != nil {
		return
	}

	err = getObjectsAndPrintToYAML(dir, "configmaps", c.ds.GetAllConfigMaps, scheme)
	if err != nil {
		return
	}

	err = c.generateSystemBackupYAMLsForServices(dir, "services", scheme)
	if err != nil {
		return
	}

	err = getObjectsAndPrintToYAML(dir, "storageclasses", c.ds.GetAllLonghornStorageClassList, scheme)
	if err != nil {
		return
	}

	err = getObjectsAndPrintToYAML(dir, "persistentvolumes", c.ds.GetAllPersistentVolumesWithLonghornProvisioner, scheme)
	if err != nil {
		return
	}

	return getObjectsAndPrintToYAML(dir, "persistentvolumeclaims", c.ds.GetAllPersistentVolumeClaimsByPersistentVolumeProvisioner, scheme)
}

func (c *SystemBackupController) generateSystemBackupYAMLsForServices(dir, name string, scheme *runtime.Scheme) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to generate Longhorn Services")
	}()

	obj, err := c.ds.GetAllServicesList()
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "failed to get all Longhorn services")

	}

	serviceList, ok := obj.(*corev1.ServiceList)
	if !ok {
		return errors.Wrap(err, "failed to convert to serviceList object")
	}

	services := []corev1.Service{}
	for _, service := range serviceList.Items {
		if service.Spec.ClusterIP != "" {
			service.Spec.ClusterIP = ""
		}

		if service.Spec.ClusterIPs != nil {
			service.Spec.ClusterIPs = nil
		}

		services = append(services, service)
	}

	serviceList.Items = services

	return getObjectsAndPrintToYAML(dir, name, func() (runtime.Object, error) {
		return serviceList, nil
	}, scheme)
}

func (c *SystemBackupController) generateSystemBackupYAMLsForRoles(dir, roleName, roleBindingName string, scheme *runtime.Scheme) (err error) {
	// Generate Role YAML
	roleObj, err := c.ds.GetAllRoleList()
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "failed to get all Longhorn roles")
	}

	roleList, ok := roleObj.(*rbacv1.RoleList)
	if !ok {
		return errors.Wrap(err, "failed to convert to roleList object")
	}

	err = getObjectsAndPrintToYAML(dir, roleName, func() (runtime.Object, error) {
		return roleList, nil
	}, scheme)
	if err != nil {
		return
	}

	// Generate RoleBinding YAML
	return getObjectsAndPrintToYAML(dir, roleBindingName, c.ds.GetAllRoleBindingList, scheme)
}

func (c *SystemBackupController) generateSystemBackupYAMLsForServiceAccount(dir,
	serviceAccountName, clusterRoleName, clusterRoleBindingName string,
	scheme *runtime.Scheme) (err error) {
	serviceAccountObj, err := c.ds.GetAllServiceAccountList()
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "failed to get all Longhorn ServiceAccounts")

	}

	serviceAccountList, ok := serviceAccountObj.(*corev1.ServiceAccountList)
	if !ok {
		return errors.Wrap(err, "failed to convert to ServiceAccountList object")
	}

	err = getObjectsAndPrintToYAML(dir, serviceAccountName, func() (runtime.Object, error) {
		return serviceAccountList, nil
	}, scheme)
	if err != nil {
		return
	}

	// Generate ClusterRoleBinding from Longhorn ServieAccount
	clusterRoleBindingObj, err := c.ds.GetAllClusterRoleBindingList()
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "failed to get all ClusterRoleBindings")

	}

	clusterRoleBindingList, ok := clusterRoleBindingObj.(*rbacv1.ClusterRoleBindingList)
	if !ok {
		return errors.Wrap(err, "failed to convert to ClusterRoleBindingList object")
	}

	err = c.generateSystemBackupYAMLsForClusterRoleBindingsByServiceAccounts(
		clusterRoleBindingList, serviceAccountList, dir, clusterRoleName, scheme,
	)
	if err != nil {
		return err
	}

	// Generate ClusterRole YAML from ClusterRoleBinding
	clusterRoleObj, err := c.ds.GetAllClusterRoleList()
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "failed to get all ClusterRoles")

	}

	clusterRoleList, ok := clusterRoleObj.(*rbacv1.ClusterRoleList)
	if !ok {
		return errors.Wrap(err, "failed to convert to ClusterRoleList object")
	}

	return c.generateSystemBackupYAMLsForClusterRolesByClusterRoleBindings(
		clusterRoleList, clusterRoleBindingList, dir, clusterRoleBindingName, scheme,
	)
}

func (c *SystemBackupController) generateSystemBackupYAMLsForClusterRolesByClusterRoleBindings(
	clusterRoleList *rbacv1.ClusterRoleList,
	clusterRoleBindingList *rbacv1.ClusterRoleBindingList,
	dir, name string, scheme *runtime.Scheme) (err error) {
	RoleRefNames := map[string]struct{}{}
	for _, clusterRoleBinding := range clusterRoleBindingList.Items {
		if _, exist := RoleRefNames[clusterRoleBinding.RoleRef.Name]; exist {
			continue
		}
		RoleRefNames[clusterRoleBinding.RoleRef.Name] = struct{}{}
	}

	filtered := []rbacv1.ClusterRole{}
	for _, clusterRole := range clusterRoleList.Items {
		if _, exist := RoleRefNames[clusterRole.Name]; !exist {
			continue
		}
		filtered = append(filtered, clusterRole)

	}
	clusterRoleList.Items = filtered

	return getObjectsAndPrintToYAML(dir, name, func() (runtime.Object, error) {
		return clusterRoleList, nil
	}, scheme)
}

func (c *SystemBackupController) generateSystemBackupYAMLsForClusterRoleBindingsByServiceAccounts(
	clusterRoleBindingList *rbacv1.ClusterRoleBindingList,
	serviceAccountList *corev1.ServiceAccountList,
	dir, name string, scheme *runtime.Scheme) (err error) {
	filtered := []rbacv1.ClusterRoleBinding{}
	for _, clusterRoleBinding := range clusterRoleBindingList.Items {
		shouldBackup := false
		for _, serviceAccount := range serviceAccountList.Items {
			for _, subject := range clusterRoleBinding.Subjects {
				if subject.Kind != types.KubernetesKindServiceAccount {
					continue
				}
				if subject.Name != serviceAccount.Name {
					continue
				}
				if subject.Namespace != serviceAccount.Namespace {
					continue
				}
				shouldBackup = true
				break
			}
			if shouldBackup {
				break
			}
		}
		if shouldBackup {
			filtered = append(filtered, clusterRoleBinding)
		}
	}
	clusterRoleBindingList.Items = filtered

	return getObjectsAndPrintToYAML(dir, name, func() (runtime.Object, error) {
		return clusterRoleBindingList, nil
	}, scheme)
}

func (c *SystemBackupController) generateSystemBackupYAMLsForAPIExtensions(dir string) (err error) {
	scheme := runtime.NewScheme()
	err = apiextensionsv1.AddToScheme(scheme)
	if err != nil {
		return errors.Wrap(err, "failed to add API Extension to scheme")
	}

	return getObjectsAndPrintToYAML(dir, "customresourcedefinitions", c.ds.GetAllLonghornCustomResourceDefinitions, scheme)
}

type GetRuntimeObjectListFunc func() (runtime.Object, error)

func getObjectsAndPrintToYAML(dir, name string, getListFunc GetRuntimeObjectListFunc, scheme *runtime.Scheme) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to generate %v", name)
	}()

	obj, err := getListFunc()
	if err != nil {
		return
	}

	err = addTypeInformationToObject(obj, scheme)
	if err != nil {
		return
	}

	err = os.MkdirAll(dir, os.FileMode(0755))
	if err != nil {
		return
	}

	path := filepath.Join(dir, name+".yaml")
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warnf("Failed to close YAML file: %s", path)
		}
	}()

	printer := printers.YAMLPrinter{}
	err = printer.PrintObj(obj, f)
	if err != nil {
		return
	}

	return nil
}

func addTypeInformationToObject(obj runtime.Object, scheme *runtime.Scheme) error {
	gvks, _, err := scheme.ObjectKinds(obj)
	if err != nil {
		return errors.Wrap(err, "failed to set ObjectKind, could missing apiVersion or kind and cannot assign it")
	}

	for _, gvk := range gvks {
		if len(gvk.Kind) == 0 {
			continue
		}
		if len(gvk.Version) == 0 || gvk.Version == runtime.APIVersionInternal {
			continue
		}
		obj.GetObjectKind().SetGroupVersionKind(gvk)
		break
	}

	return nil
}

func cleanupLocalSystemBackupFiles(archievePath, tempDir string, log logrus.FieldLogger) {
	if err := os.Remove(archievePath); err != nil && !os.IsNotExist(err) {
		log.WithError(err).Warn(SystemBackupErrRemove)
	}

	if err := os.RemoveAll(tempDir); err != nil && !os.IsNotExist(err) {
		log.WithError(err).Warn(SystemBackupErrRemoveAll)
	}
}
