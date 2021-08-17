package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
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

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

const (
	BackupStatusQueryInterval = 2 * time.Second
)

type BackupController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	bStoreSynced cache.InformerSynced
}

func NewBackupController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backupInformer lhinformers.BackupInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *BackupController {
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

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-backup-controller"}),

		bStoreSynced: backupInformer.Informer().HasSynced,
	}

	backupInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bc.enqueueBackup,
		UpdateFunc: func(old, cur interface{}) { bc.enqueueBackup(cur) },
		DeleteFunc: bc.enqueueBackup,
	})

	return bc
}

func (bc *BackupController) enqueueBackup(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bc.queue.AddRateLimited(key)
}

func (bc *BackupController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bc.queue.ShutDown()

	bc.logger.Infof("Start Longhorn Backup controller")
	defer bc.logger.Infof("Shutting down Longhorn Backup controller")

	if !cache.WaitForNamedCacheSync(bc.name, stopCh, bc.bStoreSynced) {
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

	if bc.queue.NumRequeues(key) < maxRetries {
		bc.logger.WithError(err).Warnf("Error syncing Longhorn backup %v", key)
		bc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	bc.logger.WithError(err).Warnf("Dropping Longhorn backup %v out of the queue", key)
	bc.queue.Forget(key)
}

func (bc *BackupController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync backup %v", bc.name, key)
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
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	// Find the backup volume name from label
	backupVolumeName, err := bc.getBackupVolumeName(backup)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			return nil // Ignore error to prevent enqueue
		}
		log.WithError(err).Warning("Cannot find backup volume name")
		return err
	}

	if backup.Status.LastSyncedAt == nil {
		backup.Status.LastSyncedAt = &metav1.Time{Time: time.Time{}}
		if backup, err = bc.ds.UpdateBackupStatus(backup); err != nil {
			return err
		}
	}

	// Examine DeletionTimestamp to determine if object is under deletion
	if !backup.DeletionTimestamp.IsZero() {
		// No need to delete the backup from the remote backup target
		if !backup.Spec.FileCleanupRequired {
			return bc.ds.RemoveFinalizerForBackup(backup)
		}

		// Initialize a backup target client
		credential, err := bc.ds.GetCredentialFromSecret(backupTarget.Spec.CredentialSecret)
		if err != nil {
			return err
		}
		backupTargetClient, err := engineapi.NewBackupTargetClient(defaultEngineImage, backupTarget.Spec.BackupTargetURL, credential)
		if err != nil {
			log.WithError(err).Error("Error init backup target client")
			return nil // Ignore error to prevent enqueue
		}

		backupURL := backupstore.EncodeBackupURL(backup.Name, backupVolumeName, backupTargetClient.URL)
		if err := backupTargetClient.DeleteBackup(backupURL); err != nil {
			log.WithError(err).Error("Error deleting remote backup")
			return err
		}

		// Request backup_volume_controller to reconcile BackupVolume immediately if it's the last backup
		backupVolume, err := bc.ds.GetBackupVolume(backupVolumeName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil // Ignore error to prevent enqueue
			}
			return err
		}

		if backupVolume.Status.LastBackupName == backup.Name {
			backupVolume.Spec.SyncRequestedAt = &metav1.Time{Time: time.Now().Add(time.Second).UTC()}
			if _, err = bc.ds.UpdateBackupVolume(backupVolume); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Errorf("Error updating backup volume %s spec", backupVolumeName)
				// Do not return err to enqueue since backup_controller is responsible to
				// reconcile Backup CR spec, waits the backup_volume_controller next reconcile time
				// to update it's BackupVolume CR status
			}
		}
		return bc.ds.RemoveFinalizerForBackup(backup)
	}

	syncTime := &metav1.Time{Time: time.Now().UTC()}
	existingBackup := backup.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingBackup.Status, backup.Status) {
			return
		}
		if _, err := bc.ds.UpdateBackupStatus(backup); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", backupName)
			bc.enqueueBackup(backup)
		}
	}()

	// Perform backup snapshot to remote backup target
	if backup.Spec.SnapshotName != "" && backup.Status.State == "" {
		// Initialize a backup target client
		credential, err := bc.ds.GetCredentialFromSecret(backupTarget.Spec.CredentialSecret)
		if err != nil {
			return err
		}
		backupTargetClient, err := engineapi.NewBackupTargetClient(defaultEngineImage, backupTarget.Spec.BackupTargetURL, credential)
		if err != nil {
			log.WithError(err).Error("Error init backup target client")
			return nil // Ignore error to prevent enqueue
		}

		// Initialize a engine client
		engine, err := bc.ds.GetVolumeCurrentEngine(backupVolumeName)
		if err != nil {
			return err
		}
		engineCollection := &engineapi.EngineCollection{}
		engineClient, err := GetClientForEngine(engine, engineCollection, engine.Status.CurrentImage)
		if err != nil {
			return err
		}

		if err := bc.backupCreation(log, engineClient, backupTargetClient.URL, backupTargetClient.Credential, backup); err != nil {
			return err
		}
		return nil
	}

	// The backup config had synced
	if !backup.Status.LastSyncedAt.IsZero() &&
		backup.Status.LastSyncedAt.Time.After(backup.Spec.SyncRequestedAt.Time) {
		return nil
	}

	// Initialize a backup target client
	credential, err := bc.ds.GetCredentialFromSecret(backupTarget.Spec.CredentialSecret)
	if err != nil {
		return err
	}
	backupTargetClient, err := engineapi.NewBackupTargetClient(defaultEngineImage, backupTarget.Spec.BackupTargetURL, credential)
	if err != nil {
		log.WithError(err).Error("Error init a backup target client")
		return nil // Ignore error to prevent enqueue
	}

	backupURL := backupstore.EncodeBackupURL(backup.Name, backupVolumeName, backupTargetClient.URL)
	backupInfo, err := backupTargetClient.InspectBackupConfig(backupURL)
	if err != nil {
		log.WithError(err).Error("Error inspecting backup config")
		return nil // Ignore error to prevent enqueue
	}
	if backupInfo == nil {
		return nil
	}

	// Update Backup CR status
	backup.Status.State = types.BackupStateCompleted
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
	backup.Status.LastSyncedAt = syncTime
	return nil
}

func (bc *BackupController) isResponsibleFor(b *longhorn.Backup, defaultEngineImage string) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	isResponsible := isControllerResponsibleFor(bc.controllerID, bc.ds, b.Name, "", b.Status.OwnerID)

	readyNodesWithEI, err := bc.ds.ListReadyNodesWithEngineImage(defaultEngineImage)
	if err != nil {
		return false, err
	}
	// No node in the system has the default engine image,
	// Fall back to the default logic where we pick a running node to be the owner
	if len(readyNodesWithEI) == 0 {
		return isResponsible, nil
	}

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

func (bc *BackupController) backupCreation(log logrus.FieldLogger, engineClient engineapi.EngineClient, url string, credential map[string]string, backup *longhorn.Backup) error {
	volumeName := engineClient.Name()

	log = log.WithFields(
		logrus.Fields{
			"vol":      volumeName,
			"snapshot": backup.Spec.SnapshotName,
			"label":    backup.Spec.Labels,
		},
	)

	logEvent := func(err error) {
		if err != nil {
			bc.eventRecorder.Eventf(backup, corev1.EventTypeWarning, string(backup.Status.State),
				"Snapshot %s backup %s volume %s label %v: %v", backup.Spec.SnapshotName, backup.Name, volumeName, backup.Spec.Labels, err)
			log.WithError(err).Debugf("state %s", string(backup.Status.State))
			return
		}
		bc.eventRecorder.Eventf(backup, corev1.EventTypeNormal, string(backup.Status.State),
			"Snapshot %s backup %s volume %s label %v", backup.Spec.SnapshotName, backup.Name, volumeName, backup.Spec.Labels)
		log.Debugf("state %s", string(backup.Status.State))
	}

	// Get the volume CR
	volume, err := bc.ds.GetVolume(volumeName)
	if err != nil {
		return err
	}

	// Backing image validation
	biName := volume.Spec.BackingImage
	biChecksum := ""
	if biName != "" {
		bi, err := bc.ds.GetBackingImage(biName)
		if err != nil {
			return err
		}
		bv, err := bc.ds.GetBackupVolumeRO(volumeName)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if bv != nil &&
			bv.Status.BackingImageChecksum != "" && bi.Status.Checksum != "" &&
			bv.Status.BackingImageChecksum != bi.Status.Checksum {
			return fmt.Errorf("the backing image %v checksum %v in the backup volume doesn't match the current checksum %v", biName, bv.Status.BackingImageChecksum, bi.Status.Checksum)
		}
		biChecksum = bi.Status.Checksum
	}

	backup.Status.State = types.BackupStatePending
	logEvent(nil)

	go func() {
		var err error
		existingBackup := backup.DeepCopy()
		defer func() {
			if err != nil {
				return
			}
			if reflect.DeepEqual(existingBackup.Status, backup.Status) {
				return
			}
			if _, err := bc.ds.UpdateBackupStatus(backup); err != nil {
				log.WithError(err).Errorf("Error update backup status")
			}
		}()

		if _, err = engineClient.SnapshotBackup(backup.Name, backup.Spec.SnapshotName, url, biName, biChecksum, backup.Spec.Labels, credential); err != nil {
			backup.Status.State = types.BackupStateError
			logEvent(err)
			return
		}

		backup.Status.State = types.BackupStateInProgress
		logEvent(nil)

		// Monitor snapshot backup progress
		for {
			engines, err := bc.ds.ListVolumeEngines(volumeName)
			if err != nil {
				backup.Status.State = types.BackupStateUnknown
				logEvent(err)
				return
			}

			bks := &types.BackupStatus{}
			for _, e := range engines {
				backupStatusList := e.Status.BackupStatus
				for _, b := range backupStatusList {
					if b.SnapshotName == backup.Spec.SnapshotName {
						bks = b
						break
					}
				}
			}
			if bks == nil {
				backup.Status.State = types.BackupStateUnknown
				logEvent(fmt.Errorf("cannot find backup status"))
				return
			}
			if bks.Error != "" {
				backup.Status.State = types.BackupStateError
				logEvent(errors.New(bks.Error))
				return
			}

			// TODO:
			//   use resource monitoring https://github.com/longhorn/longhorn/issues/2441
			//   to trigger updates backup volume to run reconcile immediately
			if bks.Progress == 100 {
				// Request backup_volume_controller to reconcile BackupVolume immediately.
				backupVolume, err := bc.ds.GetBackupVolume(volumeName)
				if err == nil {
					backupVolume.Spec.SyncRequestedAt = &metav1.Time{Time: time.Now().Add(time.Second).UTC()}
					if _, err = bc.ds.UpdateBackupVolume(backupVolume); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
						log.WithError(err).Errorf("Error updating backup volume %s spec", volumeName)
					}
				} else if err != nil && apierrors.IsNotFound(err) {
					backupVolume := &longhorn.BackupVolume{
						ObjectMeta: metav1.ObjectMeta{
							Name: volumeName,
						},
						Spec: types.BackupVolumeSpec{
							SyncRequestedAt: &metav1.Time{Time: time.Now().Add(time.Second).UTC()},
						},
					}
					if _, err = bc.ds.CreateBackupVolume(backupVolume); err != nil && !apierrors.IsAlreadyExists(err) {
						log.WithError(err).Errorf("Error creating backup volume %s into cluster", volumeName)
					}
				}

				backup.Status.State = types.BackupStateCompleted
				logEvent(nil)
				return
			}
			time.Sleep(BackupStatusQueryInterval)
		}
	}()

	return nil
}
