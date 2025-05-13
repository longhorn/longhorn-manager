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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type BackingImageController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the backing image
	controllerID   string
	serviceAccount string
	bimImageName   string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewBackingImageController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string, controllerID, serviceAccount, backingImageManagerImage string) (*BackingImageController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: typedv1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	bic := &BackingImageController{
		baseController: newBaseController("longhorn-backing-image", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,
		bimImageName:   backingImageManagerImage,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-backing-image-controller"}),

		ds: ds,
	}

	var err error
	if _, err = ds.BackingImageInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImage,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImage(cur) },
		DeleteFunc: bic.enqueueBackingImage,
	}); err != nil {
		return nil, err
	}
	bic.cacheSyncs = append(bic.cacheSyncs, ds.BackingImageInformer.HasSynced)

	if _, err = ds.BackingImageManagerInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForBackingImageManager,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForBackingImageManager(cur) },
		DeleteFunc: bic.enqueueBackingImageForBackingImageManager,
	}, 0); err != nil {
		return nil, err
	}
	bic.cacheSyncs = append(bic.cacheSyncs, ds.BackingImageManagerInformer.HasSynced)

	if _, err = ds.BackingImageDataSourceInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForBackingImageDataSource,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForBackingImageDataSource(cur) },
		DeleteFunc: bic.enqueueBackingImageForBackingImageDataSource,
	}, 0); err != nil {
		return nil, err
	}
	bic.cacheSyncs = append(bic.cacheSyncs, ds.BackingImageDataSourceInformer.HasSynced)

	if _, err = ds.ReplicaInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForReplica,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForReplica(cur) },
		DeleteFunc: bic.enqueueBackingImageForReplica,
	}, 0); err != nil {
		return nil, err
	}
	bic.cacheSyncs = append(bic.cacheSyncs, ds.ReplicaInformer.HasSynced)

	if _, err = ds.NodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		UpdateFunc: bic.enqueueBackingImageForNodeUpdate,
	}, 0); err != nil {
		return nil, err
	}
	bic.cacheSyncs = append(bic.cacheSyncs, ds.NodeInformer.HasSynced)

	return bic, nil
}

func (bic *BackingImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bic.queue.ShutDown()

	bic.logger.Info("Starting Longhorn Backing Image controller")
	defer bic.logger.Info("Shut down Longhorn Backing Image controller")

	if !cache.WaitForNamedCacheSync("longhorn backing images", stopCh, bic.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(bic.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (bic *BackingImageController) worker() {
	for bic.processNextWorkItem() {
	}
}

func (bic *BackingImageController) processNextWorkItem() bool {
	key, quit := bic.queue.Get()

	if quit {
		return false
	}
	defer bic.queue.Done(key)

	err := bic.syncBackingImage(key.(string))
	bic.handleErr(err, key)

	return true
}

func (bic *BackingImageController) handleErr(err error, key interface{}) {
	if err == nil {
		bic.queue.Forget(key)
		return
	}

	log := bic.logger.WithField("BackingImage", key)
	if bic.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn backing image")
		bic.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn backing image out of the queue")
	bic.queue.Forget(key)
}

func getLoggerForBackingImage(logger logrus.FieldLogger, bi *longhorn.BackingImage) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"backingImageName": bi.Name,
		},
	)
}

func (bic *BackingImageController) syncBackingImage(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync backing image for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != bic.namespace {
		return nil
	}

	backingImage, err := bic.ds.GetBackingImage(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to get backing image %v", name)
	}

	log := getLoggerForBackingImage(bic.logger, backingImage)

	if !bic.isResponsibleFor(backingImage) {
		return nil
	}
	if backingImage.Status.OwnerID != bic.controllerID {
		backingImage.Status.OwnerID = bic.controllerID
		backingImage, err = bic.ds.UpdateBackingImageStatus(backingImage)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Backing image got new owner %v", bic.controllerID)
	}

	if backingImage.DeletionTimestamp != nil {
		replicas, err := bic.ds.ListReplicasByBackingImage(backingImage.Name)
		if err != nil {
			return err
		}
		if len(replicas) != 0 {
			log.Warn("Waiting for all replicas stopping using this backing image before removing the finalizer")
			return nil
		}
		if _, err := bic.IsBackingImageDataSourceCleaned(backingImage); err != nil {
			log.WithError(err).Warn("Waiting until backing image data source is cleaned before removing the finalizer")
			return nil
		}
		log.Info("Cleaning up the record for backing image managers and remove the finalizer")
		if err := bic.cleanupBackingImageManagers(backingImage); err != nil {
			return err
		}
		return bic.ds.RemoveFinalizerForBackingImage(backingImage)
	}

	// UUID is immutable once it's set.
	// Should make sure UUID is not empty before syncing with other resources.
	if backingImage.Status.UUID == "" {
		backingImage.Status.UUID = util.RandomID()
		if backingImage, err = bic.ds.UpdateBackingImageStatus(backingImage); err != nil {
			if !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			bic.enqueueBackingImage(backingImage)
			return nil
		}
		bic.eventRecorder.Eventf(backingImage, corev1.EventTypeNormal, constant.EventReasonUpdate, "Initialized UUID to %v", backingImage.Status.UUID)
	}

	existingBackingImage := backingImage.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingBackingImage.Spec, backingImage.Spec) {
			if _, err := bic.ds.UpdateBackingImage(backingImage); err != nil && apierrors.IsConflict(errors.Cause(err)) {
				log.WithError(err).Debugf("Requeue %v due to conflict", key)
				bic.enqueueBackingImage(backingImage)
			}
		}
		if reflect.DeepEqual(existingBackingImage.Status, backingImage.Status) {
			return
		}
		if _, err := bic.ds.UpdateBackingImageStatus(backingImage); err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			bic.enqueueBackingImage(backingImage)
		}
	}()

	if backingImage.Status.DiskFileStatusMap == nil {
		backingImage.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{}
	}
	if backingImage.Status.DiskLastRefAtMap == nil {
		backingImage.Status.DiskLastRefAtMap = map[string]string{}
	}

	if err := bic.handleBackingImageDataSource(backingImage); err != nil {
		return err
	}

	// We cannot continue without `Spec.DiskFileSpecMap`. The backing image data source controller can update it.
	if backingImage.Spec.DiskFileSpecMap == nil {
		return nil
	}

	if err := bic.handleBackingImageManagers(backingImage); err != nil {
		return err
	}

	if err := bic.syncBackingImageFileInfo(backingImage); err != nil {
		return err
	}

	if err := bic.updateDiskLastReferenceMap(backingImage); err != nil {
		return err
	}

	if err := bic.replenishBackingImageCopies(backingImage); err != nil {
		return err
	}

	bic.cleanupEvictionRequestedBackingImageCopies(backingImage)

<<<<<<< HEAD
=======
	if types.IsDataEngineV2(backingImage.Spec.DataEngine) {
		return bic.handleV2BackingImage(backingImage)
	}

	return nil
}

func (bic *BackingImageController) handleV2BackingImage(bi *longhorn.BackingImage) (err error) {
	if err := bic.prepareFirstV2Copy(bi); err != nil {
		return errors.Wrapf(err, "failed to prepare the first v2 backing image")
	}

	if err := bic.syncV2StatusWithInstanceManager(bi); err != nil {
		return errors.Wrapf(err, "failed to sync v2 backing image status from instance manager")
	}

	if err := bic.deleteInvalidV2Copy(bi); err != nil {
		return errors.Wrapf(err, "failed to delete invalid v2 backing image")
	}

	if err := bic.prepareV2Copy(bi); err != nil {
		return errors.Wrapf(err, "failed to prepare v2 backing image")
	}

	return nil
}

func (bic *BackingImageController) isFirstV2CopyInState(bi *longhorn.BackingImage, state longhorn.BackingImageState) bool {
	if bi.Status.V2FirstCopyDisk != "" && bi.Status.V2FirstCopyStatus == state {
		return true
	}

	if bi.Status.V2FirstCopyDisk != "" {
		if status, exists := bi.Status.DiskFileStatusMap[bi.Status.V2FirstCopyDisk]; exists && status.State == state {
			return true
		}
	}

	return false
}

func (bic *BackingImageController) cleanupFirstFailedV2Copy(bi *longhorn.BackingImage) (err error) {
	if bi.Status.V2FirstCopyDisk == "" {
		return nil
	}

	log := getLoggerForBackingImage(bic.logger, bi)

	node, _, err := bic.ds.GetReadyDiskNodeRO(bi.Status.V2FirstCopyDisk)
	if err != nil {
		return errors.Wrapf(err, "failed to get the ready disk node for disk %v", bi.Status.V2FirstCopyDisk)
	}

	engineClientProxy, err := bic.getEngineClientProxy(node.Name, longhorn.DataEngineTypeV2, log)
	if err != nil {
		return errors.Wrapf(err, "failed to get the engine client proxy for node %v", node.Name)
	}
	defer engineClientProxy.Close()

	// clean up the failed first v2 copy
	err = engineClientProxy.SPDKBackingImageDelete(bi.Name, bi.Status.V2FirstCopyDisk)
	if err != nil {
		return errors.Wrapf(err, "failed to delete the failed v2 copy on the disk %v", bi.Status.V2FirstCopyDisk)
	}
	delete(bi.Spec.DiskFileSpecMap, bi.Status.V2FirstCopyDisk)
	delete(bi.Status.DiskFileStatusMap, bi.Status.V2FirstCopyDisk)
	bi.Status.V2FirstCopyDisk = ""
	bi.Status.V2FirstCopyStatus = longhorn.BackingImageStatePending
	return nil
}

func (bic *BackingImageController) deleteAllV1FileCopies(bi *longhorn.BackingImage) {
	for diskUUID, fileSpec := range bi.Spec.DiskFileSpecMap {
		if types.IsDataEngineV1(fileSpec.DataEngine) {
			delete(bi.Spec.DiskFileSpecMap, diskUUID)
		}
	}
}

func (bic *BackingImageController) prepareFirstV2Copy(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)

	// The preparation state transition: Pending -> InProgress -> Ready/Failed
	// we retry when failed by deleting the failed copy and cleanup the state.

	// If the first v2 copy is ready, we can delete all the v1 file copies and return.
	isPrepared := bic.isFirstV2CopyInState(bi, longhorn.BackingImageStateReady)
	if isPrepared {
		bi.Status.V2FirstCopyStatus = longhorn.BackingImageStateReady
		bic.deleteAllV1FileCopies(bi)
		bic.v2CopyBackoff.DeleteEntry(bi.Status.V2FirstCopyDisk)
		return nil
	}

	// If the first v2 copy is failed, we cleanup the failed copy and retry the preparation.
	// If the first v2 copy is in unknown, that means the instance manager may crahsh when creating the first v2 copy.
	// We cleanup the unknown copy and retry the preparation.
	isFailedToPrepare := bic.isFirstV2CopyInState(bi, longhorn.BackingImageStateFailed)
	isUnknown := bic.isFirstV2CopyInState(bi, longhorn.BackingImageStateUnknown)
	if isFailedToPrepare || isUnknown {
		bi.Status.V2FirstCopyStatus = longhorn.BackingImageStateFailed
		return bic.cleanupFirstFailedV2Copy(bi)
	}

	// If the first v2 copy is in progress, we wait for the next reconciliation
	isInProgress := bic.isFirstV2CopyInState(bi, longhorn.BackingImageStateInProgress)
	if isInProgress {
		bi.Status.V2FirstCopyStatus = longhorn.BackingImageStateInProgress
		bi.Spec.DiskFileSpecMap[bi.Status.V2FirstCopyDisk] = &longhorn.BackingImageDiskFileSpec{DataEngine: longhorn.DataEngineTypeV2}
		return nil
	}

	// Wait for the first v1 file to be ready.
	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Warn("Backing image data source not found when preparing first v2 copy, reconcile later")
			return nil
		}
		return errors.Wrap(err, "failed to get the backing image data source when preparing first v2 copy")
	}
	if !bids.Spec.FileTransferred {
		log.Info("Backing image data source has not prepared the first v1 file, reconcile later")
		return nil
	}
	firstV1FileDiskUUID := bids.Spec.DiskUUID
	if status, exists := bi.Status.DiskFileStatusMap[firstV1FileDiskUUID]; exists && status.State != longhorn.BackingImageStateReady {
		log.Infof("The first v1 file copy is not ready, reconcile later")
		return nil
	}

	firstV2CopyNode, firstV2CopyDiskName, err := bic.findReadyNodeAndDiskForFirstV2Copy(bi, firstV1FileDiskUUID, log)
	if err != nil {
		return errors.Wrap(err, "failed to find a ready disk for the first v2 backing image copy")
	}

	firstV2CopyNodeName := firstV2CopyNode.Name
	firstV2CopyDiskUUID := firstV2CopyNode.Status.DiskStatus[firstV2CopyDiskName].DiskUUID
	log.Infof("Found the ready node %v disk %v for the first v2 backing image copy", firstV2CopyNodeName, firstV2CopyDiskUUID)

	if bic.v2CopyBackoff.IsInBackOffSinceUpdate(firstV2CopyDiskUUID, time.Now()) {
		log.Debugf("Skip preparing first v2 backing image copy to disk %v since it is still in the backoff window", firstV2CopyDiskUUID)
		return nil
	}

	// Get proxy client for creating v2 backing image copy
	engineClientProxy, err := bic.getEngineClientProxy(firstV2CopyNodeName, longhorn.DataEngineTypeV2, log)
	if err != nil {
		return errors.Wrapf(err, "failed to get the engine client proxy for node %v", firstV2CopyNodeName)
	}
	defer engineClientProxy.Close()

	// Create v2 backing image copy by downloading the data from first file copy
	fileDownloadAddress, err := bic.getFileDownloadAddress(bi, firstV1FileDiskUUID)
	if err != nil {
		return errors.Wrap(err, "failed to get the v1 file download address when preparing first v2 copy")
	}
	_, err = engineClientProxy.SPDKBackingImageCreate(bi.Name, bi.Status.UUID, firstV2CopyDiskUUID, bi.Status.Checksum, fileDownloadAddress, "", uint64(bi.Status.Size))
	if err != nil {
		if types.ErrorAlreadyExists(err) {
			log.Infof("Backing image already exists when preparing first v2 copy on disk %v", firstV2CopyDiskUUID)
		}
		bic.v2CopyBackoff.Next(firstV2CopyDiskUUID, time.Now())
		return errors.Wrapf(err, "failed to create backing image on disk %v when preparing first v2 copy for backing image %v", firstV2CopyDiskUUID, bi.Name)
	}

	bi.Status.V2FirstCopyDisk = firstV2CopyDiskUUID
	bi.Status.V2FirstCopyStatus = longhorn.BackingImageStateInProgress
	bic.v2CopyBackoff.Next(firstV2CopyDiskUUID, time.Now())
	return nil
}

func (bic *BackingImageController) getFileDownloadAddress(bi *longhorn.BackingImage, diskUUID string) (string, error) {
	bimName := types.GetBackingImageManagerName(bic.bimImageName, diskUUID)
	bim, err := bic.ds.GetBackingImageManager(bimName)
	if err != nil {
		return "", err
	}
	bimCli, err := engineapi.NewBackingImageManagerClient(bim)
	if err != nil {
		return "", err
	}
	// get the file download address of sync server in the backing image manager
	filePath, addr, err := bimCli.PrepareDownload(bi.Name, bi.Status.UUID)
	if err != nil {
		return "", err
	}

	fileDownloadAddress := fmt.Sprintf("http://%s/v1/files/%s/download", addr, url.PathEscape(filePath))
	return fileDownloadAddress, nil
}

func (bic *BackingImageController) deleteInvalidV2Copy(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)

	if bi.Status.V2FirstCopyStatus != longhorn.BackingImageStateReady {
		return nil
	}

	v2CopiesUnknown := []string{}
	v2CopiesFailed := []string{}
	missingSpecV2Copies := []string{}
	hasReadyV2BackingImage := false

	for v2DiskUUID, copyStatus := range bi.Status.DiskFileStatusMap {
		// only handle v2 copy in this function
		if types.IsDataEngineV1(copyStatus.DataEngine) {
			continue
		}

		// Delete the v2 copy if the spec record is removed
		if _, exists := bi.Spec.DiskFileSpecMap[v2DiskUUID]; !exists {
			missingSpecV2Copies = append(missingSpecV2Copies, v2DiskUUID)
			continue
		}

		if copyStatus.State == longhorn.BackingImageStateReady {
			hasReadyV2BackingImage = true
			continue
		}

		if copyStatus.State == longhorn.BackingImageStateFailed {
			v2CopiesFailed = append(v2CopiesFailed, v2DiskUUID)
			continue
		}
		if copyStatus.State == longhorn.BackingImageStateUnknown {
			v2CopiesUnknown = append(v2CopiesUnknown, v2DiskUUID)
			continue
		}
	}

	if err := bic.cleanupMissingSpecV2Copies(bi, missingSpecV2Copies, log); err != nil {
		return errors.Wrap(err, "failed to delete the missing spec v2 copies")
	}

	if err := bic.cleanupUnknownV2Copies(bi, v2CopiesUnknown, hasReadyV2BackingImage, log); err != nil {
		return errors.Wrap(err, "failed to delete the unknown v2 copies")
	}

	if err := bic.cleanupFailedV2Copies(bi, v2CopiesFailed, hasReadyV2BackingImage, log); err != nil {
		return errors.Wrap(err, "failed to delete the failed v2 copies")
	}

	return nil
}

func (bic *BackingImageController) cleanupAllV2BackingImageCopies(bi *longhorn.BackingImage) (cleaned bool, err error) {
	log := getLoggerForBackingImage(bic.logger, bi)

	if err := bic.syncV2StatusWithInstanceManager(bi); err != nil {
		return false, errors.Wrapf(err, "failed to sync v2 backing image status from instance manager")
	}

	// we can delete the backing image when all the v2 copies are deleted
	if len(bi.Status.DiskFileStatusMap) == 0 {
		return true, nil
	}

	allV2CopiesCleaned := true
	for diskUUID := range bi.Status.DiskFileStatusMap {
		// The v1 backing image file will be cleaned up in cleanupBackingImageManagers
		if fileSpec, exists := bi.Spec.DiskFileSpecMap[diskUUID]; exists && !types.IsDataEngineV2(fileSpec.DataEngine) {
			continue
		}

		allV2CopiesCleaned = false

		node, _, err := bic.ds.GetReadyDiskNodeRO(diskUUID)
		if err != nil {
			return false, errors.Wrapf(err, "failed to get the ready disk node for disk %v", diskUUID)
		}

		log.Infof("Deleting the v2 copy on node %v disk %v for backing image %v", node.Name, diskUUID, bi.Name)
		if err := bic.deleteV2Copy(bi, diskUUID, node, log); err != nil {
			return false, errors.Wrapf(err, "failed to delete the v2 copy on disk %v", diskUUID)
		}

		delete(bi.Status.DiskFileStatusMap, diskUUID)
		bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonDelete, "Deleted v2 backing image %v on disk %v on node %v", bi.Name, diskUUID, node.Name)
	}
	// only return true when there is no v2 disk file status after syncing the status from instance manager
	return allV2CopiesCleaned, nil
}

func (bic *BackingImageController) prepareV2Copy(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)

	if bi.Status.V2FirstCopyStatus != longhorn.BackingImageStateReady {
		return nil
	}

	readyV2CopyDiskUUIDs := []string{}
	requireV2CopyDiskUUIDs := []string{}

	for v2DiskUUID := range bi.Spec.DiskFileSpecMap {
		biStatus, exists := bi.Status.DiskFileStatusMap[v2DiskUUID]
		if exists {
			if biStatus.State == longhorn.BackingImageStateReady {
				readyV2CopyDiskUUIDs = append(readyV2CopyDiskUUIDs, v2DiskUUID)
			}
			if backingImageInProgress(biStatus.State) {
				log.Info("There is one v2 backing image copy in progress, prepare the backing image one at a time")
				return nil
			}
		} else {
			requireV2CopyDiskUUIDs = append(requireV2CopyDiskUUIDs, v2DiskUUID)
		}
	}

	if len(readyV2CopyDiskUUIDs) == 0 {
		log.Infof("Only sync the backing image to other disks when there is one ready v2 backing image copy")
		return nil
	}

	sourceV2DiskUUID := readyV2CopyDiskUUIDs[0]
	// Only sync one backing image copy at a time, so we can control the concurrent limit of syncing for the backing image manager.
	for _, v2DiskUUID := range requireV2CopyDiskUUIDs {
		if bic.v2CopyBackoff.IsInBackOffSinceUpdate(v2DiskUUID, time.Now()) {
			log.Debugf("Skip syncing backing image copy to disk %v immediately since it is still in the backoff window", v2DiskUUID)
			continue
		}

		node, _, err := bic.ds.GetReadyDiskNodeRO(v2DiskUUID)
		if err != nil {
			bic.v2CopyBackoff.Next(v2DiskUUID, time.Now())
			return errors.Wrapf(err, "failed to get the ready disk node for disk %v", v2DiskUUID)
		}

		if err := bic.syncV2Copies(bi, sourceV2DiskUUID, v2DiskUUID, node, log); err != nil {
			return errors.Wrapf(err, "failed to sync the backing image to disk %v", v2DiskUUID)
		}

		// To prevent it from requesting the same disk again, the status will be updated later based on the status from the instance manager
		bi.Status.DiskFileStatusMap[v2DiskUUID] = &longhorn.BackingImageDiskFileStatus{
			State: longhorn.BackingImageStateInProgress,
		}
		bic.v2CopyBackoff.Next(v2DiskUUID, time.Now())
		bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonCreate, "Created v2 backing image %v on disk %v on node %v", bi.Name, v2DiskUUID, node.Name)
		return nil
	}

	return nil
}

// ExtractBackingImageAndDiskUUID extracts the BackingImageName and DiskUUID from the string pattern "bi-${BackingImageName}-disk-${DiskUUID}"
func extractBackingImageAndDiskUUID(biNameDiskUUID string) (string, string, error) {
	// Define the regular expression pattern
	// This captures the BackingImageName and DiskUUID while allowing for hyphens in both.
	re := regexp.MustCompile(`^bi-([a-zA-Z0-9-]+)-disk-([a-zA-Z0-9-]+)$`)

	// Try to find a match
	matches := re.FindStringSubmatch(biNameDiskUUID)
	if matches == nil {
		return "", "", fmt.Errorf("biNameDiskUUID does not match the expected pattern")
	}

	// Extract BackingImageName and DiskUUID from the matches
	backingImageName := matches[1]
	diskUUID := matches[2]

	return backingImageName, diskUUID, nil
}

func (bic *BackingImageController) syncV2StatusWithInstanceManager(bi *longhorn.BackingImage) error {
	ims, err := bic.ds.ListInstanceManagersRO()
	if err != nil {
		return err
	}

	updatedCopyDiskUUIDs := map[string]bool{}
	for _, im := range ims {
		if !types.IsDataEngineV2(im.Spec.DataEngine) {
			continue
		}
		if im.DeletionTimestamp != nil {
			continue
		}

		// If the instance manager is not in ready state
		// update the v2 copy in that instance manager to unknown.
		if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
			continue
		}

		for biNameDiskUUID, statusInfo := range im.Status.BackingImages {
			biName, diskUUID, err := extractBackingImageAndDiskUUID(biNameDiskUUID)
			if err != nil {
				logrus.WithError(err).Warnf("failed to parse the backing image name and disk uuid from %v", biNameDiskUUID)
				continue
			}

			if biName != bi.Name {
				continue
			}

			if statusInfo.UUID != bi.Status.UUID {
				continue
			}

			if err := bic.updateStatusWithFileInfo(bi,
				diskUUID, statusInfo.Message, statusInfo.CurrentChecksum, statusInfo.State, statusInfo.Progress, longhorn.DataEngineTypeV2); err != nil {
				return err
			}
			if statusInfo.Size > 0 {
				if bi.Status.Size == 0 {
					bi.Status.Size = statusInfo.Size
					bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonUpdate, "Set size to %v", bi.Status.Size)
				}
				if bi.Status.Size != statusInfo.Size {
					if bi.Status.DiskFileStatusMap[diskUUID].State != longhorn.BackingImageStateFailed {
						msg := fmt.Sprintf("found mismatching size %v reported by instance manager %v in disk %v, the size recorded in status is %v",
							statusInfo.Size, im.Name, diskUUID, bi.Status.Size)
						bic.eventRecorder.Eventf(bi, corev1.EventTypeWarning, constant.EventReasonUpdate, msg)
						bi.Status.DiskFileStatusMap[diskUUID].State = longhorn.BackingImageStateFailed
						bi.Status.DiskFileStatusMap[diskUUID].Message = msg
					}
				}
			}

			updatedCopyDiskUUIDs[diskUUID] = true
		}
	}

	for diskUUID, copyStatus := range bi.Status.DiskFileStatusMap {
		// only handle v2 copies
		if fileSpec, exists := bi.Spec.DiskFileSpecMap[diskUUID]; exists && !types.IsDataEngineV2(fileSpec.DataEngine) {
			continue
		}

		if copyStatus.State == longhorn.BackingImageStateReady {
			bic.v2CopyBackoff.DeleteEntry(diskUUID)
		}

		// Update the status to unknown if the instance manager is unknown or the status is not found from the instance manager
		if _, exists := updatedCopyDiskUUIDs[diskUUID]; !exists {
			bi.Status.DiskFileStatusMap[diskUUID].State = longhorn.BackingImageStateUnknown
			bi.Status.DiskFileStatusMap[diskUUID].Message = "The copy status is unknown because the status is not found from the instance manager"
		}
	}
>>>>>>> 96d7e664 (chore: update log messages)
	return nil
}

func (bic *BackingImageController) replenishBackingImageCopies(bi *longhorn.BackingImage) (err error) {
	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrap(err, "failed to get the backing image data source")
	}
	// only maintain the minNumberOfReplicas after BackingImage is transferred to BackingImageManager
	if !bids.Spec.FileTransferred {
		return nil
	}

	concurrentReplenishLimit, err := bic.ds.GetSettingAsInt(types.SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit)
	if err != nil {
		return err
	}

	// If disabled backing image replenishing, skip all the replenish except first copy.
	if concurrentReplenishLimit == 0 {
		return nil
	}

	nonFailedCopies := 0
	for diskUUID := range bi.Spec.DiskFileSpecMap {
		fileStatus, exists := bi.Status.DiskFileStatusMap[diskUUID]
		if !exists || (fileStatus.State != longhorn.BackingImageStateFailed &&
			fileStatus.State != longhorn.BackingImageStateFailedAndCleanUp &&
			fileStatus.State != longhorn.BackingImageStateUnknown) {
			// Non-existing file in status could due to not being synced from backing image manager yet.
			// Consider it as newly created copy and count it as non-failed copies.
			// So we don't create extra copy when handling copies evictions.
			nonFailedCopies += 1
		}
	}

	// Need to count the evicted copies in the nonFailedCopies then handle it in handleBackingImageCopiesEvictions
	// so we can distinguish the case of "0 healthy copy" and "there is 1 copy but being evicted".
	if nonFailedCopies == 0 {
		return nil
	} else {
		nonEvictingCount := nonFailedCopies
		for _, fileSpec := range bi.Spec.DiskFileSpecMap {
			if fileSpec.EvictionRequested {
				nonEvictingCount--
			}
		}

		if nonEvictingCount < bi.Spec.MinNumberOfCopies {
			readyNode, readyDiskName, err := bic.ds.GetReadyNodeDiskForBackingImage(bi)
			logrus.Infof("replicate the copy to node: %v, disk: %v", readyNode, readyDiskName)
			if err != nil {
				logrus.WithError(err).Warnf("failed to create the backing image copy")
				return nil
			}
			// BackingImageManager will then sync the BackingImage to the disk
			bi.Spec.DiskFileSpecMap[readyNode.Status.DiskStatus[readyDiskName].DiskUUID] = &longhorn.BackingImageDiskFileSpec{}
		}
	}

	return nil
}

func (bic *BackingImageController) cleanupEvictionRequestedBackingImageCopies(bi *longhorn.BackingImage) {
	log := getLoggerForBackingImage(bic.logger, bi)

	// If there is no non-evicting healthy backing image copy,
	// Longhorn should retain one evicting healthy backing image copy for replenishing.
	hasNonEvictingHealthyBackingImageCopy := false
	evictingHealthyBackingImageCopyDiskUUID := ""
	for diskUUID, fileSpec := range bi.Spec.DiskFileSpecMap {
		fileStatus := bi.Status.DiskFileStatusMap[diskUUID]
		if fileStatus == nil { // it is newly added, consider it as non healthy
			continue
		} else {
			if fileStatus.State != longhorn.BackingImageStateReady {
				continue
			}
		}

		if !fileSpec.EvictionRequested {
			hasNonEvictingHealthyBackingImageCopy = true
			break
		}
		evictingHealthyBackingImageCopyDiskUUID = diskUUID
	}

	for diskUUID, fileSpec := range bi.Spec.DiskFileSpecMap {
		if !fileSpec.EvictionRequested {
			continue
		}
		if !hasNonEvictingHealthyBackingImageCopy && diskUUID == evictingHealthyBackingImageCopyDiskUUID {
			msg := fmt.Sprintf("Failed to evict backing image copy on disk %v for now since there is no other healthy backing image copy", diskUUID)
			log.Warn(msg)
			bic.eventRecorder.Event(bi, corev1.EventTypeNormal, constant.EventReasonFailedDeleting, msg)
			continue
		}
		// Backing image controller update the spec here because
		// only this controller can gather all the information of all the copies of this backing image at once.
		// By deleting the disk from the spec, backing image manager controller will delete the copy on that disk.
		// TODO: introduce a new CRD for the backing image copy so we can delete the copy like volume controller deletes replicas.
		delete(bi.Spec.DiskFileSpecMap, diskUUID)
		log.Infof("Evicted backing image copy on disk %v", diskUUID)
	}
}

func (bic *BackingImageController) IsBackingImageDataSourceCleaned(bi *longhorn.BackingImage) (cleaned bool, err error) {
	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, errors.Wrap(err, "failed to get the backing image data source")
	}

	if bids.Spec.FileTransferred {
		return true, nil
	}

	if bids.Status.CurrentState == longhorn.BackingImageStateFailedAndCleanUp {
		return true, nil
	}

	return false, fmt.Errorf("backing image data source status is %v not %v", bids.Status.CurrentState, longhorn.BackingImageStateFailedAndCleanUp)
}

func (bic *BackingImageController) cleanupBackingImageManagers(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)

	bimMap, err := bic.ds.ListBackingImageManagers()
	if err != nil {
		return err
	}
	for _, bim := range bimMap {
		if bim.DeletionTimestamp != nil {
			continue
		}
		bimLog := log.WithField("backingImageManager", bim.Name)
		// Directly clean up old backing image managers (including incompatible managers).
		// New backing image managers can detect and reuse the existing backing image files if necessary.
		if bim.Spec.Image != bic.bimImageName {
			bimLog.Info("Deleting old/non-default backing image manager")
			if err := bic.ds.DeleteBackingImageManager(bim.Name); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonDelete, "deleted old/non-default backing image manager %v in disk %v on node %v", bim.Name, bim.Spec.DiskUUID, bim.Spec.NodeID)
			continue
		}
		// This sync loop cares about the backing image managers related to the current backing image only.
		if _, isRelatedToCurrentBI := bim.Spec.BackingImages[bi.Name]; !isRelatedToCurrentBI {
			continue
		}
		// The entry in the backing image manager matches the current backing image.
		if _, isStillRequiredByCurrentBI := bi.Spec.DiskFileSpecMap[bim.Spec.DiskUUID]; isStillRequiredByCurrentBI && bi.DeletionTimestamp == nil {
			if bim.Spec.BackingImages[bi.Name] == bi.Status.UUID {
				continue
			}
		}

		// The current backing image doesn't require this manager any longer, or the entry in backing image manager doesn't match the backing image uuid.
		delete(bim.Spec.BackingImages, bi.Name)
		if bim, err = bic.ds.UpdateBackingImageManager(bim); err != nil {
			return err
		}
		if len(bim.Spec.BackingImages) == 0 {
			if err := bic.ds.DeleteBackingImageManager(bim.Name); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonDelete, "deleting unused backing image manager %v in disk %v on node %v", bim.Name, bim.Spec.DiskUUID, bim.Spec.NodeID)
			continue
		}
	}

	return nil
}

func (bic *BackingImageController) handleBackingImageDataSource(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)

	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if bids == nil {
		log.Info("Creating backing image data source")
		var readyDiskUUID, readyDiskPath, readyNodeID string
		isReadyFile := false
		foundReadyDisk := false
		if bi.Spec.DiskFileSpecMap != nil {
			for diskUUID := range bi.Spec.DiskFileSpecMap {
				node, diskName, err := bic.ds.GetReadyDiskNode(diskUUID)
				if err != nil {
					if !types.ErrorIsNotFound(err) {
						return err
					}
					continue
				}
				foundReadyDisk = true
				readyNodeID = node.Name
				readyDiskUUID = diskUUID
				readyDiskPath = node.Spec.Disks[diskName].Path
				// Prefer to pick up a disk contains the ready file if possible.
				if fileStatus, ok := bi.Status.DiskFileStatusMap[diskUUID]; ok && fileStatus.State == longhorn.BackingImageStateReady {
					isReadyFile = true
					break
				}
			}
		}

		if !foundReadyDisk {
			readyNode, readyDiskName, err := bic.ds.GetReadyNodeDiskForBackingImage(bi)
			if err != nil {
				return err
			}

			// For clone, we choose the same node and disk as the source backing image
			if bi.Spec.SourceType == longhorn.BackingImageDataSourceTypeClone {
				readyNode, readyDiskName, err = bic.findReadyNodeAndDiskForClone(bi)
				if err != nil {
					return err
				}
			}

			foundReadyDisk = true
			readyNodeID = readyNode.Name
			readyDiskUUID = readyNode.Status.DiskStatus[readyDiskName].DiskUUID
			readyDiskPath = readyNode.Spec.Disks[readyDiskName].Path
		}
		if !foundReadyDisk {
			return fmt.Errorf("failed to find a ready disk for backing image data source creation")
		}

		bids = &longhorn.BackingImageDataSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:            bi.Name,
				OwnerReferences: datastore.GetOwnerReferencesForBackingImage(bi),
			},
			Spec: longhorn.BackingImageDataSourceSpec{
				NodeID:     readyNodeID,
				UUID:       bi.Status.UUID,
				DiskUUID:   readyDiskUUID,
				DiskPath:   readyDiskPath,
				Checksum:   bi.Spec.Checksum,
				SourceType: bi.Spec.SourceType,
				Parameters: bi.Spec.SourceParameters,
			},
		}
		if isReadyFile {
			bids.Spec.FileTransferred = true
		}
		if bids.Spec.Parameters == nil {
			bids.Spec.Parameters = map[string]string{}
		}
		if bids.Spec.SourceType == longhorn.BackingImageDataSourceTypeExportFromVolume {
			bids.Labels = map[string]string{types.GetLonghornLabelKey(types.LonghornLabelExportFromVolume): bids.Spec.Parameters[longhorn.DataSourceTypeExportFromVolumeParameterVolumeName]}
		}
		if bids, err = bic.ds.CreateBackingImageDataSource(bids); err != nil {
			return err
		}
	}
	existingBIDS := bids.DeepCopy()

	if bids.Spec.UUID == "" {
		bids.Spec.UUID = bi.Status.UUID
	}

	recoveryWaitIntervalSettingValue, err := bic.ds.GetSettingAsInt(types.SettingNameBackingImageRecoveryWaitInterval)
	if err != nil {
		return err
	}
	recoveryWaitInterval := time.Duration(recoveryWaitIntervalSettingValue) * time.Second

	// If all files in Spec.Disk becomes unavailable and there is no extra ready files.
	allFilesUnavailable := true
	if bi.Spec.DiskFileSpecMap != nil {
		for diskUUID := range bi.Spec.DiskFileSpecMap {
			fileStatus, ok := bi.Status.DiskFileStatusMap[diskUUID]
			if !ok {
				allFilesUnavailable = false
				break
			}
			if fileStatus.State != longhorn.BackingImageStateFailed {
				allFilesUnavailable = false
				break
			}
			if fileStatus.LastStateTransitionTime == "" {
				allFilesUnavailable = false
				break
			}
			lastStateTransitionTime, err := util.ParseTime(fileStatus.LastStateTransitionTime)
			if err != nil {
				return err
			}
			if lastStateTransitionTime.Add(recoveryWaitInterval).After(time.Now()) {
				allFilesUnavailable = false
				break
			}
		}
	}
	if allFilesUnavailable {
		// Check if there are extra available files outside of Spec.DiskFileSpecMap
		for diskUUID, fileStatus := range bi.Status.DiskFileStatusMap {
			if _, exists := bi.Spec.DiskFileSpecMap[diskUUID]; exists {
				continue
			}
			if fileStatus.State != longhorn.BackingImageStateFailed {
				allFilesUnavailable = false
				break
			}
			if fileStatus.LastStateTransitionTime == "" {
				allFilesUnavailable = false
				break
			}
			lastStateTransitionTime, err := util.ParseTime(fileStatus.LastStateTransitionTime)
			if err != nil {
				return err
			}
			if lastStateTransitionTime.Add(recoveryWaitInterval).After(time.Now()) {
				allFilesUnavailable = false
				break
			}
		}
	}

	// Check if the data source already finished the 1st file preparing.
	if !bids.Spec.FileTransferred && bids.Status.CurrentState == longhorn.BackingImageStateReady {
		fileStatus, exists := bi.Status.DiskFileStatusMap[bids.Spec.DiskUUID]
		if exists && fileStatus.State == longhorn.BackingImageStateReady {
			bids.Spec.FileTransferred = true
			log.Info("Default backing image manager successfully took over the file, will mark the data source as file transferred")
		}
	} else if bids.Spec.FileTransferred && allFilesUnavailable {
		switch bids.Spec.SourceType {
		case longhorn.BackingImageDataSourceTypeDownload:
			log.Info("Preparing to re-download backing image via data source since all existing files become unavailable")
			bids.Spec.FileTransferred = false
			bids.Spec.NodeID = ""
			bids.Spec.DiskUUID = ""
			bids.Spec.DiskPath = ""
		default:
			log.Warnf("Failed to recover backing image after all existing files becoming unavailable, since the data source with type %v doesn't support restarting", bids.Spec.SourceType)
		}
	}

	if !bids.Spec.FileTransferred {
		node, diskName, err := bic.ds.GetReadyDiskNode(bids.Spec.DiskUUID)
		// If the disk is still ready, no matter file fetching is in progress or failed, we don't need to re-schedule the BackingImageDataSource.
		changeNodeDisk := err != nil || node.Name != bids.Spec.NodeID || node.Spec.Disks[diskName].Path != bids.Spec.DiskPath || node.Status.DiskStatus[diskName].DiskUUID != bids.Spec.DiskUUID
		if changeNodeDisk {
			log.Warn("Backing image data source current node and disk is not ready, need to switch to another ready node and disk")
			readyNode, readyDiskName, err := bic.ds.GetReadyNodeDiskForBackingImage(bi)
			if err != nil {
				return err
			}

			// For clone, we choose the same node and disk as the source backing image
			if bi.Spec.SourceType == longhorn.BackingImageDataSourceTypeClone {
				readyNode, readyDiskName, err = bic.findReadyNodeAndDiskForClone(bi)
				if err != nil {
					return nil
				}
			}

			bids.Spec.NodeID = readyNode.Name
			bids.Spec.DiskUUID = readyNode.Status.DiskStatus[readyDiskName].DiskUUID
			bids.Spec.DiskPath = readyNode.Spec.Disks[readyDiskName].Path
		}
	}

	if !reflect.DeepEqual(bids, existingBIDS) {
		if _, err := bic.ds.UpdateBackingImageDataSource(bids); err != nil {
			return err
		}
	}
	return nil
}

func (bic *BackingImageController) handleBackingImageManagers(bi *longhorn.BackingImage) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to handle backing image managers")
	}()

	log := getLoggerForBackingImage(bic.logger, bi)

	if err := bic.cleanupBackingImageManagers(bi); err != nil {
		return err
	}

	for diskUUID := range bi.Spec.DiskFileSpecMap {
		noDefaultBIM := true
		requiredBIs := map[string]string{}

		bimMap, err := bic.ds.ListBackingImageManagersByDiskUUID(diskUUID)
		if err != nil {
			return err
		}
		for _, bim := range bimMap {
			// Add current backing image record to the backing image manager
			if bim.DeletionTimestamp == nil && bim.Spec.Image == bic.bimImageName {
				if uuidInManager, exists := bim.Spec.BackingImages[bi.Name]; !exists || uuidInManager != bi.Status.UUID {
					bim.Spec.BackingImages[bi.Name] = bi.Status.UUID
					if _, err = bic.ds.UpdateBackingImageManager(bim); err != nil {
						return err
					}
				}
				noDefaultBIM = false
				break
			}
			// Otherwise, migrate records from non-default managers
			for biName, uuid := range bim.Spec.BackingImages {
				requiredBIs[biName] = uuid
			}
		}

		if noDefaultBIM {
			log.Infof("Creating default backing image manager for disk %v", diskUUID)

			node, diskName, err := bic.ds.GetReadyDiskNode(diskUUID)
			if err != nil {
				if !types.ErrorIsNotFound(err) {
					return err
				}
				log.WithField("diskUUID", diskUUID).WithError(err).Warn("Disk is not ready hence backing image manager can not be created")
				continue
			}
			requiredBIs[bi.Name] = bi.Status.UUID
			manifest := bic.generateBackingImageManagerManifest(node, diskName, requiredBIs)
			bim, err := bic.ds.CreateBackingImageManager(manifest)
			if err != nil {
				return err
			}

			bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonCreate, "created default backing image manager %v in disk %v on node %v", bim.Name, bim.Spec.DiskUUID, bim.Spec.NodeID)
		}
	}

	return nil
}

// syncBackingImageFileInfo blindly updates the disk file info based on the results of backing image managers.
func (bic *BackingImageController) syncBackingImageFileInfo(bi *longhorn.BackingImage) (err error) {
	log := getLoggerForBackingImage(bic.logger, bi)
	defer func() {
		err = errors.Wrap(err, "failed to sync backing image file state")
	}()

	currentDiskFiles := map[string]struct{}{}

	// Sync with backing image data source when the first file is not ready and not taken over by the backing image manager.
	bids, err := bic.ds.GetBackingImageDataSource(bi.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		log.Warn("Failed to find backing image data source, but controller will continue syncing backing image")
	}
	if bids != nil && bids.Status.CurrentState != longhorn.BackingImageStateReady {
		currentDiskFiles[bids.Spec.DiskUUID] = struct{}{}
		// Due to the possible file type conversion (from raw to qcow2), the size of a backing image data source may changed when the file becomes `ready-for-transfer`.
		// To avoid mismatching, the controller will blindly update bi.Status.Size based on bids.Status.Size here.
		if bids.Status.Size != 0 && bids.Status.Size != bi.Status.Size {
			bi.Status.Size = bids.Status.Size
		}
		if err := bic.updateStatusWithFileInfo(bi,
			bids.Spec.DiskUUID, bids.Status.Message, bids.Status.Checksum, bids.Status.CurrentState, bids.Status.Progress); err != nil {
			return err
		}
	}

	// The file info may temporarily become empty when the data source just transfers the file
	// but the backing image manager has not updated the map.

	bimMap, err := bic.ds.ListBackingImageManagers()
	if err != nil {
		return err
	}
	for _, bim := range bimMap {
		if bim.DeletionTimestamp != nil {
			continue
		}
		info, exists := bim.Status.BackingImageFileMap[bi.Name]
		if !exists {
			continue
		}
		if info.UUID != bi.Status.UUID {
			continue
		}
		// If the backing image data source is up and preparing the 1st file,
		// backing image should ignore the file info of the related backing image manager.
		if bids != nil && bids.Spec.DiskUUID == bim.Spec.DiskUUID && bids.Status.CurrentState != longhorn.BackingImageStateReady {
			continue
		}
		currentDiskFiles[bim.Spec.DiskUUID] = struct{}{}
		if err := bic.updateStatusWithFileInfo(bi,
			bim.Spec.DiskUUID, info.Message, info.CurrentChecksum, info.State, info.Progress); err != nil {
			return err
		}
		if info.Size > 0 {
			if bi.Status.Size == 0 {
				bi.Status.Size = info.Size
				bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonUpdate, "Set size to %v", bi.Status.Size)
			}
			if bi.Status.Size != info.Size {
				if bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].State != longhorn.BackingImageStateFailed {
					msg := fmt.Sprintf("found mismatching size %v reported by backing image manager %v in disk %v, the size recorded in status is %v",
						info.Size, bim.Name, bim.Spec.DiskUUID, bi.Status.Size)
					log.Error(msg)
					bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].State = longhorn.BackingImageStateFailed
					bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].Message = msg
				}
			}
		}
		if info.VirtualSize > 0 {
			if bi.Status.VirtualSize == 0 {
				bi.Status.VirtualSize = info.VirtualSize
				bic.eventRecorder.Eventf(bi, corev1.EventTypeNormal, constant.EventReasonUpdate, "Set virtualSize to %v", bi.Status.VirtualSize)
			}
			if bi.Status.VirtualSize != info.VirtualSize {
				if bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].State != longhorn.BackingImageStateFailed {
					msg := fmt.Sprintf("found mismatching virtualSize %v reported by backing image manager %v in disk %v, the virtualSize recorded in status is %v",
						info.VirtualSize, bim.Name, bim.Spec.DiskUUID, bi.Status.VirtualSize)
					log.Error(msg)
					bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].State = longhorn.BackingImageStateFailed
					bi.Status.DiskFileStatusMap[bim.Spec.DiskUUID].Message = msg
				}
			}
		}
	}

	for diskUUID := range bi.Status.DiskFileStatusMap {
		if _, exists := currentDiskFiles[diskUUID]; !exists {
			delete(bi.Status.DiskFileStatusMap, diskUUID)
		}
	}

	for diskUUID := range bi.Status.DiskFileStatusMap {
		if bi.Status.DiskFileStatusMap[diskUUID].LastStateTransitionTime == "" {
			bi.Status.DiskFileStatusMap[diskUUID].LastStateTransitionTime = util.Now()
		}
	}

	return nil
}

func (bic *BackingImageController) updateStatusWithFileInfo(bi *longhorn.BackingImage,
	diskUUID, message, checksum string, state longhorn.BackingImageState, progress int) error {
	log := getLoggerForBackingImage(bic.logger, bi)

	if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
		bi.Status.DiskFileStatusMap[diskUUID] = &longhorn.BackingImageDiskFileStatus{}
	}
	if bi.Status.DiskFileStatusMap[diskUUID].State != state {
		bi.Status.DiskFileStatusMap[diskUUID].LastStateTransitionTime = util.Now()
		bi.Status.DiskFileStatusMap[diskUUID].State = state
	}
	bi.Status.DiskFileStatusMap[diskUUID].Progress = progress
	bi.Status.DiskFileStatusMap[diskUUID].Message = message

	if checksum != "" {
		if bi.Status.Checksum == "" {
			// This is field is immutable once it is set.
			bi.Status.Checksum = checksum
		}
		if bi.Status.Checksum != checksum {
			if bi.Status.DiskFileStatusMap[diskUUID].State != longhorn.BackingImageStateFailed {
				msg := fmt.Sprintf("Backing image recorded checksum %v doesn't match the file checksum %v in disk %v", bi.Status.Checksum, checksum, diskUUID)
				log.Error(msg)
				bi.Status.DiskFileStatusMap[diskUUID].State = longhorn.BackingImageStateFailed
				bi.Status.DiskFileStatusMap[diskUUID].Message = msg
			}
		}
	}

	return nil
}

func (bic *BackingImageController) updateDiskLastReferenceMap(bi *longhorn.BackingImage) error {
	replicas, err := bic.ds.ListReplicasByBackingImage(bi.Name)
	if err != nil {
		return err
	}
	filesInUse := map[string]struct{}{}
	for _, replica := range replicas {
		filesInUse[replica.Spec.DiskID] = struct{}{}
		delete(bi.Status.DiskLastRefAtMap, replica.Spec.DiskID)
	}
	for diskUUID := range bi.Status.DiskLastRefAtMap {
		if _, exists := bi.Spec.DiskFileSpecMap[diskUUID]; !exists {
			delete(bi.Status.DiskLastRefAtMap, diskUUID)
		}
	}
	for diskUUID := range bi.Spec.DiskFileSpecMap {
		_, isActiveFile := filesInUse[diskUUID]
		_, isRecordedHistoricFile := bi.Status.DiskLastRefAtMap[diskUUID]
		if !isActiveFile && !isRecordedHistoricFile {
			bi.Status.DiskLastRefAtMap[diskUUID] = util.Now()
		}
	}

	return nil
}

func (bic *BackingImageController) generateBackingImageManagerManifest(node *longhorn.Node, diskName string, requiredBackingImages map[string]string) *longhorn.BackingImageManager {
	return &longhorn.BackingImageManager{
		ObjectMeta: metav1.ObjectMeta{
			Labels: types.GetBackingImageManagerLabels(node.Name, node.Status.DiskStatus[diskName].DiskUUID),
			Name:   types.GetBackingImageManagerName(bic.bimImageName, node.Status.DiskStatus[diskName].DiskUUID),
		},
		Spec: longhorn.BackingImageManagerSpec{
			Image:         bic.bimImageName,
			NodeID:        node.Name,
			DiskUUID:      node.Status.DiskStatus[diskName].DiskUUID,
			DiskPath:      node.Spec.Disks[diskName].Path,
			BackingImages: requiredBackingImages,
		},
	}
}

func (bic *BackingImageController) enqueueBackingImage(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	bic.queue.Add(key)
}

func (bic *BackingImageController) enqueueBackingImageForBackingImageManager(obj interface{}) {
	bim, isBIM := obj.(*longhorn.BackingImageManager)
	if !isBIM {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		bim, ok = deletedState.Obj.(*longhorn.BackingImageManager)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non BackingImageManager object: %#v", deletedState.Obj))
			return
		}
	}

	for biName := range bim.Spec.BackingImages {
		key := bim.Namespace + "/" + biName
		bic.queue.Add(key)
	}
	for biName := range bim.Status.BackingImageFileMap {
		key := bim.Namespace + "/" + biName
		bic.queue.Add(key)
	}
}

func (bic *BackingImageController) enqueueBackingImageForBackingImageDataSource(obj interface{}) {
	bic.enqueueBackingImage(obj)
}

func (bic *BackingImageController) enqueueBackingImageForNodeUpdate(oldObj, currObj interface{}) {
	oldNode, ok := oldObj.(*longhorn.Node)
	if !ok {
		deletedState, ok := oldObj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", oldObj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		oldNode, ok = deletedState.Obj.(*longhorn.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	currNode, ok := currObj.(*longhorn.Node)
	if !ok {
		deletedState, ok := currObj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", currObj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		currNode, ok = deletedState.Obj.(*longhorn.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	diskBackingImageMap, err := bic.ds.GetDiskBackingImageMap()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get disk backing image map when handling node update"))
		return
	}

	// if a node or disk changes its EvictionRequested, enqueue all backing image copies on that node/disk
	evictionRequestedChangeOnNodeLevel := currNode.Spec.EvictionRequested != oldNode.Spec.EvictionRequested
	for diskName, newDiskSpec := range currNode.Spec.Disks {
		oldDiskSpec, ok := oldNode.Spec.Disks[diskName]
		evictionRequestedChangeOnDiskLevel := !ok || (newDiskSpec.EvictionRequested != oldDiskSpec.EvictionRequested)
		if diskStatus, existed := currNode.Status.DiskStatus[diskName]; existed && (evictionRequestedChangeOnNodeLevel || evictionRequestedChangeOnDiskLevel) {
			diskUUID := diskStatus.DiskUUID
			for _, backingImage := range diskBackingImageMap[diskUUID] {
				bic.enqueueBackingImage(backingImage)
			}
		}
	}

}

func (bic *BackingImageController) enqueueBackingImageForReplica(obj interface{}) {
	replica, isReplica := obj.(*longhorn.Replica)
	if !isReplica {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		replica, ok = deletedState.Obj.(*longhorn.Replica)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Replica object: %#v", deletedState.Obj))
			return
		}
	}

	if replica.Spec.BackingImage != "" {
		bic.logger.WithField("replica", replica.Name).WithField("backingImage", replica.Spec.BackingImage).Trace("Enqueuing backing image for replica")
		key := replica.Namespace + "/" + replica.Spec.BackingImage
		bic.queue.Add(key)
		return
	}
}

func (bic *BackingImageController) isResponsibleFor(bi *longhorn.BackingImage) bool {
	return isControllerResponsibleFor(bic.controllerID, bic.ds, bi.Name, "", bi.Status.OwnerID)
}

// For cloning, we choose the same node and disk as the source backing image
func (bic *BackingImageController) findReadyNodeAndDiskForClone(bi *longhorn.BackingImage) (*longhorn.Node, string, error) {
	sourceBackingImageName := bi.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterBackingImage]
	sourceBackingImage, err := bic.ds.GetBackingImageRO(sourceBackingImageName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get source backing image %v during cloning", sourceBackingImageName)
	}
	readyNode, readyDiskName, err := bic.ds.GetOneBackingImageReadyNodeDisk(sourceBackingImage)
	if err != nil {
		return nil, "", fmt.Errorf("failed to find one ready source backing image %v during cloning", sourceBackingImageName)
	}
	return readyNode, readyDiskName, nil
}
