package controller

import (
	"fmt"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"
	"reflect"
	"strconv"
	"time"
)

type VolumeAttachmentController struct {
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

func NewLonghornVolumeAttachmentController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *VolumeAttachmentController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	vac := &VolumeAttachmentController{
		baseController: newBaseController("longhorn-volume-attachment", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-volume-attachment-controller"}),
	}

	ds.LHVolumeAttachmentInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vac.enqueueVolumeAttachment,
		UpdateFunc: func(old, cur interface{}) { vac.enqueueVolumeAttachment(cur) },
		DeleteFunc: vac.enqueueVolumeAttachment,
	}, 0)
	vac.cacheSyncs = append(vac.cacheSyncs, ds.LHVolumeAttachmentInformer.HasSynced)

	ds.VolumeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vac.enqueueForLonghornVolume,
		UpdateFunc: func(old, cur interface{}) { vac.enqueueForLonghornVolume(cur) },
		DeleteFunc: vac.enqueueForLonghornVolume,
	}, 0)
	vac.cacheSyncs = append(vac.cacheSyncs, ds.VolumeInformer.HasSynced)

	return vac
}

func (vac *VolumeAttachmentController) enqueueVolumeAttachment(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}
	vac.queue.Add(key)
}

func (vac *VolumeAttachmentController) enqueueForLonghornVolume(obj interface{}) {
	vol, ok := obj.(*longhorn.Volume)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		// use the last known state, to enqueue, dependent objects
		vol, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	volumeAttachments, err := vac.ds.ListLonghornVolumeAttachmentByVolumeRO(vol.Name)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list Longhorn VolumeAttachment of volume %v: %v", vol.Name, err))
		return
	}

	for _, va := range volumeAttachments {
		vac.enqueueVolumeAttachment(va)
	}
}

func (vac *VolumeAttachmentController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vac.queue.ShutDown()

	vac.logger.Infof("Start Longhorn VolumeAttachment controller")
	defer vac.logger.Infof("Shutting down Longhorn VolumeAttachment controller")

	if !cache.WaitForNamedCacheSync(vac.name, stopCh, vac.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vac.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vac *VolumeAttachmentController) worker() {
	for vac.processNextWorkItem() {
	}
}

func (vac *VolumeAttachmentController) processNextWorkItem() bool {
	key, quit := vac.queue.Get()
	if quit {
		return false
	}
	defer vac.queue.Done(key)
	err := vac.syncHandler(key.(string))
	vac.handleErr(err, key)
	return true
}

func (vac *VolumeAttachmentController) handleErr(err error, key interface{}) {
	if err == nil {
		vac.queue.Forget(key)
		return
	}

	vac.logger.WithError(err).Warnf("Error syncing Longhorn VolumeAttachment %v", key)
	vac.queue.AddRateLimited(key)
	return
}

func (vac *VolumeAttachmentController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync VolumeAttachment %v", vac.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vac.namespace {
		return nil
	}
	return vac.reconcile(name)
}

func (vac *VolumeAttachmentController) reconcile(vaName string) (err error) {
	va, err := vac.ds.GetLHVolumeAttachment(vaName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	isResponsible, err := vac.isResponsibleFor(va)
	if err != nil {
		return err
	}
	if !isResponsible {
		return nil
	}

	//log := getLoggerForLHVolumeAttachment(vac.logger, va)

	vol, err := vac.ds.GetVolume(va.Spec.Volume)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// remove finalizer
			return
		}
		return err
	}

	// handle VA deletion flow

	existingVA := va.DeepCopy()
	existingVol := vol.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if !reflect.DeepEqual(existingVol.Spec, vol.Spec) {
			if _, err = vac.ds.UpdateVolume(vol); err != nil {
				return
			}

		}
		if !reflect.DeepEqual(existingVA.Status, va.Status) {
			if _, err = vac.ds.UpdateLHVolumeAttachmetStatus(va); err != nil {
				return
			}
		}
		return
	}()

	vac.handleVolumeDetachment(va, vol)

	vac.handleVolumeAttachment(va, vol)

	return vac.handleVAStatusUpdate(va, vol)
}

func (vac *VolumeAttachmentController) handleVolumeDetachment(va *longhorn.VolumeAttachment, vol *longhorn.Volume) {
	// Volume is already trying to detach
	if vol.Spec.NodeID == "" {
		return
	}

	if !shouldDoDetach(va, vol) {
		return
	}

	// There is no attachment that request the current vol.Spec.NodeID.
	// Therefore, set desire state of volume to empty
	vol.Spec.NodeID = ""
	// reset the attachment parameter
	setAttachmentParameter(&longhorn.Attachment{}, vol)
	return
}

func shouldDoDetach(va *longhorn.VolumeAttachment, vol *longhorn.Volume) bool {
	// For auto salvage logic
	// TODO: create Auto Salvage controller to hanlde this logic instead of AD controller
	if vol.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		return true
	}
	for _, attachment := range va.Spec.Attachments {
		// Found one attachment that is still requesting volume to attach to the current node
		if attachment.NodeID == vol.Spec.NodeID && verifyAttachmentParameters(attachment, vol) {
			return false
		}
	}
	return true
}

func (vac *VolumeAttachmentController) handleVolumeAttachment(va *longhorn.VolumeAttachment, vol *longhorn.Volume) {
	// Wait for volume to be ready to be fully detached
	if vol.Spec.NodeID != "" ||
		vol.Spec.MigrationNodeID != "" ||
		vol.Status.PendingNodeID != "" ||
		vol.Status.State != longhorn.VolumeStateDetached {
		return
	}

	// For auto salvage logic
	// TODO: create Auto Salvage controller to hanlde this logic instead of AD controller
	if vol.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		return
	}

	attachment := selectAttachmentToAttach(va)
	if attachment == nil {
		return
	}

	vol.Spec.NodeID = attachment.NodeID
	setAttachmentParameter(attachment, vol)
	return
}

func selectAttachmentToAttach(va *longhorn.VolumeAttachment) *longhorn.Attachment {
	if len(va.Spec.Attachments) == 0 {
		return nil
	}

	highPriorityAttachments := []*longhorn.Attachment{}
	maxAttacherPriorityLevel := 0
	for _, attachment := range va.Spec.Attachments {
		priorityLevel := longhorn.GetAttacherPriorityLevel(attachment.Type)
		if priorityLevel > maxAttacherPriorityLevel {
			maxAttacherPriorityLevel = priorityLevel
		}
	}

	for _, attachment := range va.Spec.Attachments {
		priorityLevel := longhorn.GetAttacherPriorityLevel(attachment.Type)
		if priorityLevel == maxAttacherPriorityLevel {
			highPriorityAttachments = append(highPriorityAttachments, attachment)
		}
	}

	// TODO: sort by time

	// sort by name
	shortestNameAttachment := highPriorityAttachments[0]
	for _, attachment := range highPriorityAttachments {
		if attachment.ID < shortestNameAttachment.ID {
			shortestNameAttachment = attachment
		}
	}

	return shortestNameAttachment
}

func (vac *VolumeAttachmentController) handleVAStatusUpdate(va *longhorn.VolumeAttachment, vol *longhorn.Volume) error {
	// initialize the va.Status.Attachments map if needed
	if va.Status.Attachments == nil {
		va.Status.Attachments = make(map[string]*longhorn.Attachment)
	}

	// Attachments that desires detaching
	for _, attachment := range va.Status.Attachments {
		if _, ok := va.Spec.Attachments[attachment.ID]; !ok {
			updateStatusForDesiredDetachingAttachment(attachment, va, vol)
		}
	}

	// Attachments that are requesting to attach
	for _, attachment := range va.Spec.Attachments {
		updateStatusForDesiredAttachingAttachment(attachment, va, vol)
	}
	return nil
}

func updateStatusForDesiredDetachingAttachment(attachment *longhorn.Attachment, va *longhorn.VolumeAttachment, vol *longhorn.Volume) {
	//TODO: How to handle vol.Status.IsStandby volume
	delete(va.Status.Attachments, attachment.ID)
}

func updateStatusForDesiredAttachingAttachment(attachment *longhorn.Attachment, va *longhorn.VolumeAttachment, vol *longhorn.Volume) {
	if _, ok := va.Status.Attachments[attachment.ID]; !ok {
		copiedAttachment := longhorn.Attachment{
			ID:         attachment.ID,
			Type:       attachment.Type,
			NodeID:     attachment.NodeID,
			Parameters: copyStringMap(attachment.Parameters),
		}
		va.Status.Attachments[attachment.ID] = &copiedAttachment
	}

	attachment = va.Status.Attachments[attachment.ID]
	if vol.Spec.NodeID == "" {
		return
	}

	if vol.Spec.NodeID != attachment.NodeID {
		attachment.AttachError = &longhorn.VolumeError{
			// TODO: Do we need to generate TimeStamp here?
			Message: fmt.Sprintf("cannot attach the volume to node %v because volume has already desired to be attached to node %v", attachment.NodeID, vol.Spec.NodeID),
		}
		return
	}

	if vol.Status.CurrentNodeID == attachment.NodeID && vol.Status.State == longhorn.VolumeStateAttached {
		if !verifyAttachmentParameters(attachment, vol) {
			attachment.AttachError = &longhorn.VolumeError{
				// TODO: Do we need to generate TimeStamp here?
				Message: fmt.Sprintf("volume %v has already attached to node %v with incompatible parameters", vol.Name, vol.Status.CurrentNodeID),
			}
			return
		}
		attachment.Attached = true
		attachment.AttachError = nil
	}
	return
}

func verifyAttachmentParameters(attachment *longhorn.Attachment, vol *longhorn.Volume) bool {
	disableFrontend, _ := strconv.ParseBool(attachment.Parameters["disableFrontend"])
	return vol.Spec.DisableFrontend == disableFrontend
}

func setAttachmentParameter(attachment *longhorn.Attachment, vol *longhorn.Volume) {
	vol.Spec.DisableFrontend, _ = strconv.ParseBool(attachment.Parameters["disableFrontend"])
	vol.Spec.LastAttachedBy = attachment.Parameters["lastAttachedBy"]
}

func copyStringMap(originalMap map[string]string) map[string]string {
	CopiedMap := make(map[string]string)
	for index, element := range originalMap {
		CopiedMap[index] = element
	}
	return CopiedMap
}

func (vac *VolumeAttachmentController) isResponsibleFor(va *longhorn.VolumeAttachment) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	volume, err := vac.ds.GetVolumeRO(va.Spec.Volume)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return vac.controllerID == volume.Status.OwnerID, nil
}

func getLoggerForLHVolumeAttachment(logger logrus.FieldLogger, va *longhorn.VolumeAttachment) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"longhornVolumeAttachment": va.Name,
		},
	)
}
