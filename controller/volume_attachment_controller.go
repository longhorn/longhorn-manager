package controller

import (
	"fmt"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
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
			if !va.DeletionTimestamp.IsZero() {
				return vac.ds.RemoveFinalizerForLHVolumeAttachment(va)
			}
			return nil
		}
		return err
	}

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
			if _, err = vac.ds.UpdateLHVolumeAttachmentStatus(va); err != nil {
				return
			}
		}
		return
	}()

	// Note that in this controller the desire state is recorded in VA.Spec
	// and the current state of the world is recorded inside volume CR

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

	// There is no attachment ticket that request the current vol.Spec.NodeID.
	// Therefore, set desire state of volume to empty
	vol.Spec.NodeID = ""
	// reset the attachment parameter for vol
	setAttachmentParameter(map[string]string{}, vol)
	return
}

func shouldDoDetach(va *longhorn.VolumeAttachment, vol *longhorn.Volume) bool {
	// For auto salvage logic
	// TODO: create Auto Salvage controller to handle this logic instead of AD controller
	if vol.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		return true
	}
	for _, attachmentTicket := range va.Spec.AttachmentTickets {
		// For the RWX volume attachment, VolumeAttachment controller will not directly handle
		// the tickets from the CSI plugin. Instead, ShareManager controller will add a
		// AttacherTypeShareManagerController ticket (as the summarization of CSI tickets) then
		// the VolumeAttachment controller is responsible for handling the AttacherTypeShareManagerController
		// tickets only. See more at https://github.com/longhorn/longhorn-manager/pull/1541#issuecomment-1429044946
		if isCSIAttacherTicketOfRegularRWXVolume(attachmentTicket, vol) {
			continue
		}
		// Found one attachmentTicket that is still requesting volume to attach to the current node
		if attachmentTicket.NodeID == vol.Spec.NodeID && verifyAttachmentParameters(attachmentTicket.Parameters, vol) {
			return false
		}
	}
	return true
}

func (vac *VolumeAttachmentController) handleVolumeAttachment(va *longhorn.VolumeAttachment, vol *longhorn.Volume) {
	// Wait for volume to be fully detached
	if !isVolumeFullyDetached(vol) {
		return
	}

	// For auto salvage logic
	// TODO: create Auto Salvage controller to handle this logic instead of AD controller
	if vol.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		return
	}

	attachmentTicket := selectAttachmentTicketToAttach(va, vol)
	if attachmentTicket == nil {
		return
	}

	vol.Spec.NodeID = attachmentTicket.NodeID
	setAttachmentParameter(attachmentTicket.Parameters, vol)
	return
}

func isVolumeFullyDetached(vol *longhorn.Volume) bool {
	return vol.Spec.NodeID == "" &&
		vol.Spec.MigrationNodeID == "" &&
		vol.Status.PendingNodeID == "" &&
		vol.Status.State == longhorn.VolumeStateDetached
}

func selectAttachmentTicketToAttach(va *longhorn.VolumeAttachment, vol *longhorn.Volume) *longhorn.AttachmentTicket {
	ticketCandidates := []*longhorn.AttachmentTicket{}
	for _, attachmentTicket := range va.Spec.AttachmentTickets {
		if isCSIAttacherTicketOfRegularRWXVolume(attachmentTicket, vol) {
			continue
		}
		ticketCandidates = append(ticketCandidates, attachmentTicket)
	}

	maxAttacherPriorityLevel := 0
	for _, attachmentTicket := range ticketCandidates {
		priorityLevel := longhorn.GetAttacherPriorityLevel(attachmentTicket.Type)
		if priorityLevel > maxAttacherPriorityLevel {
			maxAttacherPriorityLevel = priorityLevel
		}
	}

	highPriorityTicketCandidates := []*longhorn.AttachmentTicket{}
	for _, attachmentTicket := range ticketCandidates {
		priorityLevel := longhorn.GetAttacherPriorityLevel(attachmentTicket.Type)
		if priorityLevel == maxAttacherPriorityLevel {
			highPriorityTicketCandidates = append(highPriorityTicketCandidates, attachmentTicket)
		}
	}

	// TODO: sort by time

	// sort by name
	if len(highPriorityTicketCandidates) == 0 {
		return nil
	}
	shortestNameAttachmentTicket := highPriorityTicketCandidates[0]
	for _, attachmentTicket := range highPriorityTicketCandidates {
		if attachmentTicket.ID < shortestNameAttachmentTicket.ID {
			shortestNameAttachmentTicket = attachmentTicket
		}
	}

	return shortestNameAttachmentTicket
}

func (vac *VolumeAttachmentController) handleVAStatusUpdate(va *longhorn.VolumeAttachment, vol *longhorn.Volume) error {
	if va.Status.AttachmentTicketStatuses == nil {
		va.Status.AttachmentTicketStatuses = make(map[string]*longhorn.AttachmentTicketStatus)
	}

	// Attachment ticket that desires detaching
	for _, attachmentTicketStatus := range va.Status.AttachmentTicketStatuses {
		if _, ok := va.Spec.AttachmentTickets[attachmentTicketStatus.ID]; !ok {
			updateStatusForDesiredDetachingAttachmentTicket(attachmentTicketStatus.ID, va)
		}
	}

	// Attachment that requests to attach
	for _, attachmentTicket := range va.Spec.AttachmentTickets {
		if err := updateStatusForDesiredAttachingAttachmentTicket(attachmentTicket.ID, va, vol); err != nil {
			return err
		}
	}
	return nil
}

func updateStatusForDesiredDetachingAttachmentTicket(attachmentTicketID string, va *longhorn.VolumeAttachment) {
	//TODO: How to handle vol.Status.IsStandby volume
	delete(va.Status.AttachmentTicketStatuses, attachmentTicketID)
}

func updateStatusForDesiredAttachingAttachmentTicket(attachmentTicketID string, va *longhorn.VolumeAttachment, vol *longhorn.Volume) error {
	if _, ok := va.Status.AttachmentTicketStatuses[attachmentTicketID]; !ok {
		va.Status.AttachmentTicketStatuses[attachmentTicketID] = &longhorn.AttachmentTicketStatus{
			ID: attachmentTicketID,
			// TODO: handle condition initialization here
		}
	}

	attachmentTicket, ok := va.Spec.AttachmentTickets[attachmentTicketID]
	if !ok {
		return fmt.Errorf("updateStatusForDesiredAttachingAttachmentTicket: missing the attachment ticket with id %v in the va.Spec.AttachmentTickets", attachmentTicketID)
	}
	attachmentTicketStatus := va.Status.AttachmentTicketStatuses[attachmentTicketID]

	defer func() {
		attachmentTicketStatus.Generation = attachmentTicket.Generation
	}()

	// user change the VA.Attachment.Spec to node-2
	// VA controller set the vol.Spec.NodeID = ""
	// Volume controller start detach volume -> state become detached
	// VA controller set Vol.Spec.NodeID to node-2
	// If vol.Status.State is detached, VA controller VA.Attachment.Status.NodeID = ""; VA.Attachment.Status.Parameter = {}; VA.Attachment.Status.Type = ""
	// Volume controller start attach -> state becomes attached to node-2
	// If vol.Status.State is attached, VA controller VA.Attachment.Status.NodeID = node-2; VA.Attachment.Status.Parameter = {xx}; VA.Attachment.Status.Type = "xx"
	//}

	// TODO: consider handle the csi-attacher ticket for RWX volume differently
	if isCSIAttacherTicketOfRegularRWXVolume(attachmentTicket, vol) {
		if isVolumeShareAvailable(vol) {
			attachmentTicketStatus.Satisfied = true
			attachmentTicketStatus.Conditions = types.SetCondition(
				attachmentTicketStatus.Conditions,
				longhorn.AttachmentStatusConditionTypeSatisfied,
				longhorn.ConditionStatusTrue,
				"",
				"The attachment ticket is satisfied",
			)
			return nil
		}
		attachmentTicketStatus.Satisfied = false
		attachmentTicketStatus.Conditions = types.SetCondition(
			attachmentTicketStatus.Conditions,
			longhorn.AttachmentStatusConditionTypeSatisfied,
			longhorn.ConditionStatusFalse,
			"",
			"waiting for volume share to be available",
		)

		return nil
	}

	if vol.Status.CurrentNodeID == "" || vol.Status.State != longhorn.VolumeStateAttached {
		attachmentTicketStatus.Satisfied = false
		attachmentTicketStatus.Conditions = types.SetCondition(
			attachmentTicketStatus.Conditions,
			longhorn.AttachmentStatusConditionTypeSatisfied,
			longhorn.ConditionStatusFalse,
			"", // reason should be a code indicating the error type
			"",
		)

		// TODO: check if the engine image is ready on the node
		// check if the node is down
		// to set the condition for the client to consume
		return nil
	}

	if attachmentTicket.NodeID != vol.Status.CurrentNodeID {
		attachmentTicketStatus.Satisfied = false
		attachmentTicketStatus.Conditions = types.SetCondition(
			attachmentTicketStatus.Conditions,
			longhorn.AttachmentStatusConditionTypeSatisfied,
			longhorn.ConditionStatusFalse,
			"",
			fmt.Sprintf("the volume is currently attached to different node %v ", vol.Status.CurrentNodeID),
		)
		return nil
	}

	if vol.Status.CurrentNodeID == attachmentTicket.NodeID && vol.Status.State == longhorn.VolumeStateAttached {
		if !verifyAttachmentParameters(attachmentTicket.Parameters, vol) {
			attachmentTicketStatus.Satisfied = false
			attachmentTicketStatus.Conditions = types.SetCondition(
				attachmentTicketStatus.Conditions,
				longhorn.AttachmentStatusConditionTypeSatisfied,
				longhorn.ConditionStatusFalse,
				"",
				fmt.Sprintf("volume %v has already attached to node %v with incompatible parameters", vol.Name, vol.Status.CurrentNodeID),
			)
			return nil
		}
		attachmentTicketStatus.Satisfied = true
		attachmentTicketStatus.Conditions = types.SetCondition(
			attachmentTicketStatus.Conditions,
			longhorn.AttachmentStatusConditionTypeSatisfied,
			longhorn.ConditionStatusTrue,
			"",
			"",
		)
	}
	return nil
}

func verifyAttachmentParameters(parameters map[string]string, vol *longhorn.Volume) bool {
	disableFrontendString, ok := parameters["disableFrontend"]
	if !ok || disableFrontendString == longhorn.FalseValue {
		return vol.Spec.DisableFrontend == false
	} else if disableFrontendString == longhorn.TrueValue {
		return vol.Spec.DisableFrontend == true
	}
	return true
}

func setAttachmentParameter(parameters map[string]string, vol *longhorn.Volume) {
	disableFrontendString, ok := parameters["disableFrontend"]
	if !ok || disableFrontendString == longhorn.FalseValue {
		vol.Spec.DisableFrontend = false
	} else if disableFrontendString == longhorn.TrueValue {
		vol.Spec.DisableFrontend = true
	}
	vol.Spec.LastAttachedBy = parameters["lastAttachedBy"]
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

func isCSIAttacherTicketOfRegularRWXVolume(attachmentTicket *longhorn.AttachmentTicket, vol *longhorn.Volume) bool {
	if attachmentTicket == nil || vol == nil {
		return false
	}
	isRegularRWXVolume := vol.Spec.AccessMode == longhorn.AccessModeReadWriteMany && !vol.Spec.Migratable
	isCSIAttacherTicket := attachmentTicket.Type == longhorn.AttacherTypeCSIAttacher
	return isRegularRWXVolume && isCSIAttacherTicket
}

func isVolumeShareAvailable(vol *longhorn.Volume) bool {
	return vol.Spec.AccessMode == longhorn.AccessModeReadWriteMany &&
		vol.Status.ShareState == longhorn.ShareManagerStateRunning &&
		vol.Status.ShareEndpoint != ""
}
