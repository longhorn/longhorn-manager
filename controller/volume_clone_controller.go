package controller

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
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

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type VolumeCloneController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds         *datastore.DataStore
	scheduler  *scheduler.ReplicaScheduler
	cacheSyncs []cache.InformerSynced
}

func NewVolumeCloneController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) (*VolumeCloneController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	vcc := &VolumeCloneController{
		baseController: newBaseController("longhorn-volume-clone", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		scheduler: scheduler.NewReplicaScheduler(ds),

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-volume-clone-controller"}),
	}

	var err error
	if _, err = ds.VolumeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vcc.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vcc.enqueueVolume(cur) },
		DeleteFunc: vcc.enqueueVolume,
	}, 0); err != nil {
		return nil, err
	}
	vcc.cacheSyncs = append(vcc.cacheSyncs, ds.VolumeInformer.HasSynced)

	if _, err = ds.ReplicaInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    vcc.enqueueSourceVolumeForReplica,
		UpdateFunc: func(old, cur interface{}) { vcc.enqueueSourceVolumeForReplica(cur) },
	}, 0); err != nil {
		return nil, err
	}
	vcc.cacheSyncs = append(vcc.cacheSyncs, ds.ReplicaInformer.HasSynced)

	return vcc, nil
}

func (vcc *VolumeCloneController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed get key for object %#v: %v", obj, err))
		return
	}

	vcc.queue.Add(key)

	vol, ok := obj.(*longhorn.Volume)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		vol, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	if types.IsDataFromVolume(vol.Spec.DataSource) {
		if srcVolName := types.GetVolumeName(vol.Spec.DataSource); srcVolName != "" {
			// trigger sync for the source volume
			vcc.queue.Add(vcc.namespace + "/" + srcVolName)
		}
	}

}

func (vcc *VolumeCloneController) enqueueVolumeAfter(obj interface{}, duration time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("enqueueVolumeAfter: failed to get key for object %#v: %v", obj, err))
		return
	}

	vcc.queue.AddAfter(key, duration)
}

// enqueueSourceVolumeForReplica enqueues the linked-clone source volume when a
// replica with LinkedCloneSrcReplicaName is created or updated. This ensures the
// controller re-evaluates whether a source attachment ticket is needed.
func (vcc *VolumeCloneController) enqueueSourceVolumeForReplica(obj interface{}) {
	r, ok := obj.(*longhorn.Replica)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		r, ok = deletedState.Obj.(*longhorn.Replica)
		if !ok {
			return
		}
	}

	if r.Spec.LinkedCloneSrcReplicaName == "" {
		return
	}

	vol, err := vcc.ds.GetVolumeRO(r.Spec.VolumeName)
	if err != nil {
		return
	}

	if srcVolName := types.GetVolumeName(vol.Spec.DataSource); srcVolName != "" {
		vcc.queue.Add(vcc.namespace + "/" + srcVolName)
	}
}

func (vcc *VolumeCloneController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vcc.queue.ShutDown()

	vcc.logger.Info("Starting Longhorn volume clone controller")
	defer vcc.logger.Info("Shut down Longhorn volume clone controller")

	if !cache.WaitForNamedCacheSync(vcc.name, stopCh, vcc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vcc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vcc *VolumeCloneController) worker() {
	for vcc.processNextWorkItem() {
	}
}

func (vcc *VolumeCloneController) processNextWorkItem() bool {
	key, quit := vcc.queue.Get()
	if quit {
		return false
	}
	defer vcc.queue.Done(key)
	err := vcc.syncHandler(key.(string))
	vcc.handleErr(err, key)
	return true
}

func (vcc *VolumeCloneController) handleErr(err error, key interface{}) {
	if err == nil {
		vcc.queue.Forget(key)
		return
	}

	log := vcc.logger.WithField("Volume", key)
	handleReconcileErrorLogging(log, err, "Failed to sync Longhorn volume")
	vcc.queue.AddRateLimited(key)
}

func (vcc *VolumeCloneController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync volume %v", vcc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vcc.namespace {
		return nil
	}
	return vcc.reconcile(name)
}

func (vcc *VolumeCloneController) reconcile(volName string) (err error) {
	vol, err := vcc.ds.GetVolumeRO(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !vcc.isResponsibleFor(vol) {
		return nil
	}

	va, err := vcc.ds.GetLHVolumeAttachmentByVolumeName(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		vcc.enqueueVolumeAfter(vol, constant.LonghornVolumeAttachmentNotFoundRetryPeriod)
		return nil
	}
	existingVA := va.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingVA.Spec, va.Spec) {
			return
		}

		if _, err = vcc.ds.UpdateLHVolumeAttachment(va); err != nil {
			return
		}
	}()

	expectedAttachmentTickets := make(map[string]bool)

	var attachableNodes map[string]*longhorn.Node
	log := getLoggerForVolume(vcc.logger, vol)
	pickNodeID := func(v *longhorn.Volume, va *longhorn.VolumeAttachment) (chosenNodeID string, err error) {
		if attachableNodes == nil {
			attachableNodes, err = vcc.ds.ListReadyNodesWithReadyInstanceManagerRO(v.Spec.DataEngine)
			if err != nil {
				return "", err
			}
		}

		// The CurrentNodeID holds the 1st priority if it is valid.
		// The corresponding ticket is implicitly satisfied.
		if attachableNodes[v.Status.CurrentNodeID] != nil {
			log.Debugf("Picked node %v for volume %v clone attachment: currently attached node", v.Status.CurrentNodeID, v.Name)
			return v.Status.CurrentNodeID, nil
		}

		// The node already selected by other tickets holds the 2nd priority if it is valid.
		// Among tickets with the highest priority level, pick the node that appears most
		// frequently; break ties by sorting node IDs lexicographically.
		highestPriority := -1
		nodeCount := map[string]int{}
		for _, ticket := range va.Spec.AttachmentTickets {
			if attachableNodes[ticket.NodeID] == nil {
				continue
			}
			priority := longhorn.GetAttacherPriorityLevel(ticket.Type)
			if priority > highestPriority {
				highestPriority = priority
				nodeCount = map[string]int{ticket.NodeID: 1}
			} else if priority == highestPriority {
				nodeCount[ticket.NodeID]++
			}
		}
		if len(nodeCount) > 0 {
			maxCount := 0
			var candidates []string
			for nodeID, count := range nodeCount {
				if count > maxCount {
					maxCount = count
					candidates = []string{nodeID}
				} else if count == maxCount {
					candidates = append(candidates, nodeID)
				}
			}
			sort.Strings(candidates)
			log.Debugf("Picked node %v for volume %v clone attachment: majority node among highest-priority tickets", candidates[0], v.Name)
			return candidates[0], nil
		}

		// The node of v.Status.OwnerID holds the 3rd priority if it is valid
		if attachableNodes[v.Status.OwnerID] != nil {
			log.Debugf("Picked node %v for volume %v clone attachment: volume owner node", v.Status.OwnerID, v.Name)
			return v.Status.OwnerID, nil
		}

		// Otherwise, pick up the 1st node in sorted order for determinism
		candidates := make([]string, 0, len(attachableNodes))
		for n := range attachableNodes {
			candidates = append(candidates, n)
		}
		sort.Strings(candidates)
		if len(candidates) > 0 {
			log.Debugf("Picked node %v for volume %v clone attachment: first available ready node", candidates[0], v.Name)
			return candidates[0], nil
		}

		log.Warnf("Cannot find a valid node for volume %v clone attachment, will clean up stale tickets and retry", v.Name)
		return "", nil
	}

	// case 1: this volume is target of a clone and the cloning hasn't completed
	if isCloneTargetActive(vol) {
		cloningAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeVolumeCloneController, volName)
		var attachNodeID string
		attachNodeID, err = vcc.scheduler.GetReadyNodeForVolumeAttach(vol, vol.Status.OwnerID)
		if err != nil {
			return err
		}

		if attachNodeID == "" {
			vcc.enqueueVolumeAfter(vol, constant.LonghornVolumeAttachmentNotFoundRetryPeriod)
		} else {
			createOrUpdateAttachmentTicket(va, cloningAttachmentTicketID, attachNodeID, longhorn.TrueValue, longhorn.AttacherTypeVolumeCloneController)
			expectedAttachmentTickets[cloningAttachmentTicketID] = true
		}
	}

	// case 2: this volume is source of a clone (initial clone in progress)
	vols, err := vcc.ds.ListVolumesRO()
	if err != nil {
		return err
	}
	var srcNodeID string
	for _, v := range vols {
		if isCloneTargetCopyInProgress(v) && types.GetVolumeName(v.Spec.DataSource) == vol.Name {
			if srcNodeID == "" {
				srcNodeID, err = pickNodeID(vol, va)
				if err != nil {
					return err
				}
			}
			if srcNodeID != "" {
				cloningAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeVolumeCloneController, v.Name)
				createOrUpdateAttachmentTicket(va, cloningAttachmentTicketID, srcNodeID, longhorn.AnyValue, longhorn.AttacherTypeVolumeCloneController)
				expectedAttachmentTickets[cloningAttachmentTicketID] = true
			}
		}
	}

	// case 3: this volume is source of a linked-clone target that needs rebuild
	// (post-initial-clone: clone completed or awaiting healthy, and has replicas pending rebuild)
	// Use a single node for all clone tickets to avoid attaching the source to multiple nodes.
	srcNodeID = ""
	for _, v := range vols {
		if !isLinkedClonePotentiallyNeedingSource(v, vol.Name) {
			continue
		}
		// Check if this clone volume actually has replicas pending linked-clone rebuild
		if !vcc.hasReplicasPendingLinkedCloneRebuild(v.Name) {
			continue
		}
		if srcNodeID == "" {
			srcNodeID, err = pickNodeID(vol, va)
			if err != nil {
				return err
			}
		}
		if srcNodeID != "" {
			cloningAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeVolumeCloneController, v.Name)
			createOrUpdateAttachmentTicket(va, cloningAttachmentTicketID, srcNodeID, longhorn.AnyValue, longhorn.AttacherTypeVolumeCloneController)
			expectedAttachmentTickets[cloningAttachmentTicketID] = true
		}
	}

	// Delete unexpected attachment tickets
	for attachmentTicketID, attachmentTicket := range va.Spec.AttachmentTickets {
		if attachmentTicket.Type == longhorn.AttacherTypeVolumeCloneController {
			if _, ok := expectedAttachmentTickets[attachmentTicketID]; !ok {
				delete(va.Spec.AttachmentTickets, attachmentTicketID)
			}
		}
	}

	return nil
}

// hasReplicasPendingLinkedCloneRebuild returns true if the volume has at least one
// replica that needs a linked-clone rebuild (has LinkedCloneSrcReplicaName set but
// is not yet healthy).
func (vcc *VolumeCloneController) hasReplicasPendingLinkedCloneRebuild(volumeName string) bool {
	replicas, err := vcc.ds.ListVolumeReplicasRO(volumeName)
	if err != nil {
		return false
	}
	for _, r := range replicas {
		if r.Spec.LinkedCloneSrcReplicaName != "" && r.Spec.HealthyAt == "" && r.Spec.FailedAt == "" {
			return true
		}
	}
	return false
}

func (vcc *VolumeCloneController) isResponsibleFor(vol *longhorn.Volume) bool {
	return vcc.controllerID == vol.Status.OwnerID
}
