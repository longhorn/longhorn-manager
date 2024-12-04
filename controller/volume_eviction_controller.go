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

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type VolumeEvictionController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds         *datastore.DataStore
	cacheSyncs []cache.InformerSynced
	scheduler  *scheduler.ReplicaScheduler
}

func NewVolumeEvictionController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) (*VolumeEvictionController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	vec := &VolumeEvictionController{
		baseController: newBaseController("longhorn-volume-eviction", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-volume-eviction-controller"}),
	}

	vec.scheduler = scheduler.NewReplicaScheduler(ds)

	var err error
	if _, err = ds.VolumeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    vec.enqueueVolume,
		UpdateFunc: func(old, cur interface{}) { vec.enqueueVolume(cur) },
		DeleteFunc: vec.enqueueVolume,
	}); err != nil {
		return nil, err
	}
	vec.cacheSyncs = append(vec.cacheSyncs, ds.VolumeInformer.HasSynced)

	// TODO: do we need to watch replica CR?

	return vec, nil
}

func (vec *VolumeEvictionController) enqueueVolume(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	vec.queue.Add(key)
}

func (vec *VolumeEvictionController) enqueueVolumeAfter(obj interface{}, duration time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("enqueueVolumeAfter: failed to get key for object %#v: %v", obj, err))
		return
	}

	vec.queue.AddAfter(key, duration)
}

func (vec *VolumeEvictionController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer vec.queue.ShutDown()

	vec.logger.Info("Starting Longhorn eviction controller")
	defer vec.logger.Info("Shut down Longhorn eviction controller")

	if !cache.WaitForNamedCacheSync(vec.name, stopCh, vec.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(vec.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (vec *VolumeEvictionController) worker() {
	for vec.processNextWorkItem() {
	}
}

func (vec *VolumeEvictionController) processNextWorkItem() bool {
	key, quit := vec.queue.Get()
	if quit {
		return false
	}
	defer vec.queue.Done(key)
	err := vec.syncHandler(key.(string))
	vec.handleErr(err, key)
	return true
}

func (vec *VolumeEvictionController) handleErr(err error, key interface{}) {
	if err == nil {
		vec.queue.Forget(key)
		return
	}

	log := vec.logger.WithField("Volume", key)
	handleReconcileErrorLogging(log, err, "Failed to sync Longhorn volume")
	vec.queue.AddRateLimited(key)
}

func (vec *VolumeEvictionController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: failed to sync volume %v", vec.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != vec.namespace {
		return nil
	}
	return vec.reconcile(name)
}

func (vec *VolumeEvictionController) reconcile(volName string) (err error) {
	vol, err := vec.ds.GetVolume(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	if !vec.isResponsibleFor(vol) {
		return nil
	}

	replicas, err := vec.ds.ListVolumeReplicas(vol.Name)
	if err != nil {
		return err
	}

	va, err := vec.ds.GetLHVolumeAttachmentByVolumeName(volName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		vec.enqueueVolumeAfter(vol, constant.LonghornVolumeAttachmentNotFoundRetryPeriod)
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

		if _, err = vec.ds.UpdateLHVolumeAttachment(va); err != nil {
			return
		}
	}()

	evictingAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeVolumeEvictionController, volName)

	if hasReplicaEvictionRequested(replicas) {
		if vec.hasDiskCandidateForReplicaEviction(replicas, vol) {
			createOrUpdateAttachmentTicket(va, evictingAttachmentTicketID, vol.Status.OwnerID, longhorn.AnyValue, longhorn.AttacherTypeVolumeEvictionController)
		}
	} else {
		delete(va.Spec.AttachmentTickets, evictingAttachmentTicketID)
	}

	return nil
}

func (vec *VolumeEvictionController) hasDiskCandidateForReplicaEviction(replicas map[string]*longhorn.Replica, volume *longhorn.Volume) bool {
	for _, replica := range replicas {
		replicaCopy := replica.DeepCopy()
		replicaCopy.Spec.HardNodeAffinity = ""

		diskCandidates, multiError, err := vec.scheduler.FindDiskCandidates(replicaCopy, replicas, volume)
		if err != nil {
			vec.logger.WithError(err).Warnf("Failed to find disk candidates for evicting replica %q", replica.Name)
			return false
		}

		if len(diskCandidates) == 0 {
			aggregatedReplicaScheduledError := util.NewMultiError(longhorn.ErrorReplicaScheduleEvictReplicaFailed)
			if multiError != nil {
				aggregatedReplicaScheduledError.Append(multiError)
			}
			vec.logger.Warnf("No disk candidates for evicting replica %q: %v", replica.Name, aggregatedReplicaScheduledError.Join())
			return false
		}
	}

	vec.logger.Infof("Found disk candidates for evicting replicas of volume %q", volume.Name)
	return true
}

func (vec *VolumeEvictionController) isResponsibleFor(vol *longhorn.Volume) bool {
	return vec.controllerID == vol.Status.OwnerID
}
