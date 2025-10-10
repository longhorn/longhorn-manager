package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/controller"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/csi/crypto"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const shareManagerLeaseDurationSeconds = 7 // This should be slightly more than twice the share-manager lease renewal interval.

type nfsServerConfig struct {
	enableFastFailover bool
	leaseLifetime      int
	gracePeriod        int
}

type ShareManagerController struct {
	*baseController

	namespace      string
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	backoff *flowcontrol.Backoff
}

func NewShareManagerController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,

	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string) (*ShareManagerController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)

	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	c := &ShareManagerController{
		baseController: newBaseController("longhorn-share-manager", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-share-manager-controller"}),

		ds: ds,

		backoff: newBackoff(context.TODO()),
	}

	var err error
	// need shared volume manager informer
	if _, err = ds.ShareManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueShareManager,
		UpdateFunc: func(old, cur interface{}) { c.enqueueShareManager(cur) },
		DeleteFunc: c.enqueueShareManager,
	}); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.ShareManagerInformer.HasSynced)

	// need information for volumes, to be able to claim them
	if _, err = ds.VolumeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueShareManagerForVolume,
		UpdateFunc: func(old, cur interface{}) { c.enqueueShareManagerForVolume(cur) },
		DeleteFunc: c.enqueueShareManagerForVolume,
	}, 0); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.VolumeInformer.HasSynced)

	// we are only interested in pods for which we are responsible for managing
	if _, err = ds.PodInformer.AddEventHandlerWithResyncPeriod(cache.FilteringResourceEventHandler{
		FilterFunc: isShareManagerPod,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    c.enqueueShareManagerForPod,
			UpdateFunc: func(old, cur interface{}) { c.enqueueShareManagerForPod(cur) },
			DeleteFunc: c.enqueueShareManagerForPod,
		},
	}, 0); err != nil {
		return nil, err
	}
	c.cacheSyncs = append(c.cacheSyncs, ds.PodInformer.HasSynced)

	return c, nil
}

func getLoggerForShareManager(logger logrus.FieldLogger, sm *longhorn.ShareManager) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"shareManager": sm.Name,
			"volume":       sm.Name,
			"owner":        sm.Status.OwnerID,
			"state":        sm.Status.State,
		},
	)
}

func (c *ShareManagerController) enqueueShareManager(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

func (c *ShareManagerController) enqueueShareManagerForVolume(obj interface{}) {
	volume, isVolume := obj.(*longhorn.Volume)
	if !isVolume {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue the ShareManager
		volume, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Volume object: %#v", deletedState.Obj))
			return
		}
	}

	if isRegularRWXVolume(volume) {
		// we can queue the key directly since a share manager only manages a single volume from it's own namespace
		// and there is no need for us to retrieve the whole object, since we already know the volume name
		key := volume.Namespace + "/" + volume.Name
		c.queue.Add(key)
		return
	}
}

func (c *ShareManagerController) enqueueShareManagerForPod(obj interface{}) {
	pod, isPod := obj.(*corev1.Pod)
	if !isPod {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue the ShareManager
		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Pod object: %#v", deletedState.Obj))
			return
		}
	}

	// we can queue the key directly since a share manager only manages pods from it's own namespace
	// and there is no need for us to retrieve the whole object, since the share manager name is stored in the label
	smName := pod.Labels[types.GetLonghornLabelKey(types.LonghornLabelShareManager)]
	key := pod.Namespace + "/" + smName
	c.queue.Add(key)

}

func isShareManagerPod(obj interface{}) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			return false
		}
	}

	// This only matches once the pod is fully constructed, which may be the point.
	podContainers := pod.Spec.Containers
	for _, con := range podContainers {
		if con.Name == types.LonghornLabelShareManager {
			return true
		}
	}
	return false
}

func (c *ShareManagerController) checkLeasesAndEnqueueAnyStale() error {
	enabled, err := c.ds.GetSettingAsBool(types.SettingNameRWXVolumeFastFailover)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}

	sms, err := c.ds.ListShareManagersRO()
	if err != nil {
		return err
	}
	for _, sm := range sms {
		isStale, _, err := c.isShareManagerPodStale(sm)
		if err != nil {
			return err
		}
		if isStale {
			c.enqueueShareManager(sm)
		}
	}

	return nil
}

func (c *ShareManagerController) runLeaseCheck(stopCh <-chan struct{}) {
	c.logger.Infof("Starting lease check goroutine")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			c.logger.Info("Share manager lease check is ending")
			return
		case <-ticker.C:
			if err := c.checkLeasesAndEnqueueAnyStale(); err != nil {
				c.logger.WithError(err).Warn("Failed to check share-manager leases.")
			}
		}
	}
}

func (c *ShareManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting Longhorn share manager controller")
	defer c.logger.Info("Shut down Longhorn share manager controller")

	if !cache.WaitForNamedCacheSync("longhorn-share-manager-controller", stopCh, c.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	go c.runLeaseCheck(stopCh)
	<-stopCh
}

func (c *ShareManagerController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *ShareManagerController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	err := c.syncShareManager(key.(string))
	c.handleErr(err, key)
	return true
}

func (c *ShareManagerController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	log := c.logger.WithField("ShareManager", key)
	if c.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn share manager")
		c.queue.AddRateLimited(key)
		return
	}

	handleReconcileErrorLogging(log, err, "Dropping Longhorn share manager out of the queue")
	c.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (c *ShareManagerController) syncShareManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}

	sm, err := c.ds.GetShareManager(name)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to retrieve share manager %v", name)
		}
		return nil
	}
	log := getLoggerForShareManager(c.logger, sm)

	isResponsible, err := c.isResponsibleFor(sm)
	if err != nil {
		return err
	}
	if !isResponsible {
		return nil
	}

	if sm.Status.OwnerID != c.controllerID {
		sm.Status.OwnerID = c.controllerID
		sm, err = c.ds.UpdateShareManagerStatus(sm)
		if err != nil {
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		// This node may be an interim owner picked by isResponsibleFor until the pod is created
		// and scheduled, at which point ownership will transfer to the pod's spec.nodename.
		// But we need some controller to assume responsibility and do the restart.
		log.Infof("Share manager got new owner %v", c.controllerID)
	}

	if sm.DeletionTimestamp != nil {
		if err := c.cleanupShareManagerPod(sm); err != nil {
			return err
		}

		err = c.ds.DeleteConfigMap(c.namespace, types.GetConfigMapNameFromShareManagerName(sm.Name))
		if err != nil && !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to delete the configmap (recovery backend) for share manager %v", sm.Name)
		}

		return c.ds.RemoveFinalizerForShareManager(sm)
	}

	isStale, leaseHolder, err := c.isShareManagerPodStale(sm)
	if err != nil {
		return err
	}
	if isStale {
		isDelinquent, _, err := c.ds.IsRWXVolumeDelinquent(sm.Name)
		if err != nil {
			return err
		}
		if !isDelinquent {
			// Turn off the traffic to the admission webhook and recovery backend on the suspected node.
			// Trying to talk to it will delay any effort to modify resources.
			// Only turn off the other nodes to avoid a deadlock in a single node cluster.
			if c.controllerID != leaseHolder {
				labels := types.MergeStringMaps(types.GetAdmissionWebhookLabel(), types.GetRecoveryBackendLabel())
				if err := c.ds.RemoveLabelFromManagerPod(leaseHolder, labels); err != nil {
					return errors.Wrapf(err, "failed to turn off admission webhook/recovery backed on node %v", leaseHolder)
				}
			}

			log.Infof("Marking share manager %v delinquent", sm.Name)
			if err := c.markShareManagerDelinquent(sm); err != nil {
				return err
			}
		}
	}

	// update at the end, after the whole reconcile loop
	existingShareManager := sm.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingShareManager.Status, sm.Status) {
			_, err = c.ds.UpdateShareManagerStatus(sm)
		}

		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debug("Requeue share manager due to conflict")
			c.enqueueShareManager(sm)
			err = nil
		}
	}()

	if err = c.syncShareManagerVolume(sm); err != nil {
		return err
	}

	if err = c.syncShareManagerPod(sm); err != nil {
		return err
	}

	if err = c.syncShareManagerEndpoint(sm); err != nil {
		return err
	}

	return nil
}

func (c *ShareManagerController) syncShareManagerEndpoint(sm *longhorn.ShareManager) error {
	// running is once the pod is in ready state
	// which means the nfs server is up and running with the volume attached
	// the cluster service ip doesn't change for the lifetime of the volume
	if sm.Status.State != longhorn.ShareManagerStateRunning {
		sm.Status.Endpoint = ""
		return nil
	}

	service, err := c.ds.GetService(sm.Namespace, sm.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	log := getLoggerForShareManager(c.logger, sm)
	if service == nil {
		log.Warn("Unsetting endpoint due to missing service for share-manager")
		sm.Status.Endpoint = ""
		return nil
	}

	storageNetworkForRWXVolume, err := c.ds.IsStorageNetworkForRWXVolume()
	if err != nil {
		return err
	}

	if storageNetworkForRWXVolume {
		serviceFqdn := fmt.Sprintf("%v.%v.svc.cluster.local", sm.Name, sm.Namespace)
		sm.Status.Endpoint = fmt.Sprintf("nfs://%v/%v", serviceFqdn, sm.Name)
	} else {
		endpoint := service.Spec.ClusterIP
		if service.Spec.IPFamilies[0] == corev1.IPv6Protocol {
			endpoint = fmt.Sprintf("[%v]", endpoint)
		}
		sm.Status.Endpoint = fmt.Sprintf("nfs://%v/%v", endpoint, sm.Name)
	}

	return nil
}

// isShareManagerRequiredForVolume checks if a share manager should export a volume
// a nil volume does not require a share manager
func (c *ShareManagerController) isShareManagerRequiredForVolume(sm *longhorn.ShareManager, volume *longhorn.Volume, va *longhorn.VolumeAttachment) bool {
	if volume == nil {
		return false
	}

	if volume.Spec.AccessMode != longhorn.AccessModeReadWriteMany {
		return false
	}

	// let the auto salvage take care of it
	if volume.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		return false
	}

	// let the normal restore process take care of it
	if volume.Status.RestoreRequired {
		return false
	}

	// volume is used in maintenance mode
	if volume.Spec.DisableFrontend || volume.Status.FrontendDisabled {
		return false
	}

	// no need to expose (DR) standby volumes via a share manager
	if volume.Spec.Standby || volume.Status.IsStandby {
		return false
	}

	for _, attachmentTicket := range va.Spec.AttachmentTickets {
		if isRegularRWXVolume(volume) && isCSIAttacherTicket(attachmentTicket) {
			return true
		}
	}
	return false
}

func (c *ShareManagerController) createShareManagerAttachmentTicket(sm *longhorn.ShareManager, va *longhorn.VolumeAttachment) error {
	log := getLoggerForShareManager(c.logger, sm)
	podName := types.GetShareManagerPodNameFromShareManagerName(sm.Name)
	pod, err := c.ds.GetPodRO(c.namespace, podName)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to retrieve pod %v for share manager from datastore", podName)
	}

	// This is not a fatal error, just wait for the pod.
	if pod == nil {
		log.Infof("No share-manager pod yet to attach the volume %v", va.Name)
		return nil
	}
	if pod.Spec.NodeName == "" {
		log.Infof("Share-manager pod %v for volume %v reports no owner node", pod.Name, va.Name)
		return nil
	}
	if pod.Status.Phase != corev1.PodPending && pod.Status.Phase != corev1.PodRunning {
		log.Infof("Share-manager pod %v for volume %v reports unusable phase %v", pod.Name, va.Name, pod.Status.Phase)
		return nil
	}
	nodeID := pod.Spec.NodeName

	shareManagerAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeShareManagerController, sm.Name)
	shareManagerAttachmentTicket, ok := va.Spec.AttachmentTickets[shareManagerAttachmentTicketID]
	if !ok {
		//create new one
		shareManagerAttachmentTicket = &longhorn.AttachmentTicket{
			ID:     shareManagerAttachmentTicketID,
			Type:   longhorn.AttacherTypeShareManagerController,
			NodeID: nodeID,
			Parameters: map[string]string{
				longhorn.AttachmentParameterDisableFrontend: longhorn.FalseValue,
			},
		}
		log.Infof("Adding volume attachment ticket: %v to attach the volume %v to node %v", shareManagerAttachmentTicketID, va.Name, nodeID)
	}
	if shareManagerAttachmentTicket.NodeID != nodeID {
		log.Infof("Attachment ticket %v request a new node %v from old node %v", shareManagerAttachmentTicket.ID, nodeID, shareManagerAttachmentTicket.NodeID)
		shareManagerAttachmentTicket.NodeID = nodeID
	}

	va.Spec.AttachmentTickets[shareManagerAttachmentTicketID] = shareManagerAttachmentTicket

	return nil
}

// unmountShareManagerVolume unmounts the volume in the share manager pod.
// It is a best effort operation and will not return an error if it fails.
func (c *ShareManagerController) unmountShareManagerVolume(sm *longhorn.ShareManager) {
	log := getLoggerForShareManager(c.logger, sm)

	podName := types.GetShareManagerPodNameFromShareManagerName(sm.Name)
	pod, err := c.ds.GetPod(podName)
	if err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Errorf("Failed to retrieve pod %v for share manager from datastore", podName)
		return
	}

	if pod == nil {
		return
	}

	log.Infof("Unmounting volume in share manager pod")

	client, err := engineapi.NewShareManagerClient(sm, pod)
	if err != nil {
		log.WithError(err).Errorf("Failed to create share manager client for pod %v", podName)
		return
	}
	defer func(client io.Closer) {
		if closeErr := client.Close(); closeErr != nil {
			c.logger.WithError(closeErr).Warn("Failed to close share manager client")
		}
	}(client)

	if err := client.Unmount(); err != nil {
		log.WithError(err).Warnf("Failed to unmount share manager pod %v", podName)
	}
}

// mountShareManagerVolume checks, exports and mounts the volume in the share manager pod.
func (c *ShareManagerController) mountShareManagerVolume(sm *longhorn.ShareManager) error {
	podName := types.GetShareManagerPodNameFromShareManagerName(sm.Name)
	pod, err := c.ds.GetPod(podName)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to retrieve pod %v for share manager from datastore", podName)
	}

	if pod == nil {
		return fmt.Errorf("pod %v for share manager not found", podName)
	}

	client, err := engineapi.NewShareManagerClient(sm, pod)
	if err != nil {
		return errors.Wrapf(err, "failed to create share manager client for pod %v", podName)
	}
	defer func(client io.Closer) {
		if closeErr := client.Close(); closeErr != nil {
			c.logger.WithError(closeErr).Warn("Failed to close share manager client")
		}
	}(client)

	if err := client.Mount(); err != nil {
		return errors.Wrapf(err, "failed to mount share manager pod %v", podName)
	}

	return nil
}

func (c *ShareManagerController) detachShareManagerVolume(sm *longhorn.ShareManager, va *longhorn.VolumeAttachment) {
	log := getLoggerForShareManager(c.logger, sm)

	shareManagerAttachmentTicketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeShareManagerController, sm.Name)
	if _, ok := va.Spec.AttachmentTickets[shareManagerAttachmentTicketID]; ok {
		log.Infof("Removing volume attachment ticket: %v to detach the volume %v", shareManagerAttachmentTicketID, va.Name)
		delete(va.Spec.AttachmentTickets, shareManagerAttachmentTicketID)
	}
}

// syncShareManagerVolume controls volume attachment and provides the following state transitions
// running -> running (do nothing)
// stopped -> stopped (rest state)
// starting, stopped, error -> starting (requires pod, volume attachment)
// starting, running, error -> stopped (no longer required, volume detachment)
// controls transitions to starting, stopped
func (c *ShareManagerController) syncShareManagerVolume(sm *longhorn.ShareManager) (err error) {
	log := getLoggerForShareManager(c.logger, sm)
	volume, err := c.ds.GetVolume(sm.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if sm.Status.State != longhorn.ShareManagerStateStopped {
				log.Infof("Stopping share manager because the volume %v is not found", sm.Name)
				sm.Status.State = longhorn.ShareManagerStateStopping
			}
			return nil
		}
		return errors.Wrap(err, "failed to retrieve volume for share manager from datastore")
	}

	va, err := c.ds.GetLHVolumeAttachmentByVolumeName(volume.Name)
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

		if _, err = c.ds.UpdateLHVolumeAttachment(va); err != nil {
			return
		}
	}()

	isDelinquent, _, err := c.ds.IsRWXVolumeDelinquent(sm.Name)
	if err != nil {
		return err
	}

	if !c.isShareManagerRequiredForVolume(sm, volume, va) {
		c.unmountShareManagerVolume(sm)

		c.detachShareManagerVolume(sm, va)
		if sm.Status.State != longhorn.ShareManagerStateStopped {
			log.Info("Stopping share manager since it is no longer required")
			sm.Status.State = longhorn.ShareManagerStateStopping
		}
		return nil
	} else if sm.Status.State == longhorn.ShareManagerStateRunning && !isDelinquent {
		err := c.mountShareManagerVolume(sm)
		if err != nil {
			log.WithError(err).Error("Failed to mount share manager volume")
			sm.Status.State = longhorn.ShareManagerStateError
		}
		return nil
	}

	// in a single node cluster, there is no other manager that can claim ownership so we are prevented from creation
	// of the share manager pod and need to ensure that the volume gets detached, so that the engine can be stopped as well
	// we only check for running, since we don't want to nuke valid pods, not schedulable only means no new pods.
	// in the case of a drain kubernetes will terminate the running pod, which we will mark as error in the sync pod method
	if !c.ds.IsNodeSchedulable(sm.Status.OwnerID) {
		c.unmountShareManagerVolume(sm)

		c.detachShareManagerVolume(sm, va)
		if sm.Status.State != longhorn.ShareManagerStateStopped {
			log.Info("Failed to start share manager, node is not schedulable")
			sm.Status.State = longhorn.ShareManagerStateStopping
		}
		return nil
	}

	// we wait till a transition to stopped before ramp up again
	if sm.Status.State == longhorn.ShareManagerStateStopping {
		return nil
	}

	// we manage volume auto detach/attach in the starting state, once the pod is running
	// the volume health check will be responsible for failing the pod which will lead to error state
	if sm.Status.State != longhorn.ShareManagerStateStarting {
		log.Info("Starting share manager")
		sm.Status.State = longhorn.ShareManagerStateStarting
	}

	// For the RWX volume attachment, VolumeAttachment controller will not directly handle
	// the tickets from the CSI plugin. Instead, ShareManager controller will add a
	// AttacherTypeShareManagerController ticket (as the summarization of CSI tickets) then
	// the VolumeAttachment controller is responsible for handling the AttacherTypeShareManagerController
	// tickets only. See more at https://github.com/longhorn/longhorn-manager/pull/1541#issuecomment-1429044946
	// Failure in creating the ticket should not derail the rest of share manager sync, so swallow it.
	_ = c.createShareManagerAttachmentTicket(sm, va)

	return nil
}

// markShareManagerLeaseDelinquent zeros the acquire time field as a flag that the volume
// should be fast-tracked for detaching away from the current lease-holding node.
func (c *ShareManagerController) markShareManagerDelinquent(sm *longhorn.ShareManager) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to markShareManagerDelinquent")
	}()

	lease, err := c.ds.GetLease(sm.Name)
	if err != nil {
		return err
	}

	lease.Spec.AcquireTime = &metav1.MicroTime{Time: time.Time{}}
	if _, err := c.ds.UpdateLease(lease); err != nil {
		return err
	}
	return nil
}

// clearShareManagerLease just zeros out the renew time field preparatory to pod
// cleanup, so it won't be flagged as stale in normal-path code.
func (c *ShareManagerController) clearShareManagerLease(sm *longhorn.ShareManager) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to clearShareManagerLease")
	}()

	log := getLoggerForShareManager(c.logger, sm)

	lease, err := c.ds.GetLease(sm.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	holder := *lease.Spec.HolderIdentity
	if !lease.Spec.RenewTime.IsZero() {
		log.Infof("Clearing lease %v held by node %v for share manager pod cleanup.", sm.Name, holder)
		lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Time{}}
		if _, err := c.ds.UpdateLease(lease); err != nil {
			return err
		}
	}

	return nil
}

func (c *ShareManagerController) cleanupShareManagerPod(sm *longhorn.ShareManager) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to cleanupShareManagerPod")
	}()

	log := getLoggerForShareManager(c.logger, sm)

	// Are we cleaning up after a lease timeout?
	isStale, leaseHolder, err := c.isShareManagerPodStale(sm)
	if err != nil {
		return err
	}

	// Clear the lease so we won't try to act on apparent staleness.  Staleness is now either dealt with or moot.
	err = c.clearShareManagerLease(sm)
	if err != nil {
		return err
	}

	podName := types.GetShareManagerPodNameFromShareManagerName(sm.Name)
	pod, err := c.ds.GetPod(podName)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to retrieve pod %v for share manager from datastore", podName)
	}

	if pod == nil {
		return nil
	}

	log.Infof("Deleting share manager pod")
	if err := c.ds.DeletePod(podName); err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Warnf("Failed to delete share manager pod")
		return err
	}

	// Force delete if the pod's node is known dead, or likely so since it let
	// the lease time out and another node's controller is cleaning up after it.
	nodeFailed, _ := c.ds.IsNodeDownOrDeleted(pod.Spec.NodeName)
	if nodeFailed || (isStale && leaseHolder != c.controllerID) {
		log.Info("Force deleting pod to allow fail over since node of share manager pod is down")
		gracePeriod := int64(0)
		err := c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), podName, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod})
		if err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrap(err, "failed to force delete share manager pod")
		}
	}

	return nil
}

// syncShareManagerPod controls pod existence and provides the following state transitions
// stopping -> stopped (no more pod)
// stopped -> stopped (rest state)
// starting -> starting (pending, volume attachment)
// starting ,running, error -> error (restart, remount volumes)
// starting, running -> running (share ready to use)
// controls transitions to running, error
func (c *ShareManagerController) syncShareManagerPod(sm *longhorn.ShareManager) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to syncShareManagerPod")
	}()

	defer func() {
		if sm.Status.State == longhorn.ShareManagerStateStopping ||
			sm.Status.State == longhorn.ShareManagerStateStopped ||
			sm.Status.State == longhorn.ShareManagerStateError {
			err = c.cleanupShareManagerPod(sm)
			if err != nil {
				return
			}
		}
	}()

	// if we are in stopped state there is nothing to do but cleanup any outstanding pods
	// no need for remount, since we don't have any active workloads in this state
	if sm.Status.State == longhorn.ShareManagerStateStopped {
		return nil
	}

	log := getLoggerForShareManager(c.logger, sm)
	pod, err := c.ds.GetPod(types.GetShareManagerPodNameFromShareManagerName(sm.Name))
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "failed to retrieve pod for share manager from datastore")
	} else if pod == nil {
		if sm.Status.State == longhorn.ShareManagerStateStopping {
			log.Info("Updating share manager to stopped state before starting since share manager pod is gone")
			sm.Status.State = longhorn.ShareManagerStateStopped
			return nil
		}

		// there should only ever be no pod if we are in pending state
		// if there is no pod in any other state transition to error so we start over
		if sm.Status.State != longhorn.ShareManagerStateStarting {
			log.Info("Updating share manager to error state since it has no pod but is not in starting state, requires cleanup with remount")
			sm.Status.State = longhorn.ShareManagerStateError
			return nil
		}

		backoffID := sm.Name
		if c.backoff.IsInBackOffSinceUpdate(backoffID, time.Now()) {
			backoffDuration := c.backoff.Get(backoffID)
			log.Infof("Skipping pod creation for share manager %s, will retry after backoff of %s", sm.Name, backoffDuration)
			return enqueueAfterDelay(c.queue, sm, backoffDuration)
		}
		log.Infof("Creating pod for share manager %s", sm.Name)
		c.backoff.Next(backoffID, time.Now())
		if pod, err = c.createShareManagerPod(sm); err != nil {
			return errors.Wrap(err, "failed to create pod for share manager")
		}
	}

	// If the node where the pod is running on become defective, we clean up the pod by setting sm.Status.State to STOPPED or ERROR
	// A new pod will be recreated by the share manager controller.  We might get an early warning of that by the pod going stale.
	isStale, _, err := c.isShareManagerPodStale(sm)
	if err != nil {
		return err
	}
	if isStale {
		log.Warnf("ShareManager Pod %v is stale", pod.Name)

		// if we just transitioned to the starting state, while the prior cleanup is still in progress we will switch to error state
		// which will lead to a bad loop of starting (new workload) -> error (remount) -> stopped (cleanup sm)
		if sm.Status.State == longhorn.ShareManagerStateStopping {
			return nil
		}

		if sm.Status.State != longhorn.ShareManagerStateStopped {
			log.Info("Updating share manager to error state")
			sm.Status.State = longhorn.ShareManagerStateError
		}

		return nil
	}

	isDown, err := c.ds.IsNodeDownOrDeleted(pod.Spec.NodeName)
	if err != nil {
		log.WithError(err).Warnf("Failed to check IsNodeDownOrDeleted(%v) when syncShareManagerPod", pod.Spec.NodeName)
	} else if isDown {
		log.Infof("Node %v is down", pod.Spec.NodeName)
	}
	if pod.DeletionTimestamp != nil || isDown {
		// if we just transitioned to the starting state, while the prior cleanup is still in progress we will switch to error state
		// which will lead to a bad loop of starting (new workload) -> error (remount) -> stopped (cleanup sm)
		if sm.Status.State == longhorn.ShareManagerStateStopping {
			return nil
		}

		if sm.Status.State != longhorn.ShareManagerStateStopped {
			log.Info("Updating share manager to error state, requires cleanup with remount")
			sm.Status.State = longhorn.ShareManagerStateError
		}

		return nil
	}

	// if we have an deleted pod but are supposed to be stopping
	// we don't modify the share-manager state
	if sm.Status.State == longhorn.ShareManagerStateStopping {
		return nil
	}

	switch pod.Status.Phase {
	case corev1.PodPending:
		if sm.Status.State != longhorn.ShareManagerStateStarting {
			log.Warnf("Share Manager has state %v but the related pod is pending.", sm.Status.State)
			sm.Status.State = longhorn.ShareManagerStateError
		}
	case corev1.PodRunning:
		// pod readiness is based on the availability of the nfs server
		// nfs server is only started after the volume is attached and mounted
		allContainersReady := true
		for _, st := range pod.Status.ContainerStatuses {
			allContainersReady = allContainersReady && st.Ready
		}

		if allContainersReady {
			if sm.Status.State == longhorn.ShareManagerStateStarting {
				sm.Status.State = longhorn.ShareManagerStateRunning
			}
			if sm.Status.State != longhorn.ShareManagerStateRunning {
				sm.Status.State = longhorn.ShareManagerStateError
			}
		}
		// If !allContainersReady, we will sync again when the situation changes.
	default:
		log.Infof("Share manager pod %v in unexpected phase: %v, setting sharemanager to error state.", sm.Name, pod.Status.Phase)
		sm.Status.State = longhorn.ShareManagerStateError
	}

	return nil
}

func (c *ShareManagerController) getAffinityFromStorageClass(sc *storagev1.StorageClass) *corev1.Affinity {
	var matchLabelExpressions []corev1.NodeSelectorRequirement

	for _, topology := range sc.AllowedTopologies {
		for _, expression := range topology.MatchLabelExpressions {
			matchLabelExpressions = append(matchLabelExpressions, corev1.NodeSelectorRequirement{
				Key:      expression.Key,
				Operator: corev1.NodeSelectorOpIn,
				Values:   expression.Values,
			})
		}
	}

	if len(matchLabelExpressions) == 0 {
		return nil
	}

	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: matchLabelExpressions,
					},
				},
			},
		},
	}
}

func (c *ShareManagerController) addStaleNodeAntiAffinity(affinity *corev1.Affinity, staleNode string) *corev1.Affinity {
	var matchFields []corev1.NodeSelectorRequirement

	matchFields = append(matchFields, corev1.NodeSelectorRequirement{
		Key:      "metadata.name",
		Operator: corev1.NodeSelectorOpNotIn,
		Values:   []string{staleNode},
	})

	// Note the difference between MatchFields and MatchExpressions.
	//See https://stackoverflow.com/questions/67018171/kubernetes-what-are-valid-node-fields
	nodeAntiAffinity := &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{
			corev1.PreferredSchedulingTerm{
				Weight: 100,
				Preference: corev1.NodeSelectorTerm{
					MatchFields: matchFields,
				},
			},
		},
	}

	if affinity == nil {
		affinity = &corev1.Affinity{
			NodeAffinity: nodeAntiAffinity,
		}
	} else {
		affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = nodeAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	}

	return affinity
}

func (c *ShareManagerController) getShareManagerNodeSelectorFromStorageClass(sc *storagev1.StorageClass) map[string]string {
	value, ok := sc.Parameters["shareManagerNodeSelector"]
	if !ok {
		return map[string]string{}
	}

	nodeSelector, err := types.UnmarshalNodeSelector(value)
	if err != nil {
		c.logger.WithError(err).Warnf("Failed to unmarshal node selector %v", value)
		return map[string]string{}
	}

	return nodeSelector
}

func (c *ShareManagerController) getShareManagerTolerationsFromStorageClass(sc *storagev1.StorageClass) []corev1.Toleration {
	value, ok := sc.Parameters["shareManagerTolerations"]
	if !ok {
		return []corev1.Toleration{}
	}

	tolerations, err := types.UnmarshalTolerations(value)
	if err != nil {
		c.logger.WithError(err).Warnf("Failed to unmarshal tolerations %v", value)
		return []corev1.Toleration{}
	}

	return tolerations
}

func (c *ShareManagerController) checkStorageNetworkApplied() (bool, error) {
	targetSettings := []types.SettingName{types.SettingNameStorageNetwork, types.SettingNameStorageNetworkForRWXVolumeEnabled}
	for _, item := range targetSettings {
		if applied, err := c.ds.GetSettingApplied(item); err != nil || !applied {
			return applied, err
		}
	}
	return true, nil
}

func (c *ShareManagerController) canCleanupService(shareManagerName string) (bool, error) {
	service, err := c.ds.GetService(c.namespace, shareManagerName)
	if err != nil {
		// if NotFound, means the service/endpoint is already cleaned up
		// The service and endpoint are related with the kubernetes endpoint controller.
		// It means once the service is deleted, the corresponding endpoint will be deleted automatically.
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "failed to get service")
	}

	// check the settings status of storage network and storage network for RWX volume
	settingsApplied, err := c.checkStorageNetworkApplied()
	if err != nil {
		return false, errors.Wrap(err, "failed to check if the storage network settings are applied")
	}
	if !settingsApplied {
		c.logger.Warn("Storage network settings are not applied, do nothing")
		return false, nil
	}

	storageNetworkForRWXVolume, err := c.ds.IsStorageNetworkForRWXVolume()
	if err != nil {
		return false, err
	}

	// no need to cleanup because looks the service file is correct
	if storageNetworkForRWXVolume {
		if service.Spec.ClusterIP == core.ClusterIPNone {
			return false, nil
		}
	} else {
		if service.Spec.ClusterIP != core.ClusterIPNone {
			return false, nil
		}
	}
	return true, nil
}

func (c *ShareManagerController) cleanupService(shareManager *longhorn.ShareManager) error {
	if ok, err := c.canCleanupService(shareManager.Name); !ok || err != nil {
		if err != nil {
			return errors.Wrapf(err, "failed to check if we can cleanup service and endpoint for share manager %v", shareManager.Name)
		}
		return nil
	}

	// let's cleanup
	c.logger.Infof("Deleting Service for share manager %v", shareManager.Name)
	err := c.ds.DeleteService(c.namespace, shareManager.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to delete service for share manager %v", shareManager.Name)
	}

	// we don't need to cleanup the endpoint because the kubernetes endpoints_controller
	// will sync the service then clean up the corresponding endpoint.
	// https://github.com/kubernetes/kubernetes/blob/v1.31.0/pkg/controller/endpoint/endpoints_controller.go#L374-L392

	return nil
}

func (c *ShareManagerController) createServiceAndEndpoint(shareManager *longhorn.ShareManager) error {
	// check if we need to create the service
	_, err := c.ds.GetService(c.namespace, shareManager.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to get service for share manager %v", shareManager.Name)
		}

		c.logger.Infof("Creating Service for share manager %v", shareManager.Name)
		_, err = c.ds.CreateService(c.namespace, c.createServiceManifest(shareManager))
		if err != nil {
			return errors.Wrapf(err, "failed to create service for share manager %v", shareManager.Name)
		}
	}

	// Only create the Endpoint if it doesn't exist. For service using the selector, the Endpoint will be created by the service controller.
	_, err = c.ds.GetKubernetesEndpointRO(shareManager.Name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to get Endpoint for share manager %v", shareManager.Name)
		}

		_, err := c.createEndpoint(shareManager)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to create Endpoint for share manager %v", shareManager.Name)
			}
			c.logger.Debugf("Found existing Endpoint for share manager %v", shareManager.Name)
		}
	}

	return nil
}

// createShareManagerPod ensures existence of corresponding service and lease.
// it's assumed that the pvc for this share manager already exists.
func (c *ShareManagerController) createShareManagerPod(sm *longhorn.ShareManager) (*corev1.Pod, error) {
	log := getLoggerForShareManager(c.logger, sm)

	tolerations, err := c.ds.GetSettingTaintToleration()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get taint toleration setting before creating share manager pod")
	}

	nodeSelector, err := c.ds.GetSettingSystemManagedComponentsNodeSelector()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get node selector setting before creating share manager pod")
	}

	tolerationsByte, err := json.Marshal(tolerations)
	if err != nil {
		return nil, err
	}
	annotations := map[string]string{types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix): string(tolerationsByte)}

	imagePullPolicy, err := c.ds.GetSettingImagePullPolicy()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get image pull policy before creating share manager pod")
	}

	setting, err := c.ds.GetSettingWithAutoFillingRO(types.SettingNameRegistrySecret)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get registry secret setting before creating share manager pod")
	}
	registrySecret := setting.Value

	setting, err = c.ds.GetSettingWithAutoFillingRO(types.SettingNamePriorityClass)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get priority class setting before creating share manager pod")
	}
	priorityClass := setting.Value

	err = c.cleanupService(sm)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to cleanup service for share manager %v", sm.Name)
	}

	err = c.createServiceAndEndpoint(sm)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create service and endpoint for share manager %v", sm.Name)
	}

	nfsConfig := &nfsServerConfig{
		enableFastFailover: false,
		leaseLifetime:      60,
		gracePeriod:        90,
	}

	enabled, err := c.ds.GetSettingAsBool(types.SettingNameRWXVolumeFastFailover)
	if err != nil {
		return nil, err
	}
	if enabled {
		nfsConfig = &nfsServerConfig{
			enableFastFailover: true,
			leaseLifetime:      20,
			gracePeriod:        30,
		}

		if _, err := c.ds.GetLeaseRO(sm.Name); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, errors.Wrapf(err, "failed to get lease for share manager %v", sm.Name)
			}

			if _, err = c.ds.CreateLease(c.createLeaseManifest(sm)); err != nil {
				return nil, errors.Wrapf(err, "failed to create lease for share manager %v", sm.Name)
			}
		}

	}

	volume, err := c.ds.GetVolume(sm.Name)
	if err != nil {
		return nil, err
	}

	pv, err := c.ds.GetPersistentVolume(volume.Status.KubernetesStatus.PVName)
	if err != nil {
		return nil, err
	}

	var affinity *corev1.Affinity

	var formatOptions []string

	if pv.Spec.StorageClassName != "" {
		sc, err := c.ds.GetStorageClass(pv.Spec.StorageClassName)
		if err != nil {
			c.logger.WithError(err).Warnf("Failed to get storage class %v, will continue the share manager pod creation", pv.Spec.StorageClassName)
		} else {
			affinity = c.getAffinityFromStorageClass(sc)

			// Find the node selector from the storage class and merge it with the system managed components node selector
			if nodeSelector == nil {
				nodeSelector = map[string]string{}
			}
			nodeSelectorFromStorageClass := c.getShareManagerNodeSelectorFromStorageClass(sc)
			for k, v := range nodeSelectorFromStorageClass {
				nodeSelector[k] = v
			}

			// Find the tolerations from the storage class and merge it with the global tolerations
			if tolerations == nil {
				tolerations = []corev1.Toleration{}
			}
			tolerationsFromStorageClass := c.getShareManagerTolerationsFromStorageClass(sc)
			tolerations = append(tolerations, tolerationsFromStorageClass...)

			// A storage class can override mkfs parameters which need to be passed to the share manager
			formatOptions = c.splitFormatOptions(sc)
		}
	}

	isDelinquent, delinquentNode, err := c.ds.IsRWXVolumeDelinquent(sm.Name)
	if err != nil {
		return nil, err
	}
	if isDelinquent && delinquentNode != "" {
		log.Infof("Creating anti-affinity for share manager pod against delinquent node %v", delinquentNode)
		affinity = c.addStaleNodeAntiAffinity(affinity, delinquentNode)
	}

	fsType := pv.Spec.CSI.FSType
	mountOptions := pv.Spec.MountOptions

	var cryptoKey string
	var cryptoParams *crypto.EncryptParams
	if volume.Spec.Encrypted {
		secretRef := pv.Spec.CSI.NodePublishSecretRef
		secret, err := c.ds.GetSecretRO(secretRef.Namespace, secretRef.Name)
		if err != nil {
			return nil, err
		}

		cryptoKey = string(secret.Data[types.CryptoKeyValue])
		if len(cryptoKey) == 0 {
			return nil, fmt.Errorf("missing %v in secret for encrypted RWX volume %v", types.CryptoKeyValue, volume.Name)
		}
		cryptoParams = crypto.NewEncryptParams(
			string(secret.Data[types.CryptoKeyProvider]),
			string(secret.Data[types.CryptoKeyCipher]),
			string(secret.Data[types.CryptoKeyHash]),
			string(secret.Data[types.CryptoKeySize]),
			string(secret.Data[types.CryptoPBKDF]))
	}

	manifest := c.createPodManifest(sm, volume.Spec.DataEngine, annotations, tolerations, affinity, imagePullPolicy, nil, registrySecret,
		priorityClass, nodeSelector, fsType, formatOptions, mountOptions, cryptoKey, cryptoParams, nfsConfig)

	storageNetwork, err := c.ds.GetSettingWithAutoFillingRO(types.SettingNameStorageNetwork)
	if err != nil {
		return nil, err
	}

	storageNetworkForRWXVolumeEnabled, err := c.ds.GetSettingAsBool(types.SettingNameStorageNetworkForRWXVolumeEnabled)
	if err != nil {
		return nil, err
	}

	if types.IsStorageNetworkForRWXVolume(storageNetwork, storageNetworkForRWXVolumeEnabled) {
		nadAnnot := string(types.CNIAnnotationNetworks)
		if storageNetwork.Value != types.CniNetworkNone {
			manifest.Annotations[nadAnnot] = types.CreateCniAnnotationFromSetting(storageNetwork)
		}
	}

	pod, err := c.ds.CreatePod(manifest)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create pod for share manager %v", sm.Name)
	}
	log.WithField("pod", pod.Name).Infof("Created pod for share manager on node %v", pod.Spec.NodeName)
	return pod, nil
}

func (c *ShareManagerController) splitFormatOptions(sc *storagev1.StorageClass) []string {
	if mkfsParams, ok := sc.Parameters["mkfsParams"]; ok {
		regex, err := regexp.Compile("-[a-zA-Z_]+(?:\\s*=?\\s*(?:\"[^\"]*\"|'[^']*'|[^\\r\\n\\t\\f\\v -]+))?")

		if err != nil {
			c.logger.WithError(err).Warnf("Failed to compile regex for mkfsParams %v, will continue the share manager pod creation", mkfsParams)
			return nil
		}

		matches := regex.FindAllString(mkfsParams, -1)

		if matches == nil {
			c.logger.Warnf("No valid mkfs parameters found in \"%v\", will continue the share manager pod creation", mkfsParams)
			return nil
		}

		return matches
	}

	c.logger.Debug("No mkfs parameters found, will continue the share manager pod creation")
	return nil
}

func (c *ShareManagerController) createServiceManifest(sm *longhorn.ShareManager) *corev1.Service {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            sm.Name,
			Namespace:       c.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForShareManager(sm, false),
			Labels:          types.GetShareManagerInstanceLabel(sm.Name),
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:     "nfs",
					Port:     2049,
					Protocol: corev1.ProtocolTCP,
				},
			},
		},
	}

	log := getLoggerForShareManager(c.logger, sm)

	storageNetworkForRWXVolume, err := c.ds.IsStorageNetworkForRWXVolume()
	if err != nil {
		log.WithError(err).Warnf("Failed to check storage network for RWX volume")
	}

	if storageNetworkForRWXVolume {
		// Create a headless service do it doesn't use a cluster IP. This allows
		// directly reaching the share manager pods using their individual
		// IP address.
		log.Debug("Using headless service for share manager because storage network is detected")
		service.Spec.ClusterIP = core.ClusterIPNone
	} else {
		log.Debug("Using selector service for share manager because storage network is not detected")
		service.Spec.ClusterIP = "" // we let the cluster assign a random ip
		service.Spec.Selector = types.GetShareManagerInstanceLabel(sm.Name)
	}

	return service
}

func (c *ShareManagerController) createEndpoint(sm *longhorn.ShareManager) (*corev1.Endpoints, error) { // nolint: staticcheck
	labels := types.GetShareManagerInstanceLabel(sm.Name)

	newObj := &corev1.Endpoints{ // nolint: staticcheck
		ObjectMeta: metav1.ObjectMeta{
			Name:            sm.Name,
			Namespace:       sm.Namespace,
			OwnerReferences: datastore.GetOwnerReferencesForShareManager(sm, false),
			Labels:          labels,
		},
		Subsets: []corev1.EndpointSubset{}, // nolint: staticcheck
	}

	c.logger.Infof("Creating Endpoint for share manager %v", sm.Name)
	return c.ds.CreateKubernetesEndpoint(newObj)
}

func (c *ShareManagerController) createLeaseManifest(sm *longhorn.ShareManager) *coordinationv1.Lease {
	// No current holder, share-manager pod will fill it with its owning node.
	holderIdentity := ""
	leaseDurationSeconds := int32(shareManagerLeaseDurationSeconds)
	leaseTransitions := int32(0)
	zeroTime := time.Time{}

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:            sm.Name,
			Namespace:       c.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForShareManager(sm, false),
			Labels:          types.GetShareManagerInstanceLabel(sm.Name),
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holderIdentity,
			LeaseDurationSeconds: &leaseDurationSeconds,
			AcquireTime:          &metav1.MicroTime{Time: zeroTime},
			RenewTime:            &metav1.MicroTime{Time: zeroTime},
			LeaseTransitions:     &leaseTransitions,
		},
	}

	return lease
}

func (c *ShareManagerController) createPodManifest(sm *longhorn.ShareManager, dataEngine longhorn.DataEngineType, annotations map[string]string, tolerations []corev1.Toleration,
	affinity *corev1.Affinity, pullPolicy corev1.PullPolicy, resourceReq *corev1.ResourceRequirements, registrySecret, priorityClass string,
	nodeSelector map[string]string, fsType string, formatOptions []string, mountOptions []string, cryptoKey string, cryptoParams *crypto.EncryptParams,
	nfsConfig *nfsServerConfig) *corev1.Pod {

	// command args for the share-manager
	args := []string{"--debug", "daemon", "--volume", sm.Name, "--data-engine", string(dataEngine)}

	if len(fsType) > 0 {
		args = append(args, "--fs", fsType)
	}

	if len(mountOptions) > 0 {
		args = append(args, "--mount", strings.Join(mountOptions, ","))
	}

	privileged := true
	podSpec := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            types.GetShareManagerPodNameFromShareManagerName(sm.Name),
			Namespace:       sm.Namespace,
			Labels:          types.GetShareManagerLabels(sm.Name, sm.Spec.Image),
			Annotations:     annotations,
			OwnerReferences: datastore.GetOwnerReferencesForShareManager(sm, true),
		},
		Spec: corev1.PodSpec{
			Affinity:           affinity,
			ServiceAccountName: c.serviceAccount,
			Tolerations:        util.GetDistinctTolerations(tolerations),
			NodeSelector:       nodeSelector,
			PriorityClassName:  priorityClass,
			Containers: []corev1.Container{
				{
					Name:            types.LonghornLabelShareManager,
					Image:           sm.Spec.Image,
					ImagePullPolicy: pullPolicy,
					// Command: []string{"longhorn-share-manager"},
					Args: args,
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"cat", "/var/run/ganesha.pid"},
							},
						},
						InitialDelaySeconds: datastore.PodProbeInitialDelay,
						TimeoutSeconds:      datastore.PodProbeTimeoutSeconds,
						PeriodSeconds:       datastore.PodProbePeriodSeconds,
						FailureThreshold:    datastore.PodLivenessProbeFailureThreshold,
					},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	podSpec.Spec.Containers[0].Env = []corev1.EnvVar{
		{
			Name:  "FAST_FAILOVER",
			Value: fmt.Sprint(nfsConfig.enableFastFailover),
		},
		{
			Name:  "LEASE_LIFETIME",
			Value: fmt.Sprint(nfsConfig.leaseLifetime),
		},
		{
			Name:  "GRACE_PERIOD",
			Value: fmt.Sprint(nfsConfig.gracePeriod),
		},
	}

	if len(formatOptions) > 0 {
		podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env, []corev1.EnvVar{
			{
				Name:  "FS_FORMAT_OPTIONS",
				Value: fmt.Sprint(strings.Join(formatOptions, ":")),
			},
		}...)
	}

	// this is an encrypted volume the cryptoKey is base64 encoded
	if len(cryptoKey) > 0 {
		podSpec.Spec.Containers[0].Env = append(podSpec.Spec.Containers[0].Env, []corev1.EnvVar{
			{
				Name:  "ENCRYPTED",
				Value: "True",
			},
			{
				Name:  "PASSPHRASE",
				Value: string(cryptoKey),
			},
			{
				Name:  "CRYPTOKEYCIPHER",
				Value: string(cryptoParams.GetKeyCipher()),
			},
			{
				Name:  "CRYPTOKEYHASH",
				Value: string(cryptoParams.GetKeyHash()),
			},
			{
				Name:  "CRYPTOKEYSIZE",
				Value: string(cryptoParams.GetKeySize()),
			},
			{
				Name:  "CRYPTOPBKDF",
				Value: string(cryptoParams.GetPBKDF()),
			},
		}...)
	}

	podSpec.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{
			Name:      "host-dev",
			MountPath: "/dev",
		},
		{
			Name:      "host-sys",
			MountPath: "/sys",
		},
		{
			Name:      "host-proc",
			MountPath: "/host/proc", // we use this to enter the host namespace
		},
		{
			Name:      "lib-modules",
			MountPath: "/lib/modules",
			ReadOnly:  true,
		},
	}

	podSpec.Spec.Volumes = []corev1.Volume{
		{
			Name: "host-dev",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/dev",
				},
			},
		},
		{
			Name: "host-sys",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/sys",
				},
			},
		},
		{
			Name: "host-proc",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/proc",
				},
			},
		},
		{
			Name: "lib-modules",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/lib/modules",
				},
			},
		},
	}

	if registrySecret != "" {
		podSpec.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}

	if resourceReq != nil {
		podSpec.Spec.Containers[0].Resources = *resourceReq
	}
	types.AddGoCoverDirToPod(podSpec)

	return podSpec
}

// isShareManagerPodStale checks the associated lease CR to see whether the current pod (if any)
// has fallen behind on renewing the lease.  If there is any error finding out, we assume not stale.
func (c *ShareManagerController) isShareManagerPodStale(sm *longhorn.ShareManager) (stale bool, holder string, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to check isShareManagerPodStale")
	}()

	log := getLoggerForShareManager(c.logger, sm)

	enabled, err := c.ds.GetSettingAsBool(types.SettingNameRWXVolumeFastFailover)
	if err != nil {
		return false, "", err
	}
	if !enabled {
		return false, "", nil
	}

	leaseName := sm.Name
	lease, err := c.ds.GetLeaseRO(leaseName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return false, "", err
		}
		return false, "", nil
	}

	// Consider it stale if
	// - there is a lease-holding node, ie there is a share-manager version running that knows to acquire the lease.
	// - the pod is not being cleaned up,
	// - the lease duration is a sane value, and
	// - the time of renewal is longer ago than the lease duration.
	holder = *lease.Spec.HolderIdentity
	if holder == "" {
		return false, "", nil
	}
	if (lease.Spec.RenewTime).IsZero() {
		return false, holder, nil
	}
	if *lease.Spec.LeaseDurationSeconds < shareManagerLeaseDurationSeconds {
		log.Warnf("Lease for %v has a crazy value for duration: %v seconds.  Ignoring.", leaseName, *lease.Spec.LeaseDurationSeconds)
		return false, holder, nil
	}
	expireTime := lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	if time.Now().After(expireTime) {
		log.Warnf("Lease for %v held by %v is stale, expired %v ago", leaseName, holder, time.Since(expireTime))
		stale = true
	}

	return stale, holder, nil
}

// isResponsibleFor in most controllers only checks if the node of the current owner is known
// by kubernetes to be down.  But in the case where the lease is stale or the node is unschedulable
// we want to transfer ownership and mark the node as delinquent for related status checking.
func (c *ShareManagerController) isResponsibleFor(sm *longhorn.ShareManager) (bool, error) {
	log := getLoggerForShareManager(c.logger, sm)

	// We prefer keeping the owner of the share manager CR to be the node
	// where the share manager pod got scheduled and is running.
	preferredOwnerID := ""
	podName := types.GetShareManagerPodNameFromShareManagerName(sm.Name)
	pod, err := c.ds.GetPodRO(c.namespace, podName)
	if err == nil && pod != nil {
		preferredOwnerID = pod.Spec.NodeName
	}

	// Base class method is used to decide based on node schedulable condition.
	isResponsible := isControllerResponsibleFor(c.controllerID, c.ds, sm.Name, preferredOwnerID, sm.Status.OwnerID)

	readyAndSchedulableNodes, err := c.ds.ListReadyAndSchedulableNodesRO()
	if err != nil {
		return false, err
	}
	if len(readyAndSchedulableNodes) == 0 {
		return isResponsible, nil
	}

	preferredOwnerSchedulable := c.ds.IsNodeSchedulable(preferredOwnerID)
	currentOwnerSchedulable := c.ds.IsNodeSchedulable(sm.Status.OwnerID)
	currentNodeSchedulable := c.ds.IsNodeSchedulable(c.controllerID)

	isPreferredOwner := currentNodeSchedulable && isResponsible
	continueToBeOwner := currentNodeSchedulable && !preferredOwnerSchedulable && c.controllerID == sm.Status.OwnerID
	requiresNewOwner := currentNodeSchedulable && !preferredOwnerSchedulable && !currentOwnerSchedulable

	isNodeAvailable := func(node string) bool {
		isUnavailable, _ := c.ds.IsNodeDownOrDeletedOrMissingManager(node)
		return node != "" && !isUnavailable
	}

	// If the lease is stale, we assume the owning node is down but not officially dead.
	// Some node has to take over, and it might as well be this one, if another one
	// has not already.
	isStale, leaseHolder, err := c.isShareManagerPodStale(sm)
	if err != nil {
		return false, err
	}
	if isStale {
		// Avoid race between nodes by checking for an existing interim owner.
		if leaseHolder != sm.Status.OwnerID &&
			c.controllerID != sm.Status.OwnerID &&
			isNodeAvailable(sm.Status.OwnerID) &&
			c.ds.IsNodeSchedulable(sm.Status.OwnerID) {
			return false, nil
		}
		log.Infof("Interim owner %v taking responsibility for stale lease-holder %v", c.controllerID, leaseHolder)
		return currentNodeSchedulable, nil
	}

	return isPreferredOwner || continueToBeOwner || requiresNewOwner, nil
}
