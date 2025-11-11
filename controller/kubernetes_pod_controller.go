package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	controllerAgentName = "longhorn-kubernetes-pod-controller"

	remountRequestDelayDuration = 5 * time.Second
)

type KubernetesPodController struct {
	*baseController

	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewKubernetesPodController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string) (*KubernetesPodController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	kc := &KubernetesPodController{
		baseController: newBaseController("longhorn-kubernetes-pod", logger),

		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-kubernetes-pod-controller"}),
	}

	var err error
	if _, err = ds.PodInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    kc.enqueuePodChange,
		UpdateFunc: func(old, cur interface{}) { kc.enqueuePodChange(cur) },
		DeleteFunc: kc.enqueuePodChange,
	}); err != nil {
		return nil, err
	}
	kc.cacheSyncs = append(kc.cacheSyncs, ds.PodInformer.HasSynced)

	return kc, nil
}

func (kc *KubernetesPodController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer kc.queue.ShutDown()

	kc.logger.Infof("Starting %v", controllerAgentName)
	defer kc.logger.Infof("Shut down %v", controllerAgentName)

	if !cache.WaitForNamedCacheSync(controllerAgentName, stopCh, kc.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(kc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (kc *KubernetesPodController) worker() {
	for kc.processNextWorkItem() {
	}
}

func (kc *KubernetesPodController) processNextWorkItem() bool {
	key, quit := kc.queue.Get()
	if quit {
		return false
	}
	defer kc.queue.Done(key)
	err := kc.syncHandler(key.(string))
	kc.handleErr(err, key)
	return true
}

func (kc *KubernetesPodController) handleErr(err error, key interface{}) {
	if err == nil {
		kc.queue.Forget(key)
		return
	}

	log := kc.logger.WithField("Pod", key)
	if kc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn kubernetes pod")
		kc.queue.AddRateLimited(key)
		return
	}

	handleReconcileErrorLogging(log, err, "Dropping Longhorn kubernetes pod out of the queue")
	kc.queue.Forget(key)
	utilruntime.HandleError(err)
}

func getLoggerForPod(logger logrus.FieldLogger, pod *corev1.Pod) *logrus.Entry {
	return logger.WithFields(logrus.Fields{
		"pod":  pod.Name,
		"node": pod.Spec.NodeName,
	})
}

func (kc *KubernetesPodController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync pod %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	pod, err := kc.ds.GetPodRO(namespace, name)
	if err != nil {
		return errors.Wrapf(err, "Error getting Pod: %s", name)
	}
	if pod == nil {
		return nil
	}
	nodeID := pod.Spec.NodeName
	if nodeID == "" {
		kc.logger.WithField("pod", pod.Name).Trace("skipping pod check since pod is not scheduled yet")
		return nil
	}

	if isCSIPluginPod(pod) {
		return kc.handleWorkloadPodDeletionIfCSIPluginPodIsDown(pod)
	}

	if err := kc.cleanupForceDeletedPodResources(pod); err != nil {
		return err
	}

	if err := kc.handlePodDeletionIfNodeDown(pod, nodeID, namespace); err != nil {
		return err
	}

	if err := kc.handlePodDeletionIfVolumeRequestRemount(pod); err != nil {
		return err
	}

	return nil
}

// handleWorkloadPodDeletionIfCSIPluginPodIsDown deletes workload pods of RWX volumes
// on the same node of the deleted CSI plugin pod.
//
// When storage network is enabled for RWX volumes, the NFS client is mounted from
// the CSI plugin pod's PID namespace. If the CSI plugin pod unexpectedly goes
// down, its namespace is also deleted, potentially leaving behind dangling NFS
// mount entries on the node. This occurs because the kernel's NFS client is unaware
// of the dependency on the CSI plugin pod's network namespace.
//
// To address this issue, this function identifies workload pods using RWX volumes
// on the same node as the deleted CSI plugin pod and triggers their deletion. This
// process in turn remounts the volume, effectively handling the dangling mount
// entries.
//
// Note that this scenario only applies when storage network is used. If the volume
// relies on cluster network instead, the CSI plugin pod utilizes the host PID namespace
// . Consequently, the mount entry persists on the host namespace even after the
// CSI plugin pod is down.
func (kc *KubernetesPodController) handleWorkloadPodDeletionIfCSIPluginPodIsDown(csiPod *corev1.Pod) error {
	logAbort := "Aborting deletion of RWX volume workload pods for NFS remount"
	logSkip := "Skipping deletion of RWX volume workload pods for NFS remount"

	log := getLoggerForPod(kc.logger, csiPod)

	if csiPod.DeletionTimestamp.IsZero() {
		return nil
	}

	isStorageNetworkForRWXVolume, err := kc.ds.IsStorageNetworkForRWXVolume()
	if err != nil {
		log.WithError(err).Warnf("%s. Failed to check isStorageNetwork", logAbort)
		return nil
	}
	if !isStorageNetworkForRWXVolume {
		return nil
	}

	log.Info("CSI plugin pod on node is down, handling workload pods")

	autoDeletePodWhenVolumeDetachedUnexpectedly, err := kc.ds.GetSettingAsBool(types.SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly)
	if err != nil {
		return err
	}

	if !autoDeletePodWhenVolumeDetachedUnexpectedly {
		log.Warnf("%s. The setting %v is not enabled. Without restart the workload pod may lead to an unresponsive mount point", logAbort, types.SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly)
		return nil
	}

	// Find relevant PersistentVolumes.
	var persistentVolumes []*corev1.PersistentVolume
	persistentVolume, err := kc.ds.ListPersistentVolumesRO()
	if err != nil {
		return err
	}
	for _, pv := range persistentVolume {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == types.LonghornDriverName {
			persistentVolumes = append(persistentVolumes, pv)
		}
	}

	// Find RWX volumes.
	var filteredVolumes []*longhorn.Volume
	for _, persistentVolume := range persistentVolumes {
		_log := log.WithField("volume", persistentVolume.Name)

		volume, err := kc.ds.GetVolumeRO(persistentVolume.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				_log.WithError(err).Warnf("%s. Volume is not found", logSkip)
				continue
			}
			return err
		}

		// Exclude non-RWX volumes.
		if volume.Spec.AccessMode != longhorn.AccessModeReadWriteMany {
			_log.Debugf("%s. Volume access mode is %v", logSkip, volume.Spec.AccessMode)
			continue
		}

		if util.IsMigratableVolume(volume) {
			_log.Debugf("%s. Volume is migratable RWX volume", logSkip)
			continue
		}

		filteredVolumes = append(filteredVolumes, volume)
	}

	// Find workload Pods to delete.
	var filteredPods []*corev1.Pod
	for _, volume := range filteredVolumes {
		for _, workloadstatus := range volume.Status.KubernetesStatus.WorkloadsStatus {
			_log := log.WithFields(logrus.Fields{
				"volume":      volume.Name,
				"workloadPod": workloadstatus.PodName,
			})

			pod, err := kc.kubeClient.CoreV1().Pods(volume.Status.KubernetesStatus.Namespace).Get(context.TODO(), workloadstatus.PodName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					_log.WithError(err).Debugf("%s. Workload pod is not found", logSkip)
					continue
				}
				return err
			}

			if !pod.DeletionTimestamp.IsZero() {
				_log.Debugf("%s. Workload pod is being deleted", logSkip)
				continue
			}

			if pod.CreationTimestamp.After(csiPod.DeletionTimestamp.Time) {
				_log.WithFields(logrus.Fields{
					"creationTimestamp": pod.CreationTimestamp,
					"deletionTimestamp": csiPod.DeletionTimestamp},
				).Infof("%s. Workload pod is created after CSI plugin pod deletion timestamp", logSkip)
			}

			// Only delete pod which has controller to make sure that the pod will be recreated by its controller
			if metav1.GetControllerOf(pod) == nil {
				_log.Warnf("%s. Workload pod is not managed by a controller", logSkip)
				continue
			}

			if pod.Spec.NodeName == csiPod.Spec.NodeName {
				kc.eventRecorder.Eventf(volume, corev1.EventTypeWarning, constant.EventReasonRemount, "Requesting workload pod %v deletion to remount NFS share after unexpected CSI plugin pod %v restart on node %v", pod.Name, csiPod.Name, csiPod.Spec.NodeName)
				filteredPods = append(filteredPods, pod)
			}
		}
	}

	for _, pod := range filteredPods {
		log.WithField("workloadPod", pod.Name).Info("Deleting workload pod on CSI plugin node")
		err = kc.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				return nil
			}
			return err
		}
	}

	return nil
}

// isControllerInBlacklist returns true if the owner reference kind is in the blacklist.
func (kc *KubernetesPodController) isControllerInBlacklist(resource *metav1.OwnerReference) bool {
	if resource == nil {
		return false
	}

	blacklist, err := kc.ds.GetSettingBlacklistForAutoDeletePodWhenVolumeDetachedUnexpectedly()
	if err != nil {
		kc.logger.WithError(err).Warnf("%v: failed to get blacklist for auto-delete pod when volume detached unexpectedly, assuming no blacklist", controllerAgentName)
		return false
	}

	if len(blacklist) == 0 {
		return false
	}

	api, _, ok := strings.Cut(resource.APIVersion, "/")
	if !ok {
		return false
	}

	apiKind := fmt.Sprintf("%s/%s", api, resource.Kind)
	if _, exists := blacklist[apiKind]; exists {
		return true
	}

	return false
}

// cleanupForceDeletedPodResources removes stale resources left behind when a pod
// is force-deleted (i.e., deletion grace period is zero).
func (kc *KubernetesPodController) cleanupForceDeletedPodResources(pod *corev1.Pod) error {
	if !isControllerResponsibleFor(kc.controllerID, kc.ds, pod.Name, "", pod.Spec.NodeName) {
		return nil
	}

	if pod.DeletionTimestamp.IsZero() {
		return nil
	}

	if *pod.DeletionGracePeriodSeconds != 0 {
		return nil
	}

	// Cleanup volume attachments associated with the force-deleted pod.
	// Ref: https://github.com/longhorn/longhorn/issues/10689
	volumeAttachments, err := kc.getVolumeAttachmentsOfPod(pod)
	if err != nil {
		return err
	}

	for _, volumeAttachment := range volumeAttachments {
		shouldDeleteVolumeAttachment, err := kc.shouldDeleteVolumeAttachmentForForceDeletedPod(pod, volumeAttachment)
		if err != nil {
			logrus.WithError(err).Errorf("%v: failed to check if volume attachment %q for force-deleted pod %q should be deleted", controllerAgentName, volumeAttachment.Name, pod.Name)
			continue
		}

		if !shouldDeleteVolumeAttachment {
			continue
		}

		kc.logger.Infof("%v: deleting volume attachment %q for force-deleted pod %q", controllerAgentName, volumeAttachment.Name, pod.Name)

		err = kc.kubeClient.StorageV1().VolumeAttachments().Delete(context.TODO(), volumeAttachment.Name, metav1.DeleteOptions{})
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				continue
			}
			return errors.Wrapf(err, "failed to delete volume attachment %q for force-deleted pod %q", volumeAttachment.Name, pod.Name)
		}

		kc.logger.Infof("%v: deleted volume attachment %q for force-deleted pod %q", controllerAgentName, volumeAttachment.Name, pod.Name)
	}

	return nil
}

// shouldDeleteVolumeAttachmentForForceDeletedPod checks whether the VolumeAttachment
// associated with a force-deleted Pod should be deleted.
func (kc *KubernetesPodController) shouldDeleteVolumeAttachmentForForceDeletedPod(pod *corev1.Pod, volumeAttachment *storagev1.VolumeAttachment) (bool, error) {
	if !volumeAttachment.DeletionTimestamp.IsZero() {
		return false, nil // Already marked for deletion.
	}

	if !volumeAttachment.Status.Attached {
		return false, nil // Not currently attached, let Kubernetes handle its lifecycle.
	}

	persistentVolumeName := volumeAttachment.Spec.Source.PersistentVolumeName
	if persistentVolumeName == nil {
		kc.logger.Infof("%v: volume attachment %q has no associated persistent volume; skipping cleanup for force-deleted pod %q",
			controllerAgentName, volumeAttachment.Name, pod.Name)
		return false, nil // No persistent volume, let Kubernetes handle its lifecycle.
	}

	persistentVolume, err := kc.ds.GetPersistentVolumeRO(*volumeAttachment.Spec.Source.PersistentVolumeName)
	if err != nil {
		return false, err
	}

	claimRef := persistentVolume.Spec.ClaimRef
	if claimRef == nil {
		kc.logger.Infof("%v: persistent volume %q has no claimRef; cleaning up volume attachment %q for force-deleted pod %q",
			controllerAgentName, persistentVolume.Name, volumeAttachment.Name, pod.Name)
		return true, nil // No claim, safe to delete.
	}

	pods, err := kc.ds.ListPodsByPersistentVolumeClaimName(claimRef.Name, pod.Namespace)
	if err != nil {
		return false, err
	}

	if conflictingPod := kc.getPodWithConflictedAttachment(pods, pod); conflictingPod != nil {
		kc.logger.Infof("%v: found conflicting pod %q with attachment for volume %q; cleaning up volume attachment %q for force-deleted pod %q",
			controllerAgentName, conflictingPod.Name, persistentVolume.Name, volumeAttachment.Name, pod.Name)
		return true, nil // There are pods that require the volume and run on another node.
	}

	return false, nil
}

// getPodWithConflictedAttachment returns the first pod in Pending phase from the
// given list of pods that has a "Multi-Attach error" event caused by the specified
// conflictingPod
func (kc *KubernetesPodController) getPodWithConflictedAttachment(pods []*corev1.Pod, conflictingPod *corev1.Pod) *corev1.Pod {
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}

		if pod.Status.Phase != corev1.PodPending {
			continue
		}

		events, err := kc.ds.GetResourceEventList("Pod", pod.Name, pod.Namespace)
		if err != nil {
			logrus.WithError(err).Warnf("%v: failed to get events for pod %v", controllerAgentName, pod.Name)
			continue
		}

		for _, event := range events.Items {
			if !strings.Contains(event.Message, "Multi-Attach error") {
				continue
			}

			if strings.Contains(event.Message, conflictingPod.Name) {
				return pod
			}

			logrus.Debugf("%s: pod %v has Multi-Attach error, but not caused by pod %v, skipping cleanup",
				controllerAgentName, pod.Name, conflictingPod.Name)
		}
	}

	return nil
}

// handlePodDeletionIfNodeDown determines whether we are allowed to forcefully delete a pod
// from a failed node based on the users chosen NodeDownPodDeletionPolicy.
// This is necessary because Kubernetes never forcefully deletes pods on a down node,
// the pods are stuck in terminating state forever and Longhorn volumes are not released.
// We provide an option for users to help them automatically force delete terminating pods
// of StatefulSet/Deployment on the downed node. By force deleting, k8s will detach Longhorn volumes
// and spin up replacement pods on a new node.
//
// Force delete a pod when all of the below conditions are meet:
// 1. NodeDownPodDeletionPolicy is different than DoNothing
// 2. pod belongs to a StatefulSet/Deployment depend on NodeDownPodDeletionPolicy
// 3. node containing the pod is down
// 4. the pod is terminating and the DeletionTimestamp has passed.
// 5. pod has a PV with provisioner driver.longhorn.io
func (kc *KubernetesPodController) handlePodDeletionIfNodeDown(pod *corev1.Pod, nodeID string, namespace string) error {
	deletionPolicy := types.NodeDownPodDeletionPolicyDoNothing
	if deletionSetting, err := kc.ds.GetSettingValueExisted(types.SettingNameNodeDownPodDeletionPolicy); err == nil {
		deletionPolicy = types.NodeDownPodDeletionPolicy(deletionSetting)
	}

	shouldDelete := (deletionPolicy == types.NodeDownPodDeletionPolicyDeleteStatefulSetPod && isOwnedByStatefulSet(pod)) ||
		(deletionPolicy == types.NodeDownPodDeletionPolicyDeleteDeploymentPod && isOwnedByDeployment(pod)) ||
		(deletionPolicy == types.NodeDownPodDeletionPolicyDeleteBothStatefulsetAndDeploymentPod && (isOwnedByStatefulSet(pod) || isOwnedByDeployment(pod)))

	if !shouldDelete {
		return nil
	}

	isNodeDown, err := kc.ds.IsNodeDownOrDeleted(nodeID)
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate Node %v for pod %v in handlePodDeletionIfNodeDown", nodeID, pod.Name)
	}
	if !isNodeDown {
		return nil
	}

	if pod.DeletionTimestamp == nil {
		return nil
	}

	// make sure the volumeattachments of the pods are gone first
	// ref: https://github.com/longhorn/longhorn/issues/2947
	volumeAttachments, err := kc.getVolumeAttachmentsOfPod(pod)
	if err != nil {
		return err
	}
	for _, va := range volumeAttachments {
		if va.DeletionTimestamp == nil {
			err := kc.kubeClient.StorageV1().VolumeAttachments().Delete(context.TODO(), va.Name, metav1.DeleteOptions{})
			if err != nil {
				if datastore.ErrorIsNotFound(err) {
					continue
				}
				return err
			}
			kc.logger.Infof("%v: deleted volume attachment %v for pod %v on downed node %v", controllerAgentName, va.Name, pod.Name, nodeID)
		}
		// wait the volumeattachment object to be deleted
		kc.logger.Infof("%v: wait for volume attachment %v for pod %v on downed node %v to be deleted", controllerAgentName, va.Name, pod.Name, nodeID)
		return nil
	}

	if pod.DeletionTimestamp.After(time.Now()) {
		return nil
	}

	gracePeriod := int64(0)
	err = kc.kubeClient.CoreV1().Pods(namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to forcefully delete Pod %v on the downed Node %v in handlePodDeletionIfNodeDown", pod.Name, nodeID)
	}
	kc.logger.Infof("%v: Forcefully deleted pod %v on downed node %v", controllerAgentName, pod.Name, nodeID)

	return nil
}

func (kc *KubernetesPodController) getVolumeAttachmentsOfPod(pod *corev1.Pod) ([]*storagev1.VolumeAttachment, error) {
	var res []*storagev1.VolumeAttachment
	volumeAttachments, err := kc.ds.ListVolumeAttachmentsRO()
	if err != nil {
		return nil, err
	}

	pvs := make(map[string]bool)

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}

		pvc, err := kc.ds.GetPersistentVolumeClaimRO(pod.Namespace, vol.PersistentVolumeClaim.ClaimName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				continue
			}
			return nil, err
		}
		pvs[pvc.Spec.VolumeName] = true
	}

	for _, va := range volumeAttachments {
		if va.Spec.NodeName != pod.Spec.NodeName {
			continue
		}
		if va.Spec.Attacher != types.LonghornDriverName {
			continue
		}
		if va.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		if _, ok := pvs[*va.Spec.Source.PersistentVolumeName]; !ok {
			continue
		}
		res = append(res, va)
	}

	return res, nil
}

// handlePodDeletionIfVolumeRequestRemount will delete the pod which is using a volume that has requested remount.
// By deleting the consuming pod, Kubernetes will recreated them, reattaches, and remounts the volume.
func (kc *KubernetesPodController) handlePodDeletionIfVolumeRequestRemount(pod *corev1.Pod) error {
	// Only handle pod that is on the same node as this manager
	if pod.Spec.NodeName != kc.controllerID {
		return nil
	}

	autoDeletePodWhenVolumeDetachedUnexpectedly, err := kc.ds.GetSettingAsBool(types.SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly)
	if err != nil {
		return err
	}
	if !autoDeletePodWhenVolumeDetachedUnexpectedly {
		return nil
	}

	// Only delete pod
	// 1. which has controller to make sure that the pod will be recreated by its controller
	// 2. whose controller kind is not in the blacklist
	if ownerRef := metav1.GetControllerOf(pod); ownerRef == nil || kc.isControllerInBlacklist(ownerRef) {
		return nil
	}

	volumeList, err := kc.getAssociatedVolumes(pod)
	if err != nil {
		return err
	}

	isStorageNetworkForRWXVolume, err := kc.ds.IsStorageNetworkForRWXVolume()
	if err != nil {
		kc.logger.WithError(err).Warn("Failed to check isStorageNetwork, assuming not")
	}

	// Only delete pod which has startTime < vol.Status.RemountRequestAt AND timeNow > vol.Status.RemountRequestAt + delayDuration
	// The delayDuration is to make sure that we don't repeatedly delete the pod too fast
	// when vol.Status.RemountRequestAt is updated too quickly by volumeController
	if pod.Status.StartTime == nil {
		return nil
	}

	// Avoid repeat deletion
	if pod.DeletionTimestamp != nil {
		return nil
	}

	podStartTime := pod.Status.StartTime.Time
	for _, vol := range volumeList {
		if vol.Status.RemountRequestedAt == "" {
			continue
		}

		// NFS clients can generally recover without a restart/remount when the NFS server restarts using the same Cluster IP.
		// A remount is required when the storage network for RWX is in use because the new NFS server has a different IP.
		if isRegularRWXVolume(vol) && !isStorageNetworkForRWXVolume {
			continue
		}

		remountRequestedAt, err := time.Parse(time.RFC3339, vol.Status.RemountRequestedAt)
		if err != nil {
			return err
		}

		timeNow := time.Now()
		if podStartTime.Before(remountRequestedAt) {
			if !timeNow.After(remountRequestedAt.Add(remountRequestDelayDuration)) {
				kc.logger.Infof("Current time is not %v seconds after request remount, requeue the pod %v to handle it later", remountRequestDelayDuration.Seconds(), pod.GetName())
				kc.enqueuePodAfter(pod, remountRequestDelayDuration)
				return nil
			}

			gracePeriod := int64(30)
			err := kc.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.GetName(), metav1.DeleteOptions{
				GracePeriodSeconds: &gracePeriod,
			})
			if err != nil && !datastore.ErrorIsNotFound(err) {
				return err
			}
			kc.logger.Infof("Deleted pod %v so that Kubernetes will handle remounting volume %v", pod.GetName(), vol.GetName())
			return nil
		}

	}

	return nil
}

func isOwnedByStatefulSet(pod *corev1.Pod) bool {
	if ownerRef := metav1.GetControllerOf(pod); ownerRef != nil {
		return ownerRef.Kind == types.KubernetesKindStatefulSet
	}
	return false
}

func isOwnedByDeployment(pod *corev1.Pod) bool {
	if ownerRef := metav1.GetControllerOf(pod); ownerRef != nil {
		return ownerRef.Kind == types.KubernetesKindReplicaSet
	}
	return false
}

// enqueuePodChange determines if the pod requires processing based on whether the pod has a PV created by us (driver.longhorn.io)
func (kc *KubernetesPodController) enqueuePodChange(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	if isCSIPluginPod(pod) {
		if pod.Spec.NodeName == kc.controllerID {
			kc.queue.Add(key)
		}
		return
	}

	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}

		pvc, err := kc.ds.GetPersistentVolumeClaimRO(pod.Namespace, v.PersistentVolumeClaim.ClaimName)
		if datastore.ErrorIsNotFound(err) {
			continue
		}
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", pvc, err))
			return
		}

		pv, err := kc.getAssociatedPersistentVolume(pvc)
		if datastore.ErrorIsNotFound(err) {
			continue
		}
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("error getting Persistent Volume for PVC: %v", pvc))
			return
		}

		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == types.LonghornDriverName {
			kc.queue.Add(key)
			break
		}
	}
}

func (kc *KubernetesPodController) getAssociatedPersistentVolume(pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolume, error) {
	pvName := pvc.Spec.VolumeName
	return kc.ds.GetPersistentVolumeRO(pvName)
}

func (kc *KubernetesPodController) getAssociatedVolumes(pod *corev1.Pod) ([]*longhorn.Volume, error) {
	log := getLoggerForPod(kc.logger, pod)
	var volumeList []*longhorn.Volume
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}

		pvc, err := kc.ds.GetPersistentVolumeClaimRO(pod.Namespace, v.PersistentVolumeClaim.ClaimName)
		if datastore.ErrorIsNotFound(err) {
			log.WithError(err).Warn("Cannot auto-delete Pod when the associated PersistentVolumeClaim is not found")
			continue
		}
		if err != nil {
			return nil, err
		}

		pv, err := kc.getAssociatedPersistentVolume(pvc)
		if datastore.ErrorIsNotFound(err) {
			log.WithError(err).Warn("Cannot auto-delete Pod when the associated PersistentVolume is not found")
			continue
		}
		if err != nil {
			return nil, err
		}

		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == types.LonghornDriverName {
			vol, err := kc.ds.GetVolume(pv.Spec.CSI.VolumeHandle)
			if datastore.ErrorIsNotFound(err) {
				log.WithError(err).Warn("Cannot auto-delete Pod when the associated Volume is not found")
				continue
			}
			if err != nil {
				return nil, err
			}
			volumeList = append(volumeList, vol)
		}
	}

	return volumeList, nil
}

func (kc *KubernetesPodController) enqueuePodAfter(obj interface{}, delay time.Duration) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	kc.queue.AddAfter(key, delay)
}

func isCSIPluginPod(obj interface{}) bool {
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

	if len(pod.OwnerReferences) > 0 {
		for _, ownerRef := range pod.OwnerReferences {
			if ownerRef.Kind != types.KubernetesKindDaemonSet {
				continue
			}

			if ownerRef.Name != types.CSIPluginName {
				continue
			}

			return true
		}
	}
	return false
}
