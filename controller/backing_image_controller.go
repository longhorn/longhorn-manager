package controller

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

type BackingImageController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the backing image
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	biStoreSynced cache.InformerSynced
	rStoreSynced  cache.InformerSynced
	pStoreSynced  cache.InformerSynced
}

func NewBackingImageController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	backingImageInformer lhinformers.BackingImageInformer,
	replicaInformer lhinformers.ReplicaInformer,
	podInformer coreinformers.PodInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID, serviceAccount string) *BackingImageController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: typedv1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	bic := &BackingImageController{
		baseController: newBaseController("longhorn-backing-image", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-backing-image-controller"}),

		ds: ds,

		biStoreSynced: backingImageInformer.Informer().HasSynced,
		rStoreSynced:  replicaInformer.Informer().HasSynced,
		pStoreSynced:  podInformer.Informer().HasSynced,
	}

	backingImageInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImage,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImage(cur) },
		DeleteFunc: bic.enqueueBackingImage,
	})

	replicaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForReplica,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForReplica(cur) },
		DeleteFunc: bic.enqueueBackingImageForReplica,
	})

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    bic.enqueueBackingImageForPod,
		UpdateFunc: func(old, cur interface{}) { bic.enqueueBackingImageForPod(cur) },
		DeleteFunc: bic.enqueueBackingImageForPod,
	})

	return bic
}

func (bic *BackingImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer bic.queue.ShutDown()

	logrus.Infof("Start Longhorn Backing Image controller")
	defer logrus.Infof("Shutting down Longhorn Backing Image controller")

	if !cache.WaitForNamedCacheSync("longhorn backing images", stopCh, bic.biStoreSynced, bic.rStoreSynced, bic.pStoreSynced) {
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

	if bic.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn backing image %v: %v", key, err)
		bic.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn backing image %v out of the queue: %v", key, err)
	bic.queue.Forget(key)
}

func getLoggerForBackingImage(logger logrus.FieldLogger, bi *longhorn.BackingImage) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"backingImageName": bi.Name,
			"imageURL":         bi.Spec.ImageURL,
		},
	)
}

func (bic *BackingImageController) syncBackingImage(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync backing image for %v", key)
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
		if !datastore.ErrorIsNotFound(err) {
			bic.logger.WithField("backingImage", name).WithError(err).Error("Failed to retrieve backing image from datastore")
			return err
		}
		bic.logger.WithField("backingImage", name).Debug("Can't find backing image, may have been deleted")
		return nil
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
		log.Infof("Backing Image got new owner %v", bic.controllerID)
	}

	if backingImage.DeletionTimestamp != nil {
		pods, err := bic.ds.ListBackingImageRelatedPods(backingImage.Name)
		if err != nil {
			return err
		}
		replicas, err := bic.ds.ListReplicasByBackingImage(backingImage.Name)
		if err != nil {
			return err
		}
		// Rely on the foreground delete propagation to clean up all pods
		if len(pods) == 0 && len(replicas) == 0 {
			log.Info("The data in all disks is removed and no replica is using the backing image, then the finalizer will be removed")
			return bic.ds.RemoveFinalizerForBackingImage(backingImage)
		}
		return nil
	}

	existingBackingImage := backingImage.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingBackingImage.Status, backingImage.Status) {
			_, err = bic.ds.UpdateBackingImageStatus(backingImage)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			bic.enqueueBackingImage(backingImage)
			err = nil
		}
	}()

	if err := bic.syncBackingImageWithDownloadPods(backingImage); err != nil {
		return err
	}

	if err := bic.updateDiskLastReferenceMap(backingImage); err != nil {
		return err
	}

	return nil
}

func getDownloadPodName(biName, diskUUID string) string {
	return fmt.Sprintf("%s-download-%s", biName, diskUUID[:8])
}

// syncBackingImageWithDownloadPods controls pod existence/cleanup and provides the following state transitions
//   Pod is no longer needed:
//     downloading, downloaded, terminating, failed -> terminating (pod terminating)
//     downloading, downloaded, terminating, failed -> empty (pod removed)
//   Pod is requested:
//     empty, failed -> downloading (no pod and pod creation success)
//     empty, downloading, downloaded, terminating, failed -> failed (no pod and pod creation failure; pod terminating; pod not pending or running)
//     empty, downloading, terminating -> downloading (pod pending)
//     downloaded -> failed (pod pending/pod not ready)
//     downloading -> downloaded (pod running and ready)
func (bic *BackingImageController) syncBackingImageWithDownloadPods(backingImage *longhorn.BackingImage) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync backing image with download pods")
	}()
	log := getLoggerForBackingImage(bic.logger, backingImage)

	if backingImage.Status.DiskDownloadStateMap == nil {
		backingImage.Status.DiskDownloadStateMap = map[string]types.BackingImageDownloadState{}
	}

	// Check the records exist only in backingImage.Status.DiskDownloadStateMap
	for uuid := range backingImage.Status.DiskDownloadStateMap {
		if _, exists := backingImage.Spec.Disks[uuid]; exists {
			continue
		}
		downloadPodName := getDownloadPodName(backingImage.Name, uuid)
		downloadPod, err := bic.ds.GetPod(downloadPodName)
		if err != nil {
			return err
		}
		if downloadPod == nil {
			log.WithField("diskUUID", uuid).Debugf("Cleaned up the backing image in disk")
			delete(backingImage.Status.DiskDownloadStateMap, uuid)
			continue
		}
		if downloadPod.DeletionTimestamp == nil {
			log.WithFields(logrus.Fields{"diskUUID": uuid, "pod": downloadPodName}).Debugf("Deleting pod")
			if err := bic.ds.DeletePod(downloadPodName); err != nil {
				return err
			}
		}
		backingImage.Status.DiskDownloadStateMap[uuid] = types.BackingImageDownloadStateTerminating
	}

	for diskUUID := range backingImage.Spec.Disks {
		downloadPodName := getDownloadPodName(backingImage.Name, diskUUID)
		downloadPod, err := bic.ds.GetPod(downloadPodName)
		if err != nil {
			return err
		}

		// Create/Recreate the download pod for a disk if the old pod is gone
		// and the disk on the node are ready
		if downloadPod == nil {
			if state, exists := backingImage.Status.DiskDownloadStateMap[diskUUID]; exists {
				log.WithField("diskUUID", diskUUID).Warnf("The disk download state is %v but the download pod is nil, will directly create a new pod then",
					state)
			}
			if downloadPod, err = bic.createDownloadPod(downloadPodName, diskUUID, backingImage); err != nil || downloadPod == nil {
				log.WithFields(logrus.Fields{"diskUUID": diskUUID, "pod": downloadPodName}).WithError(err).Errorf("The download state becomes %v due to the download pod creation failure",
					types.BackingImageDownloadStateFailed)
				bic.enqueueBackingImage(backingImage)
				backingImage.Status.DiskDownloadStateMap[diskUUID] = types.BackingImageDownloadStateFailed
				continue
			}
			// The download state will be updated based on the pod phase later.
		}

		if downloadPod.DeletionTimestamp != nil {
			log.WithFields(logrus.Fields{"diskUUID": diskUUID, "pod": downloadPodName}).Warnf("The disk download state becomes %v due to the terminating pod",
				types.BackingImageDownloadStateFailed)
			backingImage.Status.DiskDownloadStateMap[diskUUID] = types.BackingImageDownloadStateFailed
			continue
		}

		switch downloadPod.Status.Phase {
		case corev1.PodPending:
			if state, exists := backingImage.Status.DiskDownloadStateMap[diskUUID]; exists && state == types.BackingImageDownloadStateDownloaded {
				backingImage.Status.DiskDownloadStateMap[diskUUID] = types.BackingImageDownloadStateFailed
			} else {
				backingImage.Status.DiskDownloadStateMap[diskUUID] = types.BackingImageDownloadStateDownloading
			}
		case corev1.PodRunning:
			isReady := false
			for _, cond := range downloadPod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					isReady = true
				}
			}
			downloadComplete := backingImage.Status.DiskDownloadStateMap[diskUUID] == types.BackingImageDownloadStateDownloaded
			if !downloadComplete && isReady {
				log.WithFields(logrus.Fields{"diskUUID": diskUUID, "pod": downloadPodName}).Infof("The download state becomes %v from %v since the download pod is ready",
					types.BackingImageDownloadStateDownloaded, backingImage.Status.DiskDownloadStateMap[diskUUID])
				backingImage.Status.DiskDownloadStateMap[diskUUID] = types.BackingImageDownloadStateDownloaded
			}

			if downloadComplete && !isReady {
				log.WithFields(logrus.Fields{"diskUUID": diskUUID, "pod": downloadPodName}).Warnf("The download state becomes %v from %v since the download pod suddenly becomes unavailable",
					types.BackingImageDownloadStateFailed, types.BackingImageDownloadStateDownloaded)
				backingImage.Status.DiskDownloadStateMap[diskUUID] = types.BackingImageDownloadStateFailed
			}
		default:
			if backingImage.Status.DiskDownloadStateMap[diskUUID] != types.BackingImageDownloadStateFailed {
				log.WithFields(logrus.Fields{"diskUUID": diskUUID, "pod": downloadPodName}).Warnf("The download state becomes %v from %v due to the download pod phase %v",
					types.BackingImageDownloadStateFailed, backingImage.Status.DiskDownloadStateMap[diskUUID], downloadPod.Status.Phase)
				backingImage.Status.DiskDownloadStateMap[diskUUID] = types.BackingImageDownloadStateFailed
			}
		}

		// Clean up the failed pod first then recreate it later
		if backingImage.Status.DiskDownloadStateMap[diskUUID] == types.BackingImageDownloadStateFailed {
			log.WithFields(logrus.Fields{"diskUUID": diskUUID, "pod": downloadPodName}).Infof("Deleting the pod since the downloading state is %v", types.BackingImageDownloadStateFailed)
			if err := bic.ds.DeletePod(downloadPodName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bic *BackingImageController) updateDiskLastReferenceMap(backingImage *longhorn.BackingImage) error {
	if backingImage.Status.DiskLastRefAtMap == nil {
		backingImage.Status.DiskLastRefAtMap = map[string]string{}
	}

	replicas, err := bic.ds.ListReplicasByBackingImage(backingImage.Name)
	if err != nil {
		return err
	}
	disksInUse := map[string]struct{}{}
	for _, replica := range replicas {
		disksInUse[replica.Spec.DiskID] = struct{}{}
		delete(backingImage.Status.DiskLastRefAtMap, replica.Spec.DiskID)
	}
	for diskUUID := range backingImage.Status.DiskLastRefAtMap {
		if _, exists := backingImage.Spec.Disks[diskUUID]; !exists {
			delete(backingImage.Status.DiskLastRefAtMap, diskUUID)
		}
	}
	for diskUUID := range backingImage.Spec.Disks {
		_, isActiveDisk := disksInUse[diskUUID]
		_, isHistoricDisk := backingImage.Status.DiskLastRefAtMap[diskUUID]
		if !isActiveDisk && !isHistoricDisk {
			backingImage.Status.DiskLastRefAtMap[diskUUID] = util.Now()
		}
	}
	return nil
}

func (bic *BackingImageController) createDownloadPod(podName, diskUUID string, backingImage *longhorn.BackingImage) (*corev1.Pod, error) {
	tolerations, err := bic.ds.GetSettingTaintToleration()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get taint toleration setting before creating instance manager pod")
	}

	registrySecretSetting, err := bic.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get registry secret setting before creating instance manager pod")
	}
	registrySecret := registrySecretSetting.Value

	tolerationsByte, err := json.Marshal(tolerations)
	if err != nil {
		return nil, err
	}

	priorityClass, err := bic.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return nil, err
	}

	imagePullPolicy, err := bic.ds.GetSettingImagePullPolicy()
	if err != nil {
		return nil, err
	}

	node, _, err := bic.ds.GetReadyDiskNode(diskUUID)
	if err != nil {
		return nil, err
	}

	diskPath := ""
	for id, diskStatus := range node.Status.DiskStatus {
		if diskStatus.DiskUUID != diskUUID {
			continue
		}
		if diskSpec, exists := node.Spec.Disks[id]; exists {
			diskPath = diskSpec.Path
		}
		break
	}
	if diskPath == "" {
		return nil, fmt.Errorf("cannot find the disk path with UUID %v on node %v", diskUUID, node.Name)
	}

	// The image is not important.
	// As long as the image is a Linux distribution with wget installed, it can be used in the download pod.
	// We choose the default engine image here so that users don't need to fetch on extra image in the air-gaped environment.
	defaultEngineImage, err := bic.ds.GetSetting(types.SettingNameDefaultEngineImage)
	if err != nil || defaultEngineImage.Value == "" {
		return nil, fmt.Errorf("failed to retrieve default engine image: %v", err)
	}

	filePathInContainer := path.Join(types.BackingImageDirectoryInContainer, types.BackingImageFileName)
	downloadedNotificationFlag := path.Join(types.BackingImageDirectoryInContainer, "downloaded")
	cmdArgs := []string{
		"/bin/bash", "-c",
		"rm " + filePathInContainer + " " + downloadedNotificationFlag + "; " +
			"wget --no-check-certificate -c " + backingImage.Spec.ImageURL + " -O " + filePathInContainer + " > /dev/null 2>&1; " +
			"if [ $? -eq 0 ]; then touch " + downloadedNotificationFlag + " && sync && echo downloaded; fi && " +
			"trap 'rm " + filePathInContainer + " " + downloadedNotificationFlag + " && echo cleaned up' EXIT && sleep infinity"}

	privileged := true
	hostToContainerPropagation := corev1.MountPropagationHostToContainer
	podManifest := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            podName,
			Namespace:       bic.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForBackingImage(backingImage),
			Labels:          types.GetBackingImageLabels(backingImage.Name, diskUUID),
			Annotations:     map[string]string{types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix): string(tolerationsByte)},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: bic.serviceAccount,
			Tolerations:        util.GetDistinctTolerations(tolerations),
			PriorityClassName:  priorityClass.Value,
			Containers: []corev1.Container{
				{
					Name:            podName,
					Image:           defaultEngineImage.Value,
					ImagePullPolicy: imagePullPolicy,
					Command:         cmdArgs,
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							MountPath:        types.BackingImageDirectoryInContainer,
							Name:             "backing-image",
							MountPropagation: &hostToContainerPropagation,
						},
					},
					StartupProbe: &corev1.Probe{
						Handler: corev1.Handler{
							Exec: &corev1.ExecAction{
								Command: []string{
									"bash", "-c",
									"ls " + filePathInContainer + " && ls " + downloadedNotificationFlag,
								},
							},
						},
						// Wait up to 3 hours for the backing image downloading.
						FailureThreshold:    2160,
						PeriodSeconds:       5,
						InitialDelaySeconds: 5,
					},
					// Choose ReadinessProbe rather than LivenessProbe due to this Kubernetes bug:
					// https://github.com/kubernetes/kubernetes/issues/95140
					ReadinessProbe: &corev1.Probe{
						Handler: corev1.Handler{
							Exec: &corev1.ExecAction{
								Command: []string{
									"bash", "-c",
									"ls " + filePathInContainer + " && ls " + downloadedNotificationFlag,
								},
							},
						},
						PeriodSeconds:       5,
						InitialDelaySeconds: 5,
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "backing-image",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: types.GetBackingImageDirectoryOnHost(diskPath, backingImage.Name),
						},
					},
				},
			},
			NodeName:      node.Name,
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	if registrySecret != "" {
		podManifest.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}

	pod, err := bic.ds.CreatePod(podManifest)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return bic.ds.GetPod(podName)
		}
		return nil, err
	}
	log := getLoggerForBackingImage(bic.logger, backingImage)
	log.WithFields(logrus.Fields{"node": node.Name, "diskUUID": diskUUID, "pod": podName}).Infof("Creating download pod for backing image")

	return pod, nil
}

func (bic *BackingImageController) enqueueBackingImage(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	bic.queue.AddRateLimited(key)
}

func (bic *BackingImageController) enqueueBackingImageForReplica(obj interface{}) {
	replica, isReplica := obj.(*longhorn.Replica)
	if !isReplica {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
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
		bic.queue.AddRateLimited(key)
		return
	}
}

func (bic *BackingImageController) enqueueBackingImageForPod(obj interface{}) {
	pod, isPod := obj.(*corev1.Pod)
	if !isPod {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained non Pod object: %#v", deletedState.Obj))
			return
		}
	}

	if biName, exists := pod.Labels[types.GetLonghornLabelKey(types.LonghornLabelBackingImage)]; exists {
		bic.logger.WithField("pod", pod.Name).WithField("backingImage", biName).Trace("Enqueuing backing image for pod")
		key := pod.Namespace + "/" + biName
		bic.queue.AddRateLimited(key)
		return
	}
}

func (bic *BackingImageController) isResponsibleFor(bi *longhorn.BackingImage) bool {
	return isControllerResponsibleFor(bic.controllerID, bic.ds, bi.Name, "", bi.Status.OwnerID)
}
