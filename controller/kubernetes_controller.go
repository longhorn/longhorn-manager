package controller

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/informers/storage/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	listerstorage "k8s.io/client-go/listers/storage/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/datastore"
	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

type KubernetesController struct {
	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	pvLister  listerv1.PersistentVolumeLister
	pvcLister listerv1.PersistentVolumeClaimLister
	pLister   listerv1.PodLister
	vaLister  listerstorage.VolumeAttachmentLister

	vStoreSynced   cache.InformerSynced
	pvStoreSynced  cache.InformerSynced
	pvcStoreSynced cache.InformerSynced
	pStoreSynced   cache.InformerSynced
	vaStoreSynced  cache.InformerSynced

	queue workqueue.RateLimitingInterface

	// key is <PVName>, value is <VolumeName>
	pvToVolumeCache sync.Map

	// for unit test
	nowHandler func() string
}

func NewKubernetesController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	volumeInformer lhinformers.VolumeInformer,
	persistentVolumeInformer coreinformers.PersistentVolumeInformer,
	persistentVolumeClaimInformer coreinformers.PersistentVolumeClaimInformer,
	podInformer coreinformers.PodInformer,
	volumeAttachmentInformer v1beta1.VolumeAttachmentInformer,
	kubeClient clientset.Interface) *KubernetesController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	kc := &KubernetesController{
		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-kubernetes-controller"}),

		pvLister:  persistentVolumeInformer.Lister(),
		pvcLister: persistentVolumeClaimInformer.Lister(),
		pLister:   podInformer.Lister(),
		vaLister:  volumeAttachmentInformer.Lister(),

		vStoreSynced:   volumeInformer.Informer().HasSynced,
		pvStoreSynced:  persistentVolumeInformer.Informer().HasSynced,
		pvcStoreSynced: persistentVolumeClaimInformer.Informer().HasSynced,
		pStoreSynced:   podInformer.Informer().HasSynced,
		vaStoreSynced:  volumeAttachmentInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Longhorn-Kubernetes"),

		pvToVolumeCache: sync.Map{},

		nowHandler: util.Now,
	}

	persistentVolumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pv := obj.(*v1.PersistentVolume)
			kc.enqueuePersistentVolume(pv)
		},
		UpdateFunc: func(old, cur interface{}) {
			curPV := cur.(*v1.PersistentVolume)
			kc.enqueuePersistentVolume(curPV)
		},
		DeleteFunc: func(obj interface{}) {
			pv := obj.(*v1.PersistentVolume)
			kc.enqueuePersistentVolume(pv)
			kc.enqueuePVDeletion(pv)
		},
	})

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			kc.enqueuePodChange(pod)
		},
		UpdateFunc: func(old, cur interface{}) {
			curPod := cur.(*v1.Pod)
			kc.enqueuePodChange(curPod)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			kc.enqueuePodChange(pod)
		},
	})

	// after volume becomes detached, try to delete the VA of lost node
	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, cur interface{}) {
			curVolume := cur.(*longhorn.Volume)
			kc.enqueueVolumeChange(curVolume)
		},
	})

	return kc
}

func (kc *KubernetesController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer kc.queue.ShutDown()

	logrus.Infof("Start kubernetes controller")
	defer logrus.Infof("Shutting down kubernetes controller")

	if !controller.WaitForCacheSync("kubernetes", stopCh,
		kc.vStoreSynced, kc.pvStoreSynced, kc.pvcStoreSynced, kc.pStoreSynced, kc.vaStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(kc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (kc *KubernetesController) worker() {
	for kc.processNextWorkItem() {
	}
}

func (kc *KubernetesController) processNextWorkItem() bool {
	key, quit := kc.queue.Get()

	if quit {
		return false
	}
	defer kc.queue.Done(key)

	err := kc.syncKubernetesStatus(key.(string))
	kc.handleErr(err, key)

	return true
}

func (kc *KubernetesController) handleErr(err error, key interface{}) {
	if err == nil {
		kc.queue.Forget(key)
		return
	}

	if kc.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn volume kubernetes status %v: %v", key, err)
		kc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Persistent Volume %v out of the queue: %v", key, err)
	kc.queue.Forget(key)
}

func (kc *KubernetesController) syncKubernetesStatus(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "kubernetes-controller: fail to sync %v", key)
	}()
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	ok, err := kc.cleanupForPVDeletion(name)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	pv, err := kc.pvLister.Get(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "Error getting Persistent Volume %s", name)
	}
	volumeName := kc.getCSIVolumeHandleFromPV(pv)
	if volumeName == "" {
		return nil
	}

	volume, err := kc.ds.GetVolume(volumeName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return err
	}

	// existing volume may be used/reused by pv
	if volume.Status.KubernetesStatus.PVName != name {
		volume.Status.KubernetesStatus = types.KubernetesStatus{}
		kc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonStart, "Persistent Volume %v started to use/reuse Longhorn volume %v", volume.Name, name)
	}
	ks := &volume.Status.KubernetesStatus

	lastPVStatus := ks.PVStatus

	ks.PVName = name
	ks.PVStatus = string(pv.Status.Phase)

	if pv.Spec.ClaimRef != nil {
		if pv.Status.Phase == v1.VolumeBound {
			// set for bounded PVC
			ks.PVCName = pv.Spec.ClaimRef.Name
			ks.Namespace = pv.Spec.ClaimRef.Namespace
			ks.LastPVCRefAt = ""
		} else {
			// PVC is no longer bound with PV. indicating history data by setting <LastPVCRefAt>
			if lastPVStatus == string(v1.VolumeBound) && ks.LastPVCRefAt == "" {
				ks.LastPVCRefAt = kc.nowHandler()
			}
		}
	} else {
		if ks.LastPVCRefAt == "" {
			if pv.Status.Phase == v1.VolumeBound {
				return fmt.Errorf("BUG: current Persistent Volume %v is in Bound phase but has no ClaimRef field", pv.Name)
			}
			// The associated PVC is removed from the PV ClaimRef
			if ks.PVCName != "" {
				ks.LastPVCRefAt = kc.nowHandler()
			}
		}
	}

	pod, err := kc.getAssociatedPod(ks.Namespace, ks.PVCName)
	if err != nil {
		return err
	}

	if pod != nil && pod.DeletionTimestamp == nil {
		ks.PodName = pod.Name
		ks.PodStatus = string(pod.Status.Phase)
		ks.WorkloadName, ks.WorkloadType = kc.detectWorkload(pod, ks)
		ks.LastPodRefAt = ""
	} else {
		if ks.PodName != "" && ks.LastPodRefAt == "" {
			ks.LastPodRefAt = kc.nowHandler()
		}
	}

	defer kc.cleanupVolumeAttachment(volume, ks)

	volume, err = kc.ds.UpdateVolume(volume)
	if err != nil {
		return err
	}

	return nil
}

func (kc *KubernetesController) getCSIVolumeHandleFromPV(pv *v1.PersistentVolume) string {
	if pv == nil {
		return ""
	}
	// try to get associated Longhorn volume name
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
		return ""
	}
	return pv.Spec.CSI.VolumeHandle
}

func (kc *KubernetesController) enqueuePersistentVolume(pv *v1.PersistentVolume) {
	key, err := controller.KeyFunc(pv)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", pv, err))
		return
	}
	kc.queue.AddRateLimited(key)
	return
}

func (kc *KubernetesController) enqueuePodChange(pod *v1.Pod) {
	if _, err := controller.KeyFunc(pod); err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", pod, err))
		return
	}

	for _, v := range pod.Spec.Volumes {
		if v.VolumeSource.PersistentVolumeClaim != nil {
			pvc, err := kc.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(v.VolumeSource.PersistentVolumeClaim.ClaimName)
			if datastore.ErrorIsNotFound(err) {
				continue
			}
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", pvc, err))
				return
			}
			pvName := pvc.Spec.VolumeName
			if pvName != "" {
				kc.queue.AddRateLimited(pvName)
			}
		}
	}
	return
}

func (kc *KubernetesController) enqueueVolumeChange(volume *longhorn.Volume) {
	if _, err := controller.KeyFunc(volume); err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", volume, err))
		return
	}
	if volume.Status.State != types.VolumeStateDetached {
		return
	}
	ks := volume.Status.KubernetesStatus
	if ks.PVName != "" && ks.PVStatus == string(v1.VolumeBound) && ks.PodName != "" && ks.PodStatus == string(v1.PodPending) && ks.LastPodRefAt == "" {
		kc.queue.AddRateLimited(volume.Status.KubernetesStatus.PVName)
	}
	return
}

func (kc *KubernetesController) enqueuePVDeletion(pv *v1.PersistentVolume) {
	if _, err := controller.KeyFunc(pv); err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", pv, err))
		return
	}
	if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
		kc.pvToVolumeCache.Store(pv.Name, pv.Spec.CSI.VolumeHandle)
	}
	return
}

func (kc *KubernetesController) cleanupForPVDeletion(pvName string) (bool, error) {
	volumeName, ok := kc.pvToVolumeCache.Load(pvName)
	if !ok {
		return false, nil
	}
	volume, err := kc.ds.GetVolume(volumeName.(string))
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			kc.pvToVolumeCache.Delete(pvName)
			return true, nil
		}
		return false, errors.Wrapf(err, "failed to get volume for cleanup in cleanupForPVDeletion")
	}
	pv, err := kc.pvLister.Get(pvName)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return false, errors.Wrapf(err, "failed to get associated pv in cleanupForPVDeletion")
	}
	if datastore.ErrorIsNotFound(err) || pv.DeletionTimestamp != nil {
		ks := &volume.Status.KubernetesStatus
		if ks.PVCName != "" && ks.LastPVCRefAt == "" {
			volume.Status.KubernetesStatus.LastPVCRefAt = kc.nowHandler()
		}
		if ks.PodName != "" && ks.LastPodRefAt == "" {
			volume.Status.KubernetesStatus.LastPodRefAt = kc.nowHandler()
		}
		volume.Status.KubernetesStatus.PVName = ""
		volume.Status.KubernetesStatus.PVStatus = ""
		volume, err = kc.ds.UpdateVolume(volume)
		if err != nil {
			return false, errors.Wrapf(err, "failed to update volume in cleanupForPVDeletion")
		}
		kc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonStop, "Persistent Volume %v stopped to use Longhorn volume %v", pvName, volume.Name)
	}
	kc.pvToVolumeCache.Delete(pvName)
	return true, nil
}

func (kc *KubernetesController) getAssociatedPod(namespace, pvcName string) (*v1.Pod, error) {
	pods, err := kc.pLister.Pods(namespace).List(labels.Everything())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list pods in getAssociatedPod")
	}
	for _, p := range pods {
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
				return p, nil
			}
		}
	}
	return nil, nil
}

func (kc *KubernetesController) detectWorkload(p *v1.Pod, ks *types.KubernetesStatus) (string, string) {
	refs := p.GetObjectMeta().GetOwnerReferences()
	for _, ref := range refs {
		if ref.Name != "" && ref.Kind != "" {
			return ref.Name, ref.Kind
		}
	}
	return "", ""
}

func (kc *KubernetesController) cleanupVolumeAttachment(volume *longhorn.Volume, ks *types.KubernetesStatus) {
	// PV and PVC should exist. Pod should be Pending
	if !(ks.PVStatus == string(v1.VolumeBound) && ks.PVCName != "" && ks.LastPVCRefAt == "") {
		return
	}
	if !(ks.PodName != "" && ks.PodStatus == string(v1.PodPending) && ks.LastPodRefAt == "") {
		return
	}

	va, err := kc.getVolumeAttachment(ks)
	if err != nil {
		logrus.Errorf("failed to get VolumeAttachment in cleanupVolumeAttachment: %v", err)
		return
	}
	if va == nil {
		return
	}

	cleanupFlag, err := kc.ds.IsNodeDownOrDeleted(va.Spec.NodeName)
	if err != nil {
		logrus.Errorf("failed to detect node %v in cleanupVolumeAttachment: %v", va.Spec.NodeName, err)
		return
	}
	// the node VolumeAttachment on is declared `NotReady` or doesn't exist.
	if !cleanupFlag {
		return
	}

	err = kc.kubeClient.StorageV1beta1().VolumeAttachments().Delete(va.Name, &metav1.DeleteOptions{})
	if err != nil {
		logrus.Errorf("failed to delete VolumeAttachment %v in cleanupVolumeAttachment: %v", va.Name, err)
		return
	}
	kc.eventRecorder.Eventf(volume, v1.EventTypeNormal, EventReasonDelete, "Cleanup Volume Attachment on 'NotReady' Node: %v", va.Name)
	return
}

func (kc *KubernetesController) getVolumeAttachment(ks *types.KubernetesStatus) (*storagev1.VolumeAttachment, error) {
	vas, err := kc.vaLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, va := range vas {
		if *va.Spec.Source.PersistentVolumeName == ks.PVName {
			return va, nil
		}
	}
	return nil, nil
}
