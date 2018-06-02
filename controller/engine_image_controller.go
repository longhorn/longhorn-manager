package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers_v1beta2 "k8s.io/client-go/informers/apps/v1beta2"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

var (
	ownerKindEngineImage = longhorn.SchemeGroupVersion.WithKind("EngineImage").String()
)

type EngineImageController struct {
	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the engine image
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	iStoreSynced  cache.InformerSynced
	dsStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

func NewEngineImageController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	engineImageInformer lhinformers.EngineImageInformer,
	dsInformer appsinformers_v1beta2.DaemonSetInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID string) *EngineImageController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	ic := &EngineImageController{
		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-engine-image-controller"}),

		ds: ds,

		iStoreSynced:  engineImageInformer.Informer().HasSynced,
		dsStoreSynced: dsInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-engine-image"),
	}

	engineImageInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			img := obj.(*longhorn.EngineImage)
			ic.enqueueEngineImage(img)
		},
		UpdateFunc: func(old, cur interface{}) {
			curImg := cur.(*longhorn.EngineImage)
			ic.enqueueEngineImage(curImg)
		},
		DeleteFunc: func(obj interface{}) {
			img := obj.(*longhorn.EngineImage)
			ic.enqueueEngineImage(img)
		},
	})

	dsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ic.enqueueControlleeChange(obj)
		},
		UpdateFunc: func(old, cur interface{}) {
			ic.enqueueControlleeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			ic.enqueueControlleeChange(obj)
		},
	})

	return ic
}

func (ic *EngineImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ic.queue.ShutDown()

	logrus.Infof("Start Longhorn Engine Image controller")
	defer logrus.Infof("Shutting down Longhorn Engine Image controller")

	if !controller.WaitForCacheSync("longhorn engine images", stopCh, ic.iStoreSynced, ic.dsStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(ic.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (ic *EngineImageController) worker() {
	for ic.processNextWorkItem() {
	}
}

func (ic *EngineImageController) processNextWorkItem() bool {
	key, quit := ic.queue.Get()

	if quit {
		return false
	}
	defer ic.queue.Done(key)

	err := ic.syncEngineImage(key.(string))
	ic.handleErr(err, key)

	return true
}

func (ic *EngineImageController) handleErr(err error, key interface{}) {
	if err == nil {
		ic.queue.Forget(key)
		return
	}

	if ic.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn engine image %v: %v", key, err)
		ic.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn engine image %v out of the queue: %v", key, err)
	ic.queue.Forget(key)
}

func (ic *EngineImageController) syncEngineImage(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync engine image for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != ic.namespace {
		// Not ours, don't do anything
		return nil
	}

	engineImage, err := ic.ds.GetEngineImage(name)
	if err != nil {
		return err
	}
	if engineImage == nil {
		logrus.Infof("Longhorn engine image %v has been deleted", key)
		return nil
	}

	// Not ours
	if engineImage.Spec.OwnerID != ic.controllerID {
		return nil
	}

	dsName := getEngineImageDaemonSetName(engineImage.Name)
	if engineImage.DeletionTimestamp != nil {
		if err := ic.ds.DeleteEngineImageDaemonSet(dsName); err != nil {
			return errors.Wrapf(err, "cannot cleanup daemonset of engine image %v", engineImage.Name)
		}
		logrus.Infof("Removed daemon set %v for engine image %v (%v)", dsName, engineImage.Name, engineImage.Spec.Image)
		return ic.ds.RemoveFinalizerForEngineImage(engineImage)
	}

	savedEngineImage := engineImage.DeepCopy()
	defer func() {
		// we're going to update the object assume things changes
		if err == nil && !reflect.DeepEqual(engineImage, savedEngineImage) {
			_, err = ic.ds.UpdateEngineImage(engineImage)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict", key)
			ic.enqueueEngineImage(engineImage)
			err = nil
		}
	}()

	ds, err := ic.ds.GetEngineImageDaemonSet(dsName)
	if err != nil {
		return errors.Wrapf(err, "cannot get daemonset for engine image %v", engineImage.Name)
	}
	if ds == nil {
		dsSpec := ic.createEngineImageDaemonSetSpec(engineImage)
		if err = ic.ds.CreateEngineImageDaemonSet(dsSpec); err != nil {
			return errors.Wrapf(err, "fail to create daemonset for engine image %v", engineImage.Name)
		}
		logrus.Infof("Created daemon set %v for engine image %v (%v)", dsSpec.Name, engineImage.Name, engineImage.Spec.Image)
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	if ds.Status.NumberAvailable != ds.Status.DesiredNumberScheduled {
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	engineImage.Status.State = types.EngineImageStateReady

	return nil
}

func (ic *EngineImageController) enqueueEngineImage(engineImage *longhorn.EngineImage) {
	key, err := controller.KeyFunc(engineImage)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", engineImage, err))
		return
	}

	ic.queue.AddRateLimited(key)
}

func (ic *EngineImageController) enqueueControlleeChange(obj interface{}) {
	metaObj, err := meta.Accessor(obj)
	if err != nil {
		logrus.Warnf("BUG: %v cannot be convert to metav1.Object: %v", obj, err)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		if ref.Kind != ownerKindEngineImage {
			continue
		}
		namespace := metaObj.GetNamespace()
		ic.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (ic *EngineImageController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != ownerKindEngineImage {
		return
	}
	engineImage, err := ic.ds.GetEngineImage(ref.Name)
	if err != nil || engineImage == nil {
		return
	}
	if engineImage.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	// Not ours
	if engineImage.Spec.OwnerID != ic.controllerID {
		return
	}
	ic.enqueueEngineImage(engineImage)
}

func getEngineImageDaemonSetName(engineImageName string) string {
	return "engine-image-" + engineImageName
}

func (ic *EngineImageController) createEngineImageDaemonSetSpec(ei *longhorn.EngineImage) *appsv1beta2.DaemonSet {
	dsName := getEngineImageDaemonSetName(ei.Name)
	image := ei.Spec.Image
	cmd := []string{
		"/bin/bash",
	}
	args := []string{
		"-c",
		"cp /usr/local/bin/longhorn* /data/ && echo installed && trap 'rm /data/longhorn* && echo cleaned up' EXIT && sleep infinity",
	}
	d := &appsv1beta2.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: dsName,
		},
		Spec: appsv1beta2.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetEngineImageLabel(),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   dsName,
					Labels: types.GetEngineImageLabel(),
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:            dsName,
							Image:           image,
							Command:         cmd,
							Args:            args,
							ImagePullPolicy: v1.PullAlways,
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "data",
									MountPath: "/data/",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "data",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: types.GetEngineBinaryDirectoryOnHostForImage(image),
								},
							},
						},
					},
				},
			},
		},
	}
	return d
}
