package controller

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

const (
	// Use a custom resync period so we can poll Instance Manager processes.
	imcResyncPeriod = time.Second * 5
)

type InstanceManagerController struct {
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	imStoreSynced cache.InformerSynced
	pStoreSynced  cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

func NewInstanceManagerController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	imInformer lhinformers.InstanceManagerInformer,
	pInformer coreinformers.PodInformer,
	kubeClient clientset.Interface,
	namespace, controllerID string) *InstanceManagerController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	imc := &InstanceManagerController{
		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-instance-manager-controller"}),

		ds: ds,

		imStoreSynced: imInformer.Informer().HasSynced,
		pStoreSynced:  pInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-instance-manager"),
	}

	imInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			imc.enqueueInstanceManager(im)
		},
		UpdateFunc: func(old, cur interface{}) {
			curIM := cur.(*longhorn.InstanceManager)
			imc.enqueueInstanceManager(curIM)
		},
		DeleteFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			imc.enqueueInstanceManager(im)
		},
	}, imcResyncPeriod)

	pInformer.Informer().AddEventHandlerWithResyncPeriod(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			switch t := obj.(type) {
			case *v1.Pod:
				return imc.filterInstanceManagerPod(t)
			default:
				utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", imc, obj))
				return false
			}
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*v1.Pod)
				imc.enqueueInstanceManagerPod(pod)
			},
			UpdateFunc: func(old, cur interface{}) {
				newPod := cur.(*v1.Pod)
				imc.enqueueInstanceManagerPod(newPod)
			},
			DeleteFunc: func(obj interface{}) {
				pod := obj.(*v1.Pod)
				imc.enqueueInstanceManagerPod(pod)
			},
		},
	}, imcResyncPeriod)

	return imc
}

func (imc *InstanceManagerController) filterInstanceManagerPod(obj *v1.Pod) bool {
	isInstanceManager := false
	podContainers := obj.Spec.Containers
	for _, con := range podContainers {
		if con.Name == "engine-manager" || con.Name == "replica-manager" {
			isInstanceManager = true
			break
		}
	}
	return isInstanceManager
}

func (imc *InstanceManagerController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer imc.queue.ShutDown()

	logrus.Infof("Starting Longhorn instance manager controller")
	defer logrus.Infof("Shutting down Longhorn instance manager controller")

	if !controller.WaitForCacheSync("longhorn instance manager", stopCh, imc.imStoreSynced, imc.pStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(imc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (imc *InstanceManagerController) worker() {
	for imc.processNextWorkItem() {
	}
}

func (imc *InstanceManagerController) processNextWorkItem() bool {
	key, quit := imc.queue.Get()

	if quit {
		return false
	}
	defer imc.queue.Done(key)

	err := imc.syncInstanceManager(key.(string))
	imc.handleErr(err, key)

	return true
}

func (imc *InstanceManagerController) handleErr(err error, key interface{}) {
	if err == nil {
		imc.queue.Forget(key)
		return
	}

	if imc.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn instance manager %v: %v", key, err)
		imc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn instance manager %v out of the queue: %v", key, err)
	imc.queue.Forget(key)
}

func (imc *InstanceManagerController) syncInstanceManager(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync instance manager for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != imc.namespace {
		return nil
	}

	im, err := imc.ds.GetInstanceManager(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			logrus.Infof("Longhorn instance manager %v has been deleted", key)
			return nil
		}
		return err
	}

	nodeStatusDown := false
	if im.Spec.OwnerID != "" {
		nodeStatusDown, err = imc.ds.IsNodeDownOrDeleted(im.Spec.OwnerID)
		if err != nil {
			logrus.Warnf("Found error while checking if ownerID is down or deleted: %v", err)
		}
	}

	// Node for Instance Manager came back up, take back ownership of Instance Manager.
	if im.Spec.NodeID == imc.controllerID && im.Spec.OwnerID != imc.controllerID {
		im.Spec.OwnerID = imc.controllerID
		newIM, err := imc.ds.UpdateInstanceManager(im)
		if err != nil {
			// Conflict with another controller, keep trying.
			if apierrors.IsConflict(errors.Cause(err)) {
				imc.enqueueInstanceManager(im)
				return nil
			}
			return err
		}
		im = newIM
		logrus.Debugf("Instance Manager Controller %v picked up %v", imc.controllerID, im.Name)
	} else if im.Spec.OwnerID == "" || nodeStatusDown {
		// No owner yet, or Instance Manager's Node is down. Assign to some other Node until the correct Node can take
		// over.
		im.Spec.OwnerID = imc.controllerID
		im, err = imc.ds.UpdateInstanceManager(im)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		logrus.Debugf("Instance Manager Controller %v picked up %v", imc.controllerID, im.Name)
	} else if im.Spec.OwnerID != imc.controllerID {
		// Not ours
		return nil
	}

	if im.DeletionTimestamp != nil {
		if err := imc.cleanupInstanceManager(im); err != nil {
			return err
		}
		return imc.ds.RemoveFinalizerForInstanceManager(im)
	}

	return nil
}

func (imc *InstanceManagerController) enqueueInstanceManager(instanceManager *longhorn.InstanceManager) {
	key, err := controller.KeyFunc(instanceManager)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", instanceManager, err))
		return
	}

	imc.queue.AddRateLimited(key)
}

func (imc *InstanceManagerController) enqueueInstanceManagerPod(pod *v1.Pod) {
	im, err := imc.ds.GetInstanceManager(pod.Name)

	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Warnf("Can't find instance manager for pod %v, may be deleted", pod.Name)
			return
		}
		utilruntime.HandleError(fmt.Errorf("couldn't get instance manager: %v", err))
		return
	}
	imc.enqueueInstanceManager(im)
}

// cleanupInstanceManager cleans up the Pod that was created by the Instance Manager and marks any processes that may
// be on that Instance Manager to Error. This is used when we need to recover from an error or prepare for deletion.
func (imc *InstanceManagerController) cleanupInstanceManager(im *longhorn.InstanceManager) error {
	im.Status.IP = ""
	im.Status.NodeBootID = ""

	for name, instance := range im.Status.Instances {
		instance.State = types.InstanceStateError
		instance.ErrorMsg = "Instance Manager errored"
		im.Status.Instances[name] = instance
	}

	pod, err := imc.ds.GetInstanceManagerPod(im.Name)
	if err != nil {
		return err
	}
	if pod != nil {
		if err := imc.ds.DeletePod(pod.Name); err != nil {
			return err
		}
	}
	return nil
}
