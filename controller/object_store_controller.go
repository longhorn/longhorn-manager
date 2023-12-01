package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

var (
	OneHour, _ = time.ParseDuration("1h")
)

type ObjectStoreController struct {
	*baseController

	controllerID string

	namespace string
	ds        *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewObjectStoreController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
) *ObjectStoreController {
	osc := &ObjectStoreController{
		baseController: newBaseController("object-store", logger),
		controllerID:   controllerID,
		namespace:      namespace,
		ds:             ds,
	}

	ds.ObjectStoreInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    osc.enqueueObjectStore,
			UpdateFunc: func(old, cur interface{}) { osc.enqueueObjectStore(cur) },
			DeleteFunc: osc.enqueueObjectStore,
		},
		OneHour,
	)

	ds.DeploymentInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    osc.enqueueDeployment,
			UpdateFunc: func(old, cur interface{}) { osc.enqueueDeployment(cur) },
			DeleteFunc: osc.enqueueDeployment,
		},
		0,
	)

	ds.VolumeInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    osc.enqueueVolume,
			UpdateFunc: func(old, cur interface{}) { osc.enqueueVolume(cur) },
			DeleteFunc: osc.enqueueVolume,
		},
		0,
	)

	ds.ServiceInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    osc.enqueueService,
			UpdateFunc: func(old, cur interface{}) { osc.enqueueService(cur) },
			DeleteFunc: osc.enqueueService,
		},
		0,
	)

	ds.PersistentVolumeClaimInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    osc.enqueuePVC,
			UpdateFunc: func(old, cur interface{}) { osc.enqueuePVC(cur) },
			DeleteFunc: osc.enqueuePVC,
		},
		0,
	)

	osc.cacheSyncs = append(osc.cacheSyncs, ds.ObjectStoreInformer.HasSynced)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.DeploymentInformer.HasSynced)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.VolumeInformer.HasSynced)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.ServiceInformer.HasSynced)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.PersistentVolumeClaimInformer.HasSynced)

	return osc
}

func (osc *ObjectStoreController) Run(workers int, stopCh <-chan struct{}) {
	osc.logger.Info("starting Longhorn Object Store Controller")
	defer osc.logger.Info("shut down Longhorn Object Store Controller")
	defer osc.queue.ShutDown()

	if !cache.WaitForNamedCacheSync("longhorn object stores", stopCh, osc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(osc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (osc *ObjectStoreController) worker() {
	for osc.processNextWorkItem() {
	}
}

func (osc *ObjectStoreController) processNextWorkItem() bool {
	key, quit := osc.queue.Get()
	if quit {
		return false
	}
	defer osc.queue.Done(key)

	err := osc.reconcile(key.(string))
	if err == nil {
		osc.queue.Forget(key)
		return true
	}
	osc.logger.WithError(err).Errorf("failed to reconcile object store: \"%v\", retrying", err)
	osc.queue.AddRateLimited(key)

	return true
}

func (osc *ObjectStoreController) enqueueObjectStore(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for %v: %v", obj, err))
		return
	}
	osc.queue.Add(key)
}

func (osc *ObjectStoreController) enqueueDeployment(obj interface{}) {
	dpl, ok := obj.(*appsv1.Deployment)
	if !ok {
		deleted, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		dpl, ok = deleted.Obj.(*appsv1.Deployment)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object %#v", deleted.Obj))
			return
		}

	}

	if dpl.Namespace != osc.namespace || len(dpl.ObjectMeta.OwnerReferences) < 1 {
		return // deployment has no owner reference, therefore is not related to an object store
	}
	storeName := dpl.ObjectMeta.OwnerReferences[0].Name
	store, err := osc.ds.GetObjectStoreRO(storeName)
	if err != nil {
		return // deployment has owner reference, but is not owned by an object store
	}
	key, err := cache.MetaNamespaceKeyFunc(store)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object store %v: %v", storeName, err))
		return
	}
	osc.queue.Add(key)
}

func (osc *ObjectStoreController) enqueueVolume(obj interface{}) {
	vol, ok := obj.(*longhorn.Volume)
	if !ok {
		deleted, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		vol, ok = deleted.Obj.(*longhorn.Volume)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object %#v", deleted.Obj))
			return
		}

	}

	// Volume has no owner reference, therefore is not related to an object store
	// or this instance of the longhorn manager is not the one responsible for the
	// volume, therefore it's also not responsible for the object store. No need
	// to queue.
	if len(vol.ObjectMeta.OwnerReferences) < 1 || osc.controllerID != vol.Status.OwnerID {
		return
	}
	pvcName := vol.ObjectMeta.OwnerReferences[0].Name
	pvc, err := osc.ds.GetPersistentVolumeClaimRO(osc.namespace, pvcName)
	if err != nil {
		return
	}

	if len(pvc.ObjectMeta.OwnerReferences) < 1 {
		return // PVC has no owner reference, therefore is not related to an object store
	}
	storeName := pvc.ObjectMeta.OwnerReferences[0].Name
	store, err := osc.ds.GetObjectStoreRO(storeName)
	if err != nil {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(store)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object store %v: %v", storeName, err))
		return
	}
	osc.queue.Add(key)
}

func (osc *ObjectStoreController) enqueueService(obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		deleted, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		svc, ok = deleted.Obj.(*corev1.Service)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object %#v", deleted.Obj))
			return
		}

	}

	// only consider services within the longhorn namespace and which have an
	// owner. All others can not be related to an object store.
	if svc.Namespace != osc.namespace || len(svc.ObjectMeta.OwnerReferences) < 1 {
		return
	}
	storeName := svc.ObjectMeta.OwnerReferences[0].Name
	store, err := osc.ds.GetObjectStoreRO(storeName)
	if err != nil {
		return // service has owner reference, but is not owned by an object store
	}
	key, err := cache.MetaNamespaceKeyFunc(store)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object store %v: %v", storeName, err))
		return
	}
	osc.queue.Add(key)
}

func (osc *ObjectStoreController) enqueuePVC(obj interface{}) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		deleted, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}
		pvc, ok = deleted.Obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object %#v", deleted.Obj))
			return
		}

	}

	// only consider PVCs within the longhorn namespace and which have an
	// owner. All others can not be related to an object store.
	if pvc.Namespace != osc.namespace || len(pvc.ObjectMeta.OwnerReferences) < 1 {
		return
	}
	storeName := pvc.ObjectMeta.OwnerReferences[0].Name
	store, err := osc.ds.GetObjectStoreRO(storeName)
	if err != nil {
		return //  pvc has owner reference, but is not owned by an object store
	}
	key, err := cache.MetaNamespaceKeyFunc(store)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object store %v: %v", storeName, err))
		return
	}
	osc.queue.Add(key)
}

func (osc *ObjectStoreController) reconcile(key string) (err error) {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	store, err := osc.ds.GetObjectStore(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil // already deleted, nothing to do
		}
		return err
	}

	if !osc.isResponsibleFor(store) {
		return nil
	}

	existingStore := store.DeepCopy()
	defer func() {
		if !reflect.DeepEqual(existingStore.Status, store.Status) {
			_, err = osc.ds.UpdateObjectStoreStatus(store)
		}
	}()

	// handle termination
	if !store.DeletionTimestamp.IsZero() {
		if store.Status.State != longhorn.ObjectStoreStateTerminating {
			logrus.Infof("object store %v is now terminating", store.Name)
			store.Status.State = longhorn.ObjectStoreStateTerminating
			return err
		}
		return osc.handleTerminating(store)
	}

	switch store.Status.State {
	case longhorn.ObjectStoreStateStarting, longhorn.ObjectStoreStateError:
		err = osc.handleStarting(store)

	case longhorn.ObjectStoreStateRunning:
		err = osc.handleRunning(store)

	case longhorn.ObjectStoreStateStopping:
		err = osc.handleStopping(store)

	case longhorn.ObjectStoreStateStopped:
		err = osc.handleStopped(store)

	default:
		err = osc.initializeObjectStore(store)
	}

	// set the object state to error if there is an error and the error is not
	// caused by an API server problem like a timeout.
	if err != nil && !datastore.ErrorIsTemporaryAPIError(err) {
		logrus.Errorf("object store %v has error: %v", store.Name, err)
		store.Status.State = longhorn.ObjectStoreStateError
	}
	return err
}

// This function handles the case when the object store is in "Starting"
// state. That means, all resources will be created in the K8s API, and the
// controller has to wait until they are ready and healthy until it can
// transition the object store to "Running" state. This behavior is the same
// as when the object store is in "Error" state, at which point the
// controller can also just wait until the resources are marked healthy again by
// the K8s API. If resources are found to be missing, the controller tries to
// create them.
// To manage their interaction and ensure cleanup when the object store is
// deleted, K8s owner ship relations (aka. owner references) are used with the
// following relations between the K8s objects:
//
// | ┌────────────────┐
// | │ ObjectStore    │
// | └─┬──────────────┘
// |   │
// |   │owns
// |   │
// |   │     ┌───────────────────────┐
// |   ├────►│ Service               │
// |   │     └───────────────────────┘
// |   │
// |   │     ┌───────────────────────┐
// |   ├────►│ optional S3 Ingresses │
// |   │     └───────────────────────┘
// |   │
// |   │     ┌───────────────────────┐
// |   ├────►│ Deployment            ├──────┐
// |   │     └───────────────────────┘      │owns
// |   │                                    ▼
// |   │     ┌───────────────────────┐    ┌────────────────┐
// |   ├────►│ PersistentVolumeClaim │    │ ReplicaSet     │
// |   │     └───────────────────────┘    └─┬──────────────┘
// |   │                                    │owns
// |   │                                    │
// |   │                                    ▼
// |   │     ┌───────────────────────┐    ┌────────────────┐
// |   └────►│ LonghornVolume        │    │ Pod            │
// |         └───────────────────────┘    └────────────────┘
// |                                        ▲
// |                                        │
// |                                        │waits for
// |         ┌───────────────────────┐      │shutdown
// |         │ PersistentVolume      ├──────┘
// |         └───────────────────────┘
//
// From this ownership relationship and the mount dependencies, the order of
// creation of the resources is determined.
func (osc *ObjectStoreController) handleStarting(store *longhorn.ObjectStore) (err error) {
	vol, err := osc.getOrCreateVolume(store)
	if err != nil {
		// since the "owner" controller of an object store is the same one that
		// owns the volume, but we don't track it explicitly, there is a race when
		// creating an object store between multiple controllers. Therefore
		// multiple controllers will try to create the volume and we'll see the
		// error "already exists", but need to just abort without error in that
		// case, because it just means that the volume is now created by another
		// controller and this one isn't responsible.
		if datastore.ErrorIsAlreadyExists(err) {
			return nil
		}
		return errors.Wrap(err, "API error while creating volume")
	}

	if err := osc.checkVolume(vol); err != nil {
		return nil
	}

	pv, err := osc.getOrCreatePV(store, vol)
	if err != nil {
		return errors.Wrap(err, "API error while creating volume")
	}

	pvc, err := osc.getOrCreatePVC(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating pvc")
	}

	if err := osc.checkPV(pv); err != nil {
		return nil
	}

	if err := osc.checkPVC(pvc); err != nil {
		return nil
	}

	dpl, err := osc.getOrCreateDeployment(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating deployment")
	}

	if err := osc.checkDeployment(dpl, store); err != nil {
		return nil
	}

	_, err = osc.getOrCreateService(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating service")
	}

	endpoints, store, err := osc.getOrCreateS3Endpoints(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating S3 ingresses")
	}
	osc.logger.Infof("object store %v has  %v S3 endpoint(s)", store.Name, len(endpoints))
	// if there are no public endpoints, add the implicit cluster-internal one
	if len(store.Status.Endpoints) == 0 {
		store.Status.Endpoints = append(store.Status.Endpoints, fmt.Sprintf("%v.%v.svc", store.Name, osc.namespace))
	}

	logrus.Infof("object store %v is now running", store.Name)
	store.Status.State = longhorn.ObjectStoreStateRunning
	return nil
}

// This function does a short sanity check on the various resources that are
// needed to operate the object stores. If any of them is found to be
// unhealthy, the controller will transition the object store to "Error"
// state, otherwise do nothing.
func (osc *ObjectStoreController) handleRunning(store *longhorn.ObjectStore) (err error) {
	if store.Spec.TargetState == longhorn.ObjectStoreStateStopped {
		logrus.Infof("object store %v is now stopping", store.Name)
		store.Status.State = longhorn.ObjectStoreStateStopping
		return nil
	}

	dpl, err := osc.ds.GetDeployment(store.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to find deployment %v", store.Name)
	} else if err = osc.checkDeployment(dpl, store); err != nil {
		logrus.Errorf("Object Store running but deployment not ready")
		return err
	}

	_, err = osc.ds.GetService(osc.namespace, store.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to find service %v", store.Name)
	}

	vol, err := osc.ds.GetVolume(store.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to find volume %v", store.Name)
	} else if err = osc.checkVolume(vol); err != nil {
		logrus.Errorf("Object Store running but Volume not ready")
		return err
	}

	pv, err := osc.ds.GetPersistentVolume(store.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to find PV %v", store.Name)
	} else if err = osc.checkPV(pv); err != nil {
		logrus.Errorf("Object Store running but PV not ready")
		return err
	}

	pvc, err := osc.ds.GetPersistentVolumeClaim(osc.namespace, store.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to find pvc %v", store.Name)
	} else if err = osc.checkPVC(pvc); err != nil {
		logrus.Errorf("Object Store running but PVC not bound")
		return err
	}

	return nil
}

func (osc *ObjectStoreController) handleStopping(store *longhorn.ObjectStore) (err error) {
	dpl, err := osc.ds.GetDeployment(store.Name)
	if err != nil {
		return errors.Wrap(err, "failed find Deployment to stop")
	} else if (*dpl).Spec.Replicas != nil && *((*dpl).Spec.Replicas) != 0 {
		(*dpl).Spec.Replicas = util.Int32Ptr(0)
		_, err = osc.ds.UpdateDeployment(dpl)
		return err
	} else if dpl.Status.AvailableReplicas > 0 {
		return nil // wait for shutdown
	}

	logrus.Infof("object store %v is now stopped", store.Name)
	store.Status.State = longhorn.ObjectStoreStateStopped
	return nil
}

func (osc *ObjectStoreController) handleStopped(store *longhorn.ObjectStore) (err error) {
	if store.Spec.TargetState == longhorn.ObjectStoreStateRunning {
		logrus.Infof("object store %v is now starting", store.Name)
		store.Status.State = longhorn.ObjectStoreStateStarting
		return nil
	}
	return nil
}

func (osc *ObjectStoreController) handleTerminating(store *longhorn.ObjectStore) (err error) {
	// The resources are created in the order:
	// Volume -> PV -> PVC -> Deployment -> Service
	// so we tear them down in reverse:
	// Service -> Deployment -> PVC -> PV -> Volume
	// Once that is done we can remove the finalizer, which allows the K8s API to
	// remove the object store resource as well.
	if err = osc.ds.DeleteService(osc.namespace, store.Name); err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	if err = osc.ds.DeleteDeployment(store.Name); err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	if err = osc.ds.DeletePersistentVolumeClaim(osc.namespace, store.Name); err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	if err = osc.ds.DeletePersistentVolume(store.Name); err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	if err = osc.ds.DeleteVolume(store.Name); err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	// cleanup all secrets with matching labels.
	labels := types.GetBaseLabelsForSystemManagedComponent()
	labels[types.GetLonghornLabelComponentKey()] = types.LonghornLabelObjectStore
	labels[types.GetLonghornLabelKey(types.LonghornLabelObjectStore)] = store.Name

	secrets, err := osc.ds.ListSecretsByLabels(osc.namespace, labels)
	if err != nil {
		return err
	}

	for _, secret := range secrets {
		osc.ds.DeleteSecret(osc.namespace, secret.Name)
	}

	if len(store.ObjectMeta.Finalizers) != 0 {
		return osc.ds.RemoveFinalizerForObjectStore(store)
	}

	return nil
}

func (osc *ObjectStoreController) initializeObjectStore(store *longhorn.ObjectStore) (err error) {
	if !(store.Spec.TargetState == longhorn.ObjectStoreStateStopped) {
		logrus.Infof("object store %v is now starting", store.Name)
		store.Status.State = longhorn.ObjectStoreStateStarting
	}
	return nil
}

func (osc *ObjectStoreController) getOrCreatePVC(store *longhorn.ObjectStore) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := osc.ds.GetPersistentVolumeClaim(osc.namespace, store.Name)
	if err == nil {
		return pvc, nil
	}

	if datastore.ErrorIsNotFound(err) {
		pvc, err = osc.createPVC(store)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create persistent volume claim")
		}
		return pvc, nil
	}

	return nil, err
}

func (osc *ObjectStoreController) checkPVC(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Status.Phase != corev1.ClaimBound {
		return errors.New(fmt.Sprintf("PVC %v not bound", pvc.Name))
	}
	return nil
}

func (osc *ObjectStoreController) getOrCreateVolume(store *longhorn.ObjectStore) (*longhorn.Volume, error) {
	vol, err := osc.ds.GetVolume(store.Name)
	if err == nil {
		return vol, nil
	}

	if datastore.ErrorIsNotFound(err) {
		vol, err = osc.createVolume(store)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create longhorn volume")
		}
		return vol, nil
	}

	return nil, err
}

func (osc *ObjectStoreController) checkVolume(vol *longhorn.Volume) error {
	if vol.Status.OwnerID == "" {
		return errors.New(fmt.Sprintf("volume %v has no owner", vol.Name))
	}

	if vol.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		return errors.New(fmt.Sprintf("volume %v has failed", vol.Name))
	}

	return nil
}

func (osc *ObjectStoreController) getOrCreatePV(
	store *longhorn.ObjectStore,
	volume *longhorn.Volume,
) (*corev1.PersistentVolume, error) {
	pv, err := osc.ds.GetPersistentVolume(store.Name)
	if err == nil {
		return pv, nil
	}

	if datastore.ErrorIsNotFound(err) {
		pv, err = osc.createPV(store, volume)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create persistent volume")
		}
		return pv, nil
	}

	return nil, err
}

func (osc *ObjectStoreController) checkPV(pv *corev1.PersistentVolume) error {
	if pv.Status.Phase != corev1.VolumeBound {
		return errors.New(fmt.Sprintf("PV %v not bound", pv.Name))
	}
	return nil
}

func (osc *ObjectStoreController) getOrCreateDeployment(store *longhorn.ObjectStore) (*appsv1.Deployment, error) {
	dpl, err := osc.ds.GetDeployment(store.Name)
	if err == nil {
		return dpl, nil
	}

	if datastore.ErrorIsNotFound(err) {
		dpl, err = osc.createDeployment(store)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create deployment")
		}
		return dpl, nil
	}

	return nil, err
}

func (osc *ObjectStoreController) checkDeployment(deployment *appsv1.Deployment, store *longhorn.ObjectStore) error {
	deploymentAsSeen := deployment.DeepCopy()

	if *deployment.Spec.Replicas != 1 {
		deployment.Spec.Replicas = util.Int32Ptr(1)
		return errors.New("deployment just scaled")
	} else if deployment.Status.Replicas == 0 || deployment.Status.UnavailableReplicas > 0 {
		return errors.New("deployment not ready")
	}

	if util.GetImageOfDeploymentContainerWithName(deployment, types.ObjectStoreContainerName) != store.Spec.Image {
		err := util.SetImageOfDeploymentContainerWithName(deployment, types.ObjectStoreContainerName, store.Spec.Image)
		if err != nil {
			return err
		}
		store.Status.State = longhorn.ObjectStoreStateStarting
	}

	if util.GetImageOfDeploymentContainerWithName(deployment, types.ObjectStoreUIContainerName) != store.Spec.UIImage {
		err := util.SetImageOfDeploymentContainerWithName(deployment, types.ObjectStoreUIContainerName, store.Spec.UIImage)
		if err != nil {
			return err
		}
		store.Status.State = longhorn.ObjectStoreStateStarting
	}

	// propagate telemetry setting
	settingEnableTelemetry, err := osc.ds.GetSettingAsBool(types.SettingNameAllowCollectingLonghornUsage)
	if err != nil {
		return errors.Wrapf(err, "failed to get telemetry setting while checking deployment %v", store.Name)
	}

	args := util.GetArgsOfDeploymentContainerWithName(deployment, types.ObjectStoreContainerName)
	telemetryEnabled := true
	for _, arg := range args {
		if arg == "--no-telemetry" {
			telemetryEnabled = false
		}
	}

	if settingEnableTelemetry != telemetryEnabled {
		newArgs, err := osc.getS3gwContainerArgs(store)
		if err != nil {
			return errors.Wrapf(err, "failed to get s3gw arguments for deployment %v", store.Name)
		}
		err = util.SetArgsOfDeploymentContainerWithName(deployment, types.ObjectStoreContainerName, newArgs)
		if err != nil {
			return err
		}
	}

	// if anything has been changed, update deployment
	if !reflect.DeepEqual(deploymentAsSeen, deployment) {
		_, err = osc.ds.UpdateDeployment(deployment)
		if err != nil {
			return err
		}
	}

	return nil
}

func (osc *ObjectStoreController) getOrCreateService(store *longhorn.ObjectStore) (*corev1.Service, error) {
	svc, err := osc.ds.GetService(osc.namespace, store.Name)
	if err == nil {
		return svc, nil
	}

	if datastore.ErrorIsNotFound(err) {
		svc, err = osc.createService(store)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create service")
		}
		return svc, nil
	}

	return nil, err
}

func (osc *ObjectStoreController) getOrCreateS3Endpoints(store *longhorn.ObjectStore) ([]*networkingv1.Ingress, *longhorn.ObjectStore, error) {
	ingresses := []*networkingv1.Ingress{}

	s3backend := networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: store.Name,
			Port: networkingv1.ServiceBackendPort{
				Name: "s3",
			},
		},
	}

	for _, endpoint := range store.Spec.Endpoints {
		name := fmt.Sprintf("%v-%v", store.Name, endpoint.Name)
		ingress, err := osc.ds.GetIngress(osc.namespace, name)
		if err == nil {
			ingresses = append(ingresses, ingress)
		} else if datastore.ErrorIsNotFound(err) {
			baserule := networkingv1.IngressRule{
				Host: endpoint.DomainName,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{
							{
								Path:     "/",
								PathType: util.NetworkingV1PathTypePtr(networkingv1.PathTypePrefix),
								Backend:  s3backend,
							},
						},
					},
				},
			}

			wildcardrule := networkingv1.IngressRule{
				Host: fmt.Sprintf("*.%v", endpoint.DomainName),
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{
							{
								Path:     "/",
								PathType: util.NetworkingV1PathTypePtr(networkingv1.PathTypePrefix),
								Backend:  s3backend,
							},
						},
					},
				},
			}

			ingress := &networkingv1.Ingress{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: osc.namespace,
					Labels:    types.GetObjectStoreLabels(store),
					Annotations: map[string]string{
						types.LonghornAnnotationObjectStoreName: store.Name,
					},
					OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
				},
				Spec: networkingv1.IngressSpec{
					Rules: []networkingv1.IngressRule{
						baserule,
						wildcardrule,
					},
				},
			}

			if endpoint.TLS.Name != "" {
				ingress.Spec.TLS = []networkingv1.IngressTLS{
					{
						SecretName: endpoint.TLS.Name,
						Hosts: []string{
							endpoint.DomainName,
							fmt.Sprintf("*.%v", endpoint.DomainName),
						},
					},
				}
			}

			_, err := osc.ds.CreateIngress(osc.namespace, ingress)
			if err != nil && !datastore.ErrorIsAlreadyExists(err) {
				return []*networkingv1.Ingress{}, store, err
			}

			store.Status.Endpoints = append(store.Status.Endpoints, endpoint.DomainName)
			ingresses = append(ingresses, ingress)
		} else {
			// if there was an api error
			return []*networkingv1.Ingress{}, store, err
		}
	}

	return ingresses, store, nil
}

func (osc *ObjectStoreController) createVolume(
	store *longhorn.ObjectStore,
) (*longhorn.Volume, error) {
	vol := longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      store.Name,
			Namespace: osc.namespace,
			Labels:    types.GetObjectStoreLabels(store),
			Annotations: map[string]string{
				types.LonghornAnnotationObjectStoreName: store.Name,
			},
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: longhorn.VolumeSpec{
			Size:                        resourceAsInt64(store.Spec.Size),
			Frontend:                    longhorn.VolumeFrontendBlockDev,
			AccessMode:                  longhorn.AccessModeReadWriteOnce,
			NumberOfReplicas:            store.Spec.VolumeParameters.NumberOfReplicas,
			ReplicaSoftAntiAffinity:     store.Spec.VolumeParameters.ReplicaSoftAntiAffinity,
			ReplicaZoneSoftAntiAffinity: store.Spec.VolumeParameters.ReplicaZoneSoftAntiAffinity,
			ReplicaDiskSoftAntiAffinity: store.Spec.VolumeParameters.ReplicaDiskSoftAntiAffinity,
			DiskSelector:                store.Spec.VolumeParameters.DiskSelector,
			NodeSelector:                store.Spec.VolumeParameters.NodeSelector,
			DataLocality:                store.Spec.VolumeParameters.DataLocality,
			FromBackup:                  store.Spec.VolumeParameters.FromBackup,
			StaleReplicaTimeout:         store.Spec.VolumeParameters.StaleReplicaTimeout,
			ReplicaAutoBalance:          store.Spec.VolumeParameters.ReplicaAutoBalance,
			RevisionCounterDisabled:     store.Spec.VolumeParameters.RevisionCounterDisabled,
			UnmapMarkSnapChainRemoved:   store.Spec.VolumeParameters.UnmapMarkSnapChainRemoved,
			BackendStoreDriver:          store.Spec.VolumeParameters.BackendStoreDriver,
		},
	}

	return osc.ds.CreateVolume(&vol)
}

func (osc *ObjectStoreController) createPV(
	store *longhorn.ObjectStore,
	volume *longhorn.Volume,
) (*corev1.PersistentVolume, error) {
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   store.Name,
			Labels: types.GetObjectStoreLabels(store),
			Annotations: map[string]string{
				types.LonghornAnnotationObjectStoreName: store.Name,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Capacity: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceStorage: store.Spec.Size.DeepCopy(),
			},
			StorageClassName:              types.ObjectStoreStorageClassName,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			VolumeMode:                    util.CoreV1PersistentVolumeModePtr(corev1.PersistentVolumeFilesystem),
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  osc.namespace,
				Name:       store.Name,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "driver.longhorn.io",
					VolumeHandle: volume.Name,
					FSType:       "xfs", // must be XFS to support reflink
					VolumeAttributes: map[string]string{
						"mkfsParams": "-f -m crc=1 -m reflink=1", // crc needed for reflink
					},
				},
			},
		},
	}

	return osc.ds.CreatePersistentVolume(&pv)
}

func (osc *ObjectStoreController) createPVC(
	store *longhorn.ObjectStore,
) (*corev1.PersistentVolumeClaim, error) {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      store.Name,
			Namespace: osc.namespace,
			Labels:    types.GetObjectStoreLabels(store),
			Annotations: map[string]string{
				types.LonghornAnnotationObjectStoreName: store.Name,
			},
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: store.Spec.Size.DeepCopy(),
				},
			},
			StorageClassName: util.StringPtr(types.ObjectStoreStorageClassName),
			VolumeName:       store.Name,
		},
	}

	return osc.ds.CreatePersistentVolumeClaim(osc.namespace, &pvc)
}

func (osc *ObjectStoreController) createService(store *longhorn.ObjectStore) (*corev1.Service, error) {
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      store.Name,
			Namespace: osc.namespace,
			Labels:    types.GetObjectStoreLabels(store),
			Annotations: map[string]string{
				types.LonghornAnnotationObjectStoreName: store.Name,
			},
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: corev1.ServiceSpec{
			Selector: osc.ds.GetObjectStoreSelectorLabels(store),
			Ports: []corev1.ServicePort{
				{
					Name:       types.ObjectStorePortName,
					Protocol:   "TCP",
					Port:       types.ObjectStoreServicePort, // 80
					TargetPort: intstr.FromInt(types.ObjectStoreContainerPort),
				},
				{
					Name:       types.ObjectStoreUIPortName,
					Protocol:   "TCP",
					Port:       types.ObjectStoreUIServicePort, // 8080
					TargetPort: intstr.FromInt(types.ObjectStoreUIContainerPort),
				},
				{
					Name:       types.ObjectStoreStatusPortName,
					Protocol:   "TCP",
					Port:       types.ObjectStoreStatusServicePort, // 9090
					TargetPort: intstr.FromInt(types.ObjectStoreStatusContainerPort),
				},
			},
		},
	}

	return osc.ds.CreateService(osc.namespace, &svc)
}

func (osc *ObjectStoreController) createDeployment(store *longhorn.ObjectStore) (*appsv1.Deployment, error) {

	secret, err := osc.ds.GetSecret(osc.namespace, store.Spec.Credentials.Name)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return nil, errors.Wrapf(err, "failed to find secret %v", store.Spec.Credentials.Name)
	}

	env := []corev1.EnvFromSource{}
	if secret != nil {
		env = append(env, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: store.Spec.Credentials.Name,
				},
			},
		})
	}

	imagePullPolicy, err := osc.ds.GetSettingImagePullPolicy()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get image pull policy before creating deployment %v", store.Name)
	}

	s3gwContainerArgs, err := osc.getS3gwContainerArgs(store)
	if err != nil {
		return nil, err
	}

	dpl := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      store.Name,
			Namespace: osc.namespace,
			Labels:    types.GetObjectStoreLabels(store),
			Annotations: map[string]string{
				types.LonghornAnnotationObjectStoreName: store.Name,
			},
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: osc.ds.GetObjectStoreSelectorLabels(store),
			},
			// an s3gw instance must have exclusive access to the volume, so we can
			// only spawn one replica (i.e. one s3gw instance) per object-store.
			// Due to the way the struct works, an allocated integer has to be used
			// here and not a constant.
			Replicas: util.Int32Ptr(1),
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RecreateDeploymentStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: osc.ds.GetObjectStoreSelectorLabels(store),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            types.ObjectStoreContainerName,
							Image:           store.Spec.Image,
							ImagePullPolicy: imagePullPolicy,
							Args:            s3gwContainerArgs,
							Ports: []corev1.ContainerPort{
								{
									Name:          types.ObjectStorePortName,
									ContainerPort: types.ObjectStoreContainerPort,
									Protocol:      "TCP",
								},
								{
									Name:          types.ObjectStoreStatusPortName,
									ContainerPort: types.ObjectStoreStatusContainerPort,
									Protocol:      "TCP",
								},
							},
							EnvFrom: env,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      types.ObjectStorePodVolumeName,
									MountPath: "/data",
								},
							},
						},
						{
							Name:            types.ObjectStoreUIContainerName,
							Image:           store.Spec.UIImage,
							ImagePullPolicy: imagePullPolicy,
							Args:            []string{},
							Ports: []corev1.ContainerPort{
								{
									Name:          types.ObjectStoreUIPortName,
									ContainerPort: types.ObjectStoreUIContainerPort,
									Protocol:      "TCP",
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "S3GW_SERVICE_URL",
									Value: fmt.Sprintf("http://127.0.0.1:%v", types.ObjectStoreContainerPort),
								},
								{
									Name:  "S3GW_UI_PATH",
									Value: fmt.Sprintf("/objectstore/%v", store.Name),
								},
								{
									Name:  "S3GW_INSTANCE_ID",
									Value: store.Name,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: types.ObjectStorePodVolumeName,
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: store.Name,
								},
							},
						},
					},
				},
			},
		},
	}

	registrySecretSetting, err := osc.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get registry secret setting for object store deployment")
	}

	if registrySecretSetting.Value != "" {
		dpl.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: registrySecretSetting.Value,
			},
		}
	}

	return osc.ds.CreateDeployment(&dpl)
}

// To avoid multiple longhorn managers acting on the same object store, only the
// instance responsible for the longhorn volume is considered responsible for
// the object store. This of course precludes that the volume has already been
// created.
func (osc *ObjectStoreController) isResponsibleFor(store *longhorn.ObjectStore) bool {
	vol, err := osc.ds.GetVolumeRO(store.Name)
	if err != nil {
		// if there is no volume yet, the first available controller should take over for the
		// time being. If there are no available nodes at all, we'll just take
		// responsibility - but this shouldn't ever happen
		if datastore.ErrorIsNotFound(err) {
			nodes, err := osc.ds.ListNodesSorted()
			if err != nil || len(nodes) == 0 {
				utilruntime.HandleError(fmt.Errorf("failed to find an owner for object store %v: %v", store.Name, err))
			}

			for _, node := range nodes {
				unavailable, err := osc.ds.IsNodeDownOrDeletedOrMissingManager(node.Name)
				if err == nil && !unavailable {
					return osc.controllerID == node.Name
				}
			}
			return true // this shouldn't ever happen
		}
		utilruntime.HandleError(fmt.Errorf("failed to find volume for object store %v: %v", store.Name, err))
		return false
	}

	return osc.controllerID == vol.Status.OwnerID
}

func (osc *ObjectStoreController) getS3gwContainerArgs(store *longhorn.ObjectStore) ([]string, error) {
	s3gwContainerArgs := []string{
		"--id", store.Name,
		"--debug", types.ObjectStoreLogLevel,
		"--with-status",
	}

	enableTelemetry, err := osc.ds.GetSettingAsBool(types.SettingNameAllowCollectingLonghornUsage)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get telemetry setting before creating deployment %v", store.Name)
	}
	if !enableTelemetry {
		s3gwContainerArgs = append(s3gwContainerArgs, "--no-telemetry")
	}

	domainNameArgs := []string{
		"--dns-name",
		fmt.Sprintf("%v.%v.svc", store.Name, osc.namespace),
	}
	for _, endpoint := range store.Spec.Endpoints {
		domainNameArgs = append(domainNameArgs, "--dns-name")
		domainNameArgs = append(domainNameArgs, endpoint.DomainName)
	}
	s3gwContainerArgs = append(s3gwContainerArgs, domainNameArgs...)
	return s3gwContainerArgs, nil
}

func resourceAsInt64(r resource.Quantity) int64 {
	s, _ := r.AsInt64()
	return s
}
