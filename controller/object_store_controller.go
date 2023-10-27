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
)

var (
	ErrObjectStorePVCNotReady        = errors.New("PVC not ready")
	ErrObjectStoreVolumeNotReady     = errors.New("Volume not ready")
	ErrObjectStorePVNotReady         = errors.New("PV not ready")
	ErrObjectStoreDeploymentNotReady = errors.New("Deployment not ready")
	ErrObjectStoreServiceNotReady    = errors.New("Service not ready")
	ErrObjectStoreIngressNotReady    = errors.New("Ingress not ready")
)

type ObjectStoreController struct {
	*baseController

	controllerID string

	namespace string
	ds        *datastore.DataStore
	s3gwImage string
	uiImage   string

	cacheSyncs []cache.InformerSynced
}

func NewObjectStoreController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
	objectStoreImage string,
	objectStoreUIImage string,
) *ObjectStoreController {
	osc := &ObjectStoreController{
		baseController: newBaseController("object-store", logger),
		controllerID:   controllerID,
		namespace:      namespace,
		ds:             ds,
		s3gwImage:      objectStoreImage,
		uiImage:        objectStoreUIImage,
	}

	ds.ObjectStoreInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    osc.enqueueObjectStore,
			UpdateFunc: func(old, cur interface{}) { osc.enqueueObjectStore(cur) },
			DeleteFunc: osc.enqueueObjectStore,
		},
		0,
	)

	ds.DeploymentInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: osc.enqueueDeployment,
		},
		0,
	)

	ds.VolumeInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: osc.enqueueVolume,
		},
		0,
	)

	ds.ServiceInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: osc.enqueueService,
		},
		0,
	)

	osc.cacheSyncs = append(osc.cacheSyncs, ds.ObjectStoreInformer.HasSynced)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.DeploymentInformer.HasSynced)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.VolumeInformer.HasSynced)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.ServiceInformer.HasSynced)

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

	err := osc.syncObjectStore(key.(string))
	if err == nil {
		osc.queue.Forget(key)
		return true
	}
	osc.logger.WithError(err).Errorf("error syncing object store: \"%v\", retrying", err)
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

func (osc *ObjectStoreController) enqueueDeployment(old, cur interface{}) {
	dplKey, err := controller.KeyFunc(cur)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for %v: %v", cur, err))
		return
	}
	_, dplName, err := cache.SplitMetaNamespaceKey(dplKey)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get name from key %v: %v", dplKey, err))
		return
	}
	dpl, err := osc.ds.GetDeployment(dplName)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get deployment %v: %v", dplName, err))
		return
	}
	if len(dpl.ObjectMeta.OwnerReferences) < 1 {
		return // deployment has no owner reference, therefore is not related to an object store
	}
	storeName := dpl.ObjectMeta.OwnerReferences[0].Name
	store, err := osc.ds.GetObjectStore(storeName)
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

func (osc *ObjectStoreController) enqueueVolume(old, cur interface{}) {
	volKey, err := controller.KeyFunc(cur)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for %v: %v", cur, err))
		return
	}
	_, volName, err := cache.SplitMetaNamespaceKey(volKey)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get name from key %v: %v", volKey, err))
		return
	}
	vol, err := osc.ds.GetVolume(volName)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get deployment %v: %v", volName, err))
		return
	}
	if len(vol.ObjectMeta.OwnerReferences) < 1 {
		return // Volume has no owner reference, therefore is not related to an object store
	}
	pvcName := vol.ObjectMeta.OwnerReferences[0].Name
	pvc, err := osc.ds.GetPersistentVolumeClaim(osc.namespace, pvcName)
	if err != nil {
		return
	}

	if len(pvc.ObjectMeta.OwnerReferences) < 1 {
		return // PVC has no owner reference, therefore is not related to an object store
	}
	storeName := pvc.ObjectMeta.OwnerReferences[0].Name
	store, err := osc.ds.GetObjectStore(storeName)
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

func (osc *ObjectStoreController) enqueueService(old, cur interface{}) {
	svcKey, err := controller.KeyFunc(cur)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for %v: %v", cur, err))
		return
	}
	_, svcName, err := cache.SplitMetaNamespaceKey(svcKey)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get name from key %v: %v", svcKey, err))
		return
	}
	svc, err := osc.ds.GetService(osc.namespace, svcName)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get deployment %v: %v", svcName, err))
		return
	}
	if len(svc.ObjectMeta.OwnerReferences) < 1 {
		return // deployment has no owner reference, therefore is not related to an object store
	}
	storeName := svc.ObjectMeta.OwnerReferences[0].Name
	store, err := osc.ds.GetObjectStore(storeName)
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

func (osc *ObjectStoreController) syncObjectStore(key string) error {
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
	} else if !osc.isResponsibleFor(store) {
		return nil
	}

	existingStore := store.DeepCopy()
	defer func() {
		if reflect.DeepEqual(existingStore.Status, store.Status) {
			return
		}
		store, err = osc.ds.UpdateObjectStoreStatus(store)
	}()

	// handle termination
	if !store.DeletionTimestamp.IsZero() {
		if store.Status.State != longhorn.ObjectStoreStateTerminating {
			store.Status.State = longhorn.ObjectStoreStateTerminating
			return err
		}
		return osc.handleTerminating(store)
	}

	switch store.Status.State {
	case longhorn.ObjectStoreStateStarting, longhorn.ObjectStoreStateError:
		return osc.handleStarting(store)

	case longhorn.ObjectStoreStateRunning:
		return osc.handleRunning(store)

	case longhorn.ObjectStoreStateStopping:
		return osc.handleStopping(store)

	case longhorn.ObjectStoreStateStopped:
		return osc.handleStopped(store)

	default:
		return osc.initializeObjectStore(store)
	}
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
// |   ├────►│ UI Ingress            │
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
// |   └────►│ PersistentVolumeClaim │    │ ReplicaSet     │
// |         └─┬─────────────────────┘    └─┬──────────────┘
// |           │owns                        │owns
// |           │                            │
// |           ▼                            ▼
// |         ┌───────────────────────┐    ┌────────────────┐
// |         │ LonghornVolume        │    │ Pod            │
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
	pvc, store, err := osc.getOrCreatePVC(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating pvc")
	}

	vol, store, err := osc.getOrCreateVolume(store, pvc)
	if err != nil {
		return errors.Wrap(err, "API error while creating volume")
	}

	pv, store, err := osc.getOrCreatePV(store, vol)
	if err != nil {
		return errors.Wrap(err, "API error while creating volume")
	}

	dpl, store, err := osc.getOrCreateDeployment(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating deployment")
	}

	svc, store, err := osc.getOrCreateService(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating service")
	}

	ingress, store, err := osc.getOrCreateIngress(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating UI ingress")
	}

	endpoints, store, err := osc.getOrCreateS3Endpoints(store)
	if err != nil {
		return errors.Wrap(err, "API error while creating S3 ingresses")
	}
	osc.logger.Info("created %d S3 endpoint(s)", len(endpoints))

	if err := osc.checkPVC(pvc); errors.Is(err, ErrObjectStorePVCNotReady) {
		return nil
	}

	if err := osc.checkVolume(vol); errors.Is(err, ErrObjectStoreVolumeNotReady) {
		return nil
	}

	if err := osc.checkPV(pv); errors.Is(err, ErrObjectStorePVNotReady) {
		return nil
	}

	if err := osc.checkDeployment(dpl); errors.Is(err, ErrObjectStoreDeploymentNotReady) {
		return nil
	}

	if err := osc.checkService(svc); errors.Is(err, ErrObjectStoreServiceNotReady) {
		return nil
	}

	if err := osc.checkIngress(ingress); errors.Is(err, ErrObjectStoreIngressNotReady) {
		return nil
	}

	store.Status.State = longhorn.ObjectStoreStateRunning
	return nil
}

// This function does a short sanity check on the various resources that are
// needed to operate the object stores. If any of them is found to be
// unhealthy, the controller will transition the object store to "Error"
// state, otherwise do nothing.
func (osc *ObjectStoreController) handleRunning(store *longhorn.ObjectStore) (err error) {
	if store.Spec.TargetState == longhorn.ObjectStoreStateStopped {
		store.Status.State = longhorn.ObjectStoreStateStopping
		return nil
	}

	dpl, err := osc.ds.GetDeployment(store.Name)
	if err != nil || dpl.Status.UnavailableReplicas > 0 {
		store.Status.State = longhorn.ObjectStoreStateError
		return err
	}

	_, err = osc.ds.GetService(osc.namespace, store.Name)
	if err != nil {
		store.Status.State = longhorn.ObjectStoreStateError
		return err
	}

	_, err = osc.ds.GetIngress(osc.namespace, store.Name)
	if err != nil {
		store.Status.State = longhorn.ObjectStoreStateError
		return err
	}

	pvc, err := osc.ds.GetPersistentVolumeClaim(osc.namespace, genPVCName(store))
	if err != nil || pvc.Status.Phase != corev1.ClaimBound {
		store.Status.State = longhorn.ObjectStoreStateError
		return err
	}

	_, err = osc.ds.GetVolume(genPVName(store))
	if err != nil {
		store.Status.State = longhorn.ObjectStoreStateError
		return err
	}

	_, err = osc.ds.GetPersistentVolume(genPVName(store))
	if err != nil {
		store.Status.State = longhorn.ObjectStoreStateError
		return err
	}
	return nil
}

func (osc *ObjectStoreController) handleStopping(store *longhorn.ObjectStore) (err error) {
	dpl, err := osc.ds.GetDeployment(store.Name)
	if err != nil {
	} else if *dpl.Spec.Replicas != 0 {
		dpl.Spec.Replicas = int32Ptr(0)
		_, err = osc.ds.UpdateDeployment(dpl)
		return err
	} else if dpl.Status.AvailableReplicas > 0 {
		return nil // wait for shutdown
	}

	store.Status.State = longhorn.ObjectStoreStateStopped
	return nil
}

func (osc *ObjectStoreController) handleStopped(store *longhorn.ObjectStore) (err error) {
	if store.Spec.TargetState == longhorn.ObjectStoreStateRunning {
		store.Status.State = longhorn.ObjectStoreStateStarting
		return nil
	}
	return nil
}

func (osc *ObjectStoreController) handleTerminating(store *longhorn.ObjectStore) (err error) {
	// Wait for all directly owmed resources to be deleted before removing the finalizer

	_, err = osc.ds.GetService(osc.namespace, store.Name)
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}

	_, err = osc.ds.GetIngress(osc.namespace, store.Name)
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}

	_, err = osc.ds.GetDeployment(store.Name)
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}

	_, err = osc.ds.GetPersistentVolumeClaim(osc.namespace, genPVCName(store))
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}

	_, err = osc.ds.GetPersistentVolume(genPVName(store))
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}

	_, err = osc.ds.GetVolume(genPVName(store))
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}

	return osc.ds.RemoveFinalizerForObjectStore(store)
}

func (osc *ObjectStoreController) initializeObjectStore(store *longhorn.ObjectStore) (err error) {
	if !(store.Spec.TargetState == longhorn.ObjectStoreStateStopped) {
		store.Status.State = longhorn.ObjectStoreStateStarting
	}
	return nil
}

func (osc *ObjectStoreController) getOrCreatePVC(store *longhorn.ObjectStore) (*corev1.PersistentVolumeClaim, *longhorn.ObjectStore, error) {
	pvc, err := osc.ds.GetPersistentVolumeClaim(osc.namespace, genPVCName(store))
	if err == nil {
		return pvc, store, nil
	} else if datastore.ErrorIsNotFound(err) {
		pvc, err = osc.createPVC(store)
		if err != nil {
			store.Status.State = longhorn.ObjectStoreStateError
			return nil, store, errors.Wrap(err, "failed to create persistent volume claim")
		} else if store.Status.State != longhorn.ObjectStoreStateStarting {
			store.Status.State = longhorn.ObjectStoreStateStarting
		}
		return pvc, store, nil
	}
	return nil, store, err
}

func (osc *ObjectStoreController) checkPVC(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Status.Phase != corev1.ClaimBound {
		return ErrObjectStorePVCNotReady
	}
	return nil
}

func (osc *ObjectStoreController) getOrCreateVolume(
	store *longhorn.ObjectStore,
	pvc *corev1.PersistentVolumeClaim,
) (*longhorn.Volume, *longhorn.ObjectStore, error) {
	vol, err := osc.ds.GetVolume(genPVName(store))
	if err == nil {
		return vol, store, nil
	} else if datastore.ErrorIsNotFound(err) {
		vol, err = osc.createVolume(store, pvc)
		if err != nil {
			store.Status.State = longhorn.ObjectStoreStateError
			return nil, store, errors.Wrap(err, "failed to create longhorn volume")
		} else if store.Status.State != longhorn.ObjectStoreStateStarting {
			store.Status.State = longhorn.ObjectStoreStateStarting
		}
		return vol, store, nil
	}
	return nil, store, err
}

func (osc *ObjectStoreController) checkVolume(vol *longhorn.Volume) error {
	return nil
}

func (osc *ObjectStoreController) getOrCreatePV(
	store *longhorn.ObjectStore,
	volume *longhorn.Volume,
) (*corev1.PersistentVolume, *longhorn.ObjectStore, error) {
	pv, err := osc.ds.GetPersistentVolume(genPVName(store))
	if err == nil {
		return pv, store, nil
	} else if datastore.ErrorIsNotFound(err) {
		pv, err = osc.createPV(store, volume)
		if err != nil {
			store.Status.State = longhorn.ObjectStoreStateError
			return nil, store, errors.Wrap(err, "failed to create persistent volume")
		} else if store.Status.State != longhorn.ObjectStoreStateStarting {
			store.Status.State = longhorn.ObjectStoreStateStarting
		}
		return pv, store, nil
	}
	return nil, store, err
}

func (osc *ObjectStoreController) checkPV(pv *corev1.PersistentVolume) error {
	return nil
}

func (osc *ObjectStoreController) getOrCreateDeployment(store *longhorn.ObjectStore) (*appsv1.Deployment, *longhorn.ObjectStore, error) {
	dpl, err := osc.ds.GetDeployment(store.Name)
	if err == nil {
		return dpl, store, nil
	} else if datastore.ErrorIsNotFound(err) {
		dpl, err = osc.createDeployment(store)
		if err != nil {
			store.Status.State = longhorn.ObjectStoreStateError
			return nil, store, errors.Wrap(err, "failed to create deployment")
		} else if store.Status.State != longhorn.ObjectStoreStateStarting {
			store.Status.State = longhorn.ObjectStoreStateStarting
		}
		return dpl, store, nil
	}
	return nil, store, err
}

func (osc *ObjectStoreController) checkDeployment(deployment *appsv1.Deployment) error {
	if *deployment.Spec.Replicas != 1 {
		deployment.Spec.Replicas = int32Ptr(1)
		_, err := osc.ds.UpdateDeployment(deployment)
		return err
	} else if deployment.Status.UnavailableReplicas > 0 {
		return ErrObjectStoreDeploymentNotReady
	}
	return nil
}

func (osc *ObjectStoreController) getOrCreateService(store *longhorn.ObjectStore) (*corev1.Service, *longhorn.ObjectStore, error) {
	svc, err := osc.ds.GetService(osc.namespace, store.Name)
	if err == nil {
		return svc, store, nil
	} else if datastore.ErrorIsNotFound(err) {
		svc, err = osc.createService(store)
		if err != nil {
			store.Status.State = longhorn.ObjectStoreStateError
			return nil, store, errors.Wrap(err, "failed to create service")
		} else if store.Status.State != longhorn.ObjectStoreStateStarting {
			store.Status.State = longhorn.ObjectStoreStateStarting
		}
		return svc, store, nil
	}
	return nil, store, err
}

func (osc *ObjectStoreController) checkService(svc *corev1.Service) error {
	return nil
}

func (osc *ObjectStoreController) getOrCreateIngress(store *longhorn.ObjectStore) (*networkingv1.Ingress, *longhorn.ObjectStore, error) {
	ingress, err := osc.ds.GetIngress(osc.namespace, store.Name)
	if err == nil {
		return ingress, store, nil
	} else if datastore.ErrorIsNotFound(err) {
		ingress, err = osc.createIngress(store)
		if err != nil {
			store.Status.State = longhorn.ObjectStoreStateError
			return nil, store, errors.Wrap(err, "failed to create ingress")
		} else if store.Status.State != longhorn.ObjectStoreStateStarting {
			store.Status.State = longhorn.ObjectStoreStateStarting
		}
		return ingress, store, nil
	}
	return nil, store, err
}

func (osc *ObjectStoreController) checkIngress(ingress *networkingv1.Ingress) error {
	return nil
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
								PathType: func() *networkingv1.PathType { r := networkingv1.PathType(networkingv1.PathTypePrefix); return &r }(),
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
								PathType: func() *networkingv1.PathType { r := networkingv1.PathType(networkingv1.PathTypePrefix); return &r }(),
								Backend:  s3backend,
							},
						},
					},
				},
			}

			ingress := &networkingv1.Ingress{
				ObjectMeta: metav1.ObjectMeta{
					Name:            name,
					Namespace:       osc.namespace,
					Labels:          types.GetObjectStoreLabels(store),
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
			if err != nil {
				store.Status.State = longhorn.ObjectStoreStateError
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
	pvc *corev1.PersistentVolumeClaim,
) (*longhorn.Volume, error) {
	vol := longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      genPVName(store),
			Namespace: osc.namespace,
			Labels:    types.GetObjectStoreLabels(store),
			Annotations: map[string]string{
				types.LonghornAnnotationObjectStoreName: store.Name,
			},
			OwnerReferences: osc.ds.GetOwnerReferencesForPVC(pvc),
		},
		Spec: longhorn.VolumeSpec{
			Size:                        resourceAsInt64(store.Spec.Storage.Size),
			Frontend:                    longhorn.VolumeFrontendBlockDev,
			AccessMode:                  longhorn.AccessModeReadWriteOnce,
			NumberOfReplicas:            store.Spec.Storage.NumberOfReplicas,
			ReplicaSoftAntiAffinity:     store.Spec.Storage.ReplicaSoftAntiAffinity,
			ReplicaZoneSoftAntiAffinity: store.Spec.Storage.ReplicaZoneSoftAntiAffinity,
			ReplicaDiskSoftAntiAffinity: store.Spec.Storage.ReplicaDiskSoftAntiAffinity,
			DiskSelector:                store.Spec.Storage.DiskSelector,
			NodeSelector:                store.Spec.Storage.NodeSelector,
			DataLocality:                store.Spec.Storage.DataLocality,
			FromBackup:                  store.Spec.Storage.FromBackup,
			StaleReplicaTimeout:         store.Spec.Storage.StaleReplicaTimeout,
			ReplicaAutoBalance:          store.Spec.Storage.ReplicaAutoBalance,
			RevisionCounterDisabled:     store.Spec.Storage.RevisionCounterDisabled,
			UnmapMarkSnapChainRemoved:   store.Spec.Storage.UnmapMarkSnapChainRemoved,
			BackendStoreDriver:          store.Spec.Storage.BackendStoreDriver,
		},
	}

	volume, err := osc.ds.CreateVolume(&vol)
	if err != nil && !datastore.ErrorIsAlreadyExists(err) {
		store.Status.State = longhorn.ObjectStoreStateError
		return nil, errors.Wrap(err, "failed to create Longhorn Volume")
	}
	return volume, err
}

func (osc *ObjectStoreController) createPV(
	store *longhorn.ObjectStore,
	volume *longhorn.Volume,
) (*corev1.PersistentVolume, error) {
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   genPVName(store),
			Labels: types.GetObjectStoreLabels(store),
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Capacity: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceStorage: store.Spec.Storage.Size.DeepCopy(),
			},
			StorageClassName:              "",
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			VolumeMode:                    persistentVolumeModePtr(corev1.PersistentVolumeFilesystem),
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  osc.namespace,
				Name:       genPVCName(store),
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
			Name:            genPVCName(store),
			Namespace:       osc.namespace,
			Labels:          types.GetObjectStoreLabels(store),
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: store.Spec.Storage.Size.DeepCopy(),
				},
			},
			StorageClassName: strPtr(""),
			VolumeName:       genPVName(store),
		},
	}

	return osc.ds.CreatePersistentVolumeClaim(osc.namespace, &pvc)
}

func (osc *ObjectStoreController) createService(store *longhorn.ObjectStore) (*corev1.Service, error) {
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            store.Name,
			Namespace:       osc.namespace,
			Labels:          types.GetObjectStoreLabels(store),
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: corev1.ServiceSpec{
			Selector: osc.ds.GetObjectStoreSelectorLabels(store),
			Ports: []corev1.ServicePort{
				{
					Name:       "s3",
					Protocol:   "TCP",
					Port:       types.ObjectStoreServicePort, // 80
					TargetPort: intstr.FromInt(types.ObjectStoreContainerPort),
				},
				{
					Name:       "ui",
					Protocol:   "TCP",
					Port:       types.ObjectStoreUIServicePort, // 8080
					TargetPort: intstr.FromInt(types.ObjectStoreUIContainerPort),
				},
				{
					Name:       "status",
					Protocol:   "TCP",
					Port:       types.ObjectStoreStatusServicePort, // 9090
					TargetPort: intstr.FromInt(types.ObjectStoreStatusContainerPort),
				},
			},
		},
	}

	return osc.ds.CreateService(osc.namespace, &svc)
}

func (osc *ObjectStoreController) createIngress(store *longhorn.ObjectStore) (*networkingv1.Ingress, error) {
	ingress := networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:            store.Name,
			Namespace:       osc.namespace,
			Labels:          types.GetObjectStoreLabels(store),
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     fmt.Sprintf("/%v", store.Name),
									PathType: func() *networkingv1.PathType { r := networkingv1.PathType(networkingv1.PathTypePrefix); return &r }(),
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: store.Name,
											Port: networkingv1.ServiceBackendPort{
												Name: "ui",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	return osc.ds.CreateIngress(osc.namespace, &ingress)
}

func (osc *ObjectStoreController) createDeployment(store *longhorn.ObjectStore) (*appsv1.Deployment, error) {
	registrySecretSetting, err := osc.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get registry secret setting for object store deployment")
	}
	registrySecret := []corev1.LocalObjectReference{
		{
			Name: registrySecretSetting.Value,
		},
	}

	domainNameArgs := []string{
		"--rgw-dns-name",
		fmt.Sprintf("%v.%v.svc", store.Name, osc.namespace),
	}
	for _, endpoint := range store.Spec.Endpoints {
		domainNameArgs = append(domainNameArgs, "--rgw-dns-name")
		domainNameArgs = append(domainNameArgs, endpoint.DomainName)
	}

	dpl := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            store.Name,
			Namespace:       osc.namespace,
			Labels:          types.GetObjectStoreLabels(store),
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
			Replicas: int32Ptr(1),
			Strategy: appsv1.DeploymentStrategy{
				Type: "Recreate",
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: osc.ds.GetObjectStoreSelectorLabels(store),
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: registrySecret,
					Containers: []corev1.Container{
						{
							Name:  "s3gw",
							Image: osc.getObjectStoreImage(store),
							Args: append([]string{
								"--rgw-backend-store", "sfs",
								"--rgw_frontends", fmt.Sprintf(
									"beast port=%d, status port=%d",
									types.ObjectStoreContainerPort,
									types.ObjectStoreStatusContainerPort,
								),
							}, domainNameArgs...),
							Ports: []corev1.ContainerPort{
								{
									Name:          "s3",
									ContainerPort: types.ObjectStoreContainerPort,
									Protocol:      "TCP",
								},
								{
									Name:          "status",
									ContainerPort: types.ObjectStoreStatusContainerPort,
									Protocol:      "TCP",
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: store.Spec.Credentials.Name,
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      genVolumeMountName(store),
									MountPath: "/data",
								},
							},
						},
						{
							Name:  "s3gw-ui",
							Image: osc.getObjectStoreUIImage(store),
							Args:  []string{},
							Ports: []corev1.ContainerPort{
								{
									Name:          "ui",
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
									Name:  "S3GW_UI_LOCATION",
									Value: fmt.Sprintf("/%v", store.Name),
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: genVolumeMountName(store),
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: genPVCName(store),
								},
							},
						},
					},
				},
			},
		},
	}

	return osc.ds.CreateDeployment(&dpl)
}

func (osc *ObjectStoreController) isResponsibleFor(store *longhorn.ObjectStore) bool {
	vol, err := osc.ds.GetVolumeRO(genPVName(store))
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to find volume for object store %v: %v", store.Name, err))
		return true
	}

	return osc.controllerID == vol.Status.OwnerID
}

func (osc *ObjectStoreController) getObjectStoreImage(store *longhorn.ObjectStore) string {
	if store.Spec.Image != "" {
		return store.Spec.Image
	}
	return osc.s3gwImage
}

func (osc *ObjectStoreController) getObjectStoreUIImage(store *longhorn.ObjectStore) string {
	if store.Spec.UiImage != "" {
		return store.Spec.UiImage
	}
	return osc.uiImage
}

func genPVName(store *longhorn.ObjectStore) string {
	return fmt.Sprintf("pv-%s", store.Name)
}

func genPVCName(store *longhorn.ObjectStore) string {
	return fmt.Sprintf("pvc-%s", store.Name)
}

func genVolumeMountName(store *longhorn.ObjectStore) string {
	return fmt.Sprintf("%s-data", store.Name)
}

func int32Ptr(i int32) *int32 {
	r := int32(i)
	return &r
}

func strPtr(s string) *string {
	r := string(s)
	return &r
}

func persistentVolumeModePtr(mode corev1.PersistentVolumeMode) *corev1.PersistentVolumeMode {
	m := corev1.PersistentVolumeMode(mode)
	return &m
}

func resourceAsInt64(r resource.Quantity) int64 {
	s, _ := r.AsInt64()
	return s
}
