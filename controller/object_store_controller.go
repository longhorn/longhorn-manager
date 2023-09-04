package controller

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

type ObjectStoreController struct {
	*baseController

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
		namespace:      namespace,
		ds:             ds,
		s3gwImage:      objectStoreImage,
		uiImage:        objectStoreUIImage,
	}

	ds.ObjectStoreInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    osc.enqueueObjectStore,
			UpdateFunc: func(old, cur interface{}) { osc.enqueueObjectStore(cur) },
			DeleteFunc: osc.enqueueObjectStore,
		},
	)
	osc.cacheSyncs = append(osc.cacheSyncs, ds.ObjectStoreInformer.HasSynced)

	return osc
}

func (osc *ObjectStoreController) Run(workers int, stopCh <-chan struct{}) {
	osc.logger.Info("Starting Longhorn Object Store Controller")
	defer osc.logger.Info("Shut down Longhorn Object Store Controller")
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
	} else if osc.queue.NumRequeues(key) < maxRetries {
		osc.logger.WithError(err).Errorf("error syncing object store %v, retrying", err)
		osc.queue.AddRateLimited(key)
	} else {
		utilruntime.HandleError(err)
		osc.logger.WithError(err).Errorf("error syncing object store %v, giving up", err)
		osc.queue.Forget(key)
	}

	return true
}

func (osc *ObjectStoreController) enqueueObjectStore(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Failed to get key for %v: %v", obj, err))
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
	}

	if !store.DeletionTimestamp.IsZero() && store.Status.State != longhorn.ObjectStoreStateTerminating {
		store, err = osc.setObjectStoreState(store, longhorn.ObjectStoreStateTerminating)
		if err != nil {
			return err
		}
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

	case longhorn.ObjectStoreStateTerminating:
		return osc.handleTerminating(store)

	default:
		return osc.handleUnknown(store)
	}
}

// This function handles the case when the object store is in "Starting"
// state. That means, all resources have been created in the K8s API, but the
// controller has to wait until they are ready and healthy until it can
// transition the object store to "Running" state. This behavior is the same
// as when the object store is in "Error" state, at which point the
// controller can also just wait until the resources are marked healthy again by
// the K8s API. If resources are found to be missing, the controller tries to
// create them.
func (osc *ObjectStoreController) handleStarting(store *longhorn.ObjectStore) (err error) {
	pvc, err := osc.ds.GetPersistentVolumeClaim(osc.namespace, genPVCName(store))
	if err != nil && datastore.ErrorIsNotFound(err) {
		return osc.createResources(store)
	} else if err != nil {
		return errors.Wrap(err, "API error while observing pvc")
	} else if pvc.Status.Phase != corev1.ClaimBound {
		return nil
	}

	_, err = osc.ds.GetVolume(store.Name)
	if err != nil && datastore.ErrorIsNotFound(err) {
		return osc.createResources(store)
	} else if err != nil {
		return errors.Wrap(err, "API error while observing volume")
	}

	_, err = osc.ds.GetPersistentVolume(genPVName(store))
	if err != nil && datastore.ErrorIsNotFound(err) {
		osc.createResources(store)
	} else if err != nil {
		return errors.Wrap(err, "API error while observing pv")
	}

	dpl, err := osc.ds.GetDeployment(store.Name)
	if err != nil && datastore.ErrorIsNotFound(err) {
		return osc.createResources(store)
	} else if err != nil {
		return errors.Wrap(err, "API error while observing deployment")
	} else if *dpl.Spec.Replicas != 1 {
		dpl.Spec.Replicas = int32Ptr(1)
		_, err = osc.ds.UpdateDeployment(dpl)
		return err
	} else if dpl.Status.UnavailableReplicas > 0 {
		return nil
	}

	_, err = osc.ds.GetSecret(osc.namespace, store.Name)
	if err != nil && datastore.ErrorIsNotFound(err) {
		return osc.createResources(store)
	} else if err != nil {
		return errors.Wrap(err, "API error while observing secret")
	}

	_, err = osc.ds.GetService(osc.namespace, store.Name)
	if err != nil && datastore.ErrorIsNotFound(err) {
		return osc.createResources(store)
	} else if err != nil {
		return errors.Wrap(err, "API error while observing service")
	}

	localEndpoint := *osc.getLocalEndpointURL(store)
	store.Status.Endpoints = append(store.Status.Endpoints, localEndpoint.DomainName)
	store, err = osc.setObjectStoreState(store, longhorn.ObjectStoreStateRunning)
	if err != nil {
		return err
	}
	return nil
}

// This function does a short sanity check on the various resources that are
// needed to operate the object stores. If any of them is found to be
// unhealthy, the controller will transition the object store to "Error"
// state, otherwise do nothing.
func (osc *ObjectStoreController) handleRunning(store *longhorn.ObjectStore) (err error) {
	if store.Spec.TargetState == longhorn.ObjectStoreStateStopped {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStopping)
		return nil
	}

	dpl, err := osc.ds.GetDeployment(store.Name)
	if err != nil || dpl.Status.UnavailableReplicas > 0 {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	}

	_, err = osc.ds.GetSecret(osc.namespace, store.Name)
	if err != nil {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	}

	_, err = osc.ds.GetService(osc.namespace, store.Name)
	if err != nil {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	}

	pvc, err := osc.ds.GetPersistentVolumeClaim(osc.namespace, genPVCName(store))
	if err != nil || pvc.Status.Phase != corev1.ClaimBound {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	}

	_, err = osc.ds.GetVolume(store.Name)
	if err != nil {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	}

	_, err = osc.ds.GetPersistentVolume(genPVName(store))
	if err != nil {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
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

	osc.setObjectStoreState(store, longhorn.ObjectStoreStateStopped)
	return nil
}

func (osc *ObjectStoreController) handleStopped(store *longhorn.ObjectStore) (err error) {
	if store.Spec.TargetState == longhorn.ObjectStoreStateRunning {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStarting)
		return nil
	}
	return nil
}

// The controller transitions the object store to "Terminating" state, when
// deletion is requested in the API. While in "Terminating" state, the controller
// observes the resources that make up the object store until they are
// removed. At that point, the object store resource itself is allowed to be
// removed as well, by removing the finalizer.
// This ensures that while there are resources remaining in the
// cluster, the controller will keep knowledge of the object store and
// communicate back if errors occur.
func (osc *ObjectStoreController) handleTerminating(store *longhorn.ObjectStore) (err error) {
	_, err = osc.ds.GetDeployment(store.Name)
	if err == nil {
		return nil
	} else if !datastore.ErrorIsNotFound(err) {
		return errors.Wrap(err, "API error while waiting on deployment shutdown")
	}
	_, err = osc.ds.GetSecret(osc.namespace, store.Name)
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}
	_, err = osc.ds.GetService(osc.namespace, store.Name)
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
	err = osc.ds.RemoveFinalizerForObjectStore(store)
	if err != nil {
		store, err = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	}
	return nil
}

func (osc *ObjectStoreController) handleUnknown(store *longhorn.ObjectStore) (err error) {
	err = osc.initializeObjectStore(store)
	err = osc.createResources(store)
	return err
}

func (osc *ObjectStoreController) setObjectStoreState(
	store *longhorn.ObjectStore,
	status longhorn.ObjectStoreState,
) (*longhorn.ObjectStore, error) {
	store.Status.State = status
	return osc.ds.UpdateObjectStoreStatus(store)
}

func (osc *ObjectStoreController) initializeObjectStore(store *longhorn.ObjectStore) (err error) {
	// TODO: check if we really need/want to update the spec's image here
	store.Spec.Image = osc.s3gwImage
	store.Spec.UiImage = osc.uiImage
	store.Spec.TargetState = longhorn.ObjectStoreStateRunning
	_, err = osc.ds.UpdateObjectStore(store)

	return err
}

// createResources, as the name suggests tries to create the various resources
// that implement the object store in the K8s API. To manage their
// interaction and ensure cleanup when the object store is deleted, K8s owner
// ship relations (aka. owner references) are used with the following relations
// between the K8s objects:
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
// |   ├────►│ Secret                │
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
// |         └─┬─────────────────────┘    └────────────────┘
// |           │owns                        ▲
// |           │                            │
// |           ▼                            │waits for
// |         ┌───────────────────────┐      │shutdown
// |         │ PersistentVolume      ├──────┘
// |         └───────────────────────┘
//
// From this ownership relationship and the mount dependencies, the order of
// creation of the resources is determined.
func (osc *ObjectStoreController) createResources(store *longhorn.ObjectStore) error {
	pvc, err := osc.createPVC(store)
	if err != nil && !datastore.ErrorIsAlreadyExists(err) {
		store, _ = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return errors.Wrap(err, "failed to create persisten volume claim")
	} else if store.Status.State != longhorn.ObjectStoreStateStarting {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStarting)
	}

	vol, err := osc.createVolume(store, pvc)
	if err != nil && !datastore.ErrorIsAlreadyExists(err) {
		store, _ = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return errors.Wrap(err, "failed to create Longhorn Volume")
	} else if store.Status.State != longhorn.ObjectStoreStateStarting {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStarting)
	}

	// A PV is explicitly created because we need to ensure that the filesystem is
	// XFS to make use of the reflink (aka. copy-on-write) feature. This allows
	// assembly of multipart uploads without temporary storage overhead.
	// The PV created here can only be bount by the PVC created above. The PVC
	// above can also only bind to this PV. That is ensure by the `VolumeName`
	// property in the PVC and the `ClaimRef` property in the PV respectively.
	_, err = osc.createPV(store, vol)
	if err != nil && !datastore.ErrorIsAlreadyExists(err) {
		store, _ = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return errors.Wrap(err, "failed to create persisten volume")
	} else if store.Status.State != longhorn.ObjectStoreStateStarting {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStarting)
	}

	err = osc.createSVC(store)
	if err != nil && !datastore.ErrorIsAlreadyExists(err) {
		store, _ = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	} else if store.Status.State != longhorn.ObjectStoreStateStarting {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStarting)
	}

	err = osc.createSecret(store)
	if err != nil && !datastore.ErrorIsAlreadyExists(err) {
		store, _ = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	} else if store.Status.State != longhorn.ObjectStoreStateStarting {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStarting)
	}

	err = osc.createDeployment(store)
	if err != nil && !datastore.ErrorIsAlreadyExists(err) {
		store, _ = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return err
	} else if store.Status.State != longhorn.ObjectStoreStateStarting {
		osc.setObjectStoreState(store, longhorn.ObjectStoreStateStarting)
	}

	return err
}

func (osc *ObjectStoreController) createVolume(
	store *longhorn.ObjectStore,
	pvc *corev1.PersistentVolumeClaim,
) (*longhorn.Volume, error) {
	vol := longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      store.Name,
			Namespace: osc.namespace,
			Labels:    osc.ds.GetObjectStoreLabels(store),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       "PersistentVolumeClaim",
					Name:       pvc.Name,
					UID:        pvc.UID,
				},
			},
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
		store, _ = osc.setObjectStoreState(store, longhorn.ObjectStoreStateError)
		return nil, errors.Wrap(err, "failed to create Longhorn Volume")
	}
	return volume, err
}

func (osc *ObjectStoreController) createPV(
	store *longhorn.ObjectStore,
	volume *longhorn.Volume,
) (*corev1.PersistentVolume, error) {
	blockOwnerDeletion := true
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   genPVName(store),
			Labels: osc.ds.GetObjectStoreLabels(store),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         longhorn.SchemeGroupVersion.String(),
					Kind:               types.LonghornKindVolume,
					Name:               volume.Name,
					UID:                volume.UID,
					BlockOwnerDeletion: &blockOwnerDeletion,
				},
			},
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
			VolumeMode: func() *corev1.PersistentVolumeMode {
				mode := corev1.PersistentVolumeMode(corev1.PersistentVolumeFilesystem)
				return &mode
			}(),
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
			Labels:          osc.ds.GetObjectStoreLabels(store),
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

func (osc *ObjectStoreController) createSVC(store *longhorn.ObjectStore) error {
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            store.Name,
			Namespace:       osc.namespace,
			Labels:          osc.ds.GetObjectStoreLabels(store),
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		Spec: corev1.ServiceSpec{
			Selector: osc.ds.GetObjectStoreSelectorLabels(store),
			Ports: []corev1.ServicePort{
				{
					Name:     "s3",
					Protocol: "TCP",
					Port:     types.ObjectStoreServicePort, // 80
					TargetPort: intstr.IntOrString{
						IntVal: types.ObjectStoreContainerPort, // 7480
					},
				},
				{
					Name:     "ui",
					Protocol: "TCP",
					Port:     types.ObjectStoreUIServicePort, // 8080
					TargetPort: intstr.IntOrString{
						IntVal: types.ObjectStoreUIContainerPort, // 8080
					},
				},
			},
		},
	}

	_, err := osc.ds.CreateService(osc.namespace, &svc)
	return err
}

func (osc *ObjectStoreController) createSecret(store *longhorn.ObjectStore) error {
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            store.Name,
			Namespace:       osc.namespace,
			Labels:          osc.ds.GetObjectStoreLabels(store),
			OwnerReferences: osc.ds.GetOwnerReferencesForObjectStore(store),
		},
		StringData: map[string]string{
			"RGW_DEFAULT_USER_ACCESS_KEY": store.Spec.Credentials.AccessKey,
			"RGW_DEFAULT_USER_SECRET_KEY": store.Spec.Credentials.SecretKey,
		},
	}

	_, err := osc.ds.CreateSecret(osc.namespace, &secret)
	return err
}

func (osc *ObjectStoreController) createDeployment(store *longhorn.ObjectStore) error {
	registrySecretSetting, err := osc.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return errors.Wrap(err, "failed to get registry secret setting for object store deployment")
	}
	registrySecret := []corev1.LocalObjectReference{
		{
			Name: registrySecretSetting.Value,
		},
	}

	dpl := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            store.Name,
			Namespace:       osc.namespace,
			Labels:          osc.ds.GetObjectStoreLabels(store),
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
							Image: store.Spec.Image,
							Args: []string{
								"--rgw-dns-name", (*osc.getLocalEndpointURL(store)).DomainName,
								"--rgw-backend-store", "sfs",
								"--rgw_frontends", fmt.Sprintf("beast port=%d", types.ObjectStoreContainerPort),
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "s3",
									ContainerPort: types.ObjectStoreContainerPort,
									Protocol:      "TCP",
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: store.Name,
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
							Image: store.Spec.UiImage,
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
									Name:  "S3GW_SERVICE_ADDRESS",
									Value: (*osc.getLocalEndpointURL(store)).DomainName,
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

	_, err = osc.ds.CreateDeployment(&dpl)
	return err
}

// getLocalEndpointURL generates the URL of the namespace-local endpoint for the
// object store. This endpoint is implicit
func (osc *ObjectStoreController) getLocalEndpointURL(store *longhorn.ObjectStore) *longhorn.ObjectStoreEndpointSpec {
	return &longhorn.ObjectStoreEndpointSpec{
		Name:       "local",
		DomainName: fmt.Sprintf("%s.%s.svc", store.Name, osc.namespace),
	}
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

func resourceAsInt64(r resource.Quantity) int64 {
	s, _ := r.AsInt64()
	return s
}
