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

type ObjectEndpointController struct {
	*baseController

	namespace string
	ds        *datastore.DataStore
	image     string
}

func NewObjectEndpointController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
	objectEndpointImage string,
) *ObjectEndpointController {
	oec := &ObjectEndpointController{
		baseController: newBaseController("object-endpoint", logger),
		namespace:      namespace,
		ds:             ds,
		image:          objectEndpointImage,
	}

	ds.ObjectEndpointInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    oec.enqueueObjectEndpoint,
			UpdateFunc: func(old, cur interface{}) { oec.enqueueObjectEndpoint(cur) },
			DeleteFunc: oec.enqueueObjectEndpoint,
		},
	)

	return oec
}

func (oec *ObjectEndpointController) Run(workers int, stopCh <-chan struct{}) {
	oec.logger.Info("Starting Longhorn Object Endpoint Controller")
	defer oec.logger.Info("Shut down Longhorn Object Endpoint Controller")
	defer oec.queue.ShutDown()

	for i := 0; i < workers; i++ {
		go wait.Until(oec.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (oec *ObjectEndpointController) worker() {
	for oec.processNextWorkItem() {
	}
}

func (oec *ObjectEndpointController) processNextWorkItem() bool {
	key, quit := oec.queue.Get()
	if quit {
		return false
	}
	defer oec.queue.Done(key)

	err := oec.syncObjectEndpoint(key.(string))
	if err == nil {
		oec.queue.Forget(key)
	} else if oec.queue.NumRequeues(key) < maxRetries {
		oec.logger.WithError(err).Errorf("error syncing object endpoint %v, retrying", err)
		oec.queue.AddRateLimited(key)
	} else {
		utilruntime.HandleError(err)
		oec.logger.WithError(err).Errorf("error syncing object endpoint %v, giving up", err)
		oec.queue.Forget(key)
	}

	return true
}

func (oec *ObjectEndpointController) enqueueObjectEndpoint(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Failed to get key for %v: %v", obj, err))
		return
	}
	oec.queue.Add(key)
}

func (oec *ObjectEndpointController) syncObjectEndpoint(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	endpoint, err := oec.ds.GetObjectEndpoint(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil // already deleted, nothing to do
		}
		return err
	}

	if !endpoint.DeletionTimestamp.IsZero() && endpoint.Status.State != longhorn.ObjectEndpointStateStopping {
		endpoint, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateStopping)
		if err != nil {
			return err
		}
	}

	switch endpoint.Status.State {
	case longhorn.ObjectEndpointStateStarting, longhorn.ObjectEndpointStateError:
		return oec.handleStarting(endpoint)

	case longhorn.ObjectEndpointStateRunning:
		return oec.handleRunning(endpoint)

	case longhorn.ObjectEndpointStateStopping:
		return oec.handleStopping(endpoint)

	default:
		return oec.createResources(endpoint)
	}
}

// This function handles the case when the object endpoint is in "Starting"
// state. That means, all resources have been created in the K8s API, but the
// controller has to wait until they are ready and healthy until it can
// transition the object endpoint to "Running" state. This behavior is the same
// as when the object endpoint is in "Error" state, at which point the
// controller can also just wait until the resources are marked healthy again by
// the K8s API.
func (oec *ObjectEndpointController) handleStarting(endpoint *longhorn.ObjectEndpoint) (err error) {
	dpl, err := oec.ds.GetDeployment(endpoint.Name)
	if err != nil {
		return err
	}
	if dpl.Status.UnavailableReplicas > 0 {
		return nil
	}

	_, err = oec.ds.GetSecret(oec.namespace, endpoint.Name)
	if err != nil {
		return err
	}

	_, err = oec.ds.GetService(oec.namespace, endpoint.Name)
	if err != nil {
		return err
	}

	pvc, err := oec.ds.GetPersistentVolumeClaim(oec.namespace, genPVCName(endpoint))
	if err != nil {
		return err
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return nil
	}

	_, err = oec.ds.GetPersistentVolume(genPVName(endpoint))
	if err != nil {
		return err
	}

	endpoint.Status.Endpoint = fmt.Sprintf("%s.%s.svc", endpoint.Name, oec.namespace)
	endpoint, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateRunning)
	if err != nil {
		return err
	}
	return nil
}

// This function does a short sanity check on the various resources that are
// needed to operate the object endpoint. If any of them is found to be
// unhealthy, the controller will transition the object endpoint to "Error"
// state, otherwise do nothing.
func (oec *ObjectEndpointController) handleRunning(endpoint *longhorn.ObjectEndpoint) (err error) {
	dpl, err := oec.ds.GetDeployment(endpoint.Name)
	if err != nil || dpl.Status.UnavailableReplicas > 0 {
		_, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}

	_, err = oec.ds.GetSecret(oec.namespace, endpoint.Name)
	if err != nil {
		_, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}

	_, err = oec.ds.GetService(oec.namespace, endpoint.Name)
	if err != nil {
		_, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}

	pvc, err := oec.ds.GetPersistentVolumeClaim(oec.namespace, genPVCName(endpoint))
	if err != nil || pvc.Status.Phase != corev1.ClaimBound {
		_, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}

	_, err = oec.ds.GetPersistentVolume(genPVName(endpoint))
	if err != nil {
		_, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}
	return nil
}

// The controller transitions the object endpoint to "Stopping" state, when
// deletion is requested in the API. While in "Stopping" state, the controller
// observes the resources that make up the object endpoint until they are
// removed. At that point, the object endpoint resource itself is allowed to be
// removed as well. This ensures that while there are resources remaining in the
// cluster, the controller will keep knowledge of the object endpoint and
// communicate back if errors occur.
func (oec *ObjectEndpointController) handleStopping(endpoint *longhorn.ObjectEndpoint) (err error) {
	_, err = oec.ds.GetDeployment(endpoint.Name)
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}
	_, err = oec.ds.GetSecret(oec.namespace, endpoint.Name)
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}
	_, err = oec.ds.GetService(oec.namespace, endpoint.Name)
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}
	_, err = oec.ds.GetPersistentVolumeClaim(oec.namespace, genPVCName(endpoint))
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}
	_, err = oec.ds.GetPersistentVolume(genPVName(endpoint))
	if err == nil || !datastore.ErrorIsNotFound(err) {
		return err
	}
	err = oec.ds.RemoveFinalizerForObjectEndpoint(endpoint)
	if err != nil {
		endpoint, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}
	return nil
}

func (oec *ObjectEndpointController) setObjectEndpointState(endpoint *longhorn.ObjectEndpoint, status longhorn.ObjectEndpointState) (*longhorn.ObjectEndpoint, error) {
	endpoint.Status.State = status
	return oec.ds.UpdateObjectEndpointStatus(endpoint)
}

func (oec *ObjectEndpointController) createResources(endpoint *longhorn.ObjectEndpoint) (err error) {
	if err = oec.createVolume(endpoint); err != nil && !datastore.ErrorIsAlreadyExists(err) {
		endpoint, _ = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return errors.Wrap(err, "failed to create Longhorn Volume")
	}

	// A PV is explicitly created because we need to ensure that the filesystem is
	// XFS to make use of the reflink (aka. copy-on-write) feature. This allows
	// assembly of multipart uploads without temporary storage overhead.
	// The PV created here can only be bount by the PVC created above. The PVC
	// above can also only bind to this PV. That is ensure by the `VolumeName`
	// property in the PVC and the `ClaimRef` property in the PV respectively.
	if err = oec.createPV(endpoint); err != nil && !datastore.ErrorIsAlreadyExists(err) {
		endpoint, _ = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return errors.Wrap(err, "failed to create persisten volume")
	}

	if err = oec.createPVC(endpoint); err != nil && !datastore.ErrorIsAlreadyExists(err) {
		endpoint, _ = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return errors.Wrap(err, "failed to create persisten volume claim")
	}

	if err = oec.createSVC(endpoint); err != nil && !datastore.ErrorIsAlreadyExists(err) {
		endpoint, _ = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}

	if err = oec.createSecret(endpoint); err != nil && !datastore.ErrorIsAlreadyExists(err) {
		endpoint, _ = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}

	if err = oec.createDeployment(endpoint); err != nil && !datastore.ErrorIsAlreadyExists(err) {
		endpoint, _ = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateError)
		return err
	}

	endpoint, err = oec.setObjectEndpointState(endpoint, longhorn.ObjectEndpointStateStarting)
	return err
}

func (oec *ObjectEndpointController) createVolume(endpoint *longhorn.ObjectEndpoint) error {
	vol := longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            endpoint.Name,
			Namespace:       oec.namespace,
			Labels:          oec.ds.GetObjectEndpointLabels(endpoint),
			OwnerReferences: oec.ds.GetOwnerReferencesForObjectEndpoint(endpoint),
		},
		Spec: longhorn.VolumeSpec{
			Size:       func() int64 { s, _ := endpoint.Spec.Size.AsInt64(); return s }(),
			Frontend:   longhorn.VolumeFrontendBlockDev,
			AccessMode: longhorn.AccessModeReadWriteOnce,
		},
	}

	_, err := oec.ds.CreateVolume(&vol)
	return err
}

func (oec *ObjectEndpointController) createPV(endpoint *longhorn.ObjectEndpoint) error {
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            genPVName(endpoint),
			Labels:          oec.ds.GetObjectEndpointLabels(endpoint),
			OwnerReferences: oec.ds.GetOwnerReferencesForObjectEndpoint(endpoint),
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Capacity: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceStorage: endpoint.Spec.Size.DeepCopy(),
			},
			StorageClassName:              endpoint.Spec.StorageClass,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			VolumeMode: func() *corev1.PersistentVolumeMode {
				mode := corev1.PersistentVolumeMode(corev1.PersistentVolumeFilesystem)
				return &mode
			}(),
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Namespace:  oec.namespace,
				Name:       genPVCName(endpoint),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "driver.longhorn.io",
					VolumeHandle: endpoint.Name,
					FSType:       "xfs", // must be XFS to support reflink
					VolumeAttributes: map[string]string{
						"mkfsParams": "-f -m crc=1 -m reflink=1", // crc needed for reflink
					},
				},
			},
		},
	}

	_, err := oec.ds.CreatePersistentVolume(&pv)
	return err
}

func (oec *ObjectEndpointController) createPVC(endpoint *longhorn.ObjectEndpoint) error {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            genPVCName(endpoint),
			Namespace:       oec.namespace,
			Labels:          oec.ds.GetObjectEndpointLabels(endpoint),
			OwnerReferences: oec.ds.GetOwnerReferencesForObjectEndpoint(endpoint),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: endpoint.Spec.Size.DeepCopy(),
				},
			},
			StorageClassName: &endpoint.Spec.StorageClass,
			VolumeName:       genPVName(endpoint),
		},
	}

	_, err := oec.ds.CreatePersistentVolumeClaim(oec.namespace, &pvc)
	return err
}

func (oec *ObjectEndpointController) createSVC(endpoint *longhorn.ObjectEndpoint) error {
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            endpoint.Name,
			Namespace:       oec.namespace,
			Labels:          oec.ds.GetObjectEndpointLabels(endpoint),
			OwnerReferences: oec.ds.GetOwnerReferencesForObjectEndpoint(endpoint),
		},
		Spec: corev1.ServiceSpec{
			Selector: oec.ds.GetObjectEndpointSelectorLabels(endpoint),
			Ports: []corev1.ServicePort{
				{
					Name:     "s3",
					Protocol: "TCP",
					Port:     types.ObjectEndpointServicePort,
					TargetPort: intstr.IntOrString{
						IntVal: types.ObjectEndpointContainerPort,
					},
				},
			},
		},
	}

	_, err := oec.ds.CreateService(oec.namespace, &svc)
	return err
}

func (oec *ObjectEndpointController) createSecret(endpoint *longhorn.ObjectEndpoint) error {
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            endpoint.Name,
			Namespace:       oec.namespace,
			Labels:          oec.ds.GetObjectEndpointLabels(endpoint),
			OwnerReferences: oec.ds.GetOwnerReferencesForObjectEndpoint(endpoint),
		},
		StringData: map[string]string{
			"RGW_DEFAULT_USER_ACCESS_KEY": endpoint.Spec.Credentials.AccessKey,
			"RGW_DEFAULT_USER_SECRET_KEY": endpoint.Spec.Credentials.SecretKey,
		},
	}

	_, err := oec.ds.CreateSecret(oec.namespace, &secret)
	return err
}

func (oec *ObjectEndpointController) createDeployment(endpoint *longhorn.ObjectEndpoint) error {
	registrySecretSetting, err := oec.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return errors.Wrap(err, "failed to get registry secret setting for object endpoint deployment")
	}
	registrySecret := []corev1.LocalObjectReference{
		{
			Name: registrySecretSetting.Value,
		},
	}

	dpl := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            endpoint.Name,
			Namespace:       oec.namespace,
			Labels:          oec.ds.GetObjectEndpointLabels(endpoint),
			OwnerReferences: oec.ds.GetOwnerReferencesForObjectEndpoint(endpoint),
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: oec.ds.GetObjectEndpointSelectorLabels(endpoint),
			},
			// an s3gw instance must have exclusive access to the volume, so we can
			// only spawn one replica (i.e. one s3gw instance) per object-endpoint.
			// Due to the way the struct works, an allocated integer has to be used
			// here and not a constant.
			Replicas: func() *int32 { r := int32(1); return &r }(),
			Strategy: appsv1.DeploymentStrategy{
				Type: "Recreate",
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: oec.ds.GetObjectEndpointSelectorLabels(endpoint),
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: registrySecret,
					Containers: []corev1.Container{
						{
							Name:  "s3gw",
							Image: oec.image,
							Args: []string{
								"--rgw-dns-name", fmt.Sprintf("%s.%s", endpoint.Name, oec.namespace),
								"--rgw-backend-store", "sfs",
								"--rgw_frontends", fmt.Sprintf("beast port=%d", types.ObjectEndpointContainerPort),
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "s3",
									ContainerPort: types.ObjectEndpointContainerPort,
									Protocol:      "TCP",
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: endpoint.Name,
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      genVolumeMountName(endpoint),
									MountPath: "/data",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: genVolumeMountName(endpoint),
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: genPVCName(endpoint),
								},
							},
						},
					},
				},
			},
		},
	}

	_, err = oec.ds.CreateDeployment(&dpl)
	return err
}

func genPVName(endpoint *longhorn.ObjectEndpoint) string {
	return fmt.Sprintf("pv-%s", endpoint.Name)
}

func genPVCName(endpoint *longhorn.ObjectEndpoint) string {
	return fmt.Sprintf("pvc-%s", endpoint.Name)
}

func genVolumeMountName(endpoint *longhorn.ObjectEndpoint) string {
	return fmt.Sprintf("%s-data", endpoint.Name)
}
