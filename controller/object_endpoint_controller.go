package controller

import (
	"fmt"
	"time"

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
	oec.handleError(err, key)

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

func (oec *ObjectEndpointController) handleError(err error, key interface{}) {
	if err == nil {
		oec.queue.Forget(key)
		return
	}

	if oec.queue.NumRequeues(key) < maxRetries {
		oec.logger.WithError(err).Errorf("")
		oec.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	oec.logger.WithError(err).Errorf("")
	oec.queue.Forget(key)
}

func (oec *ObjectEndpointController) syncObjectEndpoint(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	endpoint, err := oec.ds.GetObjectEndpoint(name, namespace)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			oec.handleResourceDeletion(name, namespace)
		}
		return err
	}

	if endpoint.Status.State == longhorn.ObjectEndpointStateStarting {
		// check all resources and update state if necessary
		return nil
	}

	if endpoint.Status.State == longhorn.ObjectEndpointStateRunning {
		return nil
	}

	if err := oec.handleResourceCreation(endpoint); err != nil {
		return err
	}
	return nil
}

func (oec *ObjectEndpointController) handleResourceCreation(endpoint *longhorn.ObjectEndpoint) error {
	endpoint.Status.State = longhorn.ObjectEndpointStateStarting
	endpoint, err := oec.ds.UpdateObjectEndpointStatus(endpoint)
	if err != nil {
		return err
	}

	if err := oec.handlePVCCreation(endpoint); err != nil {
		return err
	}

	if err := oec.handleSVCCreation(endpoint); err != nil {
		return err
	}

	if err := oec.handleSecretCreation(endpoint); err != nil {
		return err
	}

	if err := oec.handleDeploymentCreation(endpoint); err != nil {
		return err
	}

	return nil
}

func (oec *ObjectEndpointController) handleResourceDeletion(name string, namespace string) error {
	if err := oec.ds.DeleteDeployment(name); err != nil {
		return err
	}
	if err := oec.ds.DeleteSecret(namespace, name); err != nil {
		return err
	}
	if err := oec.ds.DeleteService(namespace, name); err != nil {
		return err
	}
	if err := oec.ds.DeletePersistentVolumeClaim(namespace, name); err != nil {
		return err
	}
	return nil
}

func (oec *ObjectEndpointController) handlePVCCreation(endpoint *longhorn.ObjectEndpoint) error {
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      endpoint.Name,
			Namespace: endpoint.Namespace,
			Labels:    oec.ds.GetObjectEndpointLabels(endpoint),
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
		},
	}

	if _, err := oec.ds.CreatePersistentVolumeClaim(endpoint.Namespace, &pvc); err != nil {
		return err
	}
	return nil
}

func (oec *ObjectEndpointController) handleSVCCreation(endpoint *longhorn.ObjectEndpoint) error {
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      endpoint.Name,
			Namespace: endpoint.Namespace,
			Labels:    oec.ds.GetObjectEndpointLabels(endpoint),
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

	if _, err := oec.ds.CreateService(endpoint.Namespace, &svc); err != nil {
		return err
	}
	return nil
}

func (oec *ObjectEndpointController) handleSecretCreation(endpoint *longhorn.ObjectEndpoint) error {
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      endpoint.Name,
			Namespace: endpoint.Namespace,
			Labels:    oec.ds.GetObjectEndpointLabels(endpoint),
		},
		StringData: map[string]string{
			"RGW_DEFAULT_USER_ACCESS_KEY": endpoint.Spec.Credentials.AccessKey,
			"RGW_DEFAULT_USER_SECRET_KEY": endpoint.Spec.Credentials.SecretKey,
		},
	}

	if _, err := oec.ds.CreateSecret(endpoint.Namespace, &secret); err != nil {
		return err
	}
	return nil
}

func (oec *ObjectEndpointController) handleDeploymentCreation(endpoint *longhorn.ObjectEndpoint) error {
	replicas := int32(1) // this must be allocated and can't be a constant
	secretref := corev1.SecretEnvSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: endpoint.Name,
		},
	}

	pvc := corev1.PersistentVolumeClaimVolumeSource{
		ClaimName: endpoint.Name,
	}

	dpl := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      endpoint.Name,
			Namespace: endpoint.Namespace,
			Labels:    oec.ds.GetObjectEndpointLabels(endpoint),
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: oec.ds.GetObjectEndpointSelectorLabels(endpoint),
			},
			Replicas: &replicas, // an s3gw instance must have exclusive access to the volume
			Strategy: appsv1.DeploymentStrategy{
				Type: "Recreate",
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: oec.ds.GetObjectEndpointSelectorLabels(endpoint),
				},
				Spec: corev1.PodSpec{
					// TODO: propagate image pull secrets
					Containers: []corev1.Container{
						{
							Name:  "s3gw",
							Image: oec.image,
							Args: []string{
								"--rgw-dns-name", fmt.Sprintf("%s.%s", endpoint.Name, endpoint.Namespace),
								"--rgw-backend-store", "sfs",
								"--debug-rgw", "1",
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
									SecretRef: &secretref,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      fmt.Sprintf("%s-lh-storage", endpoint.Name),
									MountPath: "/data",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: fmt.Sprintf("%s-lh-storage", endpoint.Name),
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &pvc,
							},
						},
					},
				},
			},
		},
	}

	oec.logger.Infof("OEC Deployment: %v", dpl)
	if _, err := oec.ds.CreateDeployment(&dpl); err != nil {
		return err
	}
	return nil
}
