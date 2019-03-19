package csi

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	appsv1beta1 "k8s.io/api/apps/v1beta1"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/rancher/longhorn-manager/types"

	longhornclient "github.com/rancher/longhorn-manager/client"
)

var VERSION = "v0.3.0"

const (
	maxRetryCountForMountPropagationCheck = 10
	durationSleepForMountPropagationCheck = 5 * time.Second
	maxRetryForDeletion                   = 120
)

func getCommonService(commonName, namespace string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      commonName,
			Namespace: namespace,
			Labels: map[string]string{
				"app": commonName,
			},
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				"app": commonName,
			},
			Ports: []v1.ServicePort{
				{
					Name: "dummy",
					Port: 12345,
				},
			},
		},
	}
}

func getCommonDeployment(commonName, namespace, serviceAccount, image string, args []string, replicaCount int32) *appsv1beta1.Deployment {
	return &appsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      commonName,
			Namespace: namespace,
		},
		Spec: appsv1beta1.DeploymentSpec{
			Replicas: &replicaCount,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": commonName,
					},
				},
				Spec: v1.PodSpec{
					ServiceAccountName: serviceAccount,
					Containers: []v1.Container{
						{
							Name:  commonName,
							Image: image,
							Args:  args,
							//ImagePullPolicy: v1.PullAlways,
							Env: []v1.EnvVar{
								{
									Name:  "ADDRESS",
									Value: "/var/lib/kubelet/plugins/io.rancher.longhorn/csi.sock",
								},
								{
									Name: "POD_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "socket-dir",
									MountPath: "/var/lib/kubelet/plugins/io.rancher.longhorn",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "socket-dir",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/var/lib/kubelet/plugins/io.rancher.longhorn",
									Type: &HostPathDirectoryOrCreate,
								},
							},
						},
					},
				},
			},
		},
	}
}

type resourceCreateFunc func(kubeClient *clientset.Clientset, obj runtime.Object) error
type resourceDeleteFunc func(kubeClient *clientset.Clientset, name, namespace string) error
type resourceGetFunc func(kubeClient *clientset.Clientset, name, namespace string) (runtime.Object, error)

func waitForDeletion(kubeClient *clientset.Clientset, name, namespace, resource string, getFunc resourceGetFunc) error {
	logrus.Debugf("Waiting for foreground deletion of %s %s", resource, name)
	for i := 0; i < maxRetryForDeletion; i++ {
		_, err := getFunc(kubeClient, name, namespace)
		if err != nil && apierrors.IsNotFound(err) {
			logrus.Debugf("Deleted %s %s in foreground", resource, name)
			return nil
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return fmt.Errorf("Foreground deletion of %s %s timed out", resource, name)
}

func deploy(kubeClient *clientset.Clientset, obj runtime.Object, resource string,
	createFunc resourceCreateFunc, deleteFunc resourceDeleteFunc, getFunc resourceGetFunc) (err error) {

	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return fmt.Errorf("BUG: invalid object for deploy %v: %v", obj, err)
	}
	annos := objMeta.GetAnnotations()
	if annos == nil {
		annos = map[string]string{}
	}
	annos[AnnotationCSIVersion] = VERSION
	objMeta.SetAnnotations(annos)
	name := objMeta.GetName()
	namespace := objMeta.GetNamespace()

	defer func() {
		err = errors.Wrapf(err, "failed to deploy %v %v", resource, name)
	}()

	existing, err := getFunc(kubeClient, name, namespace)
	if err == nil {
		existingMeta, err := meta.Accessor(existing)
		if err != nil {
			return err
		}
		annos := objMeta.GetAnnotations()
		existingAnnos := existingMeta.GetAnnotations()
		if annos[AnnotationCSIVersion] == existingAnnos[AnnotationCSIVersion] &&
			existingMeta.GetDeletionTimestamp() == nil {
			// deployment of correct version already deployed
			logrus.Debugf("Detected %v %v version %v has already been deployed",
				resource, name, annos[AnnotationCSIVersion])
			return nil
		}
	}
	// otherwise clean up the old deployment
	if err := cleanup(kubeClient, obj, resource, deleteFunc, getFunc); err != nil {
		return err
	}
	logrus.Debugf("Creating %s %s", resource, name)
	if err := createFunc(kubeClient, obj); err != nil {
		return err
	}
	logrus.Debugf("Created %s %s", resource, name)
	return nil
}

func cleanup(kubeClient *clientset.Clientset, obj runtime.Object, resource string,
	deleteFunc resourceDeleteFunc, getFunc resourceGetFunc) (err error) {

	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return fmt.Errorf("BUG: invalid object for cleanup %v: %v", obj, err)
	}
	name := objMeta.GetName()
	namespace := objMeta.GetNamespace()

	defer func() {
		err = errors.Wrapf(err, "failed to cleanup %v %v", resource, name)
	}()

	existing, err := getFunc(kubeClient, name, namespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	existingMeta, err := meta.Accessor(existing)
	if err != nil {
		return err
	}
	if existingMeta.GetDeletionTimestamp() != nil {
		return waitForDeletion(kubeClient, name, namespace, resource, getFunc)
	}
	logrus.Debugf("Deleting existing %s %s", resource, name)
	if err := deleteFunc(kubeClient, name, namespace); err != nil {
		return err
	}
	logrus.Debugf("Deleted %s %s", resource, name)
	return waitForDeletion(kubeClient, name, namespace, resource, getFunc)
}

func serviceCreateFunc(kubeClient *clientset.Clientset, obj runtime.Object) error {
	o, ok := obj.(*v1.Service)
	if !ok {
		return fmt.Errorf("BUG: cannot convert back the object")
	}
	_, err := kubeClient.CoreV1().Services(o.Namespace).Create(o)
	return err
}

func serviceDeleteFunc(kubeClient *clientset.Clientset, name, namespace string) error {
	propagation := metav1.DeletePropagationForeground
	return kubeClient.CoreV1().Services(namespace).Delete(
		name,
		&metav1.DeleteOptions{PropagationPolicy: &propagation},
	)
}

func serviceGetFunc(kubeClient *clientset.Clientset, name, namespace string) (runtime.Object, error) {
	return kubeClient.CoreV1().Services(namespace).Get(name, metav1.GetOptions{})
}

func deploymentCreateFunc(kubeClient *clientset.Clientset, obj runtime.Object) error {
	o, ok := obj.(*appsv1beta1.Deployment)
	if !ok {
		return fmt.Errorf("BUG: cannot convert back the object")
	}
	_, err := kubeClient.AppsV1beta1().Deployments(o.Namespace).Create(o)
	return err
}

func deploymentDeleteFunc(kubeClient *clientset.Clientset, name, namespace string) error {
	propagation := metav1.DeletePropagationForeground
	return kubeClient.AppsV1beta1().Deployments(namespace).Delete(
		name,
		&metav1.DeleteOptions{PropagationPolicy: &propagation},
	)
}

func deploymentGetFunc(kubeClient *clientset.Clientset, name, namespace string) (runtime.Object, error) {
	return kubeClient.AppsV1beta1().Deployments(namespace).Get(name, metav1.GetOptions{})
}

func daemonSetCreateFunc(kubeClient *clientset.Clientset, obj runtime.Object) error {
	o, ok := obj.(*appsv1beta2.DaemonSet)
	if !ok {
		return fmt.Errorf("BUG: cannot convert back the object")
	}
	_, err := kubeClient.AppsV1beta2().DaemonSets(o.Namespace).Create(o)
	return err
}

func daemonSetDeleteFunc(kubeClient *clientset.Clientset, name, namespace string) error {
	propagation := metav1.DeletePropagationForeground
	return kubeClient.AppsV1beta2().DaemonSets(namespace).Delete(
		name,
		&metav1.DeleteOptions{PropagationPolicy: &propagation},
	)
}

func daemonSetGetFunc(kubeClient *clientset.Clientset, name, namespace string) (runtime.Object, error) {
	return kubeClient.AppsV1beta2().DaemonSets(namespace).Get(name, metav1.GetOptions{})
}

// CheckMountPropagationWithNode https://github.com/kubernetes/kubernetes/issues/66086#issuecomment-404346854
func CheckMountPropagationWithNode(managerURL string) error {
	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return err
	}
	nodeCollection, err := apiClient.Node.List(&longhornclient.ListOpts{})
	for _, node := range nodeCollection.Data {
		con := node.Conditions[string(types.NodeConditionTypeMountPropagation)]
		var condition map[string]interface{}
		if con != nil {
			condition = con.(map[string]interface{})
		}
		for i := 0; i < maxRetryCountForMountPropagationCheck; i++ {
			if condition != nil && condition["status"] != nil && condition["status"].(string) != string(types.ConditionStatusUnknown) {
				break
			}
			time.Sleep(durationSleepForMountPropagationCheck)
			retryNode, err := apiClient.Node.ById(node.Name)
			if err != nil {
				return err
			}
			if retryNode.Conditions[string(types.NodeConditionTypeMountPropagation)] != nil {
				condition = retryNode.Conditions[string(types.NodeConditionTypeMountPropagation)].(map[string]interface{})
			}
		}
		if condition == nil || condition["status"] == nil || condition["status"].(string) != string(types.ConditionStatusTrue) {
			return fmt.Errorf("Node %s is not support mount propagation", node.Name)
		}
	}

	return nil
}
