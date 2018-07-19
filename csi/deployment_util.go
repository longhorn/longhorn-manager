package csi

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
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
				v1.ServicePort{
					Name: "dummy",
					Port: 12345,
				},
			},
		},
	}
}

func getCommondStatefulSet(commonName, namespace, serviceAccount, image string, args []string) *appsv1beta1.StatefulSet {
	return &appsv1beta1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      commonName,
			Namespace: namespace,
		},
		Spec: appsv1beta1.StatefulSetSpec{
			ServiceName: commonName,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": commonName,
					},
				},
				Spec: v1.PodSpec{
					ServiceAccountName: serviceAccount,
					Containers: []v1.Container{
						v1.Container{
							Name:  commonName,
							Image: image,
							Args:  args,
							Env: []v1.EnvVar{
								v1.EnvVar{
									Name:  "ADDRESS",
									Value: "/var/lib/kubelet/plugins/io.rancher.longhorn/csi.sock",
								},
							},
							//ImagePullPolicy: v1.PullAlways,
							VolumeMounts: []v1.VolumeMount{
								v1.VolumeMount{
									Name:      "socket-dir",
									MountPath: "/var/lib/kubelet/plugins/io.rancher.longhorn",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						v1.Volume{
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

func cleanupService(kubeClient *clientset.Clientset, service *v1.Service) error {
	logrus.Debugf("Trying to get the service %s", service.ObjectMeta.Name)
	svc, err := kubeClient.CoreV1().Services(service.ObjectMeta.Namespace).Get(service.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil && apierrors.IsNotFound(err) {
		return nil
	}
	if svc != nil && svc.DeletionTimestamp != nil {
		return fmt.Errorf("Object is being deleted: service %s", service.ObjectMeta.Name)
	}

	if svc != nil {
		logrus.Debugf("Got the service %s", service.ObjectMeta.Name)
		logrus.Debugf("Trying to delete the service %s", service.ObjectMeta.Name)
		propagation := metav1.DeletePropagationForeground
		if err = kubeClient.CoreV1().Services(service.ObjectMeta.Namespace).Delete(service.ObjectMeta.Name,
			&metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
			return err
		}
		logrus.Debugf("Deleted the service %s", service.ObjectMeta.Name)
	}
	return nil
}

func deployService(kubeClient *clientset.Clientset, service *v1.Service) error {
	if err := cleanupService(kubeClient, service); err != nil {
		return err
	}
	logrus.Debugf("Trying to create the service %s", service.ObjectMeta.Name)
	if _, err := kubeClient.CoreV1().Services(service.ObjectMeta.Namespace).Create(service); err != nil {
		return err
	}
	logrus.Debugf("Created the service %s", service.ObjectMeta.Name)
	return nil
}

func cleanupStatefulSet(kubeClient *clientset.Clientset, statefulSet *appsv1beta1.StatefulSet) error {
	logrus.Debugf("Trying to get the statefulset %s", statefulSet.ObjectMeta.Name)
	sfs, err := kubeClient.AppsV1beta1().StatefulSets(statefulSet.ObjectMeta.Namespace).Get(statefulSet.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil && apierrors.IsNotFound(err) {
		return nil
	}
	if sfs != nil && sfs.DeletionTimestamp != nil {
		return fmt.Errorf("Object is being deleted: statefulset %s", statefulSet.ObjectMeta.Name)
	}

	if sfs != nil {
		logrus.Debugf("Got the statefulset %s", statefulSet.ObjectMeta.Name)
		logrus.Debugf("Trying to delete the statefulset %s", statefulSet.ObjectMeta.Name)
		propagation := metav1.DeletePropagationForeground
		if err = kubeClient.AppsV1beta1().StatefulSets(statefulSet.ObjectMeta.Namespace).Delete(statefulSet.ObjectMeta.Name,
			&metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
			return err
		}
		logrus.Debugf("Deleted the statefulset %s", statefulSet.ObjectMeta.Name)
	}
	return nil
}

func deployStatefulSet(kubeClient *clientset.Clientset, statefulSet *appsv1beta1.StatefulSet) error {
	if err := cleanupStatefulSet(kubeClient, statefulSet); err != nil {
		return err
	}
	logrus.Debugf("Trying to create the statefulset %s", statefulSet.ObjectMeta.Name)
	if _, err := kubeClient.AppsV1beta1().StatefulSets(statefulSet.ObjectMeta.Namespace).Create(statefulSet); err != nil {
		return err
	}
	logrus.Debugf("Created the statefulset %s", statefulSet.ObjectMeta.Name)
	return nil
}

func cleanupDaemonSet(kubeClient *clientset.Clientset, daemonSet *appsv1beta2.DaemonSet) error {
	logrus.Debugf("Trying to get the daemonset %s", daemonSet.ObjectMeta.Name)
	ds, err := kubeClient.AppsV1beta2().DaemonSets(daemonSet.ObjectMeta.Namespace).Get(daemonSet.ObjectMeta.Name, metav1.GetOptions{})
	if err != nil && apierrors.IsNotFound(err) {
		return nil
	}
	if ds != nil && ds.DeletionTimestamp != nil {
		return fmt.Errorf("Object is being deleted: DaemonSet %s", daemonSet.ObjectMeta.Name)
	}

	if ds != nil {
		logrus.Debugf("Got the daemonset %s", daemonSet.ObjectMeta.Name)
		logrus.Debugf("Trying to delete the daemonset %s", daemonSet.ObjectMeta.Name)
		propagation := metav1.DeletePropagationForeground
		if err = kubeClient.AppsV1beta2().DaemonSets(daemonSet.ObjectMeta.Namespace).Delete(daemonSet.ObjectMeta.Name,
			&metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
			return err
		}
		logrus.Debugf("Deleted the daemonset %s", daemonSet.ObjectMeta.Name)
	}
	return nil
}

func deployDaemonSet(kubeClient *clientset.Clientset, daemonSet *appsv1beta2.DaemonSet) error {
	if err := cleanupDaemonSet(kubeClient, daemonSet); err != nil {
		return err
	}
	logrus.Debugf("Trying to create the daemonset %s", daemonSet.ObjectMeta.Name)
	if _, err := kubeClient.AppsV1beta2().DaemonSets(daemonSet.ObjectMeta.Namespace).Create(daemonSet); err != nil {
		return err
	}
	logrus.Debugf("Created the daemonset %s", daemonSet.ObjectMeta.Name)
	return nil
}
