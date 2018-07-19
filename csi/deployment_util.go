package csi

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/util/pointer"
)

const (
	maxRetryCountForMountPropagationCheck = 10
	durationSleepForMountPropagationCheck = 10 * time.Second
)

// CheckMountPropagationWithPodSpec https://github.com/kubernetes/kubernetes/issues/66086#issuecomment-404346854
func CheckMountPropagationWithPodSpec(kubeClient *clientset.Clientset, image, namespace string) error {
	commonName := "longhorn-mount-propagation-tester"
	mountName := "mountpoint"
	mountPath := "/mnt/tmp"
	podSpec := v1.PodSpec{
		Containers: []v1.Container{
			v1.Container{
				Name:  commonName,
				Image: image,
				Args: []string{
					"/bin/sh",
					"-c",
					"sleep infinity",
				},
				ImagePullPolicy: v1.PullIfNotPresent,
				VolumeMounts: []v1.VolumeMount{
					v1.VolumeMount{
						Name:             mountName,
						MountPath:        mountPath,
						MountPropagation: &MountPropagationBidirectional,
					},
				},
				SecurityContext: &v1.SecurityContext{
					Privileged: pointer.BoolPtr(true),
				},
			},
		},
		Volumes: []v1.Volume{
			v1.Volume{
				Name: mountName,
				VolumeSource: v1.VolumeSource{
					HostPath: &v1.HostPathVolumeSource{
						Path: mountPath,
					},
				},
			},
		},
	}

	daemonSet := &appsv1beta2.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      commonName,
			Namespace: namespace,
		},

		Spec: appsv1beta2.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": commonName,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": commonName,
					},
				},
				Spec: podSpec,
			},
		},
	}

	daemonSetClient := kubeClient.AppsV1beta2().DaemonSets(namespace)
	podClient := kubeClient.CoreV1().Pods(namespace)

	logrus.Debugf("Trying to create the daemonset %s for MountPropagation check", commonName)
	_, err := daemonSetClient.Create(daemonSet)
	if err != nil {
		return err
	}
	logrus.Debugf("Created the daemonset %s for MountPropagation check", commonName)

	defer func() {
		propagation := metav1.DeletePropagationForeground
		if err := daemonSetClient.Delete(commonName,
			&metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
			logrus.Warnf("Failed to delete the daemonset %s for MountPropagation check", commonName)
		}
	}()

	retryCount := 0
	for {
		if retryCount >= maxRetryCountForMountPropagationCheck {
			return fmt.Errorf("Has been retried %d times, but daemonset is still unavailable", retryCount)
		}
		time.Sleep(durationSleepForMountPropagationCheck)
		retryCount++
		ds, err := daemonSetClient.Get(commonName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				logrus.Warnf("Not found daemonset %s, will try again later", commonName)
				continue
			}
			return err
		}
		logrus.Debugf("The status of daemonset %s, NumberReady: %d, DesiredNumberScheduled: %d",
			commonName, ds.Status.NumberReady, ds.Status.DesiredNumberScheduled)
		if ds.Status.NumberReady == ds.Status.DesiredNumberScheduled {
			break
		}
	}

	pods, err := podClient.List(metav1.ListOptions{LabelSelector: fmt.Sprintf("app=%s", commonName)})
	if err != nil {
		return err
	}

	for _, pod := range pods.Items {
		for _, mount := range pod.Spec.Containers[0].VolumeMounts {
			if mount.Name == mountName {
				mountPropagationStr := ""
				if mount.MountPropagation == nil {
					mountPropagationStr = "nil"
				} else {
					mountPropagationStr = string(*mount.MountPropagation)
				}

				logrus.Debugf("Got MountPropagation %s from pod %s, node %s",
					mountPropagationStr, pod.ObjectMeta.Name, pod.Spec.NodeName)

				if mount.MountPropagation == nil || *mount.MountPropagation != MountPropagationBidirectional {
					return fmt.Errorf("The MountPropagation value %s is not expected", mountPropagationStr)
				}
			}
		}
	}

	return nil
}

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
