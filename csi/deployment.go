package csi

import (
	"sync"

	"github.com/sirupsen/logrus"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	DefaultCSIAttacherImage        = "quay.io/k8scsi/csi-attacher:v0.4.2"
	DefaultCSIProvisionerImage     = "quay.io/k8scsi/csi-provisioner:v0.4.2"
	DefaultCSIDriverRegistrarImage = "quay.io/k8scsi/driver-registrar:v0.4.1"
	DefaultCSIProvisionerName      = "rancher.io/longhorn"

	DefaultCSIDeploymentReplicaCount = 3

	AnnotationCSIVersion = "longhorn.rancher.io/version"
)

var (
	HostPathDirectory             = v1.HostPathDirectory
	HostPathDirectoryOrCreate     = v1.HostPathDirectoryOrCreate
	MountPropagationBidirectional = v1.MountPropagationBidirectional
)

type AttacherDeployment struct {
	service    *v1.Service
	deployment *appsv1beta1.Deployment
}

func NewAttacherDeployment(namespace, serviceAccount, attacherImage string) *AttacherDeployment {
	service := getCommonService(types.CSIAttacherName, namespace)

	deployment := getCommonDeployment(
		types.CSIAttacherName,
		namespace,
		serviceAccount,
		attacherImage,
		[]string{
			"--v=5",
			"--csi-address=$(ADDRESS)",
			"--leader-election",
			"--leader-election-namespace=$(POD_NAMESPACE)",
			"--leader-election-identity=$(POD_NAME)",
		},
		DefaultCSIDeploymentReplicaCount,
	)

	return &AttacherDeployment{
		service:    service,
		deployment: deployment,
	}
}

func (a *AttacherDeployment) Deploy(kubeClient *clientset.Clientset) error {
	if err := deploy(kubeClient, a.service, "service",
		serviceCreateFunc, serviceDeleteFunc, serviceGetFunc); err != nil {
		return err
	}

	return deploy(kubeClient, a.deployment, "deployment",
		deploymentCreateFunc, deploymentDeleteFunc, deploymentGetFunc)
}

func (a *AttacherDeployment) Cleanup(kubeClient *clientset.Clientset) {
	var wg sync.WaitGroup
	defer wg.Wait()

	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, a.service, "service",
			serviceDeleteFunc, serviceGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup service in attacher deployment: %v", err)
		}
	})
	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, a.deployment, "deployment",
			deploymentDeleteFunc, deploymentGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup deployment in attacher deployment: %v", err)
		}
	})
}

type ProvisionerDeployment struct {
	service    *v1.Service
	deployment *appsv1beta1.Deployment
}

func NewProvisionerDeployment(namespace, serviceAccount, provisionerImage, provisionerName string) *ProvisionerDeployment {
	service := getCommonService(types.CSIProvisionerName, namespace)

	deployment := getCommonDeployment(
		types.CSIProvisionerName,
		namespace,
		serviceAccount,
		provisionerImage,
		[]string{
			"--v=5",
			"--provisioner=" + provisionerName,
			"--csi-address=$(ADDRESS)",
			"--enable-leader-election",
		},
		DefaultCSIDeploymentReplicaCount,
	)

	return &ProvisionerDeployment{
		service:    service,
		deployment: deployment,
	}
}

func (p *ProvisionerDeployment) Deploy(kubeClient *clientset.Clientset) error {
	if err := deploy(kubeClient, p.service, "service",
		serviceCreateFunc, serviceDeleteFunc, serviceGetFunc); err != nil {
		return err
	}

	return deploy(kubeClient, p.deployment, "deployment",
		deploymentCreateFunc, deploymentDeleteFunc, deploymentGetFunc)
}

func (p *ProvisionerDeployment) Cleanup(kubeClient *clientset.Clientset) {
	var wg sync.WaitGroup
	defer wg.Wait()

	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, p.service, "service",
			serviceDeleteFunc, serviceGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup service in provisioner deployment: %v", err)
		}
	})
	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, p.deployment, "deployment",
			deploymentDeleteFunc, deploymentGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup deployment in provisioner deployment: %v", err)
		}
	})
}

type PluginDeployment struct {
	daemonSet *appsv1beta2.DaemonSet
}

func NewPluginDeployment(namespace, serviceAccount, driverRegistrarImage, managerImage, managerURL string, kubeletPluginWatcherEnabled bool) *PluginDeployment {
	args := []string{
		"--v=5",
		"--csi-address=$(ADDRESS)",
	}
	volumeMounts := []v1.VolumeMount{
		{
			Name:      "socket-dir",
			MountPath: "/var/lib/kubelet/plugins/io.rancher.longhorn",
		},
	}
	volumes := []v1.Volume{
		{
			Name: "plugin-dir",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/plugins/io.rancher.longhorn",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "pods-mount-dir",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/pods",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "socket-dir",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/plugins/io.rancher.longhorn",
					Type: &HostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "host-dev",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/dev",
				},
			},
		},
		{
			Name: "host-sys",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/sys",
				},
			},
		},
		{
			Name: "lib-modules",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/lib-modules",
				},
			},
		},
	}

	// for Kubernetes v1.12+
	if kubeletPluginWatcherEnabled {
		args = append(args, "--kubelet-registration-path=/var/lib/kubelet/plugins/io.rancher.longhorn/csi.sock")
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      "registration-dir",
			MountPath: "/registration",
		})
		volumes = append(volumes, v1.Volume{
			Name: "registration-dir",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/plugins/",
					Type: &HostPathDirectory,
				},
			},
		})
	}

	daemonSet := &appsv1beta2.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.CSIPluginName,
			Namespace: namespace,
		},

		Spec: appsv1beta2.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": types.CSIPluginName,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": types.CSIPluginName,
					},
				},
				Spec: v1.PodSpec{
					ServiceAccountName: serviceAccount,
					Containers: []v1.Container{
						{
							Name:  "driver-registrar",
							Image: driverRegistrarImage,
							Args:  args,
							Env: []v1.EnvVar{
								{
									Name:  "ADDRESS",
									Value: "/var/lib/kubelet/plugins/io.rancher.longhorn/csi.sock",
								},
								{
									Name: "KUBE_NODE_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
							},
							//ImagePullPolicy: v1.PullAlways,
							VolumeMounts: volumeMounts,
						},
						{
							Name: "longhorn-csi-plugin",
							SecurityContext: &v1.SecurityContext{
								Privileged: pointer.BoolPtr(true),
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"SYS_ADMIN",
									},
								},
								AllowPrivilegeEscalation: pointer.BoolPtr(true),
							},
							Image: managerImage,
							Args: []string{
								"longhorn-manager",
								"-d",
								"csi",
								"--nodeid=$(NODE_ID)",
								"--endpoint=$(CSI_ENDPOINT)",
								"--drivername=io.rancher.longhorn",
								"--manager-url=" + managerURL,
							},
							Env: []v1.EnvVar{
								{
									Name: "NODE_ID",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name:  "CSI_ENDPOINT",
									Value: "unix://var/lib/kubelet/plugins/io.rancher.longhorn/csi.sock",
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "plugin-dir",
									MountPath: "/var/lib/kubelet/plugins/io.rancher.longhorn",
								},
								{
									Name:             "pods-mount-dir",
									MountPath:        "/var/lib/kubelet/pods",
									MountPropagation: &MountPropagationBidirectional,
								},
								{
									Name:      "host-dev",
									MountPath: "/dev",
								},
								{
									Name:      "host-sys",
									MountPath: "/sys",
								},
								{
									Name:      "lib-modules",
									MountPath: "/lib/modules",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: volumes,
				},
			},
		},
	}

	return &PluginDeployment{
		daemonSet: daemonSet,
	}
}

func (p *PluginDeployment) Deploy(kubeClient *clientset.Clientset) error {
	return deploy(kubeClient, p.daemonSet, "daemon set",
		daemonSetCreateFunc, daemonSetDeleteFunc, daemonSetGetFunc)
}

func (p *PluginDeployment) Cleanup(kubeClient *clientset.Clientset) {
	if err := cleanup(kubeClient, p.daemonSet, "daemon set",
		daemonSetDeleteFunc, daemonSetGetFunc); err != nil {
		logrus.Warnf("Failed to cleanup DaemonSet in plugin deployment: %v", err)
	}
}
