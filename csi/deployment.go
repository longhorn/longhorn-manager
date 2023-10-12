package csi

import (
	"fmt"

	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	DefaultCSIAttacherImage            = "longhornio/csi-attacher:v3.4.0"
	DefaultCSIProvisionerImage         = "longhornio/csi-provisioner:v2.1.2"
	DefaultCSIResizerImage             = "longhornio/csi-resizer:v1.3.0"
	DefaultCSISnapshotterImage         = "longhornio/csi-snapshotter:v5.0.1"
	DefaultCSINodeDriverRegistrarImage = "longhornio/csi-node-driver-registrar:v2.5.0"
	DefaultCSILivenessProbeImage       = "longhornio/livenessprobe:v2.8.0"

	DefaultCSIAttacherReplicaCount    = 3
	DefaultCSIProvisionerReplicaCount = 3
	DefaultCSIResizerReplicaCount     = 3
	DefaultCSISnapshotterReplicaCount = 3

	DefaultCSISocketFileName             = "csi.sock"
	DefaultCSIRegistrationDirSuffix      = "/plugins_registry"
	DefaultCSIPluginsDirSuffix           = "/plugins/"
	DefaultKubernetesCSIDirSuffix        = "/kubernetes.io/csi/"
	DefaultInContainerCSISocketDir       = "/csi/"
	DefaultInContainerCSIRegistrationDir = "/registration"
	DefaultCSILivenessProbePort          = 9808

	AnnotationCSIGitCommit      = types.LonghornDriverName + "/git-commit"
	AnnotationCSIVersion        = types.LonghornDriverName + "/version"
	AnnotationKubernetesVersion = types.LonghornDriverName + "/kubernetes-version"
)

var (
	HostPathDirectoryOrCreate     = v1.HostPathDirectoryOrCreate
	MountPropagationBidirectional = v1.MountPropagationBidirectional
)

type AttacherDeployment struct {
<<<<<<< HEAD
	service    *v1.Service
=======
>>>>>>> eb568dc9 (Don't deploy dummy services with CSI components)
	deployment *appsv1.Deployment
}

func NewAttacherDeployment(namespace, serviceAccount, attacherImage, rootDir string, replicaCount int, tolerations []v1.Toleration,
	tolerationsString, priorityClass, registrySecret string, imagePullPolicy v1.PullPolicy, nodeSelector map[string]string) *AttacherDeployment {

	deployment := getCommonDeployment(
		types.CSIAttacherName,
		namespace,
		serviceAccount,
		attacherImage,
		rootDir,
		[]string{
			"--v=2",
			"--csi-address=$(ADDRESS)",
			"--timeout=1m50s",
			"--leader-election",
			"--leader-election-namespace=$(POD_NAMESPACE)",
		},
		int32(replicaCount),
		tolerations,
		tolerationsString,
		priorityClass,
		registrySecret,
		imagePullPolicy,
		nodeSelector,
	)

	return &AttacherDeployment{
		deployment: deployment,
	}
}

func (a *AttacherDeployment) Deploy(kubeClient *clientset.Clientset) error {
	return deploy(kubeClient, a.deployment, "deployment",
		deploymentCreateFunc, deploymentDeleteFunc, deploymentGetFunc)
}

func (a *AttacherDeployment) Cleanup(kubeClient *clientset.Clientset) {
	if err := cleanup(kubeClient, a.deployment, "deployment",
		deploymentDeleteFunc, deploymentGetFunc); err != nil {
		logrus.Warnf("Failed to cleanup deployment in attacher deployment: %v", err)
	}
}

type ProvisionerDeployment struct {
<<<<<<< HEAD
	service    *v1.Service
=======
>>>>>>> eb568dc9 (Don't deploy dummy services with CSI components)
	deployment *appsv1.Deployment
}

func NewProvisionerDeployment(namespace, serviceAccount, provisionerImage, rootDir string, replicaCount int, tolerations []v1.Toleration,
	tolerationsString, priorityClass, registrySecret string, imagePullPolicy v1.PullPolicy, nodeSelector map[string]string) *ProvisionerDeployment {

	deployment := getCommonDeployment(
		types.CSIProvisionerName,
		namespace,
		serviceAccount,
		provisionerImage,
		rootDir,
		[]string{
			"--v=2",
			"--csi-address=$(ADDRESS)",
			"--timeout=1m50s",
			"--leader-election",
			"--leader-election-namespace=$(POD_NAMESPACE)",
			"--default-fstype=ext4",
		},
		int32(replicaCount),
		tolerations,
		tolerationsString,
		priorityClass,
		registrySecret,
		imagePullPolicy,
		nodeSelector,
	)

	return &ProvisionerDeployment{
		deployment: deployment,
	}
}

func (p *ProvisionerDeployment) Deploy(kubeClient *clientset.Clientset) error {
	return deploy(kubeClient, p.deployment, "deployment",
		deploymentCreateFunc, deploymentDeleteFunc, deploymentGetFunc)
}

func (p *ProvisionerDeployment) Cleanup(kubeClient *clientset.Clientset) {
<<<<<<< HEAD
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

type ResizerDeployment struct {
	service    *v1.Service
=======
	if err := cleanup(kubeClient, p.deployment, "deployment",
		deploymentDeleteFunc, deploymentGetFunc); err != nil {
		logrus.WithError(err).Warn("Failed to cleanup deployment in provisioner deployment")
	}
}

type ResizerDeployment struct {
>>>>>>> eb568dc9 (Don't deploy dummy services with CSI components)
	deployment *appsv1.Deployment
}

func NewResizerDeployment(namespace, serviceAccount, resizerImage, rootDir string, replicaCount int, tolerations []v1.Toleration,
	tolerationsString, priorityClass, registrySecret string, imagePullPolicy v1.PullPolicy, nodeSelector map[string]string) *ResizerDeployment {

	deployment := getCommonDeployment(
		types.CSIResizerName,
		namespace,
		serviceAccount,
		resizerImage,
		rootDir,
		[]string{
			"--v=2",
			"--csi-address=$(ADDRESS)",
			"--timeout=1m50s",
			"--leader-election",
			"--leader-election-namespace=$(POD_NAMESPACE)",
			"--leader-election-namespace=$(POD_NAMESPACE)",
			// Issue: https://github.com/longhorn/longhorn/issues/3303
			// TODO: Remove this after upgrading the CSI resizer version that contains the fix of https://github.com/kubernetes-csi/external-resizer/issues/175
			"--handle-volume-inuse-error=false",
		},
		int32(replicaCount),
		tolerations,
		tolerationsString,
		priorityClass,
		registrySecret,
		imagePullPolicy,
		nodeSelector,
	)

	return &ResizerDeployment{
		deployment: deployment,
	}
}

func (p *ResizerDeployment) Deploy(kubeClient *clientset.Clientset) error {
	return deploy(kubeClient, p.deployment, "deployment",
		deploymentCreateFunc, deploymentDeleteFunc, deploymentGetFunc)
}

func (p *ResizerDeployment) Cleanup(kubeClient *clientset.Clientset) {
<<<<<<< HEAD
	var wg sync.WaitGroup
	defer wg.Wait()

	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, p.service, "service",
			serviceDeleteFunc, serviceGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup service in resizer deployment: %v", err)
		}
	})
	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, p.deployment, "deployment",
			deploymentDeleteFunc, deploymentGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup deployment in resizer deployment: %v", err)
		}
	})
}

type SnapshotterDeployment struct {
	service    *v1.Service
	deployment *appsv1.Deployment
}

func NewSnapshotterDeployment(namespace, serviceAccount, snapshotterImage, rootDir string, replicaCount int, tolerations []v1.Toleration,
	tolerationsString, priorityClass, registrySecret string, imagePullPolicy v1.PullPolicy, nodeSelector map[string]string) *SnapshotterDeployment {
	service := getCommonService(types.CSISnapshotterName, namespace)
=======
	if err := cleanup(kubeClient, p.deployment, "deployment",
		deploymentDeleteFunc, deploymentGetFunc); err != nil {
		logrus.WithError(err).Warn("Failed to cleanup deployment in resizer deployment")
	}
}

type SnapshotterDeployment struct {
	deployment *appsv1.Deployment
}

func NewSnapshotterDeployment(namespace, serviceAccount, snapshotterImage, rootDir string, replicaCount int, tolerations []corev1.Toleration,
	tolerationsString, priorityClass, registrySecret string, imagePullPolicy corev1.PullPolicy, nodeSelector map[string]string) *SnapshotterDeployment {
>>>>>>> eb568dc9 (Don't deploy dummy services with CSI components)

	deployment := getCommonDeployment(
		types.CSISnapshotterName,
		namespace,
		serviceAccount,
		snapshotterImage,
		rootDir,
		[]string{
			"--v=2",
			"--csi-address=$(ADDRESS)",
			"--timeout=1m50s",
			"--leader-election",
			"--leader-election-namespace=$(POD_NAMESPACE)",
		},
		int32(replicaCount),
		tolerations,
		tolerationsString,
		priorityClass,
		registrySecret,
		imagePullPolicy,
		nodeSelector,
	)

	return &SnapshotterDeployment{
		deployment: deployment,
	}
}

func (p *SnapshotterDeployment) Deploy(kubeClient *clientset.Clientset) error {
	return deploy(kubeClient, p.deployment, "deployment",
		deploymentCreateFunc, deploymentDeleteFunc, deploymentGetFunc)
}

func (p *SnapshotterDeployment) Cleanup(kubeClient *clientset.Clientset) {
<<<<<<< HEAD
	var wg sync.WaitGroup
	defer wg.Wait()

	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, p.service, "service",
			serviceDeleteFunc, serviceGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup service in snapshotter deployment: %v", err)
		}
	})
	util.RunAsync(&wg, func() {
		if err := cleanup(kubeClient, p.deployment, "deployment",
			deploymentDeleteFunc, deploymentGetFunc); err != nil {
			logrus.Warnf("Failed to cleanup deployment in snapshotter deployment: %v", err)
		}
	})
=======
	if err := cleanup(kubeClient, p.deployment, "deployment",
		deploymentDeleteFunc, deploymentGetFunc); err != nil {
		logrus.WithError(err).Warn("Failed to cleanup deployment in snapshotter deployment")
	}
>>>>>>> eb568dc9 (Don't deploy dummy services with CSI components)
}

type PluginDeployment struct {
	daemonSet *appsv1.DaemonSet
}

func NewPluginDeployment(namespace, serviceAccount, nodeDriverRegistrarImage, livenessProbeImage, managerImage, managerURL, rootDir string,
	tolerations []v1.Toleration, tolerationsString, priorityClass, registrySecret string, imagePullPolicy v1.PullPolicy, nodeSelector map[string]string) *PluginDeployment {

	daemonSet := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        types.CSIPluginName,
			Namespace:   namespace,
			Annotations: map[string]string{types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix): tolerationsString},
			Labels:      types.GetBaseLabelsForSystemManagedComponent(),
		},

		Spec: appsv1.DaemonSetSpec{
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
					Tolerations:        tolerations,
					NodeSelector:       nodeSelector,
					PriorityClassName:  priorityClass,
					HostPID:            true,
					Containers: []v1.Container{
						{
							Name:  "node-driver-registrar",
							Image: nodeDriverRegistrarImage,
							Lifecycle: &v1.Lifecycle{
								PreStop: &v1.LifecycleHandler{
									Exec: &v1.ExecAction{
										Command: []string{
											"/bin/sh", "-c",
											fmt.Sprintf("rm -rf %s/%s %s/%s-reg.sock %s/*", GetInContainerCSIRegistrationDir(), types.LonghornDriverName, GetInContainerCSIRegistrationDir(), types.LonghornDriverName, GetInContainerCSISocketDir()),
										},
									},
								},
							},
							SecurityContext: &v1.SecurityContext{
								Privileged: pointer.BoolPtr(true),
							},
							Args: []string{
								"--v=2",
								"--csi-address=$(ADDRESS)",
								"--kubelet-registration-path=" + GetCSISocketFilePath(rootDir),
							},
							Env: []v1.EnvVar{
								{
									Name:  "ADDRESS",
									Value: GetInContainerCSISocketFilePath(),
								},
							},
							ImagePullPolicy: imagePullPolicy,
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "socket-dir",
									MountPath: GetInContainerCSISocketDir(),
								},
								{
									Name:      "registration-dir",
									MountPath: GetInContainerCSIRegistrationDir(),
								},
							},
						},
						{
							Name:            "longhorn-liveness-probe",
							ImagePullPolicy: imagePullPolicy,
							Image:           livenessProbeImage,
							Args: []string{
								"--v=4",
								fmt.Sprintf("--csi-address=%s", GetInContainerCSISocketFilePath()),
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "socket-dir",
									MountPath: GetInContainerCSISocketDir(),
								},
							},
						},
						{
							Name: types.CSIPluginName,
							SecurityContext: &v1.SecurityContext{
								Privileged: pointer.BoolPtr(true),
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"SYS_ADMIN",
									},
								},
								AllowPrivilegeEscalation: pointer.BoolPtr(true),
							},
							Image:           managerImage,
							ImagePullPolicy: imagePullPolicy,
							Ports: []v1.ContainerPort{
								{
									ContainerPort: DefaultCSILivenessProbePort,
									Protocol:      v1.ProtocolTCP,
								},
							},
							LivenessProbe: &v1.Probe{
								ProbeHandler: v1.ProbeHandler{
									HTTPGet: &v1.HTTPGetAction{
										Path: "/healthz",
										Port: intstr.FromInt(DefaultCSILivenessProbePort),
									},
								},
								InitialDelaySeconds: datastore.PodProbeInitialDelay,
								TimeoutSeconds:      datastore.PodProbeTimeoutSeconds,
								PeriodSeconds:       datastore.PodProbePeriodSeconds,
								FailureThreshold:    datastore.PodLivenessProbeFailureThreshold,
							},
							Lifecycle: &v1.Lifecycle{
								PreStop: &v1.LifecycleHandler{
									Exec: &v1.ExecAction{
										Command: []string{
											"/bin/sh", "-c",
											fmt.Sprintf("rm -f %s/*", GetInContainerCSISocketDir()),
										},
									},
								},
							},
							Args: []string{
								"longhorn-manager",
								"-d",
								"csi",
								"--nodeid=$(NODE_ID)",
								"--endpoint=$(CSI_ENDPOINT)",
								fmt.Sprintf("--drivername=%s", types.LonghornDriverName),
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
									Value: GetCSIEndpoint(),
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "socket-dir",
									MountPath: GetInContainerCSISocketDir(),
								},
								{
									Name:             "kubernetes-csi-dir",
									MountPath:        GetCSIKubernetesDir(rootDir),
									MountPropagation: &MountPropagationBidirectional,
								},
								{
									Name:             "pods-mount-dir",
									MountPath:        GetCSIPodsDir(rootDir),
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
									Name:             "host",
									MountPath:        "/rootfs", // path is required for namespaced mounter
									MountPropagation: &MountPropagationBidirectional,
								},
								{
									Name:      "lib-modules",
									MountPath: "/lib/modules",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "kubernetes-csi-dir",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: GetCSIKubernetesDir(rootDir),
									Type: &HostPathDirectoryOrCreate,
								},
							},
						},
						{
							Name: "registration-dir",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: GetCSIRegistrationDir(rootDir),
									Type: &HostPathDirectoryOrCreate,
								},
							},
						},
						{
							Name: "socket-dir",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: GetCSISocketDir(rootDir),
									Type: &HostPathDirectoryOrCreate,
								},
							},
						},
						{
							Name: "pods-mount-dir",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: GetCSIPodsDir(rootDir),
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
							Name: "host",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/",
								},
							},
						},
						{
							Name: "lib-modules",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/lib/modules",
								},
							},
						},
					},
				},
			},
		},
	}

	if registrySecret != "" {
		daemonSet.Spec.Template.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
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

type DriverObjectDeployment struct {
	obj *storagev1.CSIDriver
}

func NewCSIDriverObject() *DriverObjectDeployment {
	falseFlag := true
	obj := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name: types.LonghornDriverName,
		},
		Spec: storagev1.CSIDriverSpec{
			PodInfoOnMount: &falseFlag,
		},
	}
	return &DriverObjectDeployment{
		obj: obj,
	}
}

func (d *DriverObjectDeployment) Deploy(kubeClient *clientset.Clientset) error {
	return deploy(kubeClient, d.obj, "CSI Driver",
		csiDriverObjectCreateFunc, csiDriverObjectDeleteFunc, csiDriverObjectGetFunc)
}

func (d *DriverObjectDeployment) Cleanup(kubeClient *clientset.Clientset) {
	if err := cleanup(kubeClient, d.obj, "CSI Driver",
		csiDriverObjectDeleteFunc, csiDriverObjectGetFunc); err != nil {
		logrus.Warnf("Failed to cleanup CSI Driver object in CSI Driver object deployment: %v", err)
	}
}
