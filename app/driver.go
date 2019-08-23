package app

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/util/version"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/csi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	LonghornFlexvolumeDriver = "longhorn-flexvolume-driver"

	EnvFlexvolumeDir  = "FLEXVOLUME_DIR"
	EnvKubeletRootDir = "KUBELET_ROOT_DIR"

	FlagManagerURL = "manager-url"

	FlagDriver           = "driver"
	FlagDriverCSI        = "csi"
	FlagDriverFlexvolume = "flexvolume"

	FlagFlexvolumeDir  = "flexvolume-dir"
	FlagKubeletRootDir = "kubelet-root-dir"

	FlagCSIAttacherImage        = "csi-attacher-image"
	FlagCSIProvisionerImage     = "csi-provisioner-image"
	FlagCSIDriverRegistrarImage = "csi-driver-registrar-image"
	FlagCSIProvisionerName      = "csi-provisioner-name"
	EnvCSIAttacherImage         = "CSI_ATTACHER_IMAGE"
	EnvCSIProvisionerImage      = "CSI_PROVISIONER_IMAGE"
	EnvCSIDriverRegistrarImage  = "CSI_DRIVER_REGISTRAR_IMAGE"
	EnvCSIProvisionerName       = "CSI_PROVISIONER_NAME"

	FlagCSIAttacherReplicaCount    = "csi-attacher-replica-count"
	FlagCSIProvisionerReplicaCount = "csi-provisioner-replica-count"
	EnvCSIAttacherReplicaCount     = "CSI_ATTACHER_REPLICA_COUNT"
	EnvCSIProvisionerReplicaCount  = "CSI_PROVISIONER_REPLICA_COUNT"
)

func DeployDriverCmd() cli.Command {
	return cli.Command{
		Name: "deploy-driver",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagDriver,
				Usage: "Specify the driver, choices are: flexvolume, csi. default option will deploy CSI for Kubernetes v1.10+, Flexvolume for Kubernetes v1.8 and v1.9.",
			},
			cli.StringFlag{
				Name:  FlagManagerImage,
				Usage: "Specify Longhorn manager image",
			},
			cli.StringFlag{
				Name:  FlagManagerURL,
				Usage: "Longhorn manager API URL",
			},
			cli.StringFlag{
				Name:   FlagFlexvolumeDir,
				Usage:  "Specify the location of flexvolume plugin for Kubernetes on the host",
				EnvVar: EnvFlexvolumeDir,
			},
			cli.StringFlag{
				Name:   FlagKubeletRootDir,
				Usage:  "Specify the root directory of kubelet for csi components (optional)",
				EnvVar: EnvKubeletRootDir,
			},
			cli.StringFlag{
				Name:   FlagCSIAttacherImage,
				Usage:  "Specify CSI attacher image",
				EnvVar: EnvCSIAttacherImage,
				Value:  csi.DefaultCSIAttacherImage,
			},
			cli.IntFlag{
				Name:   FlagCSIAttacherReplicaCount,
				Usage:  "Specify number of CSI attacher replicas",
				EnvVar: EnvCSIAttacherReplicaCount,
				Value:  csi.DefaultCSIAttacherReplicaCount,
			},
			cli.StringFlag{
				Name:   FlagCSIProvisionerImage,
				Usage:  "Specify CSI provisioner image",
				EnvVar: EnvCSIProvisionerImage,
				Value:  csi.DefaultCSIProvisionerImage,
			},
			cli.IntFlag{
				Name:   FlagCSIProvisionerReplicaCount,
				Usage:  "Specify number of CSI provisioner replicas",
				EnvVar: EnvCSIProvisionerReplicaCount,
				Value:  csi.DefaultCSIProvisionerReplicaCount,
			},
			cli.StringFlag{
				Name:   FlagCSIDriverRegistrarImage,
				Usage:  "Specify CSI driver-registrar image",
				EnvVar: EnvCSIDriverRegistrarImage,
				Value:  csi.DefaultCSIDriverRegistrarImage,
			},
			cli.StringFlag{
				Name:   FlagCSIProvisionerName,
				Usage:  "Specify CSI provisioner name",
				EnvVar: EnvCSIProvisionerName,
				Value:  csi.DefaultCSIProvisionerName,
			},
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
		},
		Action: func(c *cli.Context) {
			if err := deployDriver(c); err != nil {
				logrus.Fatalf("Error deploying driver: %v", err)
			}
		},
	}
}

func deployDriver(c *cli.Context) error {
	csi.VERSION = VERSION

	managerImage := c.String(FlagManagerImage)
	if managerImage == "" {
		return fmt.Errorf("require %v", FlagManagerImage)
	}
	managerURL := c.String(FlagManagerURL)
	if managerURL == "" {
		return fmt.Errorf("require %v", FlagManagerURL)
	}

	config, err := clientcmd.BuildConfigFromFlags("", c.String(FlagKubeConfig))
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get clientset")
	}

	driverDetected := false
	driver := c.String(FlagDriver)
	if driver == "" {
		driverDetected = true
		driver, err = chooseDriver(kubeClient)
		logrus.Debugf("Driver %s will be used after automatic detection", driver)
		if err != nil {
			return err
		}
	} else {
		logrus.Debugf("User specified the driver %s", driver)
	}

	if driver == FlagDriverCSI {
		err := csi.CheckMountPropagationWithNode(managerURL)
		if err != nil {
			logrus.Warnf("Got an error when checking MountPropagation with node status, %v", err)
			if driverDetected {
				logrus.Infof("MountPropagation check failed, fall back to use the Flexvolume")
				driver = FlagDriverFlexvolume
			} else {
				// if user explicitly choose CSI but we cannot deploy.
				// In this case we should error out instead.
				return fmt.Errorf("CSI cannot be deployed because MountPropagation is not set on kubelet and api-server")
			}
		}
	}

	switch driver {
	case FlagDriverCSI:
		logrus.Debug("Deploying CSI driver")
		err = deployCSIDriver(kubeClient, lhClient, c, managerImage, managerURL)
	case FlagDriverFlexvolume:
		logrus.Debug("Deploying Flexvolume driver")
		err = deployFlexvolumeDriver(kubeClient, c, managerImage, managerURL)
	default:
		return fmt.Errorf("Unsupported driver %s", driver)
	}

	return err
}

// chooseDriver can chose the right driver by k8s server version
// 1.10+ csi
// v1.8/1.9 flexvolume
func chooseDriver(kubeClient *clientset.Clientset) (string, error) {
	csiVersionMet, err := isKubernetesVersionAtLeast(kubeClient, types.CSIMinVersion)
	if err != nil {
		return "", errors.Wrap(err, "cannot choose driver automatically")
	}
	if csiVersionMet {
		return FlagDriverCSI, nil
	}
	return FlagDriverFlexvolume, nil
}

func isKubernetesVersionAtLeast(kubeClient *clientset.Clientset, vers string) (bool, error) {
	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return false, errors.Wrap(err, "failed to get Kubernetes server version")
	}
	currentVersion := version.MustParseSemantic(serverVersion.GitVersion)
	minVersion := version.MustParseSemantic(vers)
	return currentVersion.AtLeast(minVersion), nil
}

func deployCSIDriver(kubeClient *clientset.Clientset, lhClient *lhclientset.Clientset, c *cli.Context, managerImage, managerURL string) error {
	csiAttacherImage := c.String(FlagCSIAttacherImage)
	csiProvisionerImage := c.String(FlagCSIProvisionerImage)
	csiDriverRegistrarImage := c.String(FlagCSIDriverRegistrarImage)
	csiProvisionerName := c.String(FlagCSIProvisionerName)
	csiAttacherReplicaCount := c.Int(FlagCSIAttacherReplicaCount)
	csiProvisionerReplicaCount := c.Int(FlagCSIProvisionerReplicaCount)
	namespace := os.Getenv(types.EnvPodNamespace)
	serviceAccountName := os.Getenv(types.EnvServiceAccount)

	rootDir := c.String(FlagKubeletRootDir)
	if rootDir == "" {
		var err error
		rootDir, err = getProcArg(kubeClient, managerImage, serviceAccountName, ArgKubeletRootDir)
		if err != nil {
			logrus.Error(err)
			return err
		}
		logrus.Infof("Detected root dir path: %v", rootDir)
	} else {
		logrus.Infof("User specified root dir: %v", rootDir)
	}

	kubeletPluginWatcherEnabled, err := isKubernetesVersionAtLeast(kubeClient, types.KubeletPluginWatcherMinVersion)
	if err != nil {
		return err
	}

	if err := handleCSIUpgrade(kubeClient, namespace); err != nil {
		return err
	}

	setting, err := lhClient.LonghornV1alpha1().Settings(namespace).Get(string(types.SettingNameTaintToleration), metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "failed to get taint toleration setting before starting CSI driver")
	}
	tolerations, err := util.UnmarshalTolerationSetting(setting.Value)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal taint toleration setting before starting CSI driver")
	}

	attacherDeployment := csi.NewAttacherDeployment(namespace, serviceAccountName, csiAttacherImage, rootDir, csiAttacherReplicaCount, tolerations)
	if err := attacherDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	provisionerDeployment := csi.NewProvisionerDeployment(namespace, serviceAccountName, csiProvisionerImage, csiProvisionerName, rootDir, csiProvisionerReplicaCount, tolerations)
	if err := provisionerDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	pluginDeployment := csi.NewPluginDeployment(namespace, serviceAccountName, csiDriverRegistrarImage, managerImage, managerURL, rootDir, kubeletPluginWatcherEnabled, tolerations)
	if err := pluginDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	logrus.Debug("CSI deployment done")

	done := make(chan struct{})
	util.RegisterShutdownChannel(done)

	<-done

	return nil
}

func handleCSIUpgrade(kubeClient *clientset.Clientset, namespace string) error {
	// Upgrade from v0.3.x to v0.4.0, remove the existing attacher/provisioner statefulsets
	statefulSets, err := kubeClient.AppsV1beta2().StatefulSets(namespace).List(metav1.ListOptions{})
	if err != nil {
		// no existing statefulset needs to be cleaned up
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	for _, s := range statefulSets.Items {
		if (s.Name == types.CSIAttacherName || s.Name == types.CSIProvisionerName) && s.DeletionTimestamp == nil {
			propagation := metav1.DeletePropagationForeground
			if err := kubeClient.AppsV1beta2().StatefulSets(namespace).Delete(
				s.Name, &metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil {
				return err
			}
			logrus.Warnf("Statefulset %v from previous version wasn't cleaned up. Clean it up", s.Name)
		}
	}
	return nil
}

func deployFlexvolumeDriver(kubeClient *clientset.Clientset, c *cli.Context, managerImage, managerURL string) error {
	serviceAccountName := os.Getenv(types.EnvServiceAccount)
	flexvolumeDir := c.String(FlagFlexvolumeDir)
	if flexvolumeDir == "" {
		var err error
		flexvolumeDir, err = getProcArg(kubeClient, managerImage, serviceAccountName, ArgFlexvolumePluginDir)
		if err != nil {
			logrus.Error(err)
			return err
		}
		logrus.Infof("Detected flexvolume dir path: %v", flexvolumeDir)
	} else {
		logrus.Infof("User specified Flexvolume dir path: %v", flexvolumeDir)
	}

	dsOps, err := newDaemonSetOps(kubeClient)
	if err != nil {
		return err
	}

	d, err := dsOps.Get(LonghornFlexvolumeDriver)
	if err != nil {
		return err
	}
	if d != nil {
		if err := dsOps.Delete(LonghornFlexvolumeDriver); err != nil {
			return err
		}
	}
	logrus.Infof("Install Flexvolume to Kubernetes nodes directory %v", flexvolumeDir)
	if _, err := dsOps.Create(LonghornFlexvolumeDriver, getFlexvolumeDaemonSetSpec(managerImage, flexvolumeDir)); err != nil {
		return err
	}
	defer func() {
		if err := dsOps.Delete(LonghornFlexvolumeDriver); err != nil {
			logrus.Warnf("Fail to cleanup %v: %v", LonghornFlexvolumeDriver, err)
		}
	}()

	done := make(chan struct{})
	util.RegisterShutdownChannel(done)

	if err = startProvisioner(kubeClient, managerURL); err != nil {
		return err
	}
	<-done
	logrus.Debug("Stop the built-in Longhorn provisioner")

	return nil
}

func getFlexvolumeDaemonSetSpec(image, flexvolumeDir string) *appsv1beta2.DaemonSet {
	cmd := []string{
		"/entrypoint.sh",
	}
	privilege := true
	d := &appsv1beta2.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: LonghornFlexvolumeDriver,
		},
		Spec: appsv1beta2.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": LonghornFlexvolumeDriver,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: LonghornFlexvolumeDriver,
					Labels: map[string]string{
						"app": LonghornFlexvolumeDriver,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    LonghornFlexvolumeDriver,
							Image:   image,
							Command: cmd,
							SecurityContext: &v1.SecurityContext{
								Privileged: &privilege,
							},
							ImagePullPolicy: v1.PullAlways,
							Env: []v1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name:  "LONGHORN_BACKEND_SVC",
									Value: "longhorn-backend",
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "flexvolume-longhorn-mount",
									MountPath: "/flexmnt",
								},
								{
									Name:      "usr-local-bin-mount",
									MountPath: "/binmnt",
								},
								{
									Name:      "host-proc-mount",
									MountPath: "/host/proc",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "flexvolume-longhorn-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: flexvolumeDir,
								},
							},
						},
						{
							Name: "host-proc-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/proc",
								},
							},
						},
						{
							Name: "usr-local-bin-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/usr/local/bin",
								},
							},
						},
					},
				},
			},
		},
	}
	return d
}

func startProvisioner(kubeClient *clientset.Clientset, managerURL string) error {
	logrus.Debug("Enable the built-in Longhorn provisioner only for FlexVolume")

	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return errors.Wrap(err, "Cannot start Provisioner: failed to initialize Longhorn API client")
	}

	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return errors.Wrap(err, "Cannot start Provisioner: failed to get Kubernetes server version")
	}
	provisioner := controller.NewProvisioner(apiClient)
	go pvController.NewProvisionController(
		kubeClient,
		controller.LonghornProvisionerName,
		provisioner,
		serverVersion.GitVersion,
	).Run(nil)

	return nil
}

type DaemonSetOps struct {
	namespace  string
	kubeClient *clientset.Clientset
}

func newDaemonSetOps(kubeClient *clientset.Clientset) (*DaemonSetOps, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}
	return &DaemonSetOps{
		namespace, kubeClient,
	}, nil
}

func (ops *DaemonSetOps) Get(name string) (*appsv1beta2.DaemonSet, error) {
	d, err := ops.kubeClient.AppsV1beta2().DaemonSets(ops.namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return d, nil
}

func (ops *DaemonSetOps) Create(name string, d *appsv1beta2.DaemonSet) (*appsv1beta2.DaemonSet, error) {
	return ops.kubeClient.AppsV1beta2().DaemonSets(ops.namespace).Create(d)
}

func (ops *DaemonSetOps) Delete(name string) error {
	propagation := metav1.DeletePropagationForeground
	return ops.kubeClient.AppsV1beta2().DaemonSets(ops.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}
