package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	v1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/longhorn/longhorn-manager/csi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	EnvKubeletRootDir = "KUBELET_ROOT_DIR"

	FlagManagerURL = "manager-url"

	FlagKubeletRootDir = "kubelet-root-dir"

	FlagCSIAttacherImage            = "csi-attacher-image"
	FlagCSIProvisionerImage         = "csi-provisioner-image"
	FlagCSIResizerImage             = "csi-resizer-image"
	FlagCSISnapshotterImage         = "csi-snapshotter-image"
	FlagCSINodeDriverRegistrarImage = "csi-node-driver-registrar-image"
	FlagCSILivenessProbeImage       = "csi-liveness-probe-image"
	EnvCSIAttacherImage             = "CSI_ATTACHER_IMAGE"
	EnvCSIProvisionerImage          = "CSI_PROVISIONER_IMAGE"
	EnvCSIResizerImage              = "CSI_RESIZER_IMAGE"
	EnvCSISnapshotterImage          = "CSI_SNAPSHOTTER_IMAGE"
	EnvCSINodeDriverRegistrarImage  = "CSI_NODE_DRIVER_REGISTRAR_IMAGE"
	EnvCSILivenessProbeImage        = "CSI_LIVENESS_PROBE_IMAGE"

	FlagCSIAttacherReplicaCount    = "csi-attacher-replica-count"
	FlagCSIProvisionerReplicaCount = "csi-provisioner-replica-count"
	FlagCSIResizerReplicaCount     = "csi-resizer-replica-count"
	FlagCSISnapshotterReplicaCount = "csi-snapshotter-replica-count"
	EnvCSIAttacherReplicaCount     = "CSI_ATTACHER_REPLICA_COUNT"
	EnvCSIProvisionerReplicaCount  = "CSI_PROVISIONER_REPLICA_COUNT"
	EnvCSIResizerReplicaCount      = "CSI_RESIZER_REPLICA_COUNT"
	EnvCSISnapshotterReplicaCount  = "CSI_SNAPSHOTTER_REPLICA_COUNT"
)

func DeployDriverCmd() cli.Command {
	return cli.Command{
		Name: "deploy-driver",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagManagerImage,
				Usage: "Specify Longhorn manager image",
			},
			cli.StringFlag{
				Name:  FlagManagerURL,
				Usage: "Longhorn manager API URL",
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
				Name:   FlagCSIResizerImage,
				Usage:  "Specify CSI resizer image",
				EnvVar: EnvCSIResizerImage,
				Value:  csi.DefaultCSIResizerImage,
			},
			cli.IntFlag{
				Name:   FlagCSIResizerReplicaCount,
				Usage:  "Specify number of CSI resizer replicas",
				EnvVar: EnvCSIResizerReplicaCount,
				Value:  csi.DefaultCSIResizerReplicaCount,
			},
			cli.StringFlag{
				Name:   FlagCSISnapshotterImage,
				Usage:  "Specify CSI snapshotter image",
				EnvVar: EnvCSISnapshotterImage,
				Value:  csi.DefaultCSISnapshotterImage,
			},
			cli.IntFlag{
				Name:   FlagCSISnapshotterReplicaCount,
				Usage:  "Specify number of CSI snapshotter replicas",
				EnvVar: EnvCSISnapshotterReplicaCount,
				Value:  csi.DefaultCSISnapshotterReplicaCount,
			},
			cli.StringFlag{
				Name:   FlagCSINodeDriverRegistrarImage,
				Usage:  "Specify CSI node-driver-registrar image",
				EnvVar: EnvCSINodeDriverRegistrarImage,
				Value:  csi.DefaultCSINodeDriverRegistrarImage,
			},
			cli.StringFlag{
				Name:   FlagCSILivenessProbeImage,
				Usage:  "Specify CSI liveness probe image",
				EnvVar: EnvCSILivenessProbeImage,
				Value:  csi.DefaultCSILivenessProbeImage,
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

	if err := checkKubernetesVersion(kubeClient); err != nil {
		return errors.Wrap(err, "cannot start driver due to failed Kubernetes version check")
	}

	if err := csi.CheckMountPropagationWithNode(managerURL); err != nil {
		logrus.Warnf("Got an error when checking MountPropagation with node status, %v", err)
		return errors.Wrap(err, "CSI cannot be deployed because MountPropagation is not set")
	}

	logrus.Debug("Deploying CSI driver")
	return deployCSIDriver(kubeClient, lhClient, c, managerImage, managerURL)
}

func checkKubernetesVersion(kubeClient *clientset.Clientset) error {
	serverVersion, err := kubeClient.Discovery().ServerVersion()
	if err != nil {
		return errors.Wrap(err, "failed to get Kubernetes server version")
	}
	currentVersion := version.MustParseSemantic(serverVersion.GitVersion)
	minVersion := version.MustParseSemantic(types.KubernetesMinVersion)
	if !currentVersion.AtLeast(minVersion) {
		return fmt.Errorf("kubernetes version need to be at least %v, but it's %v", types.KubernetesMinVersion, serverVersion.GitVersion)
	}
	return nil
}

func deployCSIDriver(kubeClient *clientset.Clientset, lhClient *lhclientset.Clientset, c *cli.Context, managerImage, managerURL string) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to start CSI driver")
	}()
	csiAttacherImage := c.String(FlagCSIAttacherImage)
	csiProvisionerImage := c.String(FlagCSIProvisionerImage)
	csiResizerImage := c.String(FlagCSIResizerImage)
	csiSnapshotterImage := c.String(FlagCSISnapshotterImage)
	csiNodeDriverRegistrarImage := c.String(FlagCSINodeDriverRegistrarImage)
	csiLivenessProbeImage := c.String(FlagCSILivenessProbeImage)
	csiAttacherReplicaCount := c.Int(FlagCSIAttacherReplicaCount)
	csiProvisionerReplicaCount := c.Int(FlagCSIProvisionerReplicaCount)
	csiSnapshotterReplicaCount := c.Int(FlagCSISnapshotterReplicaCount)
	csiResizerReplicaCount := c.Int(FlagCSIResizerReplicaCount)
	namespace := os.Getenv(types.EnvPodNamespace)
	serviceAccountName := os.Getenv(types.EnvServiceAccount)
	rootDir := c.String(FlagKubeletRootDir)

	tolerationSetting, err := lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNameTaintToleration), metav1.GetOptions{})
	if err != nil {
		return err
	}
	tolerations, err := types.UnmarshalTolerations(tolerationSetting.Value)
	if err != nil {
		return err
	}
	tolerationsByte, err := json.Marshal(tolerations)
	if err != nil {
		return err
	}

	nodeSelectorSetting, err := lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNameSystemManagedComponentsNodeSelector), metav1.GetOptions{})
	if err != nil {
		return err
	}
	nodeSelector, err := types.UnmarshalNodeSelector(nodeSelectorSetting.Value)
	if err != nil {
		return err
	}

	priorityClassSetting, err := lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNamePriorityClass), metav1.GetOptions{})
	if err != nil {
		return err
	}
	priorityClass := priorityClassSetting.Value

	registrySecretSetting, err := lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNameRegistrySecret), metav1.GetOptions{})
	if err != nil {
		return err
	}
	registrySecret := registrySecretSetting.Value

	imagePullPolicySetting, err := lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNameSystemManagedPodsImagePullPolicy), metav1.GetOptions{})
	if err != nil {
		return err
	}

	var imagePullPolicy v1.PullPolicy
	switch imagePullPolicySetting.Value {
	case string(types.SystemManagedPodsImagePullPolicyNever):
		imagePullPolicy = v1.PullNever
	case string(types.SystemManagedPodsImagePullPolicyIfNotPresent):
		imagePullPolicy = v1.PullIfNotPresent
	case string(types.SystemManagedPodsImagePullPolicyAlways):
		imagePullPolicy = v1.PullAlways
	default:
		return fmt.Errorf("invalid image pull policy %v", imagePullPolicySetting.Value)
	}

	if rootDir == "" {
		var err error
		rootDir, err = getProcArg(kubeClient, managerImage, serviceAccountName, ArgKubeletRootDir, tolerations, priorityClass, registrySecret, nodeSelector)
		if err != nil {
			logrus.Error(err)
			return err
		}
		logrus.Infof("Detected root dir path: %v", rootDir)
	} else {
		logrus.Infof("User specified root dir: %v", rootDir)
	}

	if err := upgradeLonghornRelatedComponents(kubeClient, namespace); err != nil {
		return err
	}

	csiDriverObjectDeployment := csi.NewCSIDriverObject()
	if err := csiDriverObjectDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	attacherDeployment := csi.NewAttacherDeployment(namespace, serviceAccountName, csiAttacherImage, rootDir, csiAttacherReplicaCount, tolerations, string(tolerationsByte), priorityClass, registrySecret, imagePullPolicy, nodeSelector)
	if err := attacherDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	provisionerDeployment := csi.NewProvisionerDeployment(namespace, serviceAccountName, csiProvisionerImage, rootDir, csiProvisionerReplicaCount, tolerations, string(tolerationsByte), priorityClass, registrySecret, imagePullPolicy, nodeSelector)
	if err := provisionerDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	resizerDeployment := csi.NewResizerDeployment(namespace, serviceAccountName, csiResizerImage, rootDir, csiResizerReplicaCount, tolerations, string(tolerationsByte), priorityClass, registrySecret, imagePullPolicy, nodeSelector)
	if err := resizerDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	snapshotterDeployment := csi.NewSnapshotterDeployment(namespace, serviceAccountName, csiSnapshotterImage, rootDir, csiSnapshotterReplicaCount, tolerations, string(tolerationsByte), priorityClass, registrySecret, imagePullPolicy, nodeSelector)
	if err := snapshotterDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	pluginDeployment := csi.NewPluginDeployment(namespace, serviceAccountName, csiNodeDriverRegistrarImage, csiLivenessProbeImage, managerImage, managerURL, rootDir, tolerations, string(tolerationsByte), priorityClass, registrySecret, imagePullPolicy, nodeSelector)
	if err := pluginDeployment.Deploy(kubeClient); err != nil {
		return err
	}

	logrus.Debug("CSI deployment done")

	done := make(chan struct{})
	util.RegisterShutdownChannel(done)

	<-done

	return nil
}
