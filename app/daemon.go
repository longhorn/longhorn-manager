package app

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof" // for runtime profiling
	"os"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/pkg/errors"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/go-iscsi-helper/iscsi"

	iscsiutil "github.com/longhorn/go-iscsi-helper/util"

	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"

	"github.com/longhorn/longhorn-manager/api"
	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/meta"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/upgrade"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/util/client"
	"github.com/longhorn/longhorn-manager/webhook"

	metricscollector "github.com/longhorn/longhorn-manager/metrics_collector"
	recoverybackend "github.com/longhorn/longhorn-manager/recovery_backend"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	FlagEngineImage               = "engine-image"
	FlagInstanceManagerImage      = "instance-manager-image"
	FlagShareManagerImage         = "share-manager-image"
	FlagBackingImageManagerImage  = "backing-image-manager-image"
	FlagManagerImage              = "manager-image"
	FlagSupportBundleManagerImage = "support-bundle-manager-image"
	FlagServiceAccount            = "service-account"
	FlagKubeConfig                = "kube-config"
	FlagUpgradeVersionCheck       = "upgrade-version-check"
)

func DaemonCmd() cli.Command {
	return cli.Command{
		Name: "daemon",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagEngineImage,
				Usage: "Specify Longhorn engine image",
			},
			cli.StringFlag{
				Name:  FlagInstanceManagerImage,
				Usage: "Specify Longhorn instance manager image",
			},
			cli.StringFlag{
				Name:  FlagShareManagerImage,
				Usage: "Specify Longhorn share manager image",
			},
			cli.StringFlag{
				Name:  FlagBackingImageManagerImage,
				Usage: "Specify Longhorn backing image manager image",
			},
			cli.StringFlag{
				Name:  FlagSupportBundleManagerImage,
				Usage: "Specify Longhorn support bundle manager image",
			},
			cli.StringFlag{
				Name:  FlagManagerImage,
				Usage: "Specify Longhorn manager image",
			},
			cli.StringFlag{
				Name:  FlagServiceAccount,
				Usage: "Specify service account for manager",
			},
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
			cli.BoolFlag{
				Name:  FlagUpgradeVersionCheck,
				Usage: "Enforce version checking for upgrades. If disabled, there will be no requirement for the necessary upgrade source version",
			},
		},
		Action: func(c *cli.Context) {
			if err := startManager(c); err != nil {
				logrus.Fatalf("Error starting manager: %v", err)
			}
		},
	}
}

func startManager(c *cli.Context) error {
	var (
		err error
	)

	engineImage := c.String(FlagEngineImage)
	if engineImage == "" {
		return fmt.Errorf("require %v", FlagEngineImage)
	}
	instanceManagerImage := c.String(FlagInstanceManagerImage)
	if instanceManagerImage == "" {
		return fmt.Errorf("require %v", FlagInstanceManagerImage)
	}
	shareManagerImage := c.String(FlagShareManagerImage)
	if shareManagerImage == "" {
		return fmt.Errorf("require %v", FlagShareManagerImage)
	}
	backingImageManagerImage := c.String(FlagBackingImageManagerImage)
	if backingImageManagerImage == "" {
		return fmt.Errorf("require %v", FlagBackingImageManagerImage)
	}
	supportBundleManagerImage := c.String(FlagSupportBundleManagerImage)
	if supportBundleManagerImage == "" {
		return fmt.Errorf("require %v", FlagSupportBundleManagerImage)
	}
	managerImage := c.String(FlagManagerImage)
	if managerImage == "" {
		return fmt.Errorf("require %v", FlagManagerImage)
	}
	serviceAccount := c.String(FlagServiceAccount)
	if serviceAccount == "" {
		return fmt.Errorf("require %v", FlagServiceAccount)
	}
	kubeconfigPath := c.String(FlagKubeConfig)

	currentNodeID, err := util.GetRequiredEnv(types.EnvNodeName)
	if err != nil {
		return fmt.Errorf("failed to detect the node name")
	}

	if err := environmentCheck(kubeconfigPath, currentNodeID); err != nil {
		return errors.Wrap(err, "Failed environment check")
	}

	currentIP, err := util.GetRequiredEnv(types.EnvPodIP)
	if err != nil {
		return fmt.Errorf("failed to detect the node IP")
	}

	ctx := signals.SetupSignalContext()

	logger := logrus.StandardLogger().WithField("node", currentNodeID)

	// Conversion webhook needs to be started first since we use its port 9501 as readiness port.
	// longhorn-manager pod becomes ready only when conversion webhook is running.
	// The services in the longhorn-manager can then start to receive the requests.
	// Conversion webhook does not longhorn datastore.
	clientsWithoutDatastore, err := client.NewClients(kubeconfigPath, false, ctx.Done())
	if err != nil {
		return err
	}
	if err := webhook.StartWebhook(ctx, types.WebhookTypeConversion, clientsWithoutDatastore); err != nil {
		return err
	}
	if err := webhook.CheckWebhookServiceAvailability(types.WebhookTypeConversion); err != nil {
		return err
	}

	clients, err := client.NewClients(kubeconfigPath, true, ctx.Done())
	if err != nil {
		return err
	}
	if err := webhook.StartWebhook(ctx, types.WebhookTypeAdmission, clients); err != nil {
		return err
	}
	if err := webhook.CheckWebhookServiceAvailability(types.WebhookTypeAdmission); err != nil {
		return err
	}

	if err := recoverybackend.StartRecoveryBackend(clients); err != nil {
		return err
	}

	if err := upgrade.Upgrade(kubeconfigPath, currentNodeID, managerImage, c.Bool(FlagUpgradeVersionCheck)); err != nil {
		return err
	}

	proxyConnCounter := util.NewAtomicCounter()

	wsc, err := controller.StartControllers(logger, clients,
		currentNodeID, serviceAccount, managerImage, backingImageManagerImage, shareManagerImage,
		kubeconfigPath, meta.Version, proxyConnCounter)
	if err != nil {
		return err
	}

	m := manager.NewVolumeManager(currentNodeID, clients.Datastore, proxyConnCounter)

	metricscollector.InitMetricsCollectorSystem(logger, currentNodeID, clients.Datastore, kubeconfigPath, proxyConnCounter)

	defaultImageSettings := map[types.SettingName]string{
		types.SettingNameDefaultEngineImage:              engineImage,
		types.SettingNameDefaultInstanceManagerImage:     instanceManagerImage,
		types.SettingNameDefaultBackingImageManagerImage: backingImageManagerImage,
		types.SettingNameSupportBundleManagerImage:       supportBundleManagerImage,
	}
	if err := clients.Datastore.UpdateCustomizedSettings(defaultImageSettings); err != nil {
		return err
	}

	if err := updateRegistrySecretName(m); err != nil {
		return err
	}

	if err := initDaemonNode(clients.Datastore); err != nil {
		return err
	}

	if err := m.DeployEngineImage(engineImage); err != nil {
		return err
	}

	server := api.NewServer(m, wsc)
	router := http.Handler(api.NewRouter(server))
	router = util.FilteredLoggingHandler(map[string]struct{}{
		"/v1/apiversions":  {},
		"/v1/schemas":      {},
		"/v1/settings":     {},
		"/v1/volumes":      {},
		"/v1/nodes":        {},
		"/v1/engineimages": {},
		"/v1/events":       {},
	}, os.Stdout, router)
	router = handlers.ProxyHeaders(router)

	listen := types.GetAPIServerAddressFromIP(currentIP)
	logger.Infof("Listening on %s", listen)

	go func() {
		if err := http.ListenAndServe(listen, router); err != nil {
			logrus.Fatalf("Error longhorn backend server failed: %v", err)
		}
	}()

	go func() {
		debugAddress := "127.0.0.1:6060"
		debugHandler := http.DefaultServeMux
		logger.Infof("Debug Server listening on %s", debugAddress)
		if err := http.ListenAndServe(debugAddress, debugHandler); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf(fmt.Sprintf("ListenAndServe: %s", err))
		}
	}()

	<-ctx.Done()
	return nil
}

func environmentCheck(kubeconfigPath, nodeID string) error {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	// Check if mount propagation is supported
	if err := checkMountPropagation(kubeClient, namespace, nodeID); err != nil {
		return err
	}

	kubeNode, err := kubeClient.CoreV1().Nodes().Get(context.Background(), nodeID, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get node when checking kernel version")
	}

	// Check if kernel version is recommended
	if err := checkKernelVersion(kubeNode); err != nil {
		return err
	}

	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}
	// Check if necessary packages are installed
	if err := checkPackagesInstalled(kubeNode, namespaces); err != nil {
		return err
	}

	// Check if multipath is supported
	if err := checkMultipathd(namespaces, nodeID); err != nil {
		return err
	}

	// Check if nfs client versions are supported
	if err := checkNFSClientVersion(kubeNode, namespaces); err != nil {
		return err
	}

	nsexec, err := lhns.NewNamespaceExecutor(iscsiutil.ISCSIdProcess, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return err
	}

	return iscsi.CheckForInitiatorExistence(nsexec)
}

func checkMountPropagation(kubeClient *clientset.Clientset, namespace, nodeID string) error {

	managerPodsList, err := upgradeutil.ListManagerPods(namespace, kubeClient)
	if err != nil {
		return err
	}

	mountPropagationBidirectionalChecked := false
	dataMountPointName := "longhorn"

CHECKED:
	for _, pod := range managerPodsList {
		if pod.Spec.NodeName != nodeID {
			continue
		}
		for _, container := range pod.Spec.Containers {
			for _, volumeMount := range container.VolumeMounts {
				if volumeMount.Name != dataMountPointName {
					continue
				}
				if volumeMount.MountPropagation != nil && *volumeMount.MountPropagation == corev1.MountPropagationBidirectional {
					mountPropagationBidirectionalChecked = true
					break CHECKED
				}

			}
		}
	}
	if !mountPropagationBidirectionalChecked {
		return fmt.Errorf("failed to check mount propagation, please make sure the mount propagation is set to %v for the volume mount %v in the longhorn-manager pod spec", corev1.MountPropagationBidirectional, dataMountPointName)
	}

	return nil
}

func checkKernelVersion(kubeNode *corev1.Node) error {
	kernelVersion := kubeNode.Status.NodeInfo.KernelVersion
	brokenKernelVersion := []string{"5.15.0", "6.5.6"}
	majorKernel5150Version := 5
	brokenKernel5150PatchVersion := 94
	fixedKernel5150PatchVersion := 100

	for _, version := range brokenKernelVersion {
		if !strings.Contains(kernelVersion, version) {
			continue
		}
		var kernel, major, minor, patch int
		if _, err := fmt.Sscanf(kernelVersion, "%d.%d.%d-%d", &kernel, &major, &minor, &patch); err != nil {
			return errors.Wrap(err, "failed to parse kernel version "+kernelVersion)
		}
		if kernel == majorKernel5150Version && (fixedKernel5150PatchVersion <= patch || patch < brokenKernel5150PatchVersion) {
			break
		}
		logrus.Warnf("Node %v has a kernel version %v known to have a breakage that affects Longhorn. See description and solution at https://longhorn.io/kb/troubleshooting-rwx-volume-fails-to-attached-caused-by-protocol-not-supported", kubeNode.Name, kernelVersion)
	}
	return nil
}

func checkPackagesInstalled(kubeNode *corev1.Node, namespaces []lhtypes.Namespace) error {
	osImage := strings.ToLower(kubeNode.Status.NodeInfo.OSImage)
	queryPackagesCmd := ""
	packages := []string{}
	switch {
	case strings.Contains(osImage, "ubuntu"):
	case strings.Contains(osImage, "debian"):
		queryPackagesCmd = "dpkg -l | grep -w"
		packages = []string{"nfs-common", "open-iscsi", "cryptsetup", "dmsetup"}
	case strings.Contains(osImage, "centos"):
	case strings.Contains(osImage, "fedora"):
	case strings.Contains(osImage, "rocky"):
	case strings.Contains(osImage, "ol"):
		queryPackagesCmd = "rpm -q"
		packages = []string{"nfs-utils", "iscsi-initiator-utils", "cryptsetup", "device-mapper"}
	case strings.Contains(osImage, "suse"):
		queryPackagesCmd = "rpm -q"
		packages = []string{"nfs-client", "open-iscsi", "cryptsetup", "device-mapper"}
	case strings.Contains(osImage, "arch"):
		queryPackagesCmd = "pacman -Q"
		packages = []string{"nfs-utils", "open-iscsi", "cryptsetup", "device-mapper"}
	case strings.Contains(osImage, "gentoo"):
		queryPackagesCmd = "qlist -I"
		packages = []string{"net-fs/nfs-utils", "sys-block/open-iscsi", "sys-fs/cryptsetup", "sys-fs/lvm2"}
	case strings.Contains(osImage, "Talos"):
		logrus.Warnf("The OS %v is installed on this node %v, please make sure the necessary packages are installed", kubeNode.Name, osImage)
		return nil
	default:
		logrus.Warnf("Node %v has an unknown OS image %v, please make sure the necessary packages are installed", kubeNode.Name, osImage)
		return nil
	}

	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return err
	}
	for _, pkg := range packages {
		args := []string{pkg}
		if _, err := nsexec.Execute(nil, queryPackagesCmd, args, lhtypes.ExecuteDefaultTimeout); err != nil {
			logrus.WithError(err).Warnf("Package %v is not found in node %v", pkg, kubeNode.Name)
		}
	}

	return nil
}

func checkMultipathd(namespaces []lhtypes.Namespace, nodeID string) error {
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return err
	}
	args := []string{"multipathd", "status"}
	if result, _ := nsexec.Execute(nil, "service", args, lhtypes.ExecuteDefaultTimeout); result != "" {
		logrus.Warnf("Multipathd is running on %v, result: %+v", nodeID, result)
		logrus.Warnf("Multipathd is running on %v known to have a breakage that affects Longhorn. See description and solution at https://longhorn.io/kb/troubleshooting-volume-with-multipath", nodeID)
	}

	return nil
}

func checkNFSClientVersion(kubeNode *corev1.Node, namespaces []lhtypes.Namespace) error {
	configPath := "/boot"
	kernelVersion := kubeNode.Status.NodeInfo.KernelVersion
	nfsClientVersions := []string{"CONFIG_NFS_V4_2", "CONFIG_NFS_V4_1", "CONFIG_NFS_V4"}

	files, err := lhns.ReadDirectory(configPath)
	if err != nil {
		return err
	}

	kernelConfigPath := "/boot/config-" + kernelVersion
	configFileIsFound := false
	for _, file := range files {
		if file.Name() == kernelConfigPath {
			configFileIsFound = true
			break
		}
	}
	if !configFileIsFound {
		logrus.Warnf("Kernel config %v not found on node %v", kernelConfigPath, kubeNode.Name)
		return nil
	}

	fileContentString, err := lhns.ReadFileContent(kernelConfigPath)
	if err != nil {
		return err
	}

	fileContents := strings.Split(fileContentString, "\n")
	var nfsConfigContents []string
	for _, config := range fileContents {
		for _, ver := range nfsClientVersions {
			if strings.Contains(config, ver) {
				nfsConfigContents = append(nfsConfigContents, config)
			}
		}

	}
	for _, config := range nfsConfigContents {
		enabled := strings.TrimSpace(strings.Split(config, "=")[1])
		switch enabled {
		case "y":
			return nil
		case "m":
			modulesContentString, err := lhns.ReadFileContent("/proc/modules")
			if err != nil {
				return err
			}
			if strings.Contains(modulesContentString, "nfs") {
				return nil
			}
			logrus.Warnf("%v is not enabled on node %v", config, kubeNode.Name)
		default:
			logrus.Warnf("Unknown kernel config value for %v: %v", config, enabled)
		}
	}

	logrus.Warnf("NFS clients %v not found. At least one should be enabled", nfsClientVersions)
	return nil
}

func updateRegistrySecretName(m *manager.VolumeManager) error {
	settingRegistrySecret, err := m.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return err
	}
	managerDaemonSet, err := m.GetDaemonSetRO(types.LonghornManagerDaemonSetName)
	if err != nil {
		return err
	}
	registrySecretName := ""
	if len(managerDaemonSet.Spec.Template.Spec.ImagePullSecrets) > 0 {
		registrySecretName = managerDaemonSet.Spec.Template.Spec.ImagePullSecrets[0].Name
	}
	if settingRegistrySecret.Value != registrySecretName {
		settingRegistrySecret.Value = registrySecretName
		if _, err := m.CreateOrUpdateSetting(settingRegistrySecret); err != nil {
			return err
		}
	}
	return nil
}

func initDaemonNode(ds *datastore.DataStore) error {
	nodeName := os.Getenv("NODE_NAME")
	if _, err := ds.GetNode(nodeName); err != nil {
		// init default disk on node when starting longhorn-manager
		if datastore.ErrorIsNotFound(err) {
			if _, err = ds.CreateDefaultNode(nodeName); err != nil {
				return err
			}
		}
		return err
	}
	return nil
}
