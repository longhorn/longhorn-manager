package app

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof" // for runtime profiling
	"os"

	"github.com/gorilla/handlers"
	"github.com/pkg/errors"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	if err := environmentCheck(); err != nil {
		return errors.Wrap(err, "failed to check environment, please make sure you have iscsiadm/open-iscsi installed on the host")
	}

	currentNodeID, err := util.GetRequiredEnv(types.EnvNodeName)
	if err != nil {
		return fmt.Errorf("failed to detect the node name")
	}

	currentIP, err := util.GetRequiredEnv(types.EnvPodIP)
	if err != nil {
		return fmt.Errorf("failed to detect the node IP")
	}

	podName, err := util.GetRequiredEnv(types.EnvPodName)
	if err != nil {
		return fmt.Errorf("failed to detect the manager pod name")
	}

	podNamespace, err := util.GetRequiredEnv(types.EnvPodNamespace)
	if err != nil {
		return fmt.Errorf("failed to detect the manager pod namespace")
	}

	ctx := signals.SetupSignalContext()

	logger := logrus.StandardLogger().WithField("node", currentNodeID)

	// Conversion webhook needs to be started first since we use its port 9501 as readiness port.
	// longhorn-manager pod becomes ready only when conversion webhook is running.
	// The services in the longhorn-manager can then start to receive the requests.
	// Conversion webhook does not use datastore, since it is a prerequisite for
	// datastore operation.
	clientsWithoutDatastore, err := client.NewClients(kubeconfigPath, false, ctx.Done())
	if err != nil {
		return err
	}
	if err := webhook.StartWebhook(ctx, types.WebhookTypeConversion, clientsWithoutDatastore); err != nil {
		return err
	}

	// This adds the label for the conversion webhook's selector.  We do it the hard way without datastore to avoid chicken-and-egg.
	pod, err := clientsWithoutDatastore.Clients.K8s.CoreV1().Pods(podNamespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	labels := types.GetConversionWebhookLabel()
	for key, value := range labels {
		pod.Labels[key] = value
	}
	_, err = clientsWithoutDatastore.Clients.K8s.CoreV1().Pods(podNamespace).Update(context.Background(), pod, metav1.UpdateOptions{})
	if err != nil {
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
	if err := clients.Datastore.AddLabelToManagerPod(currentNodeID, types.GetAdmissionWebhookLabel()); err != nil {
		return err
	}
	if err := webhook.CheckWebhookServiceAvailability(types.WebhookTypeAdmission); err != nil {
		return err
	}

	if err := recoverybackend.StartRecoveryBackend(clients); err != nil {
		return err
	}

	if err := clients.Datastore.AddLabelToManagerPod(currentNodeID, types.GetRecoveryBackendLabel()); err != nil {
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

	if err := initDaemonNode(clients.Datastore, currentNodeID); err != nil {
		return err
	}

	if err := m.DeployEngineImage(engineImage); err != nil {
		return err
	}

	server := api.NewServer(m, wsc)
	router := http.Handler(api.NewRouter(server))
	router = util.FilteredLoggingHandler(os.Stdout, router)
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

func environmentCheck() error {
	// Here we only check if the necessary tool the iscsiadm is installed when Longhorn starts up.
	// Others tools and settings like kernel versions, multipathd, nfs client, etc. are checked in the node controller (every 30 sec).
	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}
	nsexec, err := lhns.NewNamespaceExecutor(iscsiutil.ISCSIdProcess, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return err
	}

	return iscsi.CheckForInitiatorExistence(nsexec)
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

func initDaemonNode(ds *datastore.DataStore, nodeName string) error {
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
