package app

import (
	"fmt"
	"net/http"
	"os"

	_ "net/http/pprof" // for runtime profiling

	"github.com/gorilla/handlers"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/go-iscsi-helper/iscsi"
	iscsiutil "github.com/longhorn/go-iscsi-helper/util"

	"github.com/longhorn/longhorn-manager/api"
	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/meta"
	metricsCollector "github.com/longhorn/longhorn-manager/metrics_collector"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/upgrade"
	"github.com/longhorn/longhorn-manager/util"
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
		logrus.Errorf("Failed environment check, please make sure you " +
			"have iscsiadm/open-iscsi installed on the host")
		return fmt.Errorf("environment check failed: %v", err)
	}

	currentNodeID, err := util.GetRequiredEnv(types.EnvNodeName)
	if err != nil {
		return fmt.Errorf("BUG: failed to detect the node name")
	}

	currentIP, err := util.GetRequiredEnv(types.EnvPodIP)
	if err != nil {
		return fmt.Errorf("BUG: failed to detect the node IP")
	}

	ctx := signals.SetupSignalContext()

	logger := logrus.StandardLogger().WithField("node", currentNodeID)

	webhookTypes := []string{types.WebhookTypeConversion, types.WebhookTypeAdmission}
	for _, webhookType := range webhookTypes {
		if err := startWebhook(ctx, serviceAccount, kubeconfigPath, webhookType); err != nil {
			return err
		}
	}

	if err := startRecoveryBackend(ctx, serviceAccount, kubeconfigPath); err != nil {
		return err
	}

	if err := upgrade.Upgrade(kubeconfigPath, currentNodeID); err != nil {
		return err
	}

	proxyConnCounter := util.NewAtomicCounter()

	ds, wsc, err := controller.StartControllers(logger, ctx.Done(), currentNodeID, serviceAccount, managerImage, kubeconfigPath, meta.Version, proxyConnCounter)
	if err != nil {
		return err
	}

	m := manager.NewVolumeManager(currentNodeID, ds, proxyConnCounter)

	metricsCollector.InitMetricsCollectorSystem(logger, currentNodeID, ds, kubeconfigPath, proxyConnCounter)

	defaultImageSettings := map[types.SettingName]string{
		types.SettingNameDefaultEngineImage:              engineImage,
		types.SettingNameDefaultInstanceManagerImage:     instanceManagerImage,
		types.SettingNameDefaultShareManagerImage:        shareManagerImage,
		types.SettingNameDefaultBackingImageManagerImage: backingImageManagerImage,
		types.SettingNameSupportBundleManagerImage:       supportBundleManagerImage,
	}
	if err := ds.UpdateCustomizedSettings(defaultImageSettings); err != nil {
		return err
	}

	if err := updateRegistrySecretName(m); err != nil {
		return err
	}

	if err := initDaemonNode(ds); err != nil {
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
		if err := http.ListenAndServe(debugAddress, debugHandler); err != nil && err != http.ErrServerClosed {
			logger.Errorf(fmt.Sprintf("ListenAndServe: %s", err))
		}
	}()

	<-ctx.Done()
	return nil
}

func environmentCheck() error {
	initiatorNSPath := iscsiutil.GetHostNamespacePath(util.HostProcPath)
	namespace, err := iscsiutil.NewNamespaceExecutor(initiatorNSPath)
	if err != nil {
		return err
	}
	if err := iscsi.CheckForInitiatorExistence(namespace); err != nil {
		return err
	}
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
