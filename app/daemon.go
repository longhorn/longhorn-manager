package app

import (
	"fmt"
	"net/http"
	"os"

	_ "net/http/pprof" // for runtime profiling

	"github.com/gorilla/handlers"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/go-iscsi-helper/iscsi"
	iscsi_util "github.com/longhorn/go-iscsi-helper/util"

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
	FlagEngineImage              = "engine-image"
	FlagInstanceManagerImage     = "instance-manager-image"
	FlagShareManagerImage        = "share-manager-image"
	FlagBackingImageManagerImage = "backing-image-manager-image"
	FlagManagerImage             = "manager-image"
	FlagServiceAccount           = "service-account"
	FlagKubeConfig               = "kube-config"
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
		return fmt.Errorf("BUG: fail to detect the node name")
	}

	currentIP, err := util.GetRequiredEnv(types.EnvPodIP)
	if err != nil {
		return fmt.Errorf("BUG: fail to detect the node IP")
	}

	done := make(chan struct{})

	logger := logrus.StandardLogger().WithField("node", currentNodeID)

	if err := upgrade.Upgrade(kubeconfigPath, currentNodeID); err != nil {
		return err
	}

	ds, wsc, err := controller.StartControllers(logger, done, currentNodeID, serviceAccount, managerImage, kubeconfigPath, meta.Version)
	if err != nil {
		return err
	}

	m := manager.NewVolumeManager(currentNodeID, ds)

	metricsCollector.InitMetricsCollectorSystem(logger, currentNodeID, ds, kubeconfigPath)

	// Initialize the required settings first
	if err := createOrUpdateDefaultImageSetting(m, types.SettingNameDefaultEngineImage, engineImage); err != nil {
		return err
	}

	if err := createOrUpdateDefaultImageSetting(m, types.SettingNameDefaultInstanceManagerImage, instanceManagerImage); err != nil {
		return err
	}

	if err := createOrUpdateDefaultImageSetting(m, types.SettingNameDefaultShareManagerImage, shareManagerImage); err != nil {
		return err
	}

	if err := createOrUpdateDefaultImageSetting(m, types.SettingNameDefaultBackingImageManagerImage, backingImageManagerImage); err != nil {
		return err
	}

	// Initialize the rest settings
	if err := ds.InitSettings(); err != nil {
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

	go http.ListenAndServe(listen, router)

	go func() {
		debugAddress := "127.0.0.1:6060"
		debugHandler := http.DefaultServeMux
		logger.Infof("Debug Server listening on %s", debugAddress)
		if err := http.ListenAndServe(debugAddress, debugHandler); err != nil && err != http.ErrServerClosed {
			logger.Errorf(fmt.Sprintf("ListenAndServe: %s", err))
		}
	}()

	util.RegisterShutdownChannel(done)
	<-done
	return nil
}

func environmentCheck() error {
	initiatorNSPath := iscsi_util.GetHostNamespacePath(util.HostProcPath)
	namespace, err := iscsi_util.NewNamespaceExecutor(initiatorNSPath)
	if err != nil {
		return err
	}
	if err := iscsi.CheckForInitiatorExistence(namespace); err != nil {
		return err
	}
	return nil
}

func createOrUpdateDefaultImageSetting(m *manager.VolumeManager, settingName types.SettingName, image string) error {
	settingDefaultImage, err := m.GetSetting(settingName)
	if err != nil {
		return err
	}
	if settingDefaultImage.Value != image {
		settingDefaultImage.Value = image
		if _, err := m.CreateOrUpdateSetting(settingDefaultImage); err != nil {
			return err
		}
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
