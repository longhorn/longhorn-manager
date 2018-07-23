package app

import (
	"fmt"
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-iscsi-helper/iscsi"
	iscsi_util "github.com/rancher/go-iscsi-helper/util"
	"github.com/urfave/cli"

	"github.com/rancher/longhorn-manager/api"
	"github.com/rancher/longhorn-manager/controller"
	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	FlagEngineImage    = "engine-image"
	FlagManagerImage   = "manager-image"
	FlagServiceAccount = "service-account"
	FlagKubeConfig     = "kube-config"
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
		return fmt.Errorf("Environment check failed: %v", err)
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

	ds, wsc, err := controller.StartControllers(done, currentNodeID, serviceAccount, managerImage, kubeconfigPath)
	if err != nil {
		return err
	}

	m := manager.NewVolumeManager(currentNodeID, ds)

	if err := updateSettingDefaultEngineImage(m, engineImage); err != nil {
		return err
	}

	if err := initDaemonNode(ds); err != nil {
		return err
	}

	if err := m.DeployAndWaitForEngineImage(engineImage); err != nil {
		return err
	}

	server := api.NewServer(m, wsc)
	router := http.Handler(api.NewRouter(server))

	listen := types.GetAPIServerAddressFromIP(currentIP)
	logrus.Infof("Listening on %s", listen)

	go http.ListenAndServe(listen, router)

	util.RegisterShutdownChannel(done)
	<-done
	return nil
}

func environmentCheck() error {
	initiatorNSPath := "/host/proc/1/ns"
	pf := iscsi_util.NewProcessFinder("/host/proc")
	ps, err := pf.FindAncestorByName("dockerd")
	if err != nil {
		logrus.Warnf("Failed to find dockerd in the process ancestors, fall back to use pid 1: %v", err)
	} else {
		initiatorNSPath = fmt.Sprintf("/host/proc/%d/ns", ps.Pid)
	}
	namespace, err := iscsi_util.NewNamespaceExecutor(initiatorNSPath)
	if err != nil {
		return err
	}
	if err := iscsi.CheckForInitiatorExistence(namespace); err != nil {
		return err
	}
	return nil
}

func updateSettingDefaultEngineImage(m *manager.VolumeManager, engineImage string) error {
	settingDefaultEngineImage, err := m.GetSetting(types.SettingNameDefaultEngineImage)
	if err != nil {
		return err
	}
	if settingDefaultEngineImage.Value != engineImage {
		settingDefaultEngineImage.Value = engineImage
		if _, err := m.CreateOrUpdateSetting(settingDefaultEngineImage); err != nil {
			return err
		}
	}
	return nil
}

func initDaemonNode(ds *datastore.DataStore) error {
	nodeName := os.Getenv("NODE_NAME")
	node, err := ds.GetNode(nodeName)
	if err != nil {
		return err
	}
	// init default mount disk on node when starting longhorn-manager
	if node == nil {
		_, err = ds.CreateDefaultNode(nodeName)
		if err != nil {
			return err
		}
	}
	return nil
}
