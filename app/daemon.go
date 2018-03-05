package app

import (
	"fmt"
	"net/http"

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

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

const (
	FlagEngineImage    = "engine-image"
	FlagManagerImage   = "manager-image"
	FlagServiceAccount = "service-account"
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

	ds, err := controller.StartControllers(currentNodeID, serviceAccount, engineImage, managerImage)
	if err != nil {
		return err
	}

	if err := initSettings(ds); err != nil {
		return err
	}

	m := manager.NewVolumeManager(currentNodeID, ds)
	server := api.NewServer(m)
	router := http.Handler(api.NewRouter(server))

	listen := types.GetAPIServerAddressFromIP(currentIP)
	logrus.Infof("Listening on %s", listen)

	return http.ListenAndServe(listen, router)
}

func environmentCheck() error {
	namespace, err := iscsi_util.NewNamespaceExecutor("/host/proc/1/ns")
	if err != nil {
		return err
	}
	if err := iscsi.CheckForInitiatorExistence(namespace); err != nil {
		return err
	}
	return nil
}

func initSettings(ds *datastore.DataStore) error {
	setting, err := ds.GetSetting()
	if err != nil {
		return err
	}
	// initialization has been done
	if setting != nil {
		return nil
	}
	setting = &longhorn.Setting{}
	setting.BackupTarget = ""
	if _, err := ds.CreateSetting(setting); err != nil {
		return err
	}
	return nil
}
