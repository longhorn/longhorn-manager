package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/urfave/cli"

	"github.com/rancher/go-iscsi-helper/iscsi"
	iscsi_util "github.com/rancher/go-iscsi-helper/util"

	"github.com/rancher/longhorn-manager/api"
	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/orchestrator/kubernetes"
	"github.com/rancher/longhorn-manager/types"

	"github.com/rancher/longhorn-manager/controller"
)

const (
	sockFile = "/var/run/longhorn/volume-manager.sock"

	FlagEngineImage  = "engine-image"
	FlagOrchestrator = "orchestrator"

	EnvEngineImage = "LONGHORN_ENGINE_IMAGE"
)

var VERSION = "0.2.0"

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{ForceColors: true})

	app := cli.NewApp()
	app.Version = VERSION
	app.Usage = "Longhorn Manager"
	app.Action = RunManager

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:   "debug, d",
			Usage:  "enable debug logging level",
			EnvVar: "RANCHER_DEBUG",
		},
		cli.StringFlag{
			Name:  FlagOrchestrator,
			Usage: "Choose orchestrator: kubernetes",
			Value: "kubernetes",
		},
		cli.StringFlag{
			Name:   FlagEngineImage,
			EnvVar: EnvEngineImage,
			Usage:  "Specify Longhorn engine image",
		},
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatalf("Critical error: %v", err)
	}

}

func RunManager(c *cli.Context) error {
	var (
		orch orchestrator.Orchestrator
		ds   datastore.DataStore
		err  error
	)

	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	engineImage := c.String(FlagEngineImage)
	if engineImage == "" {
		return fmt.Errorf("require %v", FlagEngineImage)
	}

	if err := environmentCheck(); err != nil {
		logrus.Errorf("Failed environment check, please make sure you " +
			"have iscsiadm/open-iscsi installed on the host")
		return fmt.Errorf("Environment check failed: %v", err)
	}

	orchName := c.String("orchestrator")
	if orchName != "kubernetes" {
		return fmt.Errorf("Doesn't support orchestrator other than Kubernetes")
	}

	cfg := &kubernetes.Config{
		EngineImage: engineImage,
	}
	orch, err = kubernetes.NewOrchestrator(cfg)
	if err != nil {
		return err
	}

	ds, err = datastore.NewCRDStore("")
	if err != nil {
		return errors.Wrap(err, "fail to create CRD store")
	}
	currentNodeID := orch.GetCurrentNode().ID

	engines := &engineapi.EngineCollection{}

	m, err := manager.NewVolumeManager(ds, orch, engines, engineImage)
	if err != nil {
		return err
	}

	kds, err := controller.StartControllers(currentNodeID, engineImage)
	if err != nil {
		return err
	}

	router := http.Handler(api.NewRouter(api.NewServer(m, kds)))

	listen := m.GetCurrentNode().IP + ":" + strconv.Itoa(types.DefaultAPIPort)
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
