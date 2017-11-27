package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/rancher/longhorn-manager/api"
	"github.com/rancher/longhorn-manager/crd/controller"
	"github.com/rancher/longhorn-manager/crdstore"
	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/kvstore"
	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/orchestrator/docker"
	"github.com/rancher/longhorn-manager/orchestrator/kubernetes"
	"github.com/rancher/longhorn-manager/types"
)

const (
	sockFile = "/var/run/longhorn/volume-manager.sock"

	FlagEngineImage  = "engine-image"
	FlagOrchestrator = "orchestrator"
	FlagETCDServers  = "etcd-servers"
	FlagETCDPrefix   = "etcd-prefix"

	FlagDockerNetwork = "docker-network"

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
			Usage: "Choose orchestrator: docker",
			Value: "docker",
		},

		cli.StringFlag{
			Name:   FlagEngineImage,
			EnvVar: EnvEngineImage,
			Usage:  "Specify Longhorn engine image",
		},
		cli.StringSliceFlag{
			Name:  FlagETCDServers,
			Usage: "etcd server ip and port, in format `http://etcd1:2379,http://etcd2:2379`",
		},
		cli.StringFlag{
			Name:  FlagETCDPrefix,
			Usage: "the prefix using with etcd server",
			Value: "/longhorn",
		},

		// Docker
		cli.StringFlag{
			Name:  FlagDockerNetwork,
			Usage: "use specified docker network, can be omitted for auto detection",
		},
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatalf("Critical error: %v", err)
	}

}

func RunManager(c *cli.Context) error {
	var (
		orch      orchestrator.Orchestrator
		forwarder *orchestrator.Forwarder
		err       error
		ds        datastore.DataStore
	)

	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	engineImage := c.String(FlagEngineImage)
	if engineImage == "" {
		return fmt.Errorf("require %v", FlagEngineImage)
	}

	orchName := c.String("orchestrator")
	if orchName == "docker" {
		cfg := &docker.Config{
			EngineImage: engineImage,
			Network:     c.String(FlagDockerNetwork),
		}
		orch, err = docker.NewDockerOrchestrator(cfg)
		if err != nil {
			return err
		}
	} else if orchName == "kubernetes" {
		cfg := &kubernetes.Config{
			EngineImage: engineImage,
		}
		orch, err = kubernetes.NewKuberOrchestrator(cfg)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("invalid orchestrator %v", orchName)
	}

	forwarder = orchestrator.NewForwarder(orch)
	orch = forwarder

	etcdServers := c.StringSlice(FlagETCDServers)
	if len(etcdServers) == 0 {
		// Fall back to CRD
		crd, err := crdstore.NewCRDStore("/longhorn_manager_test", "")
		if err != nil {
			return err
		}
		ds = crd
	} else {
		etcdBackend, err := kvstore.NewETCDBackend(etcdServers)
		if err != nil {
			return err
		}
		etcd, err := kvstore.NewKVStore("/longhorn_manager_test", etcdBackend)
		if err != nil {
			return err
		}
		ds = etcd
	}

	engines := &engineapi.EngineCollection{}
	rpc := manager.NewGRPCManager()

	m, err := manager.NewVolumeManager(ds, orch, engines, rpc, types.DefaultManagerPort)
	if err != nil {
		return err
	}

	if len(etcdServers) == 0 {
		crdcontroller.RegisterVolumeController(m, ds)
	}

	if forwarder != nil {
		forwarder.SetLocator(m)
		forwarder.StartServer(m.GetCurrentNode().GetOrchestratorAddress())
	}

	router := http.Handler(api.NewRouter(api.NewServer(m)))

	listen := m.GetCurrentNode().IP + ":" + strconv.Itoa(types.DefaultAPIPort)
	logrus.Infof("Listening on %s", listen)

	return http.ListenAndServe(listen, router)
}
