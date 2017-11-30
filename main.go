package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/urfave/cli"

	"github.com/rancher/longhorn-manager/api"
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
			Usage: "Choose orchestrator: kubernetes, docker",
			Value: "kubernetes",
		},

		cli.StringFlag{
			Name:   FlagEngineImage,
			EnvVar: EnvEngineImage,
			Usage:  "Specify Longhorn engine image",
		},

		// Docker
		cli.StringSliceFlag{
			Name:  FlagETCDServers,
			Usage: "etcd server ip and port, in format `http://etcd1:2379,http://etcd2:2379`",
		},
		cli.StringFlag{
			Name:  FlagETCDPrefix,
			Usage: "the prefix using with etcd server",
			Value: "/longhorn",
		},
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
		ds        datastore.DataStore
		notifier  manager.Notifier
		err       error
	)

	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	engineImage := c.String(FlagEngineImage)
	if engineImage == "" {
		return fmt.Errorf("require %v", FlagEngineImage)
	}

	orchName := c.String("orchestrator")
	if orchName == "kubernetes" {
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
		notifier = manager.NewTargetWatcher(orch.GetCurrentNode().ID)
	} else if orchName == "docker" {
		cfg := &docker.Config{
			EngineImage: engineImage,
			Network:     c.String(FlagDockerNetwork),
		}
		docker, err := docker.NewDockerOrchestrator(cfg)
		if err != nil {
			return err
		}
		orch = docker

		// Docker cannot manage cluster, we need forwarder to help
		forwarder = orchestrator.NewForwarder(docker)
		orch = forwarder

		etcdServers := c.StringSlice(FlagETCDServers)
		if len(etcdServers) == 0 {
			return fmt.Errorf("require %v", FlagETCDServers)
		}
		etcdBackend, err := kvstore.NewETCDBackend(etcdServers)
		if err != nil {
			return err
		}
		etcd, err := kvstore.NewKVStore(FlagETCDPrefix, etcdBackend)
		if err != nil {
			return err
		}
		ds = etcd
		notifier = manager.NewGRPCNotifier(orch.GetCurrentNode().IP, types.DefaultManagerPort)
	} else {
		return fmt.Errorf("invalid orchestrator %v", orchName)
	}

	engines := &engineapi.EngineCollection{}

	m, err := manager.NewVolumeManager(ds, orch, engines, notifier)
	if err != nil {
		return err
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
