package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-manager/app"
)

var VERSION = "dev"

func cmdNotFound(c *cli.Context, command string) {
	panic(fmt.Errorf("Unrecognized command: %s", command))
}

func onUsageError(c *cli.Context, err error, isSubcommand bool) error {
	panic(fmt.Errorf("Usage error, please check your command"))
}

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	a := cli.NewApp()
	a.Version = VERSION
	a.Usage = "Longhorn Manager"

	a.Before = func(c *cli.Context) error {
		if c.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return nil
	}

	a.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:   "debug, d",
			Usage:  "enable debug logging level",
			EnvVar: "RANCHER_DEBUG",
		},
	}
	a.Commands = []cli.Command{
		app.DaemonCmd(),
		app.SnapshotCmd(),
		app.DeployDriverCmd(),
		app.CSICommand(),
		app.PostUpgradeCmd(),
		app.UninstallCmd(),
	}
	a.CommandNotFound = cmdNotFound
	a.OnUsageError = onUsageError

	app.VERSION = VERSION

	if err := a.Run(os.Args); err != nil {
		logrus.Fatalf("Critical error: %v", err)
	}
}
