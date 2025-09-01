package main

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-manager/app"
	"github.com/longhorn/longhorn-manager/meta"
)

func cmdNotFound(c *cli.Context, command string) {
	panic(fmt.Errorf("unrecognized command: %s", command))
}

func onUsageError(c *cli.Context, err error, isSubcommand bool) error {
	panic(fmt.Errorf("usage error, please check your command"))
}

func main() {
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (function string, file string) {
			fileName := fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
			funcName := path.Base(f.Function)
			return funcName, fileName
		},
		TimestampFormat: time.RFC3339Nano,
		FullTimestamp:   true,
	})

	a := cli.NewApp()
	a.Usage = "Longhorn Manager"
	a.Version = meta.Version

	a.Before = func(c *cli.Context) error {
		if c.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		if c.GlobalBool("log-json") {
			logrus.SetFormatter(&logrus.JSONFormatter{})
		}
		return nil
	}

	a.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:   "debug, d",
			Usage:  "enable debug logging level",
			EnvVar: "RANCHER_DEBUG",
		},
		cli.BoolFlag{
			Name:   "log-json, j",
			Usage:  "log in json format",
			EnvVar: "RANCHER_LOG_JSON",
		},
	}
	a.Commands = []cli.Command{
		app.DaemonCmd(),
		app.RecurringJobCmd(),
		app.DeployDriverCmd(),
		app.CSICommand(),
		app.PreUpgradeCmd(),
		app.PostUpgradeCmd(),
		app.UninstallCmd(),
		app.SystemRolloutCmd(),
		// TODO: Remove MigrateForPre070VolumesCmd() after v0.8.1
		app.MigrateForPre070VolumesCmd(),
	}
	a.CommandNotFound = cmdNotFound
	a.OnUsageError = onUsageError

	if err := a.Run(os.Args); err != nil {
		logrus.Fatalf("Critical error: %v", err)
	}
}
