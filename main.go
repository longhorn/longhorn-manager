package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-manager/app"
	"github.com/longhorn/longhorn-manager/meta"
)

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

	a := &cli.Command{
		Usage:   "Longhorn Manager",
		Version: meta.Version,
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Bool("debug") {
				logrus.SetLevel(logrus.DebugLevel)
			}
			if cmd.Bool("log-json") {
				logrus.SetFormatter(&logrus.JSONFormatter{})
			}
			return ctx, nil
		},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "debug",
				Aliases: []string{"d"},
				Usage:   "enable debug logging level",
				Sources: cli.EnvVars("RANCHER_DEBUG"),
			},
			&cli.BoolFlag{
				Name:    "log-json",
				Aliases: []string{"j"},
				Usage:   "log in json format",
				Sources: cli.EnvVars("RANCHER_LOG_JSON"),
			},
		},
		Commands: []*cli.Command{
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
		},
		CommandNotFound: func(ctx context.Context, cmd *cli.Command, command string) {
			panic(fmt.Errorf("unrecognized command: %s", command))
		},
		OnUsageError: func(ctx context.Context, cmd *cli.Command, err error, isSubcommand bool) error {
			panic(fmt.Errorf("usage error, please check your command"))
		},
	}

	if err := a.Run(context.Background(), os.Args); err != nil {
		logrus.Fatalf("Critical error: %v", err)
	}
}
