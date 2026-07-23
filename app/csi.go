package app

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-manager/csi"
	"github.com/longhorn/longhorn-manager/types"
)

func CSICommand() *cli.Command {
	return &cli.Command{
		Name: "csi",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "endpoint",
				Value: csi.GetCSIEndpoint(),
				Usage: "CSI endpoint",
			},
			&cli.StringFlag{
				Name:  "drivername",
				Value: types.LonghornDriverName,
				Usage: "Name of the CSI driver",
			},
			&cli.StringFlag{
				Name:  "nodeid",
				Usage: "Node id",
			},
			&cli.StringFlag{
				Name:  "manager-url",
				Value: "",
				Usage: "Longhorn manager API URL",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if err := runCSI(cmd); err != nil {
				logrus.Fatalf("Error starting CSI manager: %v", err)
			}
			return nil
		},
	}
}

func runCSI(cmd *cli.Command) error {
	manager := csi.GetCSIManager()
	identityVersion := cmd.Root().Version
	return manager.Run(cmd.String("drivername"),
		cmd.String("nodeid"),
		cmd.String("endpoint"),
		identityVersion,
		cmd.String("manager-url"))
}
