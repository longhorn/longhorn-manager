package app

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-manager/csi"
	"github.com/longhorn/longhorn-manager/types"
)

func CSICommand() cli.Command {
	return cli.Command{
		Name: "csi",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "endpoint",
				Value: csi.GetCSIEndpoint(),
				Usage: "CSI endpoint",
			},
			cli.StringFlag{
				Name:  "drivername",
				Value: types.LonghornDriverName,
				Usage: "Name of the CSI driver",
			},
			cli.StringFlag{
				Name:  "nodeid",
				Usage: "Node id",
			},
			cli.StringFlag{
				Name:  "manager-url",
				Value: "",
				Usage: "Longhorn manager API URL",
			},
		},
		Action: func(c *cli.Context) {
			if err := runCSI(c); err != nil {
				logrus.Fatalf("Error starting CSI manager: %v", err)
			}
		},
	}
}

func runCSI(c *cli.Context) error {
	manager := csi.GetCSIManager()
	identityVersion := c.App.Version
	return manager.Run(c.String("drivername"),
		c.String("nodeid"),
		c.String("endpoint"),
		identityVersion,
		c.String("manager-url"))
}
