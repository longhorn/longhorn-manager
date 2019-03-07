package app

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/rancher/longhorn-manager/csi"
)

func CSICommand() cli.Command {
	return cli.Command{
		Name: "csi",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "endpoint",
				Value: "unix://var/lib/kubelet/plugins/io.rancher.longhorn/csi.sock",
				Usage: "CSI endpoint",
			},
			cli.StringFlag{
				Name:  "drivername",
				Value: "io.rancher.longhorn",
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
			cli.StringFlag{
				Name:  "csi-version",
				Value: "0.2.0",
				Usage: "CSI plugin version",
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
		c.String("csi-version"),
		identityVersion,
		c.String("manager-url"))
}
