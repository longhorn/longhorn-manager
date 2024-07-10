package app

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/tools/clientcmd"

	"github.com/longhorn/longhorn-manager/types"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

func PreUpgradeCmd() cli.Command {
	return cli.Command{
		Name: "pre-upgrade",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
			cli.StringFlag{
				Name:     FlagNamespace,
				EnvVar:   types.EnvPodNamespace,
				Required: true,
				Usage:    "Specify Longhorn namespace",
			},
		},
		Action: func(c *cli.Context) {
			logrus.Infof("Running pre-upgrade...")
			defer logrus.Infof("Completed pre-upgrade.")

			if err := preUpgrade(c); err != nil {
				logrus.WithError(err).Fatalf("Failed to run pre-upgrade")
			}
		},
	}
}

func preUpgrade(c *cli.Context) error {
	namespace := c.String(FlagNamespace)

	config, err := clientcmd.BuildConfigFromFlags("", c.String(FlagKubeConfig))
	if err != nil {
		return errors.Wrap(err, "failed to get client config")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get clientset")
	}

	if err := upgradeutil.CheckUpgradePath(namespace, lhClient, nil, true); err != nil {
		return err
	}

	if err := environmentCheck(); err != nil {
		return errors.Wrap(err, "failed to check environment, please make sure you have iscsiadm/open-iscsi installed on the host")
	}

	return nil
}
