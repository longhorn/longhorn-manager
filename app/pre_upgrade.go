package app

import (
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
)

const (
	PreUpgradeEventer = "longhorn-pre-upgrade"
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
			logrus.Info("Running pre-upgrade...")
			defer logrus.Info("Completed pre-upgrade.")

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

	eventBroadcaster, err := createEventBroadcaster(config)
	if err != nil {
		return errors.Wrap(err, "failed to create event broadcaster")
	}
	defer func() {
		eventBroadcaster.Shutdown()
		// Allow a little time for the event to flush, but not greatly delay response to the calling job.
		time.Sleep(5 * time.Second)
	}()

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "failed to create scheme")
	}

	eventRecorder := eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: PreUpgradeEventer})

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get clientset")
	}

	err = newPreUpgrader(namespace, lhClient, eventRecorder).Run()
	if err != nil {
		logrus.Warnf("Done with Run() ... err is %v", err)
	}

	return err
}

type preUpgrader struct {
	namespace     string
	lhClient      lhclientset.Interface
	eventRecorder record.EventRecorder
}

func newPreUpgrader(namespace string, lhClient lhclientset.Interface, eventRecorder record.EventRecorder) *preUpgrader {
	return &preUpgrader{namespace, lhClient, eventRecorder}
}

func (u *preUpgrader) Run() error {
	var err error
	defer func() {
		if err != nil {
			u.eventRecorder.Event(&corev1.ObjectReference{Namespace: u.namespace, Name: PreUpgradeEventer},
				corev1.EventTypeWarning, constant.EventReasonFailedUpgradePreCheck, err.Error())
		} else {
			u.eventRecorder.Event(&corev1.ObjectReference{Namespace: u.namespace, Name: PreUpgradeEventer},
				corev1.EventTypeNormal, constant.EventReasonPassedUpgradeCheck, "pre-upgrade check passed")
		}
	}()

	if err = upgradeutil.CheckUpgradePath(u.namespace, u.lhClient, u.eventRecorder, true); err != nil {
		return err
	}

	if err = environmentCheck(); err != nil {
		err = errors.Wrap(err, "failed to check environment, please make sure you have iscsiadm/open-iscsi installed on the host")
		return err
	}

	return nil
}
