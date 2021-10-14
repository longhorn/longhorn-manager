package app

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/longhorn/longhorn-manager/types"
)

const (
	RetryCounts   = 360
	RetryInterval = 5 * time.Second
)

func PostUpgradeCmd() cli.Command {
	return cli.Command{
		Name: "post-upgrade",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
			cli.StringFlag{
				Name:   FlagNamespace,
				EnvVar: types.EnvPodNamespace,
			},
		},
		Action: func(c *cli.Context) {
			logrus.Infof("Running post-upgrade...")
			defer logrus.Infof("Completed post-upgrade.")

			if err := postUpgrade(c); err != nil {
				logrus.Fatalf("Error during post-upgrade: %v", err)
			}
		},
	}
}

func postUpgrade(c *cli.Context) error {
	namespace := c.String(FlagNamespace)
	if namespace == "" {
		return errors.New("namespace is required")
	}

	config, err := clientcmd.BuildConfigFromFlags("", c.String(FlagKubeConfig))
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	return newPostUpgrader(namespace, kubeClient).Run()
}

type postUpgrader struct {
	namespace  string
	kubeClient kubernetes.Interface
}

func newPostUpgrader(namespace string, kubeClient kubernetes.Interface) *postUpgrader {
	return &postUpgrader{namespace, kubeClient}
}

func (u *postUpgrader) Run() error {
	if err := u.waitManagerUpgradeComplete(); err != nil {
		return err
	}

	// future routines go here
	return nil
}

func (u *postUpgrader) waitManagerUpgradeComplete() error {
	complete := false
	for i := 0; i < RetryCounts; i++ {
		ds, err := u.kubeClient.AppsV1().DaemonSets(u.namespace).Get(
			context.TODO(),
			types.LonghornManagerDaemonSetName, metav1.GetOptions{})
		if err != nil {
			logrus.Warningf("couldn't get daemonset: %v", err)
			continue
		}
		if len(ds.Spec.Template.Spec.Containers) != 1 {
			logrus.Warningf("found %d containers in manager spec", len(ds.Spec.Template.Spec.Containers))
			continue
		}

		podList, err := u.kubeClient.CoreV1().Pods(u.namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			logrus.Warningf("couldn't list pods: %v", err)
			continue
		}
		complete = true
		for _, pod := range podList.Items {
			if app, ok := pod.Labels["app"]; !ok || app != types.LonghornManagerDaemonSetName {
				continue
			}
			if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != ds.Spec.Template.Spec.Containers[0].Image {
				complete = false
				break
			}
		}
		if complete {
			logrus.Infof("Manager upgrade complete")
			break
		}
		time.Sleep(RetryInterval)
	}

	if !complete {
		return fmt.Errorf("manager upgrade is still in progress")
	}
	return nil
}
