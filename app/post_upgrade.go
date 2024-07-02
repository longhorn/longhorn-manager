package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

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
			logrus.Info("Running post-upgrade...")
			defer logrus.Info("Completed post-upgrade.")

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
		return errors.Wrap(err, "failed to get client config")
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get k8s client")
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
			logrus.Warnf("failed to get daemonset: %v", err)
			continue
		}
		podList, err := u.kubeClient.CoreV1().Pods(u.namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.Set(types.GetManagerLabels()).String()})
		if err != nil {
			logrus.Warnf("failed to list pods: %v", err)
			continue
		}
		complete = true
	PodLookupLoop:
		for _, pod := range podList.Items {
			newImage := ds.Spec.Template.Spec.Containers[0].Image
			if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != newImage {
				// we should disregard linkerd service or other sidecar containers that are not created by Longhorn
				// https://github.com/longhorn/longhorn/issues/3809
				for _, container := range pod.Spec.Containers {
					if !strings.HasPrefix(container.Name, "longhorn") {
						continue
					}
					complete = container.Image == newImage
					break PodLookupLoop
				}
			}
		}
		if complete {
			logrus.Info("Manager upgrade complete")
			break
		}
		time.Sleep(RetryInterval)
	}

	if !complete {
		return fmt.Errorf("manager upgrade is still in progress")
	}
	return nil
}
