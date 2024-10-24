package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	PostUpgradeEventer = "longhorn-post-upgrade"

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
				Name:     FlagNamespace,
				EnvVar:   types.EnvPodNamespace,
				Required: true,
				Usage:    "Specify Longhorn namespace",
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

	eventRecorder := eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: PostUpgradeEventer})

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get k8s client")
	}

	err = newPostUpgrader(namespace, kubeClient, eventRecorder).Run()
	if err != nil {
		logrus.Warnf("Done with Run() ... err is %v", err)
	}

	return err
}

type postUpgrader struct {
	namespace     string
	kubeClient    kubernetes.Interface
	eventRecorder record.EventRecorder
}

func newPostUpgrader(namespace string, kubeClient kubernetes.Interface, eventRecorder record.EventRecorder) *postUpgrader {
	return &postUpgrader{namespace, kubeClient, eventRecorder}
}

func (u *postUpgrader) Run() error {
	var err error
	defer func() {
		if err != nil {
			u.eventRecorder.Event(&corev1.ObjectReference{Namespace: u.namespace, Name: PostUpgradeEventer},
				corev1.EventTypeWarning, constant.EventReasonFailedUpgradePostCheck, err.Error())
		} else {
			u.eventRecorder.Event(&corev1.ObjectReference{Namespace: u.namespace, Name: PostUpgradeEventer},
				corev1.EventTypeNormal, constant.EventReasonPassedUpgradeCheck, "post-upgrade check passed")
		}
	}()

	if err = u.waitManagerUpgradeComplete(); err != nil {
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
		if len(ds.Spec.Template.Spec.Containers) != 1 {
			logrus.Warnf("found %d containers in manager spec", len(ds.Spec.Template.Spec.Containers))
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
