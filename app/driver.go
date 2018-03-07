package app

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/urfave/cli"

	"k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/rancher/longhorn-manager/types"
)

const (
	FlagVolumePluginDir  = "volume-plugin-dir"
	DefaultFlexvolumeDir = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"

	LonghornFlexvolumeDriver = "longhorn-flexvolume-driver"
)

func DeployFlexvolumeDriverCmd() cli.Command {
	return cli.Command{
		Name: "deploy-flexvolume-driver",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagManagerImage,
				Usage: "Specify Longhorn manager image",
			},
			cli.StringFlag{
				Name:  FlagVolumePluginDir,
				Usage: "Specify the location of flexvolume plugin for Kubernetes on the host",
			},
		},
		Action: func(c *cli.Context) {
			if err := deployFlexvolumeDriver(c); err != nil {
				logrus.Fatalf("Error deploying Flexvolume driver: %v", err)
			}
		},
	}
}

func deployFlexvolumeDriver(c *cli.Context) error {
	managerImage := c.String(FlagManagerImage)
	if managerImage == "" {
		return fmt.Errorf("require %v", FlagManagerImage)
	}

	volumePluginDir := c.String(FlagVolumePluginDir)
	if volumePluginDir == "" {
		volumePluginDir = DefaultFlexvolumeDir
	}

	dsOps, err := newDaemonSetOps()
	if err != nil {
		return err
	}

	d, err := dsOps.Get(LonghornFlexvolumeDriver)
	if err != nil {
		return err
	}
	if d != nil {
		if err := dsOps.Delete(LonghornFlexvolumeDriver); err != nil {
			return err
		}
	}
	logrus.Infof("Installing Flexvolume to Kubernetes nodes directory %v", volumePluginDir)
	if _, err := dsOps.Create(LonghornFlexvolumeDriver, getFlexvolumeDaemonSetSpec(managerImage, volumePluginDir)); err != nil {
		return err
	}
	defer func() {
		if err := dsOps.Delete(LonghornFlexvolumeDriver); err != nil {
			logrus.Warnf("Fail to cleanup %v: %v", LonghornFlexvolumeDriver, err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	done := make(chan struct{})

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Receive %v to exit", sig)
		done <- struct{}{}
	}()

	<-done
	return nil
}

func getFlexvolumeDaemonSetSpec(image, volumePluginDir string) *extensionsv1beta1.DaemonSet {
	cmd := []string{
		"/entrypoint.sh",
	}
	privilege := true
	d := &extensionsv1beta1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: LonghornFlexvolumeDriver,
		},
		Spec: extensionsv1beta1.DaemonSetSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: LonghornFlexvolumeDriver,
					Labels: map[string]string{
						"app": LonghornFlexvolumeDriver,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    LonghornFlexvolumeDriver,
							Image:   image,
							Command: cmd,
							SecurityContext: &v1.SecurityContext{
								Privileged: &privilege,
							},
							Env: []v1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name:  "LONGHORN_BACKEND_SVC",
									Value: "longhorn-backend",
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "flexvolume-longhorn-mount",
									MountPath: "/flexmnt",
								},
								{
									Name:      "usr-local-bin-mount",
									MountPath: "/binmnt",
								},
								{
									Name:      "host-proc-mount",
									MountPath: "/host/proc",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "flexvolume-longhorn-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: volumePluginDir,
								},
							},
						},
						{
							Name: "host-proc-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/proc",
								},
							},
						},
						{
							Name: "usr-local-bin-mount",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/usr/local/bin",
								},
							},
						},
					},
				},
			},
		},
	}
	return d
}

type DaemonSetOps struct {
	namespace  string
	kubeClient *clientset.Clientset
}

func newDaemonSetOps() (*DaemonSetOps, error) {
	// Only supports in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get k8s client")
	}

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("Cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}
	return &DaemonSetOps{
		namespace, kubeClient,
	}, nil
}

func (ops *DaemonSetOps) Get(name string) (*extensionsv1beta1.DaemonSet, error) {
	d, err := ops.kubeClient.ExtensionsV1beta1().DaemonSets(ops.namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return d, nil
}

func (ops *DaemonSetOps) Create(name string, d *extensionsv1beta1.DaemonSet) (*extensionsv1beta1.DaemonSet, error) {
	return ops.kubeClient.ExtensionsV1beta1().DaemonSets(ops.namespace).Create(d)
}

func (ops *DaemonSetOps) Delete(name string) error {
	propagation := metav1.DeletePropagationForeground
	return ops.kubeClient.ExtensionsV1beta1().DaemonSets(ops.namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &propagation})
}
