package app

import (
	"fmt"
	"os"

	"github.com/Jeffail/gabs"
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
	"github.com/rancher/longhorn-manager/util"
)

const (
	FlagFlexvolumeDir      = "flexvolume-dir"
	EnvFlexvolumeDir       = "FLEXVOLUME_DIR"
	FlagLonghornBackendSvc = "longhorn-backend-svc"
	EnvLonghornBackendSvc  = "LONGHORN_BACKEND_SVC"
	DefaultFlexvolumeDir   = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"

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
				Name:   FlagFlexvolumeDir,
				Usage:  "Specify the location of flexvolume plugin for Kubernetes on the host",
				EnvVar: EnvFlexvolumeDir,
			},
			cli.StringFlag{
				Name:   FlagLonghornBackendSvc,
				Usage:  "Specify the Longhorn backend service for Kubernetes on the host",
				EnvVar: EnvLonghornBackendSvc,
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

	// Only supports in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}

	flexvolumeDir := c.String(FlagFlexvolumeDir)
	if flexvolumeDir == "" {
		flexvolumeDir, err = discoverFlexvolumeDir(kubeClient)
		if err != nil {
			logrus.Warnf("Failed to detect flexvolume dir, fall back to default: ", err)
		}
		if flexvolumeDir == "" {
			flexvolumeDir = DefaultFlexvolumeDir
		}
	} else {
		logrus.Infof("User specified Flexvolume dir at: %v", flexvolumeDir)
	}

	longhornBackendSvc := c.String(FlagLonghornBackendSvc)
	if longhornBackendSvc == "" {
		longhornBackendSvc = "longhorn-backend"
	}

	dsOps, err := newDaemonSetOps(kubeClient)
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
	logrus.Infof("Install Flexvolume to Kubernetes nodes directory %v", flexvolumeDir)
	if _, err := dsOps.Create(LonghornFlexvolumeDriver, getFlexvolumeDaemonSetSpec(managerImage, flexvolumeDir, longhornBackendSvc)); err != nil {
		return err
	}
	defer func() {
		if err := dsOps.Delete(LonghornFlexvolumeDriver); err != nil {
			logrus.Warnf("Fail to cleanup %v: %v", LonghornFlexvolumeDriver, err)
		}
	}()

	done := make(chan struct{})
	util.RegisterShutdownChannel(done)

	<-done
	return nil
}

func getFlexvolumeDaemonSetSpec(image, flexvolumeDir string, longhornBackendSvc string) *extensionsv1beta1.DaemonSet {
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
									Value: longhornBackendSvc,
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
									Path: flexvolumeDir,
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

func discoverFlexvolumeDir(kubeClient *clientset.Clientset) (dir string, err error) {
	defer func() {
		err = errors.Wrap(err, "cannot discover Flexvolume Dir")
	}()
	nodeName, err := util.GetRequiredEnv(types.EnvNodeName)
	if err != nil {
		return "", fmt.Errorf("Env %v wasn't set", types.EnvNodeName)
	}
	uri := fmt.Sprintf("/api/v1/proxy/nodes/%s/configz", nodeName)
	rawConfigInBytes, err := kubeClient.Core().RESTClient().Get().RequestURI(uri).DoRaw()
	if err != nil {
		return "", errors.Wrapf(err, "cannot reach node config URI %v", uri)
	}
	jsonParsed, err := gabs.ParseJSON(rawConfigInBytes)
	if err != nil {
		return "", errors.Wrapf(err, "cannot parse json")
	}
	value, ok := jsonParsed.Path("kubeletconfig.volumePluginDir").Data().(string)
	if !ok {
		logrus.Infof("cannot find volumePluginDir key in node config, assume it's default")
		return DefaultFlexvolumeDir, nil
	}
	logrus.Infof("Discovered Flexvolume dir at: %v", value)
	return value, nil
}

type DaemonSetOps struct {
	namespace  string
	kubeClient *clientset.Clientset
}

func newDaemonSetOps(kubeClient *clientset.Clientset) (*DaemonSetOps, error) {
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
