package manager

import (
	"github.com/pkg/errors"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	"github.com/rancher/longhorn-manager/types"
)

func (m *VolumeManager) GetSetting() (*longhorn.Setting, error) {
	return m.ds.GetSetting()
}

func (m *VolumeManager) UpdateSetting(s *longhorn.Setting) (*longhorn.Setting, error) {
	if err := m.syncEngineUpgradeImage(s.EngineUpgradeImage); err != nil {
		return nil, err
	}
	return m.ds.UpdateSetting(s)
}

func (m *VolumeManager) syncEngineUpgradeImage(image string) error {
	deployed, err := m.listEngineUpgradeImage()
	if err != nil {
		return errors.Wrapf(err, "failed to get engine upgrade images")
	}

	toDeploy := make(map[string]struct{})
	toDelete := make(map[string]struct{})

	images := strings.Split(image, ",")
	for _, image := range images {
		// images can have empty member
		image = strings.TrimSpace(image)
		if image == "" {
			continue
		}
		if deployed[image] == "" {
			toDeploy[image] = struct{}{}
		} else {
			delete(deployed, image)
		}
	}
	for image := range deployed {
		toDelete[image] = struct{}{}
	}

	for image := range toDelete {
		if err := m.deleteEngineUpgradeImage(image); err != nil {
			return errors.Wrapf(err, "failed to delete engine upgrade image")
		}
	}
	for image := range toDeploy {
		if err := m.createEngineUpgradeImage(image); err != nil {
			return errors.Wrapf(err, "failed to create engine upgrade image")
		}
	}
	return nil
}

func (m *VolumeManager) listEngineUpgradeImage() (map[string]string, error) {
	return m.ds.ListEngineUpgradeImageDaemonSet()
}

func (m *VolumeManager) deleteEngineUpgradeImage(image string) error {
	dsName := getEngineUpgradeImageDeployerName(image)
	if err := m.ds.DeleteEngineUpgradeImageDaemonSet(dsName); err != nil {
		return errors.Wrapf(err, "failed to delete engine upgrade image daemonset %v", dsName)
	}
	return nil
}

func (m *VolumeManager) createEngineUpgradeImage(image string) error {
	d := createEngineUpgradeImageDaemonSetSpec(image)
	if err := m.ds.CreateEngineUpgradeImageDaemonSet(d); err != nil {
		return errors.Wrap(err, "failed to create engine upgrade image daemonset")
	}
	return nil
}

func getEngineUpgradeImageDeployerName(image string) string {
	cname := types.GetImageCanonicalName(image)
	return "engine-image-deployer-" + cname
}

func createEngineUpgradeImageDaemonSetSpec(image string) *appsv1beta2.DaemonSet {
	dsName := getEngineUpgradeImageDeployerName(image)
	cmd := []string{
		"/bin/bash",
	}
	args := []string{
		"-c",
		"cp /usr/local/bin/longhorn /upgrade-image/ && echo installed && trap 'rm /upgrade-image/longhorn && echo cleaned up' EXIT && sleep infinity",
	}
	d := &appsv1beta2.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: dsName,
		},
		Spec: appsv1beta2.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetEngineUpgradeImageLabel(),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   dsName,
					Labels: types.GetEngineUpgradeImageLabel(),
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:            dsName,
							Image:           image,
							Command:         cmd,
							Args:            args,
							ImagePullPolicy: v1.PullAlways,
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "upgrade-image",
									MountPath: "/upgrade-image/",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "upgrade-image",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: types.GetEngineUpgradeDirectoryForImageOnHost(image),
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
