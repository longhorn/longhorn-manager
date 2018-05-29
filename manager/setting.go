package manager

import (
	"github.com/pkg/errors"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

func (m *VolumeManager) GetSetting() (*longhorn.Setting, error) {
	return m.ds.GetSetting()
}

func (m *VolumeManager) UpdateSetting(s *longhorn.Setting) (*longhorn.Setting, error) {
	if err := m.syncEngineImage(s.DefaultEngineImage, s.EngineUpgradeImage); err != nil {
		return nil, err
	}
	return m.ds.UpdateSetting(s)
}

func (m *VolumeManager) syncEngineImage(defaultEngineImage, engineUpgradeImage string) error {
	deployed, err := m.listEngineImage()
	if err != nil {
		return errors.Wrapf(err, "failed to get engine image daemonset")
	}

	images := util.SplitStringToMap(engineUpgradeImage, ",")
	images[defaultEngineImage] = struct{}{}

	toDeploy := make(map[string]struct{})
	for image := range images {
		if deployed[image] == "" {
			toDeploy[image] = struct{}{}
		} else {
			delete(deployed, image)
		}
	}
	// remaining ones wasn't listed in the images
	toDelete := deployed

	for image := range toDelete {
		if err := m.deleteEngineImage(image); err != nil {
			return errors.Wrapf(err, "failed to delete engine image daemonset")
		}
	}
	for image := range toDeploy {
		if err := m.createEngineImage(image); err != nil {
			return errors.Wrapf(err, "failed to create engine image daemonset")
		}
	}
	return nil
}

func (m *VolumeManager) listEngineImage() (map[string]string, error) {
	return m.ds.ListEngineImageDaemonSet()
}

func (m *VolumeManager) deleteEngineImage(image string) error {
	dsName := getEngineImageDeployerName(image)
	if err := m.ds.DeleteEngineImageDaemonSet(dsName); err != nil {
		return errors.Wrapf(err, "failed to delete engine image daemonset %v", dsName)
	}
	return nil
}

func (m *VolumeManager) createEngineImage(image string) error {
	d := createEngineImageDaemonSetSpec(image)
	if err := m.ds.CreateEngineImageDaemonSet(d); err != nil {
		return errors.Wrap(err, "failed to create engine image daemonset")
	}
	return nil
}

func getEngineImageDeployerName(image string) string {
	cname := types.GetImageCanonicalName(image)
	return "engine-image-deployer-" + cname
}

func createEngineImageDaemonSetSpec(image string) *appsv1beta2.DaemonSet {
	dsName := getEngineImageDeployerName(image)
	cmd := []string{
		"/bin/bash",
	}
	args := []string{
		"-c",
		"cp /usr/local/bin/longhorn* /data/ && echo installed && trap 'rm /data/longhorn* && echo cleaned up' EXIT && sleep infinity",
	}
	d := &appsv1beta2.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: dsName,
		},
		Spec: appsv1beta2.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetEngineImageLabel(),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   dsName,
					Labels: types.GetEngineImageLabel(),
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
									Name:      "data",
									MountPath: "/data/",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "data",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: types.GetEngineBinaryDirectoryOnHostForImage(image),
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
