package manager

import (
	"fmt"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

var (
	WaitForEngineImageCount    = 20
	WaitForEngineImageInterval = 6 * time.Second
)

func (m *VolumeManager) ListEngineImagesByName() (map[string]*longhorn.EngineImage, error) {
	return m.ds.ListEngineImages()
}

func (m *VolumeManager) GetEngineImageByName(name string) (*longhorn.EngineImage, error) {
	return m.ds.GetEngineImage(name)
}

func (m *VolumeManager) GetEngineImage(image string) (*longhorn.EngineImage, error) {
	name := types.GetEngineImageChecksumName(image)
	return m.ds.GetEngineImage(name)
}

func (m *VolumeManager) CreateEngineImage(image string) (*longhorn.EngineImage, error) {
	image = strings.TrimSpace(image)
	if image == "" {
		return nil, fmt.Errorf("cannot create engine image with empty image")
	}

	name := types.GetEngineImageChecksumName(image)
	ei := &longhorn.EngineImage{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: types.EngineImageSpec{
			OwnerID: "", // the first controller who see it will pick it up
			Image:   image,
		},
	}
	ei, err := m.ds.CreateEngineImage(ei)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Created engine image %v (%v)", ei.Name, ei.Spec.Image)
	return ei, nil
}

func (m *VolumeManager) DeleteEngineImageByName(name string) error {
	ei, err := m.GetEngineImageByName(name)
	if err != nil {
		return errors.Wrapf(err, "unable to get engine image '%s'", name)
	}
	if ei == nil {
		return nil
	}
	defaultImage, err := m.GetDefaultEngineImage()
	if err != nil {
		return errors.Wrap(err, "unable to delete engine image")
	}
	if ei.Spec.Image == defaultImage {
		return fmt.Errorf("unable to delete the default engine image")
	}
	if ei.Status.RefCount != 0 {
		return fmt.Errorf("unable to delete the engine image while being used")
	}
	return m.ds.DeleteEngineImage(name)
}

func (m *VolumeManager) DeployAndWaitForEngineImage(image string) error {
	ei, err := m.GetEngineImage(image)
	if err != nil {
		return errors.Wrapf(err, "cannot get engine image %v", image)
	}
	if ei == nil {
		ei, err = m.CreateEngineImage(image)
		if err != nil {
			return errors.Wrapf(err, "cannot create engine image for %v", image)
		}
	}
	if err := m.WaitForEngineImage(image); err != nil {
		return errors.Wrapf(err, "failed to wait for engine image %v", image)
	}
	return nil
}

func (m *VolumeManager) WaitForEngineImage(image string) error {
	for i := 0; i < WaitForEngineImageCount; i++ {
		ei, err := m.GetEngineImage(image)
		if err != nil {
			return errors.Wrapf(err, "cannot get engine image %v", image)
		}
		if ei == nil {
			return errors.Wrapf(err, "cannot wait for non-exist engine image %v", image)
		}
		if ei.Status.State == types.EngineImageStateReady {
			logrus.Debugf("Engine image %v is ready", image)
			return nil
		}
		logrus.Debugf("Waiting for engine image %v to be ready", image)
		time.Sleep(WaitForEngineImageInterval)
	}
	return fmt.Errorf("Wait for engine image %v timed out", image)
}
