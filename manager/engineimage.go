package manager

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (m *VolumeManager) ListEngineImagesByName() (map[string]*longhorn.EngineImage, error) {
	return m.ds.ListEngineImages()
}

func (m *VolumeManager) GetEngineImageByName(name string) (*longhorn.EngineImage, error) {
	return m.ds.GetEngineImage(name)
}

func (m *VolumeManager) CreateEngineImage(image string) (*longhorn.EngineImage, error) {
	image = strings.TrimSpace(image)
	if image == "" {
		return nil, fmt.Errorf("cannot create engine image with empty image")
	}

	// make it random node's responsibility
	ownerID, err := m.getRandomOwnerID()
	if err != nil {
		return nil, err
	}

	name := types.GetEngineImageChecksumName(image)
	ei := &longhorn.EngineImage{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: types.EngineImageSpec{
			OwnerID: ownerID,
			Image:   image,
		},
	}
	ei, err = m.ds.CreateEngineImage(ei)
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
	return m.ds.DeleteEngineImage(name)
}
