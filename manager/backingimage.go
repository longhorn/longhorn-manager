package manager

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (m *VolumeManager) ListBackingImages() (map[string]*longhorn.BackingImage, error) {
	return m.ds.ListBackingImages()
}

func (m *VolumeManager) ListBackingImagesSorted() ([]*longhorn.BackingImage, error) {
	backingImageMap, err := m.ds.ListBackingImages()
	if err != nil {
		return []*longhorn.BackingImage{}, err
	}

	backingImages := make([]*longhorn.BackingImage, len(backingImageMap))
	backingImageNames, err := sortKeys(backingImageMap)
	if err != nil {
		return []*longhorn.BackingImage{}, err
	}
	for i, backingImageName := range backingImageNames {
		backingImages[i] = backingImageMap[backingImageName]
	}
	return backingImages, nil
}

func (m *VolumeManager) GetBackingImage(name string) (*longhorn.BackingImage, error) {
	return m.ds.GetBackingImage(name)
}

func (m *VolumeManager) CreateBackingImage(name, url string) (*longhorn.BackingImage, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return nil, fmt.Errorf("cannot create backing image with empty image URL")
	}

	name = util.AutoCorrectName(name, datastore.NameMaximumLength)
	if !util.ValidateName(name) {
		return nil, fmt.Errorf("invalid name %v", name)
	}

	bi := &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: types.GetBackingImageLabels(),
		},
		Spec: types.BackingImageSpec{
			ImageURL: url,
			Disks:    map[string]struct{}{},
		},
	}

	bi, err := m.ds.CreateBackingImage(bi)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created backing image %v with URL %v", name, url)
	return bi, nil
}

func (m *VolumeManager) DeleteBackingImage(name string) error {
	replicas, err := m.ds.ListReplicasByBackingImage(name)
	if err != nil {
		return err
	}
	if len(replicas) != 0 {
		return fmt.Errorf("cannot delete backing image %v since there are replicas using it", name)
	}
	if err := m.ds.DeleteBackingImage(name); err != nil {
		return err
	}
	logrus.Infof("Deleting backing image %v", name)
	return nil
}

func (m *VolumeManager) CleanUpBackingImageInDisks(name string, disks []string) (*longhorn.BackingImage, error) {
	defer logrus.Infof("Cleaning up backing image %v in disks %+v", name, disks)
	bi, err := m.GetBackingImage(name)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get backing image %v", name)
	}
	replicas, err := m.ds.ListReplicasByBackingImage(name)
	if err != nil {
		return nil, err
	}
	disksInUse := map[string]struct{}{}
	for _, r := range replicas {
		disksInUse[r.Spec.DiskID] = struct{}{}
	}
	for _, id := range disks {
		if _, exists := disksInUse[id]; exists {
			return nil, fmt.Errorf("cannot clean up backing image %v in disk %v since there is at least one replica using it", name, id)
		}
		delete(bi.Spec.Disks, id)
	}
	return m.ds.UpdateBackingImage(bi)
}
