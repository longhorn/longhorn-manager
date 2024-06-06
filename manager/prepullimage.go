package manager

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// PrePullImage creates a pre-pulling image for the instance manager and share manager images.
func (m *VolumeManager) PrePullImage(imageList []string) error {
	if err := m.clearOldImages(imageList); err != nil {
		return errors.Wrapf(err, "cannot clear olds images with URLs %v", imageList)
	}
	for _, imageURL := range imageList {
		imageName := types.GetImageChecksumName(imageURL)
		image, err := m.GetImage(imageName)
		if err != nil {
			if !datastore.ErrorIsNotFound(err) {
				return errors.Wrapf(err, "cannot get image %v", imageName)
			}
			if _, err = m.CreateImage(imageName, imageURL); err != nil {
				return errors.Wrapf(err, "cannot create image %v for URL %v", imageName, imageURL)
			}
			return nil
		}
		if _, err = m.UpdateImage(image, imageURL); err != nil {
			return errors.Wrapf(err, "cannot update image %v for URL %v", imageName, imageURL)
		}
	}
	return nil
}

func (m *VolumeManager) clearOldImages(imageList []string) error {
	imagesSet := sets.New[string]()
	for _, imageURL := range imageList {
		imagesSet.Insert(types.GetImageChecksumName(imageURL))
	}
	images, err := m.ds.ListImagesRO()
	if err != nil {
		return err
	}

	for _, image := range images {
		if !imagesSet.Has(image.Name) {
			if err := m.ds.DeleteImage(image.Name); err != nil {
				return err
			}
			logrus.Infof("Deleted old image %v", image.Name)
		}
	}

	return nil
}

// GetImage returns the image object by the image name.
func (m *VolumeManager) GetImage(image string) (*longhorn.Image, error) {
	return m.ds.GetImage(image)
}

// CreateImage creates an image object with the instance manager and share manager images.
func (m *VolumeManager) CreateImage(imageName, imageURL string) (*longhorn.Image, error) {
	imageObj := &longhorn.Image{
		ObjectMeta: metav1.ObjectMeta{
			Name:   imageName,
			Labels: types.GetImageLabels(longhorn.PrePullImageName),
		},
		Spec: longhorn.ImageSpec{
			ImageURL: imageURL,
		},
		Status: longhorn.ImageStatus{
			State: longhorn.ImageStateUnknown,
		},
	}
	imageObj, err := m.ds.CreateImage(imageObj)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created image %v (%v)", imageObj.Name, imageObj.Spec.ImageURL)
	return imageObj, nil
}

// UpdateImage updates the image object.
func (m *VolumeManager) UpdateImage(image *longhorn.Image, imageURL string) (*longhorn.Image, error) {
	var err error
	if image.Spec.ImageURL != imageURL {
		image.Spec.ImageURL = imageURL
		if image, err = m.ds.UpdateImage(image); err != nil {
			return nil, err
		}
	}
	return image, nil
}
