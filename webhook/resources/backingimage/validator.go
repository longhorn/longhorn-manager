package backingimage

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backingImageValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backingImageValidator{ds: ds}
}

func (b *backingImageValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backingimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackingImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Delete,
		},
	}
}

func (b *backingImageValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backingImage, ok := newObj.(*longhorn.BackingImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", newObj), "")
	}
	if !util.ValidateName(backingImage.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", backingImage.Name), "")
	}

	if len(backingImage.Spec.Checksum) != 0 {
		if !util.ValidateChecksumSHA512(backingImage.Spec.Checksum) {
			return werror.NewInvalidError(fmt.Sprintf("invalid checksum %v", backingImage.Spec.Checksum), "")
		}
	}

	if err := validateMinNumberOfBackingImageCopies(backingImage.Spec.MinNumberOfCopies); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	switch longhorn.BackingImageDataSourceType(backingImage.Spec.SourceType) {
	case longhorn.BackingImageDataSourceTypeDownload:
		if backingImage.Spec.SourceParameters[longhorn.DataSourceTypeDownloadParameterURL] == "" {
			return werror.NewInvalidError(fmt.Sprintf("invalid parameter %+v for source type %v", backingImage.Spec.SourceParameters, backingImage.Spec.SourceType), "")
		}
	case longhorn.BackingImageDataSourceTypeUpload:
	case longhorn.BackingImageDataSourceTypeExportFromVolume:
		volumeName := backingImage.Spec.SourceParameters[longhorn.DataSourceTypeExportFromVolumeParameterVolumeName]
		if volumeName == "" {
			return werror.NewInvalidError(fmt.Sprintf("invalid parameter %+v for source type %v", backingImage.Spec.SourceParameters, backingImage.Spec.SourceType), "")
		}
		v, err := b.ds.GetVolume(volumeName)
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("failed to get volume %v before exporting backing image", volumeName), "")
		}
		if v.Status.Robustness == longhorn.VolumeRobustnessFaulted {
			return werror.NewInvalidError(fmt.Sprintf("cannot export a backing image from faulted volume %v", volumeName), "")
		}
		eiName := types.GetEngineImageChecksumName(v.Status.CurrentImage)
		ei, err := b.ds.GetEngineImage(eiName)
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("failed to get then check engine image %v for volume %v before exporting backing image", eiName, volumeName), "")
		}
		if ei.Status.CLIAPIVersion < engineapi.CLIVersionFive {
			return werror.NewInvalidError(fmt.Sprintf("engine image %v CLI version %v doesn't support this feature, please upgrade engine for volume %v before exporting backing image from the volume", eiName, ei.Status.CLIAPIVersion, volumeName), "")
		}

		if backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType] != manager.DataSourceTypeExportFromVolumeParameterExportTypeRAW &&
			backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType] != manager.DataSourceTypeExportFromVolumeParameterExportTypeQCOW2 {
			return werror.NewInvalidError(fmt.Sprintf("unsupported export type %v", backingImage.Spec.SourceParameters[manager.DataSourceTypeExportFromVolumeParameterExportType]), "")
		}
	}

	return nil
}

func (v *backingImageValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	_, ok := oldObj.(*longhorn.BackingImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", oldObj), "")
	}
	newBackingImage, ok := newObj.(*longhorn.BackingImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", newObj), "")
	}

	if err := validateMinNumberOfBackingImageCopies(newBackingImage.Spec.MinNumberOfCopies); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (b *backingImageValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	backingImage, ok := oldObj.(*longhorn.BackingImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", oldObj), "")
	}

	replicas, err := b.ds.ListReplicasByBackingImage(backingImage.Name)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("cannot delete backing image %v since the error %v", backingImage.Name, err.Error()), "")
	}
	if len(replicas) != 0 {
		return werror.NewInvalidError(fmt.Sprintf("cannot delete backing image %v since there are replicas using it", backingImage.Name), "")
	}
	return nil
}

func validateMinNumberOfBackingImageCopies(number int) error {
	if err := types.ValidateMinNumberOfBackingIamgeCopies(number); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}
	return nil
}
