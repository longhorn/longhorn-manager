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

	bimtypes "github.com/longhorn/backing-image-manager/pkg/types"
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
	case longhorn.BackingImageDataSourceTypeClone:
		sourceBackingImageName := backingImage.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterBackingImage]
		if sourceBackingImageName == "" {
			return werror.NewInvalidError(fmt.Sprintf("invalid parameter %+v for source type %v", backingImage.Spec.SourceParameters, backingImage.Spec.SourceType), "")
		}
		sourceBackingImage, err := b.ds.GetBackingImageRO(sourceBackingImageName)
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("invalid parameter %+v for source type %v", backingImage.Spec.SourceParameters, backingImage.Spec.SourceType), "")
		}
		return b.validateCloneParameters(sourceBackingImage, backingImage)
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

func (b *backingImageValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldBackingImage, ok := oldObj.(*longhorn.BackingImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", oldObj), "")
	}

	backingImage, ok := newObj.(*longhorn.BackingImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", newObj), "")
	}

	if err := validateMinNumberOfBackingImageCopies(backingImage.Spec.MinNumberOfCopies); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if oldBackingImage.Spec.Secret != "" {
		if oldBackingImage.Spec.Secret != backingImage.Spec.Secret {
			err := fmt.Errorf("changing secret for BackingImage %v is not supported", oldBackingImage.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if oldBackingImage.Spec.SecretNamespace != "" {
		if oldBackingImage.Spec.SecretNamespace != backingImage.Spec.SecretNamespace {
			err := fmt.Errorf("changing secret namespace for BackingImage %v is not supported", oldBackingImage.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
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

func (b *backingImageValidator) validateCloneParameters(sourceBackingImage, targetBackingImage *longhorn.BackingImage) error {
	if _, exists := targetBackingImage.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterEncryption]; !exists {
		return werror.NewInvalidError("encryption should not be empty for cloning", "")
	}

	encryption := bimtypes.EncryptionType(targetBackingImage.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterEncryption])
	if encryption != bimtypes.EncryptionTypeEncrypt && encryption != bimtypes.EncryptionTypeDecrypt && encryption != bimtypes.EncryptionTypeIgnore {
		return werror.NewInvalidError(fmt.Sprintf("encryption %v is not supported", encryption), "")
	}

	if sourceBackingImage.Spec.Secret != "" {
		if encryption == bimtypes.EncryptionTypeEncrypt {
			return werror.NewInvalidError(fmt.Sprintf("cannot re-encrypt the backing image, %v is already encrypted", sourceBackingImage.Name), "")
		}
	}

	if sourceBackingImage.Spec.Secret == "" {
		if encryption == bimtypes.EncryptionTypeDecrypt {
			return werror.NewInvalidError(fmt.Sprintf("cannot re-decrypt the backing image, %v is already decrypted", sourceBackingImage.Name), "")
		}
	}

	if encryption == bimtypes.EncryptionTypeEncrypt || encryption == bimtypes.EncryptionTypeDecrypt {
		if targetBackingImage.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterSecret] == "" ||
			targetBackingImage.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterSecretNamespace] == "" {
			return werror.NewInvalidError("secret is not provided for encryption or decryption during cloneing", "")
		}

		secret := targetBackingImage.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterSecret]
		secretNamespace := targetBackingImage.Spec.SourceParameters[longhorn.DataSourceTypeCloneParameterSecretNamespace]
		_, err := b.ds.GetSecretRO(secretNamespace, secret)
		if err != nil {
			return werror.NewInvalidError(fmt.Sprintf("failed to get the secret %v in the namespace %v", secret, secretNamespace), "")
		}
	}

	return nil
}
