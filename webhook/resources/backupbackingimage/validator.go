package backupbackingimage

import (
	"fmt"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type backupBackingImageValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupBackingImageValidator{ds: ds}
}

func (bbi *backupBackingImageValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backupbackingimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackupBackingImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (bbi *backupBackingImageValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backupBackingImage, ok := newObj.(*longhorn.BackupBackingImage)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupBackingImage", newObj), "")
	}
	backingImageName := backupBackingImage.Name

	backingImage, err := bbi.ds.GetBackingImageRO(backingImageName)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to get the backing image for backup", backingImage.Spec.SourceParameters, backingImage.Spec.SourceType), "")
	}
	if backingImage.Spec.DataEngine == longhorn.DataEngineTypeV2 {
		return werror.NewInvalidError(fmt.Sprintf("v2 backing image is not supported for backup"), "")
	}

	return nil
}
