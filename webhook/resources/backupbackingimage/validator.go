package backupbackingimage

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
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
	backingImageName := backupBackingImage.Spec.BackingImage

	backingImage, err := bbi.ds.GetBackingImageRO(backingImageName)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return werror.NewInvalidError(fmt.Sprintf("failed to get the backing image %v for backup: %v", backingImageName, err), "")
	}
	// TODO: support backup for v2 data engine in the future
	if backingImage != nil && types.IsDataEngineV2(backingImage.Spec.DataEngine) {
		return werror.NewInvalidError(fmt.Sprintf("backing image %v uses v2 data engine which doesn't support backup operations", backingImageName), "")
	}

	return nil
}
