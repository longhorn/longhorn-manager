package supportbundle

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

type supportBundleValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &supportBundleValidator{ds: ds}
}

func (v *supportBundleValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "supportbundles",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.SupportBundle{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (v *supportBundleValidator) Create(request *admission.Request, newObj runtime.Object) error {
	supportBundles, err := v.ds.ListSupportBundlesRO()
	if err != nil {
		return err
	}

	failedLimit, err := v.ds.GetSettingAsInt(types.SettingNameSupportBundleFailedHistoryLimit)
	if err != nil {
		return werror.NewForbiddenError(fmt.Sprintf("failed to get %v setting", types.SettingNameSupportBundleFailedHistoryLimit))
	}

	// Comparing to all SupportBundles here because we are expecting all successful SupportBundles has been
	// downloaded and cleaned up. The existing SupportBundles should be failed or still in progress.
	if failedLimit > 0 && len(supportBundles) >= int(failedLimit) {
		return werror.NewForbiddenError(fmt.Sprintf("exceeded %v setting value %v, please increase the limit or remove some failed SupportBundles. You can also set the limit to 0 to automatically clean up all the failed SupportBundles", types.SettingNameSupportBundleFailedHistoryLimit, failedLimit))
	}

	for _, supportBundle := range supportBundles {
		if supportBundle.Status.State == longhorn.SupportBundleStateError {
			continue
		}

		return werror.NewForbiddenError(fmt.Sprintf("please try again later. Another %v is in %v phase", supportBundles[0].Name, supportBundle.Status.State))
	}

	return nil
}
