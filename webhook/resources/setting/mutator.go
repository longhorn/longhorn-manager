package setting

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

type settingMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &settingMutator{ds: ds}
}

func (s *settingMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "settings",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Setting{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (s *settingMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return s.mutate(newObj)
}

func (s *settingMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return s.mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func (s *settingMutator) mutate(newObj runtime.Object) (admission.PatchOps, error) {
	setting, ok := newObj.(*longhorn.Setting)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("newObj %v is not a *longhorn.Setting", newObj), "")
	}

	definition, isExist := types.GetSettingDefinition(types.SettingName(setting.Name))
	if !isExist {
		return nil, werror.NewInvalidError(fmt.Sprintf("setting %s does not exist", setting.Name), "metadata.name")
	}

	value, err := datastore.GetSettingValidValue(definition, setting.Value)
	if err != nil {
		return nil, werror.NewInvalidError(fmt.Sprintf("failed to get valid value for setting %s: %v", setting.Name, err), "value")
	}

	var patchOps admission.PatchOps

	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/value", "value": %q}`, value))

	return patchOps, nil
}
