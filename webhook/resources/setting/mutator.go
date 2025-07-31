package setting

import (
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
		},
	}
}

func (s *settingMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return s.mutate(newObj)
}

func (s *settingMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return s.mutate(newObj)
}

func (s *settingMutator) mutate(newObj runtime.Object) (admission.PatchOps, error) {
	setting, ok := newObj.(*longhorn.Setting)
	if !ok {
		return nil, werror.NewInvalidError("newObj is not a *longhorn.Setting", "")
	}

	definition, exists := types.GetSettingDefinition(types.SettingName(setting.Name))
	if !exists {
		return nil, werror.NewInvalidError("setting definition not found", setting.Name)
	}
	if definition.ApplicableDataEngines == nil {
		return nil, werror.NewInvalidError("setting definition does not have applicable engines", setting.Name)
	}

	var patchOps admission.PatchOps

	// If the setting is applicable to all engines, we set the defaultsByDataEngine to an empty map.
	if applicable, ok := definition.ApplicableDataEngines[longhorn.DataEngineTypeAll]; ok {
		if applicable {
			patchOps = append(patchOps, `{"op": "replace", "path": "/valuesByDataEngine", "value": {}}`)
		}
	}

	// If the setting is applicable to V1 Data Engine, we set the default value to an empty string.
	// If it is not applicable, we set the defaultsByDataEngine for V1 Data Engine to an empty string.
	if applicable, ok := definition.ApplicableDataEngines[longhorn.DataEngineTypeV1]; ok {
		patchOps = append(patchOps, `{"op": "replace", "path": "/value", "value": ""}`)
		if !applicable {
			if _, exist := setting.ValuesByDataEngine[longhorn.DataEngineTypeV1]; exist {
				patchOps = append(patchOps, `{"op": "remove", "path": "/valuesByDataEngine/v1"}`)
			}
		}
	}

	// If the setting is applicable to V2 Data Engine, we set the default value to an empty string.
	// If it is not applicable, we set the defaultsByDataEngine for V2 Data Engine
	if applicable, ok := definition.ApplicableDataEngines[longhorn.DataEngineTypeV2]; ok {
		patchOps = append(patchOps, `{"op": "replace", "path": "/value", "value": ""}`)
		if !applicable {
			if _, exist := setting.ValuesByDataEngine[longhorn.DataEngineTypeV2]; exist {
				patchOps = append(patchOps, `{"op": "remove", "path": "/valuesByDataEngine/v2"}`)
			}
		}
	}

	return patchOps, nil
}
