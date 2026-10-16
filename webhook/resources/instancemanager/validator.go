package instancemanager

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type instanceManagerValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &instanceManagerValidator{ds: ds}
}

func (i *instanceManagerValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "instancemanagers",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.InstanceManager{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (i *instanceManagerValidator) Create(request *admission.Request, newObj runtime.Object) error {
	im, ok := newObj.(*longhorn.InstanceManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.InstanceManager", newObj), "")
	}
	if err := i.validate(im); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}
	if err := validateDeprecatedV2DataEngineSpec(longhorn.V2DataEngineSpec{}, im.Spec.DataEngineSpec.V2); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (i *instanceManagerValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	newIm, ok := newObj.(*longhorn.InstanceManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.InstanceManager", newObj), "")
	}

	oldIm, ok := oldObj.(*longhorn.InstanceManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.InstanceManager", oldObj), "")
	}

	if err := i.validate(newIm); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}
	if err := validateDeprecatedV2DataEngineSpec(oldIm.Spec.DataEngineSpec.V2, newIm.Spec.DataEngineSpec.V2); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

// Unchanged values are allowed: full-object updates of an instance manager carry them over.
func validateDeprecatedV2DataEngineSpec(oldSpec, newSpec longhorn.V2DataEngineSpec) error {
	if newSpec.CPUMask != "" && newSpec.CPUMask != oldSpec.CPUMask {
		return fmt.Errorf("spec.dataEngineSpec.v2.cpuMask is deprecated, set dataEngineResources.v2.cpuMask on the Longhorn node instead")
	}
	if newSpec.CPUIsolationEnabled != "" && newSpec.CPUIsolationEnabled != oldSpec.CPUIsolationEnabled {
		return fmt.Errorf("spec.dataEngineSpec.v2.cpuIsolationEnabled is deprecated, set dataEngineResources.v2.cpuIsolationEnabled on the Longhorn node instead")
	}
	return nil
}

func (i *instanceManagerValidator) validate(im *longhorn.InstanceManager) error {
	if im.Labels == nil {
		return fmt.Errorf("labels for instanceManager %s is not set", im.Name)
	}

	if im.OwnerReferences == nil {
		return fmt.Errorf("ownerReferences for instanceManager %s is not set", im.Name)
	}

	if im.Spec.Type == "" {
		return fmt.Errorf("type for instanceManager %s is not set", im.Name)
	}

	if im.Spec.DataEngine == "" {
		return fmt.Errorf("data engine for instanceManager %s is not set", im.Name)
	}

	return nil
}
