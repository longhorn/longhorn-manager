package instancemanagerupgrade

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type instanceManagerUpgradeValidator struct {
	admission.DefaultValidator
}

func NewValidator(_ *datastore.DataStore) admission.Validator {
	return &instanceManagerUpgradeValidator{}
}

func (v *instanceManagerUpgradeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "instancemanagerupgrades",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.InstanceManagerUpgrade{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *instanceManagerUpgradeValidator) Create(request *admission.Request, newObj runtime.Object) error {
	imu, ok := newObj.(*longhorn.InstanceManagerUpgrade)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.InstanceManagerUpgrade", newObj), "")
	}

	if imu.Spec.NodeID == "" {
		return werror.NewInvalidError("spec.nodeID field is required", "spec.nodeID")
	}

	if imu.Spec.TargetImage == "" {
		return werror.NewInvalidError("spec.targetImage field is required", "spec.targetImage")
	}

	return nil
}

func (v *instanceManagerUpgradeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldIMU, ok := oldObj.(*longhorn.InstanceManagerUpgrade)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.InstanceManagerUpgrade", oldObj), "")
	}

	newIMU, ok := newObj.(*longhorn.InstanceManagerUpgrade)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.InstanceManagerUpgrade", newObj), "")
	}

	if newIMU.Spec.NodeID != oldIMU.Spec.NodeID {
		return werror.NewInvalidError("spec.nodeID field is immutable", "spec.nodeID")
	}

	if newIMU.Spec.TargetImage != oldIMU.Spec.TargetImage {
		return werror.NewInvalidError("spec.targetImage field is immutable", "spec.targetImage")
	}

	return nil
}
