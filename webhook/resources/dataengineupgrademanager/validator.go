package dataengineupgrademanager

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type dataEngineUpgradeManagerValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &dataEngineUpgradeManagerValidator{ds: ds}
}

func (u *dataEngineUpgradeManagerValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "dataengineupgrademanagers",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.DataEngineUpgradeManager{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (u *dataEngineUpgradeManagerValidator) Create(request *admission.Request, newObj runtime.Object) error {
	upgradeManager, ok := newObj.(*longhorn.DataEngineUpgradeManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DataEngineUpgradeManager", newObj), "")
	}

	nodes, err := u.ds.ListNodes()
	if err != nil {
		return werror.NewInternalError(err.Error())
	}
	if len(nodes) < 2 {
		err = fmt.Errorf("single-node cluster is not supported for data engine live upgrade")
		return werror.NewInvalidError(err.Error(), "")
	}

	if upgradeManager.Spec.DataEngine != longhorn.DataEngineTypeV2 {
		err := fmt.Errorf("data engine %v is not supported for data engine live upgrade", upgradeManager.Spec.DataEngine)
		return werror.NewInvalidError(err.Error(), "spec.dataEngine")
	}

	return nil
}

func (u *dataEngineUpgradeManagerValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldUpgradeManager, ok := oldObj.(*longhorn.DataEngineUpgradeManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DataEngineUpgradeManager", oldObj), "")
	}
	newUpgradeManager, ok := newObj.(*longhorn.DataEngineUpgradeManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DataEngineUpgradeManager", newObj), "")
	}

	if newUpgradeManager.Spec.DataEngine != longhorn.DataEngineTypeV2 {
		err := fmt.Errorf("data engine %v is not supported", newUpgradeManager.Spec.DataEngine)
		return werror.NewInvalidError(err.Error(), "spec.dataEngine")
	}

	if oldUpgradeManager.Spec.DataEngine != newUpgradeManager.Spec.DataEngine {
		return werror.NewInvalidError("spec.dataEngine field is immutable", "spec.dataEngine")
	}

	if !reflect.DeepEqual(oldUpgradeManager.Spec.Nodes, newUpgradeManager.Spec.Nodes) {
		return werror.NewInvalidError("nodes field is immutable", "spec.nodes")
	}

	return nil
}
