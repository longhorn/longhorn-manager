package dataengineupgrademanager

import (
	"fmt"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type dataEngineUpgradeManagerMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &dataEngineUpgradeManagerMutator{ds: ds}
}

func (u *dataEngineUpgradeManagerMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "dataengineupgrademanagers",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.DataEngineUpgradeManager{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (u *dataEngineUpgradeManagerMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	upgradeManager, ok := newObj.(*longhorn.DataEngineUpgradeManager)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DataEngineUpgradeManager", newObj), "")
	}
	var patchOps admission.PatchOps

	longhornLabels := types.GetDataEngineUpgradeManagerLabels()
	patchOp, err := common.GetLonghornLabelsPatchOp(upgradeManager, longhornLabels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for upgradeManager %v", upgradeManager.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	patchOp, err = common.GetLonghornFinalizerPatchOpIfNeeded(upgradeManager)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for dataEngineUpgradeManager %v", upgradeManager.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
