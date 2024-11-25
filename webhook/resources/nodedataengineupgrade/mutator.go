package nodedataengineupgrade

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

type nodeDataEngineUpgradeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &nodeDataEngineUpgradeMutator{ds: ds}
}

func (u *nodeDataEngineUpgradeMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "nodedataengineupgrades",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.NodeDataEngineUpgrade{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (u *nodeDataEngineUpgradeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	if newObj == nil {
		return nil, werror.NewInvalidError("newObj is nil", "")
	}

	return mutate(newObj)
}

func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	nodeUpgrade, ok := newObj.(*longhorn.NodeDataEngineUpgrade)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.NodeDataEngineUpgrade", newObj), "")
	}
	var patchOps admission.PatchOps

	longhornLabels := types.GetNodeDataEngineUpgradeLabels(nodeUpgrade.Spec.DataEngineUpgradeManager, nodeUpgrade.Spec.NodeID)
	patchOp, err := common.GetLonghornLabelsPatchOp(nodeUpgrade, longhornLabels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for nodeUpgrade %v", nodeUpgrade.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	patchOp, err = common.GetLonghornFinalizerPatchOpIfNeeded(nodeUpgrade)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for nodeDataEngineUpgrade %v", nodeUpgrade.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
