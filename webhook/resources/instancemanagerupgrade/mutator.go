package instancemanagerupgrade

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type instanceManagerUpgradeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &instanceManagerUpgradeMutator{ds: ds}
}

func (m *instanceManagerUpgradeMutator) Resource() admission.Resource {
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

func (m *instanceManagerUpgradeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (m *instanceManagerUpgradeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	imu, ok := newObj.(*longhorn.InstanceManagerUpgrade)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.InstanceManagerUpgrade", newObj), "")
	}

	patchOp, err := common.GetLonghornLabelsPatchOp(imu, types.GetInstanceManagerUpgradeLabels(imu.Spec.NodeID), nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for instance manager upgrade %v", imu.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps := admission.PatchOps{patchOp}

	patchOp, err = common.GetLonghornFinalizerPatchOpIfNeeded(imu)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for instance manager upgrade %v", imu.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
