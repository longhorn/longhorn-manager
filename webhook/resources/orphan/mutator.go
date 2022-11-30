package orphan

import (
	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type orphanMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &orphanMutator{ds: ds}
}

func (o *orphanMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "orphans",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Orphan{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (o *orphanMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	orphan := newObj.(*longhorn.Orphan)
	var patchOps admission.PatchOps

	// Add labels according to the orphan type
	var longhornLabels map[string]string
	switch {
	case orphan.Spec.Type == longhorn.OrphanTypeReplica:
		longhornLabels = types.GetOrphanLabelsForOrphanedDirectory(orphan.Spec.NodeID, orphan.Spec.Parameters[longhorn.OrphanDiskUUID])
	}
	if longhornLabels == nil {
		return nil, werror.NewInvalidError("invalid orphan labels", "")
	}

	patchOp, err := common.GetLonghornLabelsPatchOp(orphan, longhornLabels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for orphan %v", orphan.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	patchOp, err = common.GetLonghornFinalizerPatchOp(orphan)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for orphan %v", orphan.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}
