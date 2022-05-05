package sharemanager

import (
	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	common "github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type shareManagerMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &shareManagerMutator{ds: ds}
}

func (s *shareManagerMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "sharemanagers",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.ShareManager{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

func (s *shareManagerMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	shareManager := newObj.(*longhorn.ShareManager)
	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOp(shareManager)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for shareManager %v", shareManager.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}
