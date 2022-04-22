package orphan

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

var (
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
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
	var patchOps admission.PatchOps

	orphan := newObj.(*longhorn.Orphan)

	// Merge the user created and longhorn specific labels
	labels := orphan.Labels
	if labels == nil {
		labels = map[string]string{}
	}

	// Add labels according to the orphan type
	var longhornLabels map[string]string

	switch {
	case orphan.Spec.Type == longhorn.OrphanTypeReplica:
		longhornLabels = types.GetOrphanLabelsForOrphanedDirectory(orphan.Spec.NodeID, orphan.Spec.Parameters[longhorn.OrphanDiskUUID])
	}
	if longhornLabels == nil {
		return nil, werror.NewInvalidError("invalid orphan labels", "")
	}
	for k, v := range longhornLabels {
		labels[k] = v
	}
	bytes, err := json.Marshal(labels)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding for orphan %v labels", orphan.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)))

	// Add finalizer for orphan
	if err := util.AddFinalizer(longhornFinalizerKey, orphan); err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	bytes, err = json.Marshal(orphan.Finalizers)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding for orphan %v finalizers", orphan.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/finalizers", "value": %v}`, string(bytes)))

	return patchOps, nil
}
