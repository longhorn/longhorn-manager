package instancemanager

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type instanceManagerMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &instanceManagerMutator{ds: ds}
}

func (i *instanceManagerMutator) Resource() admission.Resource {
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

func (i *instanceManagerMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	im := newObj.(*longhorn.InstanceManager)

	patchOps := mutate(im)

	node, err := i.ds.GetNodeRO(im.Spec.NodeID)
	if err != nil {
		err = errors.Wrapf(err, "failed to get node %v", im.Spec.NodeID)
		return nil, werror.NewInvalidError(err.Error(), "spec.nodeID")
	}

	// Set labels
	labels := types.GetInstanceManagerLabels(im.Spec.NodeID, im.Spec.Image, im.Spec.Type, im.Spec.BackendStoreDriver)
	bytes, err := json.Marshal(labels)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding labels of %v ", im.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)))

	// Set ownerReferences
	if len(im.OwnerReferences) == 0 {
		nodeRef := datastore.GetOwnerReferencesForNode(node)
		bytes, err := json.Marshal(nodeRef)
		if err != nil {
			err = errors.Wrapf(err, "failed to get JSON encoding for instanceManager %v ownerReferences", im.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/ownerReferences", "value": %v}`, string(bytes)))
	}

	return patchOps, nil
}

func (i *instanceManagerMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	patchOps := mutate(newObj.(*longhorn.InstanceManager))

	return patchOps, nil
}

// mutate contains functionality shared by Create and Update.
func mutate(im *longhorn.InstanceManager) admission.PatchOps {
	var patchOps admission.PatchOps

	if im.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}

	if string(im.Spec.BackendStoreDriver) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backendStoreDriver", "value": "%s"}`, longhorn.BackendStoreDriverTypeV1))
	}

	return patchOps
}
