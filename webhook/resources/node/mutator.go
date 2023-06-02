package node

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	common "github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type nodeMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &nodeMutator{ds: ds}
}

func (n *nodeMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "nodes",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Node{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (n *nodeMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	node := newObj.(*longhorn.Node)

	patchOps, err := mutate(newObj)
	if err != nil {
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	patchOp, err := common.GetLonghornFinalizerPatchOp(node)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for node %v", node.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (n *nodeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	node := newObj.(*longhorn.Node)
	var patchOps admission.PatchOps

	if node.Spec.Tags == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/tags", "value": []}`)
	} else {
		if len(node.Spec.Tags) > 0 {
			tags := deduplicateTags(node.Spec.Tags)
			bytes, err := json.Marshal(tags)
			if err != nil {
				err = errors.Wrapf(err, "failed to get JSON encoding for node %v tags", node.Name)
				return nil, werror.NewInvalidError(err.Error(), "")
			}
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/tags", "value": %s}`, string(bytes)))
		}
	}

	if node.Spec.Disks == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/disks", "value": {}}`)
	} else {
		for name, disk := range node.Spec.Disks {
			if disk.Tags == nil {
				patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/disks/%s/tags", "value": []}`, name))
			} else {
				if len(disk.Tags) > 0 {
					tags := deduplicateTags(disk.Tags)
					bytes, err := json.Marshal(tags)
					if err != nil {
						err = errors.Wrapf(err, "failed to get JSON encoding for node %v disk labels", node.Name)
						return nil, werror.NewInvalidError(err.Error(), "")
					}
					patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/disks/%s/tags", "value": %s}`, name, string(bytes)))
				}
			}

			if disk.Type == "" {
				patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/disks/%s/diskType", "value": "filesystem"}`, name))
			}
		}
	}

	return patchOps, nil
}

func deduplicateTags(inputTags []string) []string {
	foundTags := make(map[string]struct{})
	var tags []string
	for _, tag := range inputTags {
		if _, ok := foundTags[tag]; ok {
			continue
		}
		foundTags[tag] = struct{}{}
		tags = append(tags, tag)
	}

	sort.Strings(tags)

	return tags
}
