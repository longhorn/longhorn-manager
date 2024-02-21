package node

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
	return mutate(newObj)
}

func (n *nodeMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	node, ok := newObj.(*longhorn.Node)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Node", newObj), "")
	}
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
			} else if disk.Type == longhorn.DiskTypeBlock {
				if disk.DiskDriver == longhorn.DiskDriverNone {
					patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/disks/%s/diskDriver", "value": "auto"}`, name))
				}
			}
		}
	}

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(node)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for node %v", node.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
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
