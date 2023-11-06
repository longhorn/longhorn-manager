package objectstore

import (
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"
	"github.com/pkg/errors"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type objectStoreMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &objectStoreMutator{ds: ds}
}

func (osm *objectStoreMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "objectstores",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.ObjectStore{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (osm *objectStoreMutator) Create(req *admission.Request, obj runtime.Object) (ops admission.PatchOps, err error) {
	store, ops, err := getDefaultMutations(obj)
	if err != nil {
		return nil, err
	}

	if store.Spec.TargetState == "" {
		op := "{\"op\": \"add\", \"path\": \"/spec/targetState\", \"value\": \"running\"}"
		ops = append(ops, op)
	}

	return ops, nil
}

func (osm *objectStoreMutator) Update(req *admission.Request, oldObj, newObj runtime.Object) (ops admission.PatchOps, err error) {
	_, ops, err = getDefaultMutations(newObj)
	return ops, err
}

func getDefaultMutations(obj runtime.Object) (store *longhorn.ObjectStore, ops admission.PatchOps, err error) {
	store = obj.(*longhorn.ObjectStore)

	finalizerOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(store)
	if err != nil {
		return store, nil, errors.Wrapf(err, "failed to get finializer patch for object store: %v", store.Name)
	}
	if finalizerOp != "" {
		ops = append(ops, finalizerOp)
	}

	return store, ops, nil
}
