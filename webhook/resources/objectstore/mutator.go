package objectstore

import (
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

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
	objectstore := obj.(*longhorn.ObjectStore)

	if objectstore.Spec.TargetState == "" {
		op := "{\"op\": \"add\", \"path\": \"/spec/targetState\", \"value\": \"running\"}"
		ops = append(ops, op)
	}

	return ops, nil
}

func (osm *objectStoreMutator) Update(req *admission.Request, oldObj, newObj runtime.Object) (patch admission.PatchOps, err error) {
	return patch, nil
}
