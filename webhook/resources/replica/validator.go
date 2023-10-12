package replica

import (
	"fmt"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type replicaValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &replicaValidator{ds: ds}
}

func (r *replicaValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "replicas",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Replica{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (r *replicaValidator) Create(request *admission.Request, newObj runtime.Object) error {
	replica := newObj.(*longhorn.Replica)

	if replica.Spec.BackendStoreDriver == longhorn.BackendStoreDriverTypeV2 {
		v2DataEngineEnabled, err := r.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
		if err != nil {
			err = errors.Wrapf(err, "failed to get spdk setting")
			return werror.NewInvalidError(err.Error(), "")
		}
		if !v2DataEngineEnabled {
			return werror.NewInvalidError("v2 data engine is not enabled", "")
		}
	}

	return nil
}

func (r *replicaValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldReplica := oldObj.(*longhorn.Replica)
	newReplica := newObj.(*longhorn.Replica)

	if oldReplica.Spec.BackendStoreDriver != "" {
		if oldReplica.Spec.BackendStoreDriver != newReplica.Spec.BackendStoreDriver {
			err := fmt.Errorf("changing backend store driver for replica %v is not supported", oldReplica.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
}
