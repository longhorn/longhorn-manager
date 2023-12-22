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
			admissionregv1.Delete,
		},
	}
}

func (r *replicaValidator) Create(request *admission.Request, newObj runtime.Object) error {
	replica, ok := newObj.(*longhorn.Replica)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Replica", newObj), "")
	}
	if datastore.IsBackendStoreDriverV2(replica.Spec.BackendStoreDriver) {
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
	oldReplica, ok := oldObj.(*longhorn.Replica)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Replica", oldObj), "")
	}
	newReplica, ok := newObj.(*longhorn.Replica)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Replica", newObj), "")
	}
	if oldReplica.Spec.BackendStoreDriver != "" {
		if oldReplica.Spec.BackendStoreDriver != newReplica.Spec.BackendStoreDriver {
			err := fmt.Errorf("changing backend store driver for replica %v is not supported", oldReplica.Name)
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	return nil
}

func (r *replicaValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	replica := oldObj.(*longhorn.Replica)

	if err := r.validateReplicaDeletion(replica); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (r *replicaValidator) validateReplicaDeletion(replica *longhorn.Replica) error {
	if replica.Spec.VolumeName == "" {
		return nil
	}

	volume, err := r.ds.GetVolumeRO(replica.Spec.VolumeName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to get volume %v before deleting replica", replica.Spec.VolumeName)
	}

	if volume.DeletionTimestamp != nil {
		return nil
	}

	replicas, err := r.ds.ListVolumeReplicasRO(volume.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to list replicas for volume %v before deleting replica", volume.Name)
	}

	availableReplicas := map[string]struct{}{}
	for _, r := range replicas {
		// If the healthyAt as well as failedAt are set to non-empty string,
		// the replica is still regarded as **available** because its data is probably
		// intact and can be used for rescue if other replicas are not available anymore.
		if r.Spec.HealthyAt != "" && r.DeletionTimestamp == nil {
			availableReplicas[r.Name] = struct{}{}
		}
	}
	if len(availableReplicas) == 1 {
		if _, ok := availableReplicas[replica.Name]; ok {
			return fmt.Errorf("cannot delete replica %v because volume %v only has one available replica", replica.Name, volume.Name)
		}
	}

	return nil
}
