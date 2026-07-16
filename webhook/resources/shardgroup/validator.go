package shardgroup

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type shardGroupValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &shardGroupValidator{ds: ds}
}

func (v *shardGroupValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "shardgroups",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.ShardGroup{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *shardGroupValidator) Create(request *admission.Request, newObj runtime.Object) error {
	sg, ok := newObj.(*longhorn.ShardGroup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.ShardGroup", newObj), "")
	}

	// The 1:1 ShardGroup-to-Volume naming invariant is load-bearing: the Engine
	// controller's CreateInstance path looks up the ShardGroup by Engine.Spec.VolumeName
	// (== Volume.Name), so a ShardGroup whose Name diverges from Spec.VolumeName would
	// never be discovered by the engine that needs it. Enforce equality here so the
	// convention is a contract, not just a default set by the Volume controller.
	if sg.Name != sg.Spec.VolumeName {
		return werror.NewInvalidError(
			fmt.Sprintf("ShardGroup name %q must equal spec.volumeName %q", sg.Name, sg.Spec.VolumeName),
			"spec.volumeName")
	}

	vol, err := v.ds.GetVolumeRO(sg.Spec.VolumeName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return werror.NewInvalidError(fmt.Sprintf("volume %v does not exist for ShardGroup %v", sg.Spec.VolumeName, sg.Name), "spec.volumeName")
		}
		return werror.NewInternalError(fmt.Sprintf("failed to get volume %v for ShardGroup %v: %v", sg.Spec.VolumeName, sg.Name, err))
	}

	if !types.IsDataEngineV2(vol.Spec.DataEngine) {
		return werror.NewInvalidError(fmt.Sprintf("ShardGroup %v requires a v2 data engine volume, got %v", sg.Name, vol.Spec.DataEngine), "spec.volumeName")
	}

	if err := types.ValidateECParameters(sg.Spec.DataChunks, sg.Spec.ParityChunks, sg.Spec.StripSizeKB); err != nil {
		return werror.NewInvalidError(err.Error(), "spec")
	}

	return nil
}

func (v *shardGroupValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldSG, ok := oldObj.(*longhorn.ShardGroup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.ShardGroup", oldObj), "")
	}
	newSG, ok := newObj.(*longhorn.ShardGroup)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.ShardGroup", newObj), "")
	}

	if !reflect.DeepEqual(immutableFields(oldSG), immutableFields(newSG)) {
		return werror.NewInvalidError(fmt.Sprintf("shard group %v spec fields VolumeName, DataChunks, ParityChunks, and StripSizeKB are immutable", oldSG.Name), "")
	}

	return nil
}

type shardGroupImmutable struct {
	VolumeName   string
	DataChunks   int
	ParityChunks int
	StripSizeKB  int
}

func immutableFields(sg *longhorn.ShardGroup) shardGroupImmutable {
	return shardGroupImmutable{
		VolumeName:   sg.Spec.VolumeName,
		DataChunks:   sg.Spec.DataChunks,
		ParityChunks: sg.Spec.ParityChunks,
		StripSizeKB:  sg.Spec.StripSizeKB,
	}
}
