package shard

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type shardValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &shardValidator{ds: ds}
}

func (v *shardValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "shards",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.Shard{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
			admissionregv1.Delete,
		},
	}
}

func (v *shardValidator) Create(request *admission.Request, newObj runtime.Object) error {
	shard, ok := newObj.(*longhorn.Shard)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Shard", newObj), "")
	}

	sg, err := v.ds.GetShardGroupRO(shard.Spec.ShardGroupName)
	if err != nil {
		// The informer cache can lag the controller's ShardGroup create, so a missing
		// group is transient, not a bad shard spec - keep it retryable, not Invalid.
		return werror.NewInternalError(fmt.Sprintf("failed to get ShardGroup %v to validate shard %v: %v", shard.Spec.ShardGroupName, shard.Name, err))
	}

	maxSlotIndex := sg.Spec.DataChunks + sg.Spec.ParityChunks - 1
	if shard.Spec.SlotIndex < 0 || shard.Spec.SlotIndex > maxSlotIndex {
		return werror.NewInvalidError(
			fmt.Sprintf("spec.slotIndex %v is out of range [0, %v] for ShardGroup %v with k=%v m=%v",
				shard.Spec.SlotIndex, maxSlotIndex, sg.Name, sg.Spec.DataChunks, sg.Spec.ParityChunks),
			"spec.slotIndex")
	}

	// Each slot must be held by exactly one shard - the controller keys shards by slot,
	// so a duplicate would collide. Reject an out-of-band create that reuses a slot.
	shards, err := v.ds.ListShardsByShardGroup(sg.Name)
	if err != nil {
		return werror.NewInternalError(fmt.Sprintf("failed to list shards for ShardGroup %v: %v", sg.Name, err))
	}
	for _, s := range shards {
		if s.Name != shard.Name && s.Spec.SlotIndex == shard.Spec.SlotIndex {
			return werror.NewInvalidError(
				fmt.Sprintf("spec.slotIndex %v is already used by shard %v in ShardGroup %v", shard.Spec.SlotIndex, s.Name, sg.Name),
				"spec.slotIndex")
		}
	}

	return nil
}

func (v *shardValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldShard, ok := oldObj.(*longhorn.Shard)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Shard", oldObj), "")
	}
	newShard, ok := newObj.(*longhorn.Shard)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Shard", newObj), "")
	}

	if oldShard.Spec.SlotIndex != newShard.Spec.SlotIndex {
		return werror.NewInvalidError("spec.slotIndex is immutable", "spec.slotIndex")
	}
	if oldShard.Spec.ShardGroupName != newShard.Spec.ShardGroupName {
		return werror.NewInvalidError("spec.shardGroupName is immutable", "spec.shardGroupName")
	}

	return nil
}

func (v *shardValidator) Delete(request *admission.Request, obj runtime.Object) error {
	shard, ok := obj.(*longhorn.Shard)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Shard", obj), "")
	}

	sg, err := v.ds.GetShardGroupRO(shard.Spec.ShardGroupName)
	if err != nil {
		// ShardGroup is already gone; allow the deletion.
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return werror.NewInvalidError(fmt.Sprintf("failed to get ShardGroup %v: %v", shard.Spec.ShardGroupName, err), "")
	}

	// The owning ShardGroup is being deleted, so allow this shard deletion. Otherwise the
	// healthy-shard check below would block it and the ShardGroup deletion could never finish.
	if !sg.DeletionTimestamp.IsZero() {
		return nil
	}

	shards, err := v.ds.ListShardsByShardGroup(sg.Name)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to list shards for ShardGroup %v: %v", sg.Name, err), "")
	}

	healthyCount := 0
	for _, s := range shards {
		if s.Name != shard.Name && s.Status.State == longhorn.ShardStateNormal {
			healthyCount++
		}
	}
	if healthyCount < sg.Spec.DataChunks {
		return werror.NewInvalidError(
			fmt.Sprintf("deleting shard %v would leave only %v healthy shard(s), below the minimum of %v required for ShardGroup %v",
				shard.Name, healthyCount, sg.Spec.DataChunks, sg.Name),
			"")
	}

	return nil
}
