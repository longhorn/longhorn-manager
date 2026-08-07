package diskschedule

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/multierr"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type diskScheduleValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &diskScheduleValidator{ds: ds}
}

func (v *diskScheduleValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "diskschedules",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.DiskSchedule{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
			admissionregv1.Delete,
		},
	}
}

func (v *diskScheduleValidator) Create(request *admission.Request, newObj runtime.Object) error {
	diskSchedule, ok := newObj.(*longhorn.DiskSchedule)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DiskSchedule", newObj), "")
	}

	node, getNodeErr := v.ds.GetNodeRO(diskSchedule.Spec.NodeID)
	if getNodeErr != nil {
		return werror.NewInvalidError(fmt.Sprintf("invalid node: %v", getNodeErr.Error()), "spec.nodeID")
	}
	if !node.DeletionTimestamp.IsZero() {
		return werror.NewInvalidError(fmt.Sprintf("node %v is deleted", node.Name), "spec.nodeID")
	}

	if err := validateResourceRequirement(diskSchedule.Spec.Replicas); nil != err {
		return werror.NewInvalidError(fmt.Sprintf("invalid scheduling replicas on disk %v: %v", diskSchedule.Name, err.Error()), "diskSchedule.Spec.Replicas")
	}
	if err := validateResourceRequirement(diskSchedule.Spec.BackingImages); nil != err {
		return werror.NewInvalidError(fmt.Sprintf("invalid scheduling backing images on disk %v: %v", diskSchedule.Name, err.Error()), "diskSchedule.Spec.BackingImages")
	}

	return nil
}

func (v *diskScheduleValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	_, ok := oldObj.(*longhorn.DiskSchedule)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DiskSchedule", oldObj), "")
	}
	newDiskSchedule, ok := newObj.(*longhorn.DiskSchedule)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DiskSchedule", newObj), "")
	}
	isRemovingLonghornFinalizer, err := common.IsRemovingLonghornFinalizer(oldObj, newObj)
	if err != nil {
		err = errors.Wrap(err, "failed to check if removing longhorn.io finalizer from deleted object")
		return werror.NewInvalidError(err.Error(), "")
	} else if isRemovingLonghornFinalizer {
		// We always allow the removal of the longhorn.io finalizer while an object is being deleted. It is the
		// controller's responsibility to wait for the correct conditions to attempt to remove it.
		return nil
	}

	if newDiskSchedule.Status.StorageScheduled < 0 {
		return werror.NewInvalidError("StorageScheduled should be greater than or equal to 0", "diskSchedule.Status.StorageScheduled")
	}

	if err := validateResourceRequirement(newDiskSchedule.Spec.Replicas); nil != err {
		return werror.NewInvalidError(fmt.Sprintf("invalid scheduling replicas on disk %v: %v", newDiskSchedule.Name, err.Error()), "diskSchedule.Spec.Replicas")
	}
	if err := validateResourceRequirement(newDiskSchedule.Spec.BackingImages); nil != err {
		return werror.NewInvalidError(fmt.Sprintf("invalid scheduling backing images on disk %v: %v", newDiskSchedule.Name, err.Error()), "diskSchedule.Spec.BackingImages")
	}

	return nil
}

func (v *diskScheduleValidator) Delete(request *admission.Request, obj runtime.Object) error {
	diskSchedule, ok := obj.(*longhorn.DiskSchedule)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DiskSchedule", obj), "")
	}

	isDeletedByLonghorn := false
	if diskSchedule.Annotations != nil {
		_, isDeletedByLonghorn = diskSchedule.Annotations[types.GetLonghornLabelKey(types.DeleteDiskFromLonghorn)]
	}
	if !isDeletedByLonghorn {
		return werror.NewInvalidError("disk schedule can only be deleted by Longhorn", "diskSchedule.Annotations")
	}

	return nil
}

func validateResourceRequirement(requirement map[string]int64) (err error) {
	if requirement == nil {
		return nil
	}

	for name, size := range requirement {
		if name == "" {
			err = multierr.Append(err, fmt.Errorf("resource name cannot be empty"))
		}
		if size < 0 {
			err = multierr.Append(err, fmt.Errorf("invalid size %v for resource %v", size, name))
		}
	}
	return err
}
