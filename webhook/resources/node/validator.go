package node

import (
	"fmt"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type nodeValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &nodeValidator{ds: ds}
}

func (n *nodeValidator) Resource() admission.Resource {
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

func (n *nodeValidator) Create(request *admission.Request, newObj runtime.Object) error {
	node, ok := newObj.(*longhorn.Node)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Node", newObj), "")
	}

	if node.Spec.InstanceManagerCPURequest < 0 {
		return werror.NewInvalidError("instanceManagerCPURequest should be greater than or equal to 0", "")
	}

	v2DataEngineEnabled, err := n.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
	if err != nil {
		err = errors.Wrapf(err, "failed to get spdk setting")
		return werror.NewInvalidError(err.Error(), "")
	}

	for name, disk := range node.Spec.Disks {
		if !v2DataEngineEnabled {
			if disk.Type == longhorn.DiskTypeBlock {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported since v2 data engine is disabled", name, disk.Type), "")
			}
		}

		if disk.Type == longhorn.DiskTypeBlock {
			if disk.StorageReserved != 0 {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported to reserve storage", name, disk.Type), "")
			}
		} else {
			if disk.DiskDriver != longhorn.DiskDriverNone {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported to specify disk driver", name, disk.Type), "")
			}
		}
	}

	return nil
}

func (n *nodeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldNode, ok := oldObj.(*longhorn.Node)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Node", oldObj), "")
	}
	newNode, ok := newObj.(*longhorn.Node)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Node", newObj), "")
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

	if newNode.Spec.InstanceManagerCPURequest < 0 {
		return werror.NewInvalidError("instanceManagerCPURequest should be greater than or equal to 0", "")
	}

	// Only scheduling disabled node can be evicted
	// Can not enable scheduling on an evicting node
	if newNode.Spec.EvictionRequested && newNode.Spec.AllowScheduling {
		return werror.NewInvalidError(fmt.Sprintf("need to disable scheduling on node %v for node eviction, or cancel eviction to enable scheduling on this node",
			oldNode.Name), "")
	}

	// Ensure the node controller already syncs the disk spec and status.
	if !isNodeDiskSpecAndStatusSynced(oldNode) {
		return werror.NewForbiddenError(fmt.Sprintf("spec and status of disks on node %v are being syncing and please retry later.", oldNode.Name))
	}

	// We need to make sure the tags passed in are valid before updating the node.
	_, err = util.ValidateTags(newNode.Spec.Tags)
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if newNode.Spec.InstanceManagerCPURequest != 0 {
		kubeNode, err := n.ds.GetKubernetesNodeRO(oldNode.Name)
		if err != nil {
			if !datastore.ErrorIsNotFound(err) {
				return werror.NewInvalidError(err.Error(), "")
			}
			logrus.Warnf("Kubernetes node %v has been deleted", oldNode.Name)
		} else {
			allocatableCPU := float64(kubeNode.Status.Allocatable.Cpu().MilliValue())
			instanceManagerCPUSetting, err := n.ds.GetSettingWithAutoFillingRO(types.SettingNameGuaranteedInstanceManagerCPU)
			if err != nil {
				return werror.NewInvalidError(err.Error(), "")
			}
			instanceManagerCPUInPercentage := instanceManagerCPUSetting.Value
			if newNode.Spec.InstanceManagerCPURequest > 0 {
				instanceManagerCPUInPercentage = fmt.Sprintf("%.0f", math.Round(float64(newNode.Spec.InstanceManagerCPURequest)/allocatableCPU*100.0))
			}
			// TODO: Support v2 data engine
			if err := types.ValidateCPUReservationValues(types.SettingNameGuaranteedInstanceManagerCPU, instanceManagerCPUInPercentage); err != nil {
				return werror.NewInvalidError(err.Error(), "")
			}
		}
	}

	// Only scheduling disabled disk can be evicted
	// Can not enable scheduling on an evicting disk
	for diskName, diskSpec := range newNode.Spec.Disks {
		if diskSpec.EvictionRequested && diskSpec.AllowScheduling {
			return werror.NewInvalidError(fmt.Sprintf("need to disable scheduling on disk %v for disk eviction, or cancel eviction to enable scheduling on this disk",
				diskName), "")
		}
	}

	v2DataEngineEnabled, err := n.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
	if err != nil {
		err = errors.Wrapf(err, "failed to get spdk setting")
		return werror.NewInvalidError(err.Error(), "")
	}

	// Validate Disks StorageReserved, Tags and Type
	for name, disk := range newNode.Spec.Disks {
		if disk.StorageReserved < 0 {
			return werror.NewInvalidError(fmt.Sprintf("update disk on node %v error: The storageReserved setting of disk %v(%v) is not valid, should be positive and no more than storageMaximum and storageAvailable",
				newNode.Name, name, disk.Path), "")
		}
		_, err := util.ValidateTags(disk.Tags)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
		if !v2DataEngineEnabled {
			if disk.Type == longhorn.DiskTypeBlock {
				return werror.NewInvalidError(fmt.Sprintf("update disk on node %v error: The disk %v(%v) is a block device, but the SPDK feature is not enabled",
					newNode.Name, name, disk.Path), "")
			}
		}
		if disk.Type == longhorn.DiskTypeBlock {
			if disk.StorageReserved != 0 {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported to reserve storage", name, disk.Type), "")
			}
		} else {
			if disk.DiskDriver != longhorn.DiskDriverNone {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported to specify disk driver", name, disk.Type), "")
			}
		}
	}

	// Validate delete disks
	for name, disk := range oldNode.Spec.Disks {
		if _, ok := newNode.Spec.Disks[name]; !ok {
			if disk.AllowScheduling || oldNode.Status.DiskStatus[name].StorageScheduled != 0 {
				logrus.Infof("Delete Disk on node %v error: Please disable the disk %v and remove all replicas first ", name, disk.Path)
				return werror.NewInvalidError(fmt.Sprintf("Delete Disk on node %v error: Please disable the disk %v and remove all replicas first ", name, disk.Path), "")
			}
		}
	}

	// Validate disk type change, the disk type is not allow to change
	for name, disk := range oldNode.Spec.Disks {
		if newDisk, ok := newNode.Spec.Disks[name]; ok {
			if disk.Type != "" && disk.Type != newDisk.Type {
				return werror.NewInvalidError(fmt.Sprintf("update disk on node %v error: The disk %v(%v) type is not allow to change", newNode.Name, name, disk.Path), "")
			}
		}
	}

	return nil
}

func isNodeDiskSpecAndStatusSynced(node *longhorn.Node) bool {
	if len(node.Spec.Disks) != len(node.Status.DiskStatus) {
		return false
	}

	for diskName := range node.Spec.Disks {
		if _, ok := node.Status.DiskStatus[diskName]; !ok {
			return false
		}
	}

	return true
}
