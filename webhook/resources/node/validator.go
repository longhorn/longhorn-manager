package node

import (
	"fmt"
	"math"
	"path/filepath"

	"github.com/cockroachdb/errors"
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
			admissionregv1.Delete,
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
		if err := validateDiskBlockSize(name, disk); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}

		if !v2DataEngineEnabled {
			if disk.Type == longhorn.DiskTypeBlock {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported since v2 data engine is disabled", name, disk.Type), "")
			}
		}

		if disk.Type != longhorn.DiskTypeBlock {
			if disk.DiskDriver != longhorn.DiskDriverNone {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported to specify disk driver", name, disk.Type), "")
			}
		}
	}

	// Validate no duplicate disk paths
	if err := validateNodeDiskPaths(node.Name, node.Spec.Disks); err != nil {
		return werror.NewInvalidError(err.Error(), "")
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
			logrus.WithError(err).Warnf("Kubernetes node %v has been deleted", oldNode.Name)
		} else {
			// TODO: Support v2 data engine
			instanceManagerCPUInPercentage, err := n.ds.GetSettingAsFloatByDataEngine(types.SettingNameGuaranteedInstanceManagerCPU, longhorn.DataEngineTypeV1)
			if err != nil {
				return werror.NewInvalidError(err.Error(), "")
			}
			if newNode.Spec.InstanceManagerCPURequest > 0 {
				allocatableCPU := float64(kubeNode.Status.Allocatable.Cpu().MilliValue())
				instanceManagerCPUInPercentage = math.Round(float64(newNode.Spec.InstanceManagerCPURequest) / allocatableCPU * 100.0)
			}
			instanceManagerCPUInPercentageStr := fmt.Sprintf("%.2f", instanceManagerCPUInPercentage)
			if err := types.ValidateSetting(string(types.SettingNameGuaranteedInstanceManagerCPU), instanceManagerCPUInPercentageStr); err != nil {
				return werror.NewInvalidError(err.Error(), "")
			}
		}
	}

	// We need to ensure that the name is not empty because it can lead to errors in the Longhorn
	if newNode.Spec.Name == "" {
		return werror.NewInvalidError("node name is invalid. You can't have a Spec.Name empty", "")
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
		if err := validateDiskBlockSize(name, disk); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}

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
		if disk.Type != longhorn.DiskTypeBlock {
			if disk.DiskDriver != longhorn.DiskDriverNone {
				return werror.NewInvalidError(fmt.Sprintf("disk %v type %v is not supported to specify disk driver", name, disk.Type), "")
			}
		}
	}

	// Validate no duplicate disk paths
	if err := validateNodeDiskPaths(newNode.Name, newNode.Spec.Disks); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	// Validate delete disks
	for name, disk := range oldNode.Spec.Disks {
		if _, ok := newNode.Spec.Disks[name]; !ok {
			if disk.AllowScheduling || oldNode.Status.DiskStatus[name].StorageScheduled != 0 {
				logrus.Infof("Delete Disk on node %v error: Please disable the disk %v and remove all replicas and backing images first", name, disk.Path)
				return werror.NewInvalidError(fmt.Sprintf("Delete Disk on node %v error: Please disable the disk %v and remove all replicas and backing images first ", name, disk.Path), "")
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
	if err := validateDiskBlockSizeUpdate(oldNode, newNode); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func validateDiskBlockSize(diskName string, disk longhorn.DiskSpec) error {
	if disk.BlockSize == 0 {
		return nil
	}
	if disk.BlockSize != 512 && disk.BlockSize != 4096 {
		return fmt.Errorf("disk %v block size %v is invalid; supported values are 0, 512, and 4096", diskName, disk.BlockSize)
	}

	if disk.Type != longhorn.DiskTypeBlock {
		return fmt.Errorf("disk %v type %v does not support a configurable block size", diskName, disk.Type)
	}

	if disk.DiskDriver != longhorn.DiskDriverNone && disk.DiskDriver != longhorn.DiskDriverAuto && disk.DiskDriver != longhorn.DiskDriverAio {
		return fmt.Errorf("disk %v driver %v does not support a configurable block size", diskName, disk.DiskDriver)
	}
	if disk.DiskDriver == longhorn.DiskDriverAuto && types.IsBDF(disk.Path) {
		return fmt.Errorf("disk %v uses a PCI address with the auto driver, which does not support a configurable block size", diskName)
	}

	return nil
}

func validateDiskBlockSizeUpdate(oldNode, newNode *longhorn.Node) error {
	for diskName, oldDisk := range oldNode.Spec.Disks {
		newDisk, exists := newNode.Spec.Disks[diskName]
		if !exists || oldDisk.Type != longhorn.DiskTypeBlock {
			continue
		}
		if oldDisk.BlockSize == newDisk.BlockSize {
			continue
		}

		oldDiskStatus, exists := oldNode.Status.DiskStatus[diskName]
		if !exists || oldDiskStatus == nil || oldDiskStatus.DiskUUID == "" {
			continue
		}

		if oldDiskStatus.ActualBlockSize == 0 {
			if oldDisk.BlockSize == 0 &&
				types.GetCondition(oldDiskStatus.Conditions, longhorn.DiskConditionTypeReady).Status != longhorn.ConditionStatusTrue {
				continue
			}
			return fmt.Errorf("update disk on node %v error: The actual block size of initialized disk %v(%v) is not available", newNode.Name, diskName, oldDisk.Path)
		}
		if effectiveDiskBlockSize(newDisk.BlockSize) != oldDiskStatus.ActualBlockSize {
			return fmt.Errorf("update disk on node %v error: The block size of initialized disk %v(%v) is not allowed to change", newNode.Name, diskName, oldDisk.Path)
		}
	}

	return nil
}

func effectiveDiskBlockSize(blockSize int64) int64 {
	if blockSize == 0 {
		return 512
	}

	return blockSize
}

func validateNodeDiskPaths(nodeName string, disks map[string]longhorn.DiskSpec) error {
	pathMap := map[string]string{}
	var duplicates []string

	for diskName, disk := range disks {
		// Normalize the path
		cleanPath := filepath.Clean(disk.Path)

		// Avoid resolving symlinks here because the webhook runs in a
		// pod and cannot access the target node's file system.
		if existingDisk, exists := pathMap[cleanPath]; exists {
			duplicates = append(duplicates, fmt.Sprintf("%v (disks %v and %v)", cleanPath, existingDisk, diskName))
			continue
		}

		pathMap[cleanPath] = diskName
	}

	if len(duplicates) > 0 {
		return fmt.Errorf("duplicate disk paths on node %v: %v", nodeName, duplicates)
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

func (n *nodeValidator) Delete(request *admission.Request, obj runtime.Object) error {
	node, ok := obj.(*longhorn.Node)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.Node", obj), "")
	}

	// Annotations `DeleteNodeFromLonghorn` is used to note that deleting node is by Longhorn during uninstalling.
	// When `isUninstalling` is true, the node is deleted by Longhorn, allows the deletion;
	// otherwise, continue to validate the instance and the node status.
	isUninstalling := false
	if node.Annotations != nil {
		_, isUninstalling = node.Annotations[types.GetLonghornLabelKey(types.DeleteNodeFromLonghorn)]
	}
	if isUninstalling {
		return nil
	}

	// If not uninstalling, only remove node from longhorn without any volumes on it
	replicas, err := n.ds.ListReplicasByNodeRO(node.Name)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to list replicas on node %v: %v", node.Name, err), "")
	}
	engines, err := n.ds.ListEnginesByNodeRO(node.Name)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("failed to list engines on node %v: %v", node.Name, err), "")
	}

	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	// Only could delete node from longhorn if kubernetes node missing or manager pod is missing
	if condition.Status == longhorn.ConditionStatusTrue ||
		(condition.Reason != longhorn.NodeConditionReasonKubernetesNodeGone &&
			condition.Reason != longhorn.NodeConditionReasonManagerPodMissing) ||
		node.Spec.AllowScheduling || len(replicas) > 0 || len(engines) > 0 {
		return werror.NewInvalidError(
			fmt.Sprintf("could not delete node %v with node ready condition is %v, reason is %v, node schedulable %v, and %v replica, %v engine running on it",
				node.Name, condition.Status, condition.Reason, node.Spec.AllowScheduling, len(replicas), len(engines)), "")
	}

	return nil
}
