package node

import (
	"fmt"
	"math"
	"math/big"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"

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

	if err := n.validateNodeResourceOverrides(node); err != nil {
		return err
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

	// If the Kubernetes Node is deleted without properly evicting the Longhorn
	// Node, the disk status may never be synced. Prevent disk configuration
	// changes in this state, while allowing a limited spec update to disable
	// node scheduling. Ref: https://github.com/longhorn/longhorn/issues/13494
	if _, err := n.ds.GetKubernetesNodeRO(oldNode.Name); err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return werror.NewInvalidError(err.Error(), "")
		}
		disksSpecChanged := !reflect.DeepEqual(oldNode.Spec.Disks, newNode.Spec.Disks)
		if disksSpecChanged {
			return werror.NewForbiddenError(fmt.Sprintf(
				"cannot modify disks on node %v after the Kubernetes node is deleted",
				oldNode.Name))
		}

		allowedSpec := oldNode.Spec
		allowedSpec.AllowScheduling = false
		if !oldNode.Spec.AllowScheduling || !reflect.DeepEqual(allowedSpec, newNode.Spec) {
			return werror.NewForbiddenError(fmt.Sprintf(
				"only disabling scheduling on node %v is allowed after the Kubernetes node is deleted",
				oldNode.Name))
		}
		return nil
	}
	disksSynced := isNodeDiskSpecAndStatusSynced(oldNode)
	if !disksSynced {
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

	if err := n.validateNodeResourceOverrides(newNode); err != nil {
		return err
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
		if disk.StorageReserved < 0 {
			return werror.NewInvalidError(fmt.Sprintf("update disk on node %v error: The storageReserved setting of disk %v(%v) is not valid, should be positive and no more than storageMaximum and storageAvailable",
				newNode.Name, name, disk.Path), "")
		}
		_, err := util.ValidateTags(disk.Tags)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}

		// Reject updating only block disks when the v2 Data Engine (SPDK) is disabled.
		if !v2DataEngineEnabled && disk.Type == longhorn.DiskTypeBlock {
			oldDisk, existed := oldNode.Spec.Disks[name]
			if !existed || !reflect.DeepEqual(oldDisk, disk) {
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

	return nil
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

// validateNodeResourceOverrides rejects malformed override values, contradictions within
// the node spec, and configurations guaranteed to fail.
func (n *nodeValidator) validateNodeResourceOverrides(node *longhorn.Node) error {
	var v2Resources *longhorn.NodeV2DataEngineResources
	if node.Spec.DataEngineResources != nil {
		v2Resources = node.Spec.DataEngineResources.V2
	}
	var imResources *corev1.ResourceRequirements
	if node.Spec.InstanceManagerResources != nil {
		imResources = node.Spec.InstanceManagerResources.V2
	}
	if v2Resources == nil && imResources == nil {
		return nil
	}

	kubeNode, err := n.ds.GetKubernetesNodeRO(node.Name)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return werror.NewInvalidError(err.Error(), "")
		}
		kubeNode = nil
	}

	explicitMask := ""
	if v2Resources != nil {
		if v2Resources.NumberOfCPUCores != nil {
			numberOfCPUCores := *v2Resources.NumberOfCPUCores
			if numberOfCPUCores < 0 {
				return werror.NewInvalidError("dataEngineResources.v2.numberOfCPUCores cannot be negative", "")
			}
			if numberOfCPUCores > 0 {
				if imResources != nil {
					return werror.NewInvalidError("dataEngineResources.v2.numberOfCPUCores and instanceManagerResources.v2 cannot be set together: dynamic CPU pinning derives the instance manager pod resources by itself", "")
				}
				if !strings.EqualFold(string(node.Status.CPUPolicy), string(longhorn.CPUManagerPolicyStatic)) {
					return werror.NewInvalidError(fmt.Sprintf("dataEngineResources.v2.numberOfCPUCores requires the kubelet CPU manager policy of node %v to be static", node.Name), "")
				}
				if kubeNode != nil && numberOfCPUCores > kubeNode.Status.Allocatable.Cpu().Value() {
					return werror.NewInvalidError(fmt.Sprintf("dataEngineResources.v2.numberOfCPUCores %v exceeds the %v allocatable CPUs of node %v", numberOfCPUCores, kubeNode.Status.Allocatable.Cpu().Value(), node.Name), "")
				}
			}
		}
		if v2Resources.CPUMask != nil {
			explicitMask, err = types.NormalizeCPUMask(*v2Resources.CPUMask)
			if err != nil {
				return werror.NewInvalidError(fmt.Sprintf("invalid dataEngineResources.v2.cpuMask: %v", err), "")
			}
		}
		if v2Resources.MemorySizeMiB != nil && *v2Resources.MemorySizeMiB < 0 {
			return werror.NewInvalidError("dataEngineResources.v2.memorySizeMiB cannot be negative", "")
		}
		if v2Resources.IobufSmallPoolSize != nil && *v2Resources.IobufSmallPoolSize < 0 {
			return werror.NewInvalidError("dataEngineResources.v2.iobufSmallPoolSize cannot be negative", "")
		}
		if v2Resources.IobufLargePoolSize != nil && *v2Resources.IobufLargePoolSize < 0 {
			return werror.NewInvalidError("dataEngineResources.v2.iobufLargePoolSize cannot be negative", "")
		}
	}

	if imResources != nil {
		if len(imResources.Claims) > 0 {
			return werror.NewInvalidError("instanceManagerResources.v2.claims is not supported: the instance manager pod declares no resource claims", "")
		}
		for _, list := range []corev1.ResourceList{imResources.Requests, imResources.Limits} {
			for name, quantity := range list {
				if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
					return werror.NewInvalidError(fmt.Sprintf("instanceManagerResources.v2 only supports cpu and memory: hugepages limits are derived from the effective data engine memory size, got %v", name), "")
				}
				if quantity.Sign() < 0 {
					return werror.NewInvalidError(fmt.Sprintf("instanceManagerResources.v2 %v %v cannot be negative", name, quantity.String()), "")
				}
			}
		}
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			request, hasRequest := imResources.Requests[name]
			limit, hasLimit := imResources.Limits[name]
			if hasRequest && hasLimit && request.Cmp(limit) > 0 {
				return werror.NewInvalidError(fmt.Sprintf("instanceManagerResources.v2 %v request %v exceeds limit %v", name, request.String(), limit.String()), "")
			}
		}
	}

	// A node-level pod resources override (or an explicit numberOfCPUCores of 0) turns dynamic
	// CPU pinning off for the node, so the inherited global mask becomes effective on it.
	effectiveMask, maskSource := explicitMask, "dataEngineResources.v2.cpuMask"
	switchesToStaticMask := imResources != nil ||
		(v2Resources != nil && v2Resources.NumberOfCPUCores != nil && *v2Resources.NumberOfCPUCores == 0)
	if effectiveMask == "" && switchesToStaticMask {
		globalMask, err := n.ds.GetSettingValueExistedByDataEngine(types.SettingNameDataEngineCPUMask, longhorn.DataEngineTypeV2)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
		if effectiveMask, err = types.NormalizeCPUMask(globalMask); err != nil {
			return werror.NewInvalidError(fmt.Sprintf("invalid %v setting inherited by node %v: %v", types.SettingNameDataEngineCPUMask, node.Name, err), "")
		}
		maskSource = fmt.Sprintf("inherited %v setting", types.SettingNameDataEngineCPUMask)
	}
	if effectiveMask != "" && kubeNode != nil {
		mask, ok := new(big.Int).SetString(strings.TrimPrefix(strings.ToLower(effectiveMask), "0x"), 16)
		if !ok {
			return werror.NewInvalidError(fmt.Sprintf("invalid CPU mask %v for node %v", effectiveMask, node.Name), "")
		}
		if int64(mask.BitLen()) > kubeNode.Status.Capacity.Cpu().Value() {
			return werror.NewInvalidError(fmt.Sprintf("%v %v references CPUs beyond the %v CPUs of node %v", maskSource, effectiveMask, kubeNode.Status.Capacity.Cpu().Value(), node.Name), "")
		}
	}

	hugepageEnabled, memorySizeMiB, err := n.getEffectiveHugepageAndMemorySize(node, v2Resources)
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}
	if hugepageEnabled {
		// The memory size becomes the hugepages-2Mi limit, which Kubernetes requires to be page aligned.
		if v2Resources != nil && v2Resources.MemorySizeMiB != nil && *v2Resources.MemorySizeMiB%2 != 0 {
			return werror.NewInvalidError(fmt.Sprintf("dataEngineResources.v2.memorySizeMiB %v must be a multiple of 2 when hugepages are enabled", *v2Resources.MemorySizeMiB), "")
		}
	} else if imResources != nil {
		// No-huge SPDK preallocates the memory size from regular memory at startup; a smaller
		// pod memory limit is a guaranteed OOM kill.
		if limit, ok := imResources.Limits[corev1.ResourceMemory]; ok && limit.Value()/(1<<20) < memorySizeMiB {
			return werror.NewInvalidError(fmt.Sprintf("the effective hugepage-enabled of node %v is false and the effective data engine memory size %vMi exceeds the instance manager pod memory limit %v; SPDK would be OOM-killed while preallocating", node.Name, memorySizeMiB, limit.String()), "")
		}
	}

	return nil
}

// getEffectiveHugepageAndMemorySize resolves from the node object being validated (the
// datastore still holds the previous revision), with the global settings as fallback.
func (n *nodeValidator) getEffectiveHugepageAndMemorySize(node *longhorn.Node, v2Resources *longhorn.NodeV2DataEngineResources) (bool, int64, error) {
	hugepageEnabled, err := n.ds.GetSettingAsBoolByDataEngine(types.SettingNameDataEngineHugepageEnabled, longhorn.DataEngineTypeV2)
	if err != nil {
		return false, 0, err
	}
	memorySizeMiB, err := n.ds.GetSettingAsIntByDataEngine(types.SettingNameDataEngineMemorySize, longhorn.DataEngineTypeV2)
	if err != nil {
		return false, 0, err
	}
	if v2Resources != nil {
		if v2Resources.HugepageEnabled != nil {
			hugepageEnabled = *v2Resources.HugepageEnabled
		}
		if v2Resources.MemorySizeMiB != nil {
			memorySizeMiB = *v2Resources.MemorySizeMiB
		}
	}
	return hugepageEnabled, memorySizeMiB, nil
}
