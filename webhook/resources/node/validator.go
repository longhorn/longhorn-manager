package node

import (
	"fmt"
	"math"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	"github.com/sirupsen/logrus"
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
			admissionregv1.Update,
			admissionregv1.Delete,
		},
	}
}

func (n *nodeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldNode := oldObj.(*longhorn.Node)
	newNode := newObj.(*longhorn.Node)

	// Only scheduling disabled node can be evicted
	// Can not enable scheduling on an evicting node
	if newNode.Spec.EvictionRequested && newNode.Spec.AllowScheduling {
		return werror.NewInvalidError(fmt.Sprintf("need to disable scheduling on node %v for node eviction, or cancel eviction to enable scheduling on this node",
			oldNode.Name), "")
	}

	// We need to make sure the tags passed in are valid before updating the node.
	_, err := util.ValidateTags(newNode.Spec.Tags)
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if newNode.Spec.EngineManagerCPURequest != 0 || newNode.Spec.ReplicaManagerCPURequest != 0 {
		kubeNode, err := n.ds.GetKubernetesNode(oldNode.Name)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
		allocatableCPU := float64(kubeNode.Status.Allocatable.Cpu().MilliValue())
		engineManagerCPUSetting, err := n.ds.GetSetting(types.SettingNameGuaranteedEngineManagerCPU)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
		engineManagerCPUInPercentage := engineManagerCPUSetting.Value
		if newNode.Spec.EngineManagerCPURequest > 0 {
			engineManagerCPUInPercentage = fmt.Sprintf("%.0f", math.Round(float64(newNode.Spec.EngineManagerCPURequest)/allocatableCPU*100.0))
		}
		replicaManagerCPUSetting, err := n.ds.GetSetting(types.SettingNameGuaranteedReplicaManagerCPU)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
		replicaManagerCPUInPercentage := replicaManagerCPUSetting.Value
		if newNode.Spec.ReplicaManagerCPURequest > 0 {
			replicaManagerCPUInPercentage = fmt.Sprintf("%.0f", math.Round(float64(newNode.Spec.ReplicaManagerCPURequest)/allocatableCPU*100.0))
		}
		if err := types.ValidateCPUReservationValues(engineManagerCPUInPercentage, replicaManagerCPUInPercentage); err != nil {
			return werror.NewInvalidError(err.Error(), "")
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

	// Validate StorageReserved and Disk.Tags
	for name, disk := range newNode.Spec.Disks {
		if disk.StorageReserved < 0 {
			return werror.NewInvalidError(fmt.Sprintf("update disk on node %v error: The storageReserved setting of disk %v(%v) is not valid, should be positive and no more than storageMaximum and storageAvailable",
				name, name, disk.Path), "")
		}
		_, err := util.ValidateTags(disk.Tags)
		if err != nil {
			return werror.NewInvalidError(err.Error(), "")
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

	return nil
}

func (n *nodeValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	node := oldObj.(*longhorn.Node)

	// Only remove node from longhorn without any volumes on it
	replicas, err := n.ds.ListReplicasByNodeRO(node.Name)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("could not delete node %v since listing replicas encountered %v", node.Name, err.Error()), "")
	}
	engines, err := n.ds.ListEnginesByNodeRO(node.Name)
	if err != nil {
		return werror.NewInvalidError(fmt.Sprintf("could not delete node %v since listing engines encountered %v", node.Name, err.Error()), "")
	}

	condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
	// Only could delete node from longhorn if kubernetes node missing or manager pod is missing
	if condition.Status == longhorn.ConditionStatusTrue ||
		(condition.Reason != longhorn.NodeConditionReasonKubernetesNodeGone &&
			condition.Reason != longhorn.NodeConditionReasonManagerPodMissing) ||
		node.Spec.AllowScheduling || len(replicas) > 0 || len(engines) > 0 {
		return werror.NewInvalidError(fmt.Sprintf("could not delete node %v with node ready condition is %v, reason is %v, node schedulable %v, and %v replica, %v engine running on it", node.Name,
			condition.Status, condition.Reason, node.Spec.AllowScheduling, len(replicas), len(engines)), "")
	}

	return nil
}
