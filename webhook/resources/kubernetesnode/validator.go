package kubernetesnode

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type kubeNodeValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &kubeNodeValidator{ds: ds}
}

func (v *kubeNodeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "nodes",
		Scope:      admissionregv1.ClusterScope,
		APIGroup:   corev1.SchemeGroupVersion.Group,
		APIVersion: corev1.SchemeGroupVersion.Version,
		ObjectType: &corev1.Node{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Update,
		},
	}
}

func (v *kubeNodeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldNode, ok := oldObj.(*corev1.Node)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("invalid old object: expected *corev1.Node, got %T", oldObj), "")
	}

	newNode, ok := newObj.(*corev1.Node)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("invalid new object: expected *corev1.Node, got %T", newObj), "")
	}

	// Handle only Node updates.
	v2DataEngineEnabled, err := v.ds.IsDataEngineEnabled(longhorn.DataEngineTypeV2)
	if err != nil {
		return werror.NewInternalError(err.Error())
	}
	if v2DataEngineEnabled {
		canDisabledV2DataEngine, err := v.canNodeDisableV2DataEngine(oldNode, newNode)
		if err != nil {
			return werror.NewInternalError(err.Error())
		}
		if !canDisabledV2DataEngine {
			return werror.NewInvalidError(fmt.Sprintf("cannot disable v2 data engine on node %v: there are v2 data engine instances running on this node", newNode.Name), "")
		}
	}

	return nil
}

func (v *kubeNodeValidator) canNodeDisableV2DataEngine(oldNode, newNode *corev1.Node) (bool, error) {
	if oldNode.Labels == nil {
		oldNode.Labels = map[string]string{}
	}
	if newNode.Labels == nil {
		newNode.Labels = map[string]string{}
	}

	oldV2DisabledLabel := oldNode.Labels[types.NodeDisableV2DataEngineLabelKey]
	newV2DisabledLabel := newNode.Labels[types.NodeDisableV2DataEngineLabelKey]
	if (oldV2DisabledLabel != newV2DisabledLabel) && (newV2DisabledLabel == types.NodeDisableV2DataEngineLabelKeyTrue) {
		// Check if there is any DataEngineV2 instance on this node.
		im, err := v.ds.GetRunningInstanceManagerByNodeRO(newNode.Name, longhorn.DataEngineTypeV2)
		if err != nil {
			return false, err
		}
		if im == nil {
			return true, nil
		}
		// Check if there is any instance (Engine or Replica) on this node.
		if len(im.Status.InstanceEngines)+len(im.Status.InstanceReplicas) > 0 {
			return false, nil
		}
	}

	return true, nil
}
