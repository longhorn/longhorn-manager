package nodedataengineupgrade

import (
	"context"
	"fmt"
	"time"

	"github.com/jrhouston/k8slock"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type nodeDataEngineUpgradeValidator struct {
	admission.DefaultValidator
	ds     *datastore.DataStore
	locker *k8slock.Locker
}

func NewValidator(ds *datastore.DataStore, locker *k8slock.Locker) admission.Validator {
	return &nodeDataEngineUpgradeValidator{
		ds:     ds,
		locker: locker,
	}
}

func (u *nodeDataEngineUpgradeValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "nodedataengineupgrades",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.NodeDataEngineUpgrade{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (u *nodeDataEngineUpgradeValidator) Create(request *admission.Request, newObj runtime.Object) error {
	nodeUpgrade, ok := newObj.(*longhorn.NodeDataEngineUpgrade)
	if !ok {
		return werror.NewInvalidError("object is not a *longhorn.NodeDataEngineUpgrade", "")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := u.locker.LockContext(ctx)
	if err != nil {
		err = errors.Wrapf(err, "failed to lock for nodeDataEngineUpgrade %v", nodeUpgrade.Name)
		return werror.NewInternalError(err.Error())
	}
	defer func() {
		err = u.locker.UnlockContext(ctx)
		if err != nil {
			err = errors.Wrapf(err, "failed to unlock for nodeDataEngineUpgrade %v", nodeUpgrade.Name)
		}
	}()

	if nodeUpgrade.Spec.NodeID == "" {
		return werror.NewInvalidError("nodeID is required", "spec.nodeID")
	}

	if nodeUpgrade.Spec.DataEngine != longhorn.DataEngineTypeV2 {
		err := fmt.Errorf("data engine %v is not supported", nodeUpgrade.Spec.DataEngine)
		return werror.NewInvalidError(err.Error(), "spec.dataEngine")
	}

	if nodeUpgrade.Spec.InstanceManagerImage == "" {
		err := fmt.Errorf("instanceManagerImage is required")
		return werror.NewInvalidError(err.Error(), "spec.instanceManagerImage")
	}

	if nodeUpgrade.Spec.DataEngineUpgradeManager == "" {
		err := fmt.Errorf("dataEngineUpgradeManager is required")
		return werror.NewInvalidError(err.Error(), "spec.dataEngineUpgradeManager")
	}

	return nil
}

func (u *nodeDataEngineUpgradeValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldNodeUpgrade, ok := oldObj.(*longhorn.NodeDataEngineUpgrade)
	if !ok {
		return werror.NewInvalidError("old object is not a *longhorn.NodeDataEngineUpgrade", "")
	}
	newNodeUpgrade, ok := newObj.(*longhorn.NodeDataEngineUpgrade)
	if !ok {
		return werror.NewInvalidError("new object is not a *longhorn.NodeDataEngineUpgrade", "")
	}

	if oldNodeUpgrade.Spec.NodeID != newNodeUpgrade.Spec.NodeID {
		return werror.NewInvalidError("nodeID field is immutable", "spec.nodeID")
	}

	if oldNodeUpgrade.Spec.DataEngine != newNodeUpgrade.Spec.DataEngine {
		return werror.NewInvalidError("dataEngine field is immutable", "spec.dataEngine")
	}

	if oldNodeUpgrade.Spec.InstanceManagerImage != newNodeUpgrade.Spec.InstanceManagerImage {
		return werror.NewInvalidError("instanceManagerImage field is immutable", "spec.instanceManagerImage")
	}

	if oldNodeUpgrade.Spec.DataEngineUpgradeManager != newNodeUpgrade.Spec.DataEngineUpgradeManager {
		return werror.NewInvalidError("dataEngineUpgradeManager field is immutable", "spec.dataEngineUpgradeManager")
	}

	return nil
}
