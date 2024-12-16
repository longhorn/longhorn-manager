package dataengineupgrademanager

import (
	"context"
	"fmt"
	"reflect"
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

type dataEngineUpgradeManagerValidator struct {
	admission.DefaultValidator
	ds     *datastore.DataStore
	locker *k8slock.Locker
}

func NewValidator(ds *datastore.DataStore, locker *k8slock.Locker) admission.Validator {
	return &dataEngineUpgradeManagerValidator{
		ds:     ds,
		locker: locker,
	}
}

func (u *dataEngineUpgradeManagerValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "dataengineupgrademanagers",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.DataEngineUpgradeManager{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (u *dataEngineUpgradeManagerValidator) areAllDataEngineUpgradeManagerStopped() (bool, error) {
	upgradeManagers, err := u.ds.ListDataEngineUpgradeManagers()
	if err != nil {
		return false, err
	}
	for _, upgradeManager := range upgradeManagers {
		if upgradeManager.Status.State != longhorn.UpgradeStateCompleted &&
			upgradeManager.Status.State != longhorn.UpgradeStateError {
			return false, nil
		}
	}
	return true, nil
}

func (u *dataEngineUpgradeManagerValidator) Create(request *admission.Request, newObj runtime.Object) error {
	upgradeManager, ok := newObj.(*longhorn.DataEngineUpgradeManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DataEngineUpgradeManager", newObj), "")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := u.locker.LockContext(ctx)
	if err != nil {
		err = errors.Wrapf(err, "failed to lock for dataEngineUpgradeManager %v", upgradeManager.Name)
		return werror.NewInternalError(err.Error())
	}
	defer func() {
		err = u.locker.UnlockContext(ctx)
		if err != nil {
			err = errors.Wrapf(err, "failed to unlock for dataEngineUpgradeManager %v", upgradeManager.Name)
		}
	}()

	allStopped, err := u.areAllDataEngineUpgradeManagerStopped()
	if err != nil {
		err = errors.Wrapf(err, "failed to check if all dataEngineUpgradeManager are stopped")
		return werror.NewInternalError(err.Error())
	}
	if !allStopped {
		err = fmt.Errorf("another dataEngineUpgradeManager is in progress")
		return werror.NewBadRequest(err.Error())
	}

	nodes, err := u.ds.ListNodes()
	if err != nil {
		return werror.NewInternalError(err.Error())
	}
	if len(nodes) < 2 {
		err = fmt.Errorf("single-node cluster is not supported for data engine live upgrade")
		return werror.NewInvalidError(err.Error(), "")
	}

	if upgradeManager.Spec.DataEngine != longhorn.DataEngineTypeV2 {
		err := fmt.Errorf("data engine %v is not supported for data engine live upgrade", upgradeManager.Spec.DataEngine)
		return werror.NewInvalidError(err.Error(), "spec.dataEngine")
	}

	return nil
}

func (u *dataEngineUpgradeManagerValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldUpgradeManager, ok := oldObj.(*longhorn.DataEngineUpgradeManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DataEngineUpgradeManager", oldObj), "")
	}
	newUpgradeManager, ok := newObj.(*longhorn.DataEngineUpgradeManager)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.DataEngineUpgradeManager", newObj), "")
	}

	if newUpgradeManager.Spec.DataEngine != longhorn.DataEngineTypeV2 {
		err := fmt.Errorf("data engine %v is not supported", newUpgradeManager.Spec.DataEngine)
		return werror.NewInvalidError(err.Error(), "spec.dataEngine")
	}

	if oldUpgradeManager.Spec.DataEngine != newUpgradeManager.Spec.DataEngine {
		return werror.NewInvalidError("spec.dataEngine field is immutable", "spec.dataEngine")
	}

	if !reflect.DeepEqual(oldUpgradeManager.Spec.Nodes, newUpgradeManager.Spec.Nodes) {
		return werror.NewInvalidError("nodes field is immutable", "spec.nodes")
	}

	return nil
}
