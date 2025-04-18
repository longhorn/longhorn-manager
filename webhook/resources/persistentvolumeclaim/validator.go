package persistentvolumeclaim

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type pvcValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &pvcValidator{ds: ds}
}

func (v *pvcValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "persistentvolumeclaims",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   corev1.SchemeGroupVersion.Group,
		APIVersion: corev1.SchemeGroupVersion.Version,
		ObjectType: &corev1.PersistentVolumeClaim{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Update,
		},
	}
}

func (v *pvcValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldPVC, ok := oldObj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("invalid old object: expected *corev1.PersistentVolumeClaim, got %T", oldObj), "")
	}

	newPVC, ok := newObj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("invalid new object: expected *corev1.PersistentVolumeClaim, got %T", newObj), "")
	}

	// Handle only PVC size expansion.
	oldSize := oldPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	newSize := newPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	if oldSize.Cmp(newSize) == 0 {
		// Size has not changed; no further validation needed.
		return nil
	}

	pv, err := v.ds.GetPersistentVolumeRO(newPVC.Spec.VolumeName)
	if err != nil {
		return werror.NewInternalError(err.Error())
	}

	// Skip validation if the PV is not a Longhorn volume.
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != types.LonghornDriverName {
		return nil
	}

	volume, err := v.ds.GetVolumeRO(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return werror.NewInternalError(err.Error())
	}

	return v.validateExpansionSize(oldPVC, newPVC, volume)
}

func (v *pvcValidator) validateExpansionSize(oldPVC *corev1.PersistentVolumeClaim, newPVC *corev1.PersistentVolumeClaim, volume *longhorn.Volume) error {
	oldSize := oldPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	oldSizeInt64, ok := oldSize.AsInt64()
	if !ok {
		return werror.NewInternalError(fmt.Sprintf("unable to convert old size '%v' to int64", oldSize))
	}

	newSize := newPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	newSizeInt64, ok := newSize.AsInt64()
	if !ok {
		return werror.NewInternalError(fmt.Sprintf("unable to convert new size '%v' to int64", newSize))
	}

	replicaScheduler := scheduler.NewReplicaScheduler(v.ds)
	if _, err := replicaScheduler.CheckReplicasSizeExpansion(volume, oldSizeInt64, newSizeInt64); err != nil {
		return werror.NewForbiddenError(err.Error())
	}

	return nil
}
