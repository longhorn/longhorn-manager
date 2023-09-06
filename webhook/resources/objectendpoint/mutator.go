package objectendpoint

import (
	"encoding/json"
	"fmt"
	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
)

type objectEndpointMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &objectEndpointMutator{ds: ds}
}

func (v *objectEndpointMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "objectendpoints",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.ObjectEndpoint{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *objectEndpointMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	objectEndpoint := newObj.(*longhorn.ObjectEndpoint)
	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	defaultObjectEndpointImage, _ := v.ds.GetSettingValueExisted(types.SettingNameObjectEndpointImage)
	if defaultObjectEndpointImage == "" {
		return nil, werror.NewInvalidError("BUG: Invalid empty Setting.ObjectEndpointImage", "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/image", "value": "%s"}`, defaultObjectEndpointImage))

	if objectEndpoint.Spec.FromVolumeBackup != "" {
		bName, bvName, _, err := backupstore.DecodeBackupURL(objectEndpoint.Spec.FromVolumeBackup)
		if err != nil {
			return nil, werror.NewInvalidError(fmt.Sprintf("cannot get backup and volume name from backup URL %v: %v", objectEndpoint.Spec.FromVolumeBackup, err), "")
		}

		bv, err := v.ds.GetBackupVolumeRO(bvName)
		if err != nil {
			return nil, werror.NewInvalidError(fmt.Sprintf("cannot get backup volume %s: %v", bvName, err), "")
		}

		backup, err := v.ds.GetBackupRO(bName)
		if err != nil {
			return nil, werror.NewInvalidError(fmt.Sprintf("cannot get backup %s: %v", bName, err), "")
		}

		if bv != nil && backup != nil && backup.Status.ObjectEndpointBackup != "" {
			objectEndpointBackup := &longhorn.ObjectEndpointBackup{}
			err := json.Unmarshal([]byte(backup.Status.ObjectEndpointBackup), objectEndpointBackup)
			if err != nil {
				return nil, werror.NewInvalidError(fmt.Sprintf("failed to unmarshal ObjectEndpointBackup for backup %s: %v", bName, err), "")
			}
			if objectEndpoint.Spec.Size.IsZero() {
				patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/size", "value": "%s"}`, objectEndpointBackup.Spec.Size.String()))
			} else if objectEndpointBackup.Spec.Size.Cmp(objectEndpoint.Spec.Size) > 0 {
				return nil, werror.NewInvalidError(fmt.Sprintf("cannot shink size from %v to %v", objectEndpointBackup.Spec.Size.String(), objectEndpoint.Spec.Size.String()), "")
			}
			if reflect.DeepEqual(objectEndpoint.Spec.Credentials, longhorn.ObjectEndpointCredentials{}) {
				bytes, _ := json.Marshal(objectEndpointBackup.Spec.Credentials)
				logrus.Warnf("================> %v", string(bytes))
				patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/credentials", "value": %v}`, string(bytes)))
			}
			if reflect.DeepEqual(objectEndpoint.Spec.VolumeParameters, longhorn.ObjectEndpointVolumeParameters{}) {
				objectEndpointBackup.Spec.VolumeParameters.FromBackup = objectEndpoint.Spec.FromVolumeBackup
				bytes, _ := json.Marshal(objectEndpointBackup.Spec.VolumeParameters)
				patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/volumeParameters", "value": %v}`, string(bytes)))
			}
			// TODO: handle additional logic if needed
		}
		//patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/volumeParameters/fromBackup", "value": "%s"}`, objectEndpoint.Spec.FromVolumeBackup))
	}

	// TODO: handle additional logic if needed
	return patchOps, nil

}

func (m *objectEndpointMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	_ = oldObj.(*longhorn.ObjectEndpoint)
	_ = newObj.(*longhorn.ObjectEndpoint)
	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	return patchOps, nil
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	objectEndpoint := newObj.(*longhorn.ObjectEndpoint)
	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(objectEndpoint)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for VolumeAttachment %v", objectEndpoint.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}
