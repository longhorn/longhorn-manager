package storageclass

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	storagev1 "k8s.io/api/storage/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

const dataLayoutKeyPrefix = longhorn.DataLayoutParameterPrefix + "."

var dataLayoutSubFields = sets.New[string](
	longhorn.DataLayoutParameterType,
	longhorn.DataLayoutParameterMode,
	longhorn.DataLayoutParameterDataChunks,
	longhorn.DataLayoutParameterParityChunks,
	longhorn.DataLayoutParameterStripSizeKB,
)

type storageClassValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &storageClassValidator{ds: ds}
}

func (v *storageClassValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "storageclasses",
		Scope:      admissionregv1.ClusterScope,
		APIGroup:   storagev1.SchemeGroupVersion.Group,
		APIVersion: storagev1.SchemeGroupVersion.Version,
		ObjectType: &storagev1.StorageClass{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			// kube-apiserver already rejects changes to
			// .parameters and .provisioner on StorageClass update,
			// so there's nothing new to validate there.
		},
	}
}

func (v *storageClassValidator) Create(request *admission.Request, newObj runtime.Object) error {
	sc, ok := newObj.(*storagev1.StorageClass)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *storagev1.StorageClass", newObj), "")
	}

	// Only Longhorn-provisioned StorageClasses are to be validated;
	// anything else passes through untouched.
	if sc.Provisioner != types.LonghornDriverName {
		return nil
	}

	if errs := validateDataLayout(sc.Parameters); len(errs) > 0 {
		return werror.NewInvalidError(errs.ToAggregate().Error(), "parameters")
	}
	if errs := validateVolumeAntiAffinity(sc); len(errs) > 0 {
		return werror.NewInvalidError(errs.ToAggregate().Error(), "")
	}
	return nil
}

// validateVolumeAntiAffinity requires what volume anti-affinity depends on.
// The terms are inherited from the pod when the PV is bound, so the volume must
// be provisioned for a scheduled pod. Only volumes with one replica are
// supported, and an omitted replica count would take the default replica count
// setting, which the volume webhook rejects only when the PVC is provisioned.
// EC volumes place shards instead of replicas, so the terms would not apply.
func validateVolumeAntiAffinity(sc *storagev1.StorageClass) field.ErrorList {
	errors := field.ErrorList{}
	fp := field.NewPath("parameters")

	value, ok := sc.Parameters[types.StorageClassParameterVolumeAntiAffinity]
	if !ok {
		return errors
	}
	switch longhorn.VolumeAntiAffinity(value) {
	case longhorn.VolumeAntiAffinityDisabled:
		return errors
	case longhorn.VolumeAntiAffinityEnabled:
	default:
		return append(errors, field.NotSupported(fp.Key(types.StorageClassParameterVolumeAntiAffinity), value,
			[]string{string(longhorn.VolumeAntiAffinityEnabled), string(longhorn.VolumeAntiAffinityDisabled)}))
	}

	if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		errors = append(errors, field.Invalid(field.NewPath("volumeBindingMode"), sc.VolumeBindingMode,
			fmt.Sprintf("must be %v when %v is %v", storagev1.VolumeBindingWaitForFirstConsumer,
				types.StorageClassParameterVolumeAntiAffinity, longhorn.VolumeAntiAffinityEnabled)))
	}
	if replicas, ok := sc.Parameters["numberOfReplicas"]; !ok {
		errors = append(errors, field.Required(fp.Key("numberOfReplicas"),
			fmt.Sprintf("must be \"1\" when %v is %v", types.StorageClassParameterVolumeAntiAffinity, longhorn.VolumeAntiAffinityEnabled)))
	} else if replicas != "1" {
		errors = append(errors, field.Invalid(fp.Key("numberOfReplicas"), replicas,
			fmt.Sprintf("must be \"1\" when %v is %v", types.StorageClassParameterVolumeAntiAffinity, longhorn.VolumeAntiAffinityEnabled)))
	}
	if sc.Parameters[longhorn.DataLayoutParameterType] == string(longhorn.VolumeDataLayoutTypeSharded) {
		errors = append(errors, field.Forbidden(fp.Key(types.StorageClassParameterVolumeAntiAffinity),
			fmt.Sprintf("not supported with %v %v", longhorn.DataLayoutParameterType, longhorn.VolumeDataLayoutTypeSharded)))
	}
	return errors
}

func validateDataLayout(params map[string]string) field.ErrorList {
	errors := field.ErrorList{}
	fp := field.NewPath("parameters")

	for key := range params {
		switch {
		// invalidates for parameters["dataLayout"] (no type/subfield)
		case key == longhorn.DataLayoutParameterPrefix:
			errors = append(errors, field.Invalid(fp.Key(key), key,
				fmt.Sprintf("must specify a subfield, e.g. %q", longhorn.DataLayoutParameterType)))

		// invalidates for parameters["dataLayout.<unknown>"], e.g. a typo'd subfield
		case strings.HasPrefix(key, dataLayoutKeyPrefix):
			if !dataLayoutSubFields.Has(key) {
				errors = append(errors, field.Invalid(fp.Key(key), key,
					fmt.Sprintf("unknown field %q", key)))
			}

		// invalidates for any subfield passed without "dataLayout." prefix
		default:
			if dataLayoutSubFields.Has(dataLayoutKeyPrefix + key) {
				errors = append(errors, field.Invalid(fp.Key(key), key,
					fmt.Sprintf("%q is a dataLayout field and must be prefixed as %q", key, dataLayoutKeyPrefix+key)))
			}
		}
		// NOTE: The current validation does not infer a misspelled dataLayout prefix from
		// the leaf name (e.g. "dataLayour.mode"), as field names may be shared by unrelated nested structs.
	}
	return errors
}
