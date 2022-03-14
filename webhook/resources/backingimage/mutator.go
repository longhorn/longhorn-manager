package backingimage

import (
	"encoding/json"
	"fmt"
	"strings"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
	"github.com/pkg/errors"
)

type backingImageMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backingImageMutator{ds: ds}
}

func (b *backingImageMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backingimages",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackingImage{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backingImageMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	backingImage := newObj.(*longhorn.BackingImage)

	name := util.AutoCorrectName(backingImage.Name, datastore.NameMaximumLength)
	if name != backingImage.Name {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/name", "value": "%s"}`, name))
	}

	checksum := strings.TrimSpace(backingImage.Spec.Checksum)
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/checksum", "value": "%s"}`, checksum))

	// Handle Spec.SourceParameters
	parameters := make(map[string]string, 0)
	for k, v := range backingImage.Spec.SourceParameters {
		parameters[k] = strings.TrimSpace(v)
	}

	if longhorn.BackingImageDataSourceType(backingImage.Spec.SourceType) == longhorn.BackingImageDataSourceTypeExportFromVolume {
		// By default the exported file type is raw.
		if parameters[manager.DataSourceTypeExportFromVolumeParameterExportType] == "" {
			parameters[manager.DataSourceTypeExportFromVolumeParameterExportType] = manager.DataSourceTypeExportFromVolumeParameterExportTypeRAW
		}
	}

	bytes, err := json.Marshal(parameters)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding for backing image %v sourceParameters", backingImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/sourceParameters", "value": %s}`, string(bytes)))

	// Handle Spec.Disks
	if backingImage.Spec.Disks == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/disks", "value": {}}`)
	}

	// Merge the user created and longhorn specific labels
	labels := backingImage.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	longhornLabels := types.GetBackingImageLabels()
	for k, v := range longhornLabels {
		labels[k] = v
	}
	bytes, err = json.Marshal(labels)
	if err != nil {
		err = errors.Wrapf(err, "failed to get JSON encoding for backing image %v labels", backingImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/labels", "value": %v}`, string(bytes)))

	return patchOps, nil
}

func (b *backingImageMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	backingImage := newObj.(*longhorn.BackingImage)

	// Backward compatibility
	// SourceType is set to "download" if it is empty
	if backingImage.Spec.SourceType == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/sourceType", "value": "%s"}`, longhorn.BackingImageDataSourceTypeDownload))
	}

	if backingImage.Spec.Disks == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/disks", "value": {}}`)
	}
	if backingImage.Spec.SourceParameters == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/sourceParameters", "value": {}}`)
	}

	return patchOps, nil
}
