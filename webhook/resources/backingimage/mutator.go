package backingimage

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/common"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
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
	backingImage, ok := newObj.(*longhorn.BackingImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", newObj), "")
	}

	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

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

	if longhorn.BackingImageDataSourceType(backingImage.Spec.SourceType) == longhorn.BackingImageDataSourceTypeRestore {
		if parameters[longhorn.DataSourceTypeRestoreParameterConcurrentLimit] == "" {
			concurrentLimit, err := b.ds.GetSettingAsInt(types.SettingNameBackupConcurrentLimit)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get %v value", types.SettingNameBackupConcurrentLimit)
			}
			parameters[longhorn.DataSourceTypeRestoreParameterConcurrentLimit] = strconv.FormatInt(concurrentLimit, 10)
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

	// Handle Spec.DiskFileSpecMap
	if backingImage.Spec.DiskFileSpecMap == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/diskFileSpecMap", "value": {}}`)
	}

	longhornLabels := types.GetBackingImageLabels()
	patchOp, err := common.GetLonghornLabelsPatchOp(backingImage, longhornLabels, nil)
	if err != nil {
		err := errors.Wrapf(err, "failed to get label patch for backingImage %v", backingImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}

	if backingImage.Spec.MinNumberOfCopies == 0 {
		minNumberOfCopies, err := b.getDefaultMinNumberOfBackingImageCopies()
		if err != nil {
			err = errors.Wrap(err, "failed to get valid number for setting default min number of backing image copies")
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		logrus.Infof("Use the default minimum number of copies %v for backing image %v", minNumberOfCopies, backingImage.Name)
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/minNumberOfCopies", "value": %v}`, minNumberOfCopies))
	}

	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (b *backingImageMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	backingImage, ok := newObj.(*longhorn.BackingImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", newObj), "")
	}

	var patchOps admission.PatchOps

	var err error
	if patchOps, err = mutate(newObj); err != nil {
		return nil, err
	}

	// Backward compatibility
	// SourceType is set to "download" if it is empty
	if backingImage.Spec.SourceType == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/sourceType", "value": "%s"}`, longhorn.BackingImageDataSourceTypeDownload))
	}

	if backingImage.Spec.Disks == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/disks", "value": {}}`)
	}

	// Handle Spec.DiskFileSpecMap
	if backingImage.Spec.DiskFileSpecMap == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/diskFileSpecMap", "value": {}}`)
	}

	if backingImage.Spec.SourceParameters == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/sourceParameters", "value": {}}`)
	}

	return patchOps, nil
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backingImage := newObj.(*longhorn.BackingImage)
	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(backingImage)
	if err != nil {
		err := errors.Wrapf(err, "failed to get finalizer patch for backingImage %v", backingImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	return patchOps, nil
}

func (b *backingImageMutator) getDefaultMinNumberOfBackingImageCopies() (int, error) {
	c, err := b.ds.GetSettingAsInt(types.SettingNameDefaultMinNumberOfBackingImageCopies)
	if err != nil {
		return 0, err
	}
	return int(c), nil
}
