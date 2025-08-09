package backingimage

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	bimtypes "github.com/longhorn/backing-image-manager/pkg/types"

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
				return nil, errors.Wrapf(err, "failed to get %v setting", types.SettingNameBackupConcurrentLimit)
			}
			parameters[longhorn.DataSourceTypeRestoreParameterConcurrentLimit] = strconv.FormatInt(concurrentLimit, 10)
		}

		if parameters[longhorn.DataSourceTypeRestoreParameterBackupTargetName] == "" {
			backupTargetName, err := b.findBackupTargetName(parameters[longhorn.DataSourceTypeRestoreParameterBackupURL])
			if err != nil {
				return nil, err
			}
			parameters[longhorn.DataSourceTypeRestoreParameterBackupTargetName] = backupTargetName
		}
	}

	if longhorn.BackingImageDataSourceType(backingImage.Spec.SourceType) == longhorn.BackingImageDataSourceTypeClone {
		// Use ignore as default value when encryption is not set
		if parameters[longhorn.DataSourceTypeCloneParameterEncryption] == "" {
			parameters[longhorn.DataSourceTypeCloneParameterEncryption] = string(bimtypes.EncryptionTypeIgnore)
		}

		// Inherit the secret and secretNamespace when cloning from another backing image and the encryption is ignore
		if bimtypes.EncryptionType(parameters[longhorn.DataSourceTypeCloneParameterEncryption]) == bimtypes.EncryptionTypeIgnore {
			sourceBackingImageName := parameters[longhorn.DataSourceTypeCloneParameterBackingImage]
			sourceBackingImage, err := b.ds.GetBackingImageRO(sourceBackingImageName)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get source backing image %v", sourceBackingImageName)
			}
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/secret", "value": "%s"}`, sourceBackingImage.Spec.Secret))
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/secretNamespace", "value": "%s"}`, sourceBackingImage.Spec.SecretNamespace))
			// If source backing image does not have checksum, it means the source is not ready yet.
			// Reject the creation anyway because it won't be able to clone from that source backing image in the following operation.
			if sourceBackingImage.Status.Checksum == "" {
				return nil, errors.Wrapf(err, "failed to get checksum of source backing image %v", sourceBackingImageName)
			}
			// Use the source backing image's checksum as truth
			patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/checksum", "value": "%s"}`, sourceBackingImage.Status.Checksum))
		} else {
			// Remove spec checksum because we don't trust the checksum provided by users for encryption and decryption
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/checksum", "value": ""}`)
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

	if string(backingImage.Spec.DataEngine) == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/dataEngine", "value": "%s"}`, longhorn.DataEngineTypeV1))
	}

	patchOps = append(patchOps, patchOp)

	return patchOps, nil
}

func (b *backingImageMutator) findBackupTargetName(backupURL string) (string, error) {
	bbis, err := b.ds.ListBackupBackingImagesRO()
	if err != nil {
		return "", errors.Wrapf(err, "failed to list backup backing images")
	}

	backupTargetURL, backingImageName, err := getBackupTargetURLAndBackingImageName(backupURL)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse backup URL %v", backupURL)
	}

	for _, bbi := range bbis {
		bbiBackupTargetURL, bbiBackingImageName, err := getBackupTargetURLAndBackingImageName(bbi.Status.URL)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to parse URL %v of backup backing image %v", bbi.Status.URL, bbi.Name)
			continue
		}
		if backupTargetURL == bbiBackupTargetURL && backingImageName == bbiBackingImageName {
			return bbi.Spec.BackupTargetName, nil
		}
	}

	return "", errors.Errorf("no matching backup found for URL %v and backing image %v", backupTargetURL, backingImageName)
}

func getBackupTargetURLAndBackingImageName(backupURL string) (string, string, error) {
	parsedURL, err := url.Parse(backupURL)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to parse backup URL %v", backupURL)
	}
	backingImageName := parsedURL.Query().Get("backingImage")
	if backingImageName == "" {
		return "", "", errors.Errorf("backup URL %v is missing required 'backingImage' parameter", backupURL)
	}
	switch parsedURL.Scheme {
	case types.BackupStoreTypeCIFS, types.BackupStoreTypeNFS, types.BackupStoreTypeAZBlob, types.BackupStoreTypeS3:
		parsedURL.RawQuery = ""
		return parsedURL.String(), backingImageName, nil
	default:
		return "", "", errors.Errorf("unsupported backupURL scheme %v", parsedURL.Scheme)
	}
}

func (b *backingImageMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	oldBackingImage, ok := oldObj.(*longhorn.BackingImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", oldObj), "")
	}

	backingImage, ok := newObj.(*longhorn.BackingImage)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackingImage", newObj), "")
	}

	if oldBackingImage.Spec.Secret != "" {
		if oldBackingImage.Spec.Secret != backingImage.Spec.Secret {
			err := fmt.Errorf("changing secret for BackingImage %v is not supported", oldBackingImage.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
	}

	if oldBackingImage.Spec.SecretNamespace != "" {
		if oldBackingImage.Spec.SecretNamespace != backingImage.Spec.SecretNamespace {
			err := fmt.Errorf("changing secret namespace for BackingImage %v is not supported", oldBackingImage.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
	}

	if oldBackingImage.Spec.DataEngine != backingImage.Spec.DataEngine {
		err := fmt.Errorf("changing data engine for BackingImage %v is not supported", oldBackingImage.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
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
