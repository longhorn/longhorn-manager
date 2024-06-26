package backuptarget

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/pkg/errors"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupTargetURLType string

const (
	backupTargetURLTypeBucket  = backupTargetURLType("bucket")
	backupTargetURLTypeOptions = backupTargetURLType("options")
)

type backupTargetValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &backupTargetValidator{ds: ds}
}

func (b *backupTargetValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backuptargets",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackupTarget{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
			admissionregv1.Delete,
		},
	}
}

func (b *backupTargetValidator) Create(request *admission.Request, newObj runtime.Object) error {
	backupTarget := newObj.(*longhorn.BackupTarget)

	if !util.ValidateName(backupTarget.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", backupTarget.Name), "")
	}

	if err := b.validateBackupTargetURL(backupTarget.Spec.BackupTargetURL); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := b.validateCredentialSecret(backupTarget.Spec.CredentialSecret); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if err := b.handleAWSIAMRoleAnnotation(backupTarget, nil); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}

func (b *backupTargetValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldBackupTarget := oldObj.(*longhorn.BackupTarget)
	newBackupTarget := newObj.(*longhorn.BackupTarget)

	urlChanged := oldBackupTarget.Spec.BackupTargetURL != newBackupTarget.Spec.BackupTargetURL
	secretChanged := oldBackupTarget.Spec.CredentialSecret != newBackupTarget.Spec.CredentialSecret

	if urlChanged {
		if err := b.validateBackupTargetURL(newBackupTarget.Spec.BackupTargetURL); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if secretChanged {
		if err := b.validateCredentialSecret(newBackupTarget.Spec.CredentialSecret); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if urlChanged || secretChanged {
		if err := b.validateDRVolume(newBackupTarget); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}

		if err := b.handleAWSIAMRoleAnnotation(newBackupTarget, oldBackupTarget); err != nil {
			return werror.NewInvalidError(err.Error(), "")
		}
	}

	if newBackupTarget.Annotations == nil {
		newBackupTarget.Annotations = make(map[string]string)
	}
	_, exists := newBackupTarget.Annotations[types.GetLonghornLabelKey(types.UpdateBackupTargetFromLonghorn)]
	if oldBackupTarget.Spec.Default && !newBackupTarget.Spec.Default && !exists {
		return werror.NewInvalidError("prohibit disabling a default backup target directly", "")
	}

	return nil
}

func (b *backupTargetValidator) validateBackupTargetURL(backupTargetURL string) error {
	if backupTargetURL == "" {
		return nil
	}

	u, err := url.Parse(backupTargetURL)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %v as url", backupTargetURL)
	}

	if err := checkBackupTargetURLFormat(u); err != nil {
		return err
	}

	if err := b.checkBackupTargetURLExisting(u); err != nil {
		return err
	}

	return nil
}

func checkBackupTargetURLFormat(u *url.URL) error {
	// Check whether have $ or , have been set in BackupTarget path
	regStr := `[\$\,]`
	if u.Scheme == "cifs" {
		// The $ in SMB/CIFS URIs means that the share is hidden.
		regStr = `[\,]`
	}

	reg := regexp.MustCompile(regStr)
	findStr := reg.FindAllString(u.Path, -1)
	if len(findStr) != 0 {
		return fmt.Errorf("url %s, contains %v", u.String(), strings.Join(findStr, " or "))
	}
	return nil
}

func (b *backupTargetValidator) checkBackupTargetURLExisting(u *url.URL) error {
	var btURLType backupTargetURLType
	var creatingBackupTargetPath string

	switch u.Scheme {
	case types.BackupStoreTypeAZBlob, types.BackupStoreTypeS3:
		btURLType = backupTargetURLTypeBucket
		creatingBackupTargetPath = u.String()
	case types.BackupStoreTypeCIFS, types.BackupStoreTypeNFS:
		btURLType = backupTargetURLTypeOptions
		creatingBackupTargetPath = strings.TrimRight(u.Host+u.Path, "/")
	default:
		return fmt.Errorf("url %s with the unsupported protocol %v", u.String(), u.Scheme)
	}

	bts, err := b.ds.ListBackupTargetsRO()
	if err != nil {
		return err
	}
	for _, bt := range bts {
		uExisting, err := url.Parse(bt.Spec.BackupTargetURL)
		if err != nil {
			return errors.Wrapf(err, "failed to parse %v as url", bt.Spec.BackupTargetURL)
		}
		if uExisting.Scheme != u.Scheme {
			continue
		}
		var backupTargetPath string
		if btURLType == backupTargetURLTypeBucket {
			backupTargetPath = uExisting.String()
		}
		if btURLType == backupTargetURLTypeOptions {
			backupTargetPath = strings.TrimRight(uExisting.Host+uExisting.Path, "/")
		}
		if backupTargetPath == creatingBackupTargetPath {
			return fmt.Errorf("url %s is the same to backup target %v", u.String(), bt.Name)
		}
	}

	return nil
}

func (b *backupTargetValidator) validateCredentialSecret(secretName string) error {
	namespace, err := b.ds.GetLonghornNamespace()
	if err != nil {
		return errors.Wrapf(err, "failed to get Longhorn namespace")
	}
	secret, err := b.ds.GetSecretRO(namespace.Name, secretName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to get the secret before modifying backup target credential secret")
		}
		return nil
	}
	checkKeyList := []string{
		types.AWSAccessKey,
		types.AWSIAMRoleAnnotation,
		types.AWSIAMRoleArn,
		types.AWSAccessKey,
		types.AWSSecretKey,
		types.AWSEndPoint,
		types.AWSCert,
		types.CIFSUsername,
		types.CIFSPassword,
		types.AZBlobAccountName,
		types.AZBlobAccountKey,
		types.AZBlobEndpoint,
		types.AZBlobCert,
		types.HTTPSProxy,
		types.HTTPProxy,
		types.NOProxy,
		types.VirtualHostedStyle,
	}
	for _, checkKey := range checkKeyList {
		if value, ok := secret.Data[checkKey]; ok {
			if strings.TrimSpace(string(value)) != string(value) {
				return fmt.Errorf("there is space or new line in %s", checkKey)
			}
		}
	}
	return nil
}

func (b *backupTargetValidator) handleAWSIAMRoleAnnotation(newBackupTarget, oldBackupTarget *longhorn.BackupTarget) error {
	oldBackupTargetURL := ""
	oldBackupTargetSecret := ""
	if oldBackupTarget != nil {
		oldBackupTargetURL = oldBackupTarget.Spec.BackupTargetURL
		oldBackupTargetSecret = oldBackupTarget.Spec.CredentialSecret
	}
	uOld, err := url.Parse(oldBackupTargetURL)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %v as url", oldBackupTargetURL)
	}
	newBackupTargetURL := newBackupTarget.Spec.BackupTargetURL
	u, err := url.Parse(newBackupTargetURL)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %v as url", newBackupTargetURL)
	}
	if u.Scheme == types.BackupStoreTypeS3 || (uOld.Scheme == types.BackupStoreTypeS3 && newBackupTargetURL == "") {
		if err := b.ds.HandleSecretsForAWSIAMRoleAnnotation(newBackupTargetURL, oldBackupTargetSecret, newBackupTarget.Spec.CredentialSecret, oldBackupTargetURL != newBackupTargetURL); err != nil {
			return err
		}
	}
	return nil
}

func (b *backupTargetValidator) validateDRVolume(backupTarget *longhorn.BackupTarget) error {
	vs, err := b.ds.ListDRVolumesRO()
	if err != nil {
		return errors.Wrapf(err, "failed to list standby volume when modifying BackupTarget")
	}

	backupTargetURL := backupTarget.Spec.BackupTargetURL
	if len(vs) != 0 {
		standbyVolumeNames := sets.NewString()
		for k, v := range vs {
			specCharIndex := strings.Index(v.Spec.FromBackup, "?")
			fromBackupURL := v.Spec.FromBackup[:specCharIndex]
			fromBackupTargetName, isFound := v.Labels[types.LonghornLabelBackupTarget]
			if (isFound && fromBackupTargetName == backupTarget.Name) || fromBackupURL == backupTargetURL {
				standbyVolumeNames.Insert(k)
			}
		}
		if standbyVolumeNames.Len() != 0 {
			return fmt.Errorf("cannot modify BackupTarget since there are existing standby volumes: %v", standbyVolumeNames)
		}
	}
	return nil
}

func (b *backupTargetValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	backupTarget := oldObj.(*longhorn.BackupTarget)

	if backupTarget.Annotations == nil {
		backupTarget.Annotations = make(map[string]string)
	}
	_, exists := backupTarget.Annotations[types.GetLonghornLabelKey(types.DeleteBackupTargetFromLonghorn)]
	if backupTarget.Spec.Default && !exists {
		return werror.NewInvalidError("prohibit deleting a default backup target directly", "")
	}
	return nil
}
