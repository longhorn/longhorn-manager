package backuptarget

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
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

	if err := b.ds.ValidateBackupTargetURL(backupTarget.Name, backupTarget.Spec.BackupTargetURL); err != nil {
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

func (b *backupTargetValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	oldBackupTarget := oldObj.(*longhorn.BackupTarget)
	newBackupTarget := newObj.(*longhorn.BackupTarget)

	urlChanged := oldBackupTarget.Spec.BackupTargetURL != newBackupTarget.Spec.BackupTargetURL
	secretChanged := oldBackupTarget.Spec.CredentialSecret != newBackupTarget.Spec.CredentialSecret

	if urlChanged {
		if err := b.ds.ValidateBackupTargetURL(newBackupTarget.Name, newBackupTarget.Spec.BackupTargetURL); err != nil {
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
				//ref: longhorn/longhorn#7159: appropriate warning messages for credential data
				switch {
				case strings.TrimLeft(string(value), " ") != string(value):
					return fmt.Errorf("invalid leading white space in %s", checkKey)
				case strings.TrimRight(string(value), " ") != string(value):
					return fmt.Errorf("invalid trailing white space in %s", checkKey)
				case strings.TrimLeft(string(value), "\n") != string(value):
					return fmt.Errorf("invalid leading new line in %s", checkKey)
				case strings.TrimRight(string(value), "\n") != string(value):
					return fmt.Errorf("invalid trailing new line in %s", checkKey)
				}
				return fmt.Errorf("there is space or new line in %s", checkKey)
			}
		}
	}
	return nil
}

func (b *backupTargetValidator) validateDRVolume(backupTarget *longhorn.BackupTarget) error {
	vs, err := b.ds.ListDRVolumesRO()
	if err != nil {
		return errors.Wrapf(err, "failed to list standby volume when modifying BackupTarget")
	}

	standbyVolumeNames := sets.NewString()
	for k := range vs {
		standbyVolumeNames.Insert(k)
	}
	if standbyVolumeNames.Len() != 0 {
		return fmt.Errorf("cannot modify BackupTarget since there are existing standby volumes: %v", standbyVolumeNames)
	}

	return nil
}

func (b *backupTargetValidator) Delete(request *admission.Request, oldObj runtime.Object) error {
	backupTarget := oldObj.(*longhorn.BackupTarget)

	if backupTarget.Name == types.DefaultBackupTargetName {
		exists := false
		if backupTarget.Annotations != nil {
			// Annotations `DeleteBackupTargetFromLonghorn` is used to note that deleting backup target is by Longhorn during uninstalling.
			// When `exists` is true, the backup target is deleted by Longhorn.
			_, exists = backupTarget.Annotations[types.GetLonghornLabelKey(types.DeleteBackupTargetFromLonghorn)]
		}
		if !exists {
			return werror.NewInvalidError("deleting default backup target is not allowed", "")
		}
	}

	if err := b.validateDRVolume(backupTarget); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}
