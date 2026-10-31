package types

import (
	"fmt"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// ResolveVolumeEncryption defaults and checks the key management fields of a new
// volume: no provider means secret, as before; a vault volume must name its
// encryption Secret, since the controller reads it from the CR alone.
func ResolveVolumeEncryption(spec *longhorn.VolumeSpec) (longhorn.VolumeEncryptionSpec, error) {
	encryption := spec.Encryption
	if !spec.Encrypted {
		if encryption.Provider != "" || encryption.SecretName != "" {
			return encryption, fmt.Errorf("encryption provider %q and encryption secret %q need an encrypted volume", encryption.Provider, encryption.SecretName)
		}
		return encryption, nil
	}

	switch encryption.Provider {
	case "":
		encryption.Provider = longhorn.EncryptionKeyProviderSecret
	case longhorn.EncryptionKeyProviderSecret, longhorn.EncryptionKeyProviderVault:
	default:
		return encryption, fmt.Errorf("unsupported encryption provider %q", encryption.Provider)
	}

	if encryption.Provider != longhorn.EncryptionKeyProviderVault {
		if encryption.SecretName != "" {
			return encryption, fmt.Errorf("encryption secret name is only used by the %v provider", longhorn.EncryptionKeyProviderVault)
		}
		return encryption, nil
	}

	if encryption.SecretName == "" {
		return encryption, fmt.Errorf("an encrypted volume with the %v provider must name an encryption secret", longhorn.EncryptionKeyProviderVault)
	}
	return encryption, nil
}

// BackupEncryptionProvider is the key provider a backup's data was encrypted
// with, from its labels; empty for a backup that is not known to be encrypted.
// An encrypted backup without a provider label predates the vault provider.
func BackupEncryptionProvider(labels map[string]string) longhorn.EncryptionKeyProvider {
	if provider := labels[LonghornLabelVolumeEncryptionProvider]; provider != "" {
		return longhorn.EncryptionKeyProvider(provider)
	}
	if labels[LonghornLabelVolumeEncrypted] == LonghornLabelValueEnabled {
		return longhorn.EncryptionKeyProviderSecret
	}
	return ""
}

// IsVaultEncrypted is true for the only kind of volume whose key the controller manages.
func IsVaultEncrypted(spec *longhorn.VolumeSpec) bool {
	return spec.Encrypted && spec.Encryption.Provider == longhorn.EncryptionKeyProviderVault
}
