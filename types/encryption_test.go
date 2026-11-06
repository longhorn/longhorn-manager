package types

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestResolveVolumeEncryption(t *testing.T) {
	cases := []struct {
		name       string
		spec       longhorn.VolumeSpec
		provider   longhorn.EncryptionKeyProvider
		secretName string
		wantErr    bool
	}{
		{
			name: "an unencrypted volume carries nothing",
			spec: longhorn.VolumeSpec{},
		},
		{
			name:    "encryption fields need an encrypted volume",
			spec:    longhorn.VolumeSpec{Encryption: longhorn.VolumeEncryptionSpec{Provider: longhorn.EncryptionKeyProviderVault, SecretName: "longhorn-vault"}},
			wantErr: true,
		},
		{
			// Every volume that existed before the vault provider lands here.
			name:     "an encrypted volume with no provider is a secret volume",
			spec:     longhorn.VolumeSpec{Encrypted: true},
			provider: longhorn.EncryptionKeyProviderSecret,
		},
		{
			name:    "the secret provider has no vault secret",
			spec:    longhorn.VolumeSpec{Encrypted: true, Encryption: longhorn.VolumeEncryptionSpec{Provider: longhorn.EncryptionKeyProviderSecret, SecretName: "longhorn-vault"}},
			wantErr: true,
		},
		{
			name:       "a vault volume keeps its secret name",
			spec:       longhorn.VolumeSpec{Encrypted: true, Encryption: longhorn.VolumeEncryptionSpec{Provider: longhorn.EncryptionKeyProviderVault, SecretName: "longhorn-vault"}},
			provider:   longhorn.EncryptionKeyProviderVault,
			secretName: "longhorn-vault",
		},
		{
			// Without it the controller has no way to reach a store.
			name:    "a vault volume must name a secret",
			spec:    longhorn.VolumeSpec{Encrypted: true, Encryption: longhorn.VolumeEncryptionSpec{Provider: longhorn.EncryptionKeyProviderVault}},
			wantErr: true,
		},
		{
			name:    "an unknown provider is refused",
			spec:    longhorn.VolumeSpec{Encrypted: true, Encryption: longhorn.VolumeEncryptionSpec{Provider: "kms"}},
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := c.spec
			got, err := ResolveVolumeEncryption(&spec)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Provider != c.provider || got.SecretName != c.secretName {
				t.Fatalf("expected provider %q secret %q, got provider %q secret %q", c.provider, c.secretName, got.Provider, got.SecretName)
			}
		})
	}
}
