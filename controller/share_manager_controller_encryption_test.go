package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lhtypes "github.com/longhorn/go-common-libs/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func encryptionEnvMap(t *testing.T, provider longhorn.EncryptionKeyProvider, secret map[string]string) map[string]string {
	volume := &longhorn.Volume{Spec: longhorn.VolumeSpec{
		Encrypted:  true,
		Encryption: longhorn.VolumeEncryptionSpec{Provider: provider, SecretName: "longhorn-vault"},
	}}
	env, err := shareManagerEncryptionEnv(volume, secret)
	require.NoError(t, err)
	out := map[string]string{}
	for _, e := range env {
		out[e.Name] = e.Value
	}
	return out
}

// The secret provider keeps the existing pod contract: the passphrase is handed to the pod.
func TestShareManagerEncryptionEnvSecretProvider(t *testing.T) {
	env := encryptionEnvMap(t, "", map[string]string{lhtypes.CryptoKeyValue: "s3cret"})
	assert.Equal(t, "s3cret", env["PASSPHRASE"])
	assert.Equal(t, "sha256", env["CRYPTOKEYHASH"], "cryptsetup defaults are filled in as before")
	assert.NotContains(t, env, "ENCRYPTION_SECRET_NAME")

	_, err := shareManagerEncryptionEnv(&longhorn.Volume{Spec: longhorn.VolumeSpec{Encrypted: true}}, map[string]string{})
	require.Error(t, err, "no passphrase, no pod")
}

// A vault volume's pod gets only its Secret's name: no configuration and no passphrase in the pod spec.
func TestShareManagerEncryptionEnvVaultProvider(t *testing.T) {
	env := encryptionEnvMap(t, longhorn.EncryptionKeyProviderVault, nil)
	assert.Equal(t, "vault", env["CRYPTOKEYPROVIDER"])
	assert.Equal(t, "longhorn-vault", env["ENCRYPTION_SECRET_NAME"])
	assert.NotContains(t, env, "PASSPHRASE")
}
