// Package keystore resolves the LUKS passphrase of an encrypted volume from its
// key provider. The secret provider passes the literal passphrase of the
// StorageClass Secret through; the vault provider keeps one generated passphrase
// per volume in Vault KV v2.
//
// The volume controller owns the lifecycle of a vault key: it writes the key
// before the engine starts and destroys it before the volume's finalizer is
// released. Whatever opens the LUKS mapping - the CSI node plugin or the
// share-manager pod - only reads.
package keystore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/pkg/errors"

	lhtypes "github.com/longhorn/go-common-libs/types"
)

// generatedPassphraseBytes is the entropy of a generated passphrase; it is
// stored and passed to cryptsetup base64 encoded.
const generatedPassphraseBytes = 32

var (
	// ErrKeyNotFound is returned by Get when the provider holds no key under the name.
	ErrKeyNotFound = errors.New("no key stored")
	// ErrKeyConflict is returned by Put when the check-and-set condition does not hold.
	ErrKeyConflict = errors.New("key version conflict")
	// ErrUnavailable wraps transport and server errors: the store could not answer.
	ErrUnavailable = errors.New("key store unavailable")
)

// KeyProvider is a store of passphrases addressed by name. A name is either a
// volume name, from VolumeKey, or a backup entry, from BackupKey.
type KeyProvider interface {
	// Name is the provider this store serves.
	Name() lhtypes.EncryptionKeyProvider
	// Get returns the current passphrase and its store version.
	// It returns ErrKeyNotFound when the store holds no key under the name.
	Get(ctx context.Context, name string) (passphrase []byte, version int, err error)
	// GetVersion returns one specific version, which is how a derived volume
	// reads the key that was current when its snapshot was taken.
	GetVersion(ctx context.Context, name string, version int) (passphrase []byte, err error)
	// Put stores the passphrase when the current version is cas; cas 0 means create only.
	// It returns the new version, or ErrKeyConflict when the condition fails.
	Put(ctx context.Context, name string, passphrase []byte, cas int) (version int, err error)
	// Delete removes the key. Providers without a store treat it as a no-op.
	Delete(ctx context.Context, name string) error
}

// ProviderName reads the key provider an encryption Secret names; empty means
// the secret provider, so every Secret written before the vault provider
// existed keeps its meaning.
func ProviderName(secrets map[string]string) lhtypes.EncryptionKeyProvider {
	switch p := lhtypes.EncryptionKeyProvider(secrets[lhtypes.CryptoKeyProvider]); p {
	case "", lhtypes.EncryptionKeyProviderSecret:
		return lhtypes.EncryptionKeyProviderSecret
	default:
		return p
	}
}

// Validate checks an encryption Secret for the provider it names, without
// contacting any store.
func Validate(secrets map[string]string) error {
	switch ProviderName(secrets) {
	case lhtypes.EncryptionKeyProviderSecret:
		if len(secrets[lhtypes.CryptoKeyValue]) == 0 {
			return fmt.Errorf("missing %v in the encryption secret", lhtypes.CryptoKeyValue)
		}
		return nil
	case lhtypes.EncryptionKeyProviderVault:
		_, err := parseVaultConfig(secrets)
		return err
	default:
		return fmt.Errorf("unsupported key provider %v", secrets[lhtypes.CryptoKeyProvider])
	}
}

// NewProvider builds the provider an encryption Secret names. Both providers
// are configured by one Secret of the same shape, so every caller reads the
// same map and switches here.
func NewProvider(secrets map[string]string) (KeyProvider, error) {
	switch ProviderName(secrets) {
	case lhtypes.EncryptionKeyProviderSecret:
		return newSecretProvider(secrets)
	case lhtypes.EncryptionKeyProviderVault:
		cfg, err := parseVaultConfig(secrets)
		if err != nil {
			return nil, err
		}
		return newVaultProvider(cfg)
	default:
		return nil, fmt.Errorf("unsupported key provider %v", secrets[lhtypes.CryptoKeyProvider])
	}
}

// GeneratePassphrase returns a fresh random passphrase, base64 encoded so it
// survives every transport (Vault JSON, cryptsetup stdin) unchanged.
func GeneratePassphrase() ([]byte, error) {
	raw := make([]byte, generatedPassphraseBytes)
	if _, err := rand.Read(raw); err != nil {
		return nil, errors.Wrap(err, "failed to generate passphrase")
	}
	out := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(out, raw)
	Zero(raw)
	return out, nil
}

// Zero overwrites a passphrase buffer once it is no longer needed.
func Zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// Action is what the opener must do with the device before it can be opened.
type Action string

const (
	// ActionFormat: blank device with a key stored for it; luksFormat then luksOpen.
	ActionFormat = Action("format")
	// ActionOpen: LUKS device with its key; luksOpen.
	ActionOpen = Action("open")
)

// DeviceState is what the opener observed before deciding.
type DeviceState struct {
	// IsLUKS is true when blkid reports crypto_LUKS on the device.
	IsLUKS bool
	// HasKey is true when the store holds a key for the volume.
	HasKey bool
	// Initialized is status.encryption.initialized: the device has been formatted before.
	Initialized bool
}

// Decide applies the fail-closed matrix. It never returns ActionFormat for a
// device that may hold data: a LUKS header with no key to open it, and a blank
// device after the volume was initialized, are both refused. A volume with no
// key at all is refused too, because the key is stored before the engine starts,
// so its absence here means the store lost it rather than that the volume is new.
func Decide(s DeviceState) (Action, error) {
	switch {
	case s.IsLUKS && s.HasKey:
		return ActionOpen, nil
	case s.IsLUKS:
		return "", errors.New("device has a LUKS header but no key is stored for the volume; refusing to format, the key must be recovered")
	case !s.HasKey:
		return "", errors.New("no key is stored for the volume; refusing to format a device whose key the store does not hold")
	case s.Initialized:
		return "", errors.New("device reads as blank but the volume was already formatted; refusing to format, the device may be misreporting")
	default:
		return ActionFormat, nil
	}
}

// SecretData is a Secret's data as the keystore reads it.
func SecretData(data map[string][]byte) map[string]string {
	out := make(map[string]string, len(data))
	for key, value := range data {
		out[key] = string(value)
	}
	return out
}
