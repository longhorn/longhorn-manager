package keystore

import (
	"context"
	"fmt"

	lhtypes "github.com/longhorn/go-common-libs/types"
)

// secretProvider is the existing behaviour: the passphrase is the literal
// CRYPTO_KEY_VALUE of the StorageClass Secret, shared by every volume of the class.
type secretProvider struct {
	passphrase []byte
}

func newSecretProvider(secrets map[string]string) (*secretProvider, error) {
	passphrase := secrets[lhtypes.CryptoKeyValue]
	if len(passphrase) == 0 {
		return nil, fmt.Errorf("missing %v in the encryption secret", lhtypes.CryptoKeyValue)
	}
	return &secretProvider{passphrase: []byte(passphrase)}, nil
}

func (p *secretProvider) Name() lhtypes.EncryptionKeyProvider {
	return lhtypes.EncryptionKeyProviderSecret
}

// Get always returns the shared passphrase at version 1: the Secret has no notion of versions.
func (p *secretProvider) Get(context.Context, string) ([]byte, int, error) {
	out := make([]byte, len(p.passphrase))
	copy(out, p.passphrase)
	return out, 1, nil
}

// GetVersion returns the shared passphrase for version 1, the only version a
// Secret has; any other version never existed.
func (p *secretProvider) GetVersion(_ context.Context, name string, version int) ([]byte, error) {
	if version != 1 {
		return nil, fmt.Errorf("the secret provider has no version %v of the key of %v", version, name)
	}
	out := make([]byte, len(p.passphrase))
	copy(out, p.passphrase)
	return out, nil
}

// Put is a no-op: the Secret is the store and Longhorn does not write to it.
func (p *secretProvider) Put(context.Context, string, []byte, int) (int, error) {
	return 1, nil
}

func (p *secretProvider) Delete(context.Context, string) error {
	return nil
}
