package controller

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lhkeystore "github.com/longhorn/go-common-libs/crypto/keystore"
	lhtypes "github.com/longhorn/go-common-libs/types"
)

// fakeKeyProvider is a store of one passphrase per name with KV v2 semantics:
// versions, and a create-only write that refuses an existing entry.
type fakeKeyProvider struct {
	entries map[string][][]byte
	getErr  error
}

func newFakeKeyProvider() *fakeKeyProvider {
	return &fakeKeyProvider{entries: map[string][][]byte{}}
}

func (f *fakeKeyProvider) Name() lhtypes.EncryptionKeyProvider {
	return lhtypes.EncryptionKeyProviderVault
}

func (f *fakeKeyProvider) Get(_ context.Context, name string) ([]byte, int, error) {
	if f.getErr != nil {
		return nil, 0, f.getErr
	}
	versions, ok := f.entries[name]
	if !ok {
		return nil, 0, lhkeystore.ErrKeyNotFound
	}
	// A copy, as a real provider returns: the caller is free to zero it.
	return append([]byte(nil), versions[len(versions)-1]...), len(versions), nil
}

func (f *fakeKeyProvider) GetVersion(_ context.Context, name string, version int) ([]byte, error) {
	versions, ok := f.entries[name]
	if !ok || version <= 0 || version > len(versions) {
		return nil, lhkeystore.ErrKeyNotFound
	}
	return append([]byte(nil), versions[version-1]...), nil
}

func (f *fakeKeyProvider) Put(_ context.Context, name string, passphrase []byte, cas int) (int, error) {
	current := len(f.entries[name])
	if cas != current {
		return 0, lhkeystore.ErrKeyConflict
	}
	f.entries[name] = append(f.entries[name], append([]byte(nil), passphrase...))
	return current + 1, nil
}

func (f *fakeKeyProvider) Delete(_ context.Context, name string) error {
	delete(f.entries, name)
	return nil
}

func TestCreateVolumeKey(t *testing.T) {
	ctx := context.Background()

	t.Run("a new volume gets version 1", func(t *testing.T) {
		provider := newFakeKeyProvider()
		version, err := createVolumeKey(ctx, provider, "vol", []byte("k1"), false)
		require.NoError(t, err)
		assert.Equal(t, 1, version)
	})

	// The key is written before the CR records its version. A controller that
	// dies in between must come back to the key it already wrote, not to a
	// volume whose engine can never start.
	t.Run("a crash between the write and the CR adopts the stored version", func(t *testing.T) {
		provider := newFakeKeyProvider()
		_, err := createVolumeKey(ctx, provider, "vol", []byte("k1"), false)
		require.NoError(t, err)

		version, err := createVolumeKey(ctx, provider, "vol", []byte("k2"), false)
		require.NoError(t, err)
		assert.Equal(t, 1, version, "the stored version is adopted")

		stored, _, err := provider.Get(ctx, lhkeystore.VolumeKey("vol"))
		require.NoError(t, err)
		assert.Equal(t, "k1", string(stored), "the stored key is never overwritten")
	})

	// A restored or cloned volume carries the key of the data it is a copy of.
	// Adopting a different stored key would hand it a device it cannot open.
	t.Run("a derived volume refuses a stored key that is not its own", func(t *testing.T) {
		provider := newFakeKeyProvider()
		_, err := createVolumeKey(ctx, provider, "vol", []byte("leftover"), false)
		require.NoError(t, err)

		_, err = createVolumeKey(ctx, provider, "vol", []byte("inherited"), true)
		require.Error(t, err)

		version, err := createVolumeKey(ctx, provider, "vol", []byte("leftover"), true)
		require.NoError(t, err, "the same key is the one the data was encrypted with")
		assert.Equal(t, 1, version)
	})

	t.Run("a store that cannot be read back is an error", func(t *testing.T) {
		provider := newFakeKeyProvider()
		_, err := createVolumeKey(ctx, provider, "vol", []byte("k1"), false)
		require.NoError(t, err)
		provider.getErr = errors.New("vault is sealed")

		_, err = createVolumeKey(ctx, provider, "vol", []byte("k2"), false)
		require.Error(t, err)
	})
}
