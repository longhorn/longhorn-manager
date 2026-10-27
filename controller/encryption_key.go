package controller

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/longhorn/backupstore"

	lhkeystore "github.com/longhorn/go-common-libs/crypto/keystore"
	lhtypes "github.com/longhorn/go-common-libs/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	// keyStoreTimeout keeps a Vault that never answers from holding a reconcile open.
	keyStoreTimeout = 30 * time.Second
	// encryptionKeyDeleteTimeout is a deadline, not an attempt count: workqueue
	// retries start milliseconds apart and would give up on a restarting Vault.
	encryptionKeyDeleteTimeout = 5 * time.Minute
	encryptionKeyRetryInterval = 30 * time.Second
)

// encryptionKeyProvider builds a volume's key provider from the Secret its CR
// names, so it works without a PV and for volumes not created through CSI.
func encryptionKeyProvider(ds *datastore.DataStore, namespace, secretName string) (lhkeystore.KeyProvider, error) {
	if secretName == "" {
		return nil, errors.New("the volume names no encryption secret")
	}
	secret, err := ds.GetSecretRO(namespace, secretName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read the encryption secret %v/%v", namespace, secretName)
	}
	provider, err := lhkeystore.NewProvider(lhkeystore.SecretData(secret.Data))
	if err != nil {
		return nil, err
	}
	// The Secret is mutable; only a vault provider actually stores and deletes keys.
	if provider.Name() != lhtypes.EncryptionKeyProviderVault {
		return nil, errors.Errorf("the encryption secret %v/%v selects the %v key provider, not %v",
			namespace, secretName, provider.Name(), lhtypes.EncryptionKeyProviderVault)
	}
	return provider, nil
}

// createVolumeKey stores the passphrase of a new vault volume and returns its
// version. On a refused create-only write (a controller that died before
// recording keyVersion), a blank volume adopts the stored key, since the
// controller is its only writer; a derived volume only accepts the exact key its
// data was encrypted with, as anything else is left over from an earlier volume.
func createVolumeKey(ctx context.Context, provider lhkeystore.KeyProvider, volumeName string, passphrase []byte, derived bool) (int, error) {
	version, err := provider.Put(ctx, lhkeystore.VolumeKey(volumeName), passphrase, 0)
	if err == nil {
		return version, nil
	}
	if !errors.Is(err, lhkeystore.ErrKeyConflict) {
		return 0, err
	}

	stored, version, err := provider.Get(ctx, lhkeystore.VolumeKey(volumeName))
	if err != nil {
		return 0, errors.Wrapf(err, "the key store refuses to create a key for volume %v and the stored one cannot be read back; an entry of an earlier volume of this name is in the way and has to be removed", volumeName)
	}
	defer lhkeystore.Zero(stored)
	if derived && !bytes.Equal(stored, passphrase) {
		return 0, errors.Errorf("the key store holds a key for volume %v that is not the one its data was encrypted with; it is left over from an earlier volume of this name and has to be removed", volumeName)
	}
	return version, nil
}

// derivedVolumeKeySource is where a new volume's key comes from: the backup's
// own entry for a restore, the source volume's version at snapshot time for a
// clone, nothing (keyName empty) for a blank volume.
type derivedVolumeKeySource struct {
	keyName    string
	version    int    // 0 reads the current version
	pending    bool   // the clone's snapshot or its key version is not known yet
	secretName string // the Secret of the store holding keyName; empty is the volume's own
}

// volumeKeySource reads the same spec fields the volume's data comes from.
func volumeKeySource(ds *datastore.DataStore, v *longhorn.Volume) (derivedVolumeKeySource, error) {
	if v.Spec.FromBackup != "" {
		backupName, _, _, err := backupstore.DecodeBackupURL(v.Spec.FromBackup)
		if err != nil {
			return derivedVolumeKeySource{}, errors.Wrapf(err, "failed to read the backup name of volume %v from %v", v.Name, v.Spec.FromBackup)
		}
		return derivedVolumeKeySource{keyName: lhkeystore.BackupKey(backupName)}, nil
	}

	if v.Spec.DataSource == "" || !types.IsDataFromVolume(v.Spec.DataSource) {
		return derivedVolumeKeySource{}, nil
	}

	sourceVolume := types.GetVolumeName(v.Spec.DataSource)
	snapshotName := types.GetSnapshotName(v.Spec.DataSource)
	if snapshotName == "" {
		// A clone of a live volume: the controller takes the snapshot itself.
		snapshotName = v.Status.CloneStatus.Snapshot
		if snapshotName == "" {
			return derivedVolumeKeySource{pending: true}, nil
		}
	}

	snapshot, err := ds.GetSnapshotRO(snapshotName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return derivedVolumeKeySource{pending: true}, nil
		}
		return derivedVolumeKeySource{}, errors.Wrapf(err, "failed to read snapshot %v for the key of volume %v", snapshotName, v.Name)
	}
	if snapshot.Status.EncryptionKeyVersion == 0 {
		// The snapshot controller stamps the version when the snapshot appears.
		return derivedVolumeKeySource{pending: true}, nil
	}
	source, err := ds.GetVolumeRO(sourceVolume)
	if err != nil {
		return derivedVolumeKeySource{}, errors.Wrapf(err, "failed to read source volume %v for the key of volume %v", sourceVolume, v.Name)
	}
	return derivedVolumeKeySource{
		keyName:    lhkeystore.VolumeKey(sourceVolume),
		version:    snapshot.Status.EncryptionKeyVersion,
		secretName: source.Spec.Encryption.SecretName,
	}, nil
}

// reconcileEncryptionKey stores the passphrase of a vault volume before anything
// can open it; status.encryption.keyVersion is the gate that releases the engine.
// Failures are left as a condition on the volume rather than returned.
func (c *VolumeController) reconcileEncryptionKey(v *longhorn.Volume) error {
	if !types.IsVaultEncrypted(&v.Spec) || v.Status.Encryption.KeyVersion != 0 {
		return nil
	}
	log := getLoggerForVolume(c.logger, v)

	setCondition := func(status longhorn.ConditionStatus, reason, message string) {
		v.Status.Conditions = types.SetCondition(v.Status.Conditions,
			longhorn.VolumeConditionTypeEncryptionKey, status, reason, message)
		if status != longhorn.ConditionStatusTrue {
			// Nothing in the cluster raises an event when the key store comes back.
			c.enqueueVolumeAfter(v, encryptionKeyRetryInterval)
		}
	}

	source, err := volumeKeySource(c.ds, v)
	if err != nil {
		setCondition(longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonEncryptionKeyMissing, err.Error())
		return nil
	}
	if source.pending {
		setCondition(longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonEncryptionKeySourcePending,
			"waiting for the source of the volume's data before its key can be copied")
		return nil
	}

	provider, err := encryptionKeyProvider(c.ds, c.namespace, v.Spec.Encryption.SecretName)
	if err != nil {
		setCondition(longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonEncryptionKeyUnavailable, err.Error())
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), keyStoreTimeout)
	defer cancel()

	var passphrase []byte
	if source.keyName == "" {
		// A volume that starts blank gets a key of its own.
		if passphrase, err = lhkeystore.GeneratePassphrase(); err != nil {
			setCondition(longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonEncryptionKeyMissing, err.Error())
			return nil
		}
	} else {
		// A clone may keep its key in another store than its source: read there, write here.
		sourceProvider := provider
		if source.secretName != "" && source.secretName != v.Spec.Encryption.SecretName {
			if sourceProvider, err = encryptionKeyProvider(c.ds, c.namespace, source.secretName); err != nil {
				setCondition(longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonEncryptionKeyUnavailable, err.Error())
				return nil
			}
		}
		if source.version > 0 {
			passphrase, err = sourceProvider.GetVersion(ctx, source.keyName, source.version)
		} else {
			passphrase, _, err = sourceProvider.Get(ctx, source.keyName)
		}
		if err != nil {
			reason := longhorn.VolumeConditionReasonEncryptionKeyUnavailable
			if errors.Is(err, lhkeystore.ErrKeyNotFound) {
				reason = longhorn.VolumeConditionReasonEncryptionKeyMissing
			}
			setCondition(longhorn.ConditionStatusFalse, reason,
				errors.Wrapf(err, "failed to read the key of %v the volume's data comes from", source.keyName).Error())
			return nil
		}
	}
	defer lhkeystore.Zero(passphrase)

	version, err := createVolumeKey(ctx, provider, v.Name, passphrase, source.keyName != "")
	if err != nil {
		setCondition(longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonEncryptionKeyUnavailable, err.Error())
		return nil
	}

	v.Status.Encryption.KeyVersion = version
	setCondition(longhorn.ConditionStatusTrue, "", "")
	log.Infof("Stored the encryption key of the volume at version %v, the engine can start", version)
	return nil
}

// destroyEncryptionKey removes a vault volume's key and reports whether the
// deletion may proceed. After encryptionKeyDeleteTimeout the volume is released
// anyway: an undeletable volume is worse than an orphaned key entry.
func (c *VolumeController) destroyEncryptionKey(v *longhorn.Volume) (done bool, err error) {
	if !types.IsVaultEncrypted(&v.Spec) {
		return true, nil
	}
	log := getLoggerForVolume(c.logger, v)

	provider, err := encryptionKeyProvider(c.ds, c.namespace, v.Spec.Encryption.SecretName)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), keyStoreTimeout)
		defer cancel()
		if err = provider.Delete(ctx, lhkeystore.VolumeKey(v.Name)); err == nil {
			log.Info("Destroyed the encryption key of the volume")
			c.encryptionKeyDeleteDeadlines.Delete(v.Name)
			return true, nil
		}
	}

	deadline, ok := c.encryptionKeyDeleteDeadlines.Load(v.Name)
	if !ok {
		deadline = time.Now().Add(encryptionKeyDeleteTimeout)
		c.encryptionKeyDeleteDeadlines.Store(v.Name, deadline)
	}
	if time.Now().Before(deadline.(time.Time)) {
		// Not returned: handleErr gives up after a few fast retries, well before the deadline.
		log.WithError(err).Warnf("Failed to destroy the encryption key of the volume, retrying until %v",
			deadline.(time.Time).Format(time.RFC3339))
		c.enqueueVolumeAfter(v, encryptionKeyRetryInterval)
		return false, nil
	}
	c.encryptionKeyDeleteDeadlines.Delete(v.Name)
	log.WithError(err).Errorf("Giving up on destroying the encryption key of the volume after %v; the entry stays in the key store",
		encryptionKeyDeleteTimeout)
	return true, nil
}

// isWaitingForEncryptionKey is the engine gate of a vault volume.
func isWaitingForEncryptionKey(v *longhorn.Volume) bool {
	return types.IsVaultEncrypted(&v.Spec) && v.Status.Encryption.KeyVersion == 0
}
