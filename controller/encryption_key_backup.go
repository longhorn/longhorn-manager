package controller

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	lhkeystore "github.com/longhorn/go-common-libs/crypto/keystore"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// storeBackupEncryptionKey gives a completed backup its own key entry, because
// a backup outlives its volume and the volume's entry goes with it. The write is
// create-only, so repeating it is harmless.
func storeBackupEncryptionKey(ds *datastore.DataStore, namespace string, log logrus.FieldLogger, backup *longhorn.Backup, volumeName string) error {
	volume, err := ds.GetVolumeRO(volumeName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return err
	}
	if !types.IsVaultEncrypted(&volume.Spec) {
		return nil
	}
	if volume.Status.Encryption.KeyVersion == 0 {
		return errors.Errorf("volume %v has no stored encryption key to copy to backup %v", volumeName, backup.Name)
	}

	provider, err := encryptionKeyProvider(ds, namespace, volume.Spec.Encryption.SecretName)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), keyStoreTimeout)
	defer cancel()

	// The backup holds the data as of its snapshot, so it is that version's key.
	version := volume.Status.Encryption.KeyVersion
	if snapshot, err := ds.GetSnapshotRO(backup.Spec.SnapshotName); err == nil && snapshot.Status.EncryptionKeyVersion != 0 {
		version = snapshot.Status.EncryptionKeyVersion
	}

	passphrase, err := provider.GetVersion(ctx, lhkeystore.VolumeKey(volumeName), version)
	if err != nil {
		return errors.Wrapf(err, "failed to read version %v of the key of volume %v for backup %v", version, volumeName, backup.Name)
	}
	defer lhkeystore.Zero(passphrase)

	if _, err := provider.Put(ctx, lhkeystore.BackupKey(backup.Name), passphrase, 0); err != nil {
		if errors.Is(err, lhkeystore.ErrKeyConflict) {
			// Already written by an earlier pass.
			return nil
		}
		return errors.Wrapf(err, "failed to store the encryption key of backup %v", backup.Name)
	}
	log.Infof("Stored the encryption key of backup %v, taken from version %v of volume %v", backup.Name, version, volumeName)
	return nil
}

// destroyBackupEncryptionKey removes a backup's entry along with its data.
func destroyBackupEncryptionKey(ds *datastore.DataStore, namespace string, log logrus.FieldLogger, backup *longhorn.Backup, volumeName string) {
	volume, err := ds.GetVolumeRO(volumeName)
	if err != nil || !types.IsVaultEncrypted(&volume.Spec) {
		// Without the volume nothing names the vault; leave the entry.
		return
	}

	provider, err := encryptionKeyProvider(ds, namespace, volume.Spec.Encryption.SecretName)
	if err != nil {
		log.WithError(err).Warnf("Failed to reach the key store to destroy the encryption key of backup %v", backup.Name)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), keyStoreTimeout)
	defer cancel()
	if err := provider.Delete(ctx, lhkeystore.BackupKey(backup.Name)); err != nil {
		log.WithError(err).Warnf("Failed to destroy the encryption key of backup %v; the entry stays in the key store", backup.Name)
		return
	}
	log.Infof("Destroyed the encryption key of backup %v", backup.Name)
}
