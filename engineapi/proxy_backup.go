package engineapi

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/longhorn/backupstore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

func (p *Proxy) SnapshotBackup(e *longhorn.Engine,
	snapshotName, backupName, backupTarget,
	backingImageName, backingImageChecksum string,
	labels, credential map[string]string) (string, string, error) {
	if snapshotName == VolumeHeadName {
		return "", "", fmt.Errorf("invalid operation: cannot backup %v", VolumeHeadName)
	}

	snap, err := p.SnapshotGet(e, snapshotName)
	if err != nil {
		return "", "", errors.Wrapf(err, "error getting snapshot '%s', engine '%s'", snapshotName, e.Name)
	}

	if snap == nil {
		return "", "", errors.Errorf("could not find snapshot '%s' to backup, engine '%s'", snapshotName, e.Name)
	}

	// get environment variables if backup for s3
	credentialEnv, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return "", "", err
	}

	backupID, replicaAddress, err := p.grpcClient.SnapshotBackup(p.DirectToURL(e),
		backupName, snapshotName, backupTarget,
		backingImageName, backingImageChecksum,
		labels, credentialEnv,
	)
	if err != nil {
		return "", "", err
	}

	return backupID, replicaAddress, nil
}

func (p *Proxy) SnapshotBackupStatus(e *longhorn.Engine, backupName, replicaAddress string) (status *longhorn.EngineBackupStatus, err error) {
	recv, err := p.grpcClient.SnapshotBackupStatus(p.DirectToURL(e), backupName, replicaAddress)
	if err != nil {
		return nil, err
	}

	return (*longhorn.EngineBackupStatus)(recv), nil
}

func (p *Proxy) BackupRestore(e *longhorn.Engine, backupTarget, backupName, backupVolumeName, lastRestored string, credential map[string]string) error {
	backupURL := backupstore.EncodeBackupURL(backupName, backupVolumeName, backupTarget)

	// get environment variables if backup for s3
	envs, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return err
	}

	return p.grpcClient.BackupRestore(p.DirectToURL(e), backupURL, backupTarget, backupVolumeName, envs)
}

func (p *Proxy) BackupRestoreStatus(e *longhorn.Engine) (status map[string]*longhorn.RestoreStatus, err error) {
	recv, err := p.grpcClient.BackupRestoreStatus(p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	status = map[string]*longhorn.RestoreStatus{}
	for k, v := range recv {
		status[k] = (*longhorn.RestoreStatus)(v)
	}
	return status, nil
}

func (p *Proxy) BackupNameList(destURL, volumeName string, credential map[string]string) (names []string, err error) {
	if volumeName == "" {
		return nil, nil
	}

	// get environment variables if backup for s3
	envs, err := getBackupCredentialEnv(destURL, credential)
	if err != nil {
		return nil, err
	}

	recv, err := p.grpcClient.BackupVolumeList(destURL, volumeName, false, envs)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	volume, ok := recv[volumeName]
	if !ok {
		return nil, fmt.Errorf("cannot find %s in the backups", volumeName)
	}

	names = []string{}
	if volume.Messages[string(backupstore.MessageTypeError)] != "" {
		return names, errors.New(volume.Messages[string(backupstore.MessageTypeError)])
	}

	for backupName := range volume.Backups {
		names = append(names, backupName)
	}
	sort.Strings(names)
	return names, nil
}

func (p *Proxy) BackupVolumeNameList(destURL string, credential map[string]string) (names []string, err error) {
	// get environment variables if backup for s3
	envs, err := getBackupCredentialEnv(destURL, credential)
	if err != nil {
		return nil, err
	}

	recv, err := p.grpcClient.BackupVolumeList(destURL, "", true, envs)
	if err != nil {
		if types.ErrorIsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	names = []string{}
	for volumeName := range recv {
		names = append(names, volumeName)
	}
	sort.Strings(names)
	return names, nil
}
