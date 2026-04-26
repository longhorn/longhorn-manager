package engineapi

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/backupstore"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) SnapshotBackup(obj DataEngineObject, snapshotName, backupName, backupTarget,
	backingImageName, backingImageChecksum, compressionMethod string, concurrentLimit int, storageClassName string,
	labels, credential, parameters map[string]string) (string, string, error) {
	if snapshotName == etypes.VolumeHeadName {
		return "", "", fmt.Errorf("invalid operation: cannot backup %v", etypes.VolumeHeadName)
	}

	snap, err := p.SnapshotGet(obj, snapshotName)
	if err != nil {
		return "", "", errors.Wrapf(err, "error getting snapshot '%s', engine '%s'", snapshotName, obj.GetEngineName())
	}

	if snap == nil {
		return "", "", errors.Errorf("could not find snapshot '%s' to backup, engine '%s'", snapshotName, obj.GetEngineName())
	}

	// get environment variables if backup for s3
	credentialEnv, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return "", "", err
	}

	backupID, replicaAddress, err := p.grpcClient.SnapshotBackup(obj.GetDataEngine(), obj.GetEngineName(), obj.GetVolumeName(), p.DirectToURL(obj),
		backupName, snapshotName, backupTarget, backingImageName, backingImageChecksum, compressionMethod, concurrentLimit, storageClassName, labels, credentialEnv, parameters,
	)
	if err != nil {
		return "", "", err
	}

	return backupID, replicaAddress, nil
}

func (p *Proxy) SnapshotBackupStatus(obj DataEngineObject, backupName, replicaAddress, replicaName string) (status *longhorn.EngineBackupStatus, err error) {
	recv, err := p.grpcClient.SnapshotBackupStatus(obj.GetDataEngine(), obj.GetEngineName(), obj.GetVolumeName(),
		p.DirectToURL(obj), backupName, replicaAddress, replicaName)
	if err != nil {
		return nil, err
	}

	return (*longhorn.EngineBackupStatus)(recv), nil
}

func (p *Proxy) BackupRestore(e *longhorn.Engine, backupTarget, backupName, backupVolumeName, lastRestored string,
	credential map[string]string, concurrentLimit int) error {
	backupURL := backupstore.EncodeBackupURL(backupName, backupVolumeName, backupTarget)

	// get environment variables if backup for s3
	envs, err := getBackupCredentialEnv(backupTarget, credential)
	if err != nil {
		return err
	}

	return p.grpcClient.BackupRestore(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName, p.DirectToURL(e),
		backupURL, backupTarget, backupVolumeName, envs, concurrentLimit)
}

func (p *Proxy) BackupRestoreStatus(e *longhorn.Engine) (status map[string]*longhorn.RestoreStatus, err error) {
	recv, err := p.grpcClient.BackupRestoreStatus(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	status = map[string]*longhorn.RestoreStatus{}
	for k, v := range recv {
		status[k] = (*longhorn.RestoreStatus)(v)
	}
	return status, nil
}

func (p *Proxy) CleanupBackupMountPoints() (err error) {
	return p.grpcClient.CleanupBackupMountPoints()
}
