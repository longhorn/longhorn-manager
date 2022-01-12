package client

import (
	"encoding/json"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
)

func (c *ProxyClient) SnapshotBackup(serviceAddress,
	backupName, snapshotName, backupTarget,
	backingImageName, backingImageChecksum string,
	labels map[string]string, envs []string) (backupID, replicaAddress string, err error) {
	if serviceAddress == "" {
		return "", "", errors.Wrapf(ErrParameter, "failed to backup snapshot")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Backing up snapshot %v to %v via proxy", snapshotName, backupName)

	req := &rpc.EngineSnapshotBackupRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		Envs:                 envs,
		BackupName:           backupName,
		SnapshotName:         snapshotName,
		BackupTarget:         backupTarget,
		BackingImageName:     backingImageName,
		BackingImageChecksum: backingImageChecksum,
		Labels:               labels,
	}
	recv, err := c.service.SnapshotBackup(c.ctx, req)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to backup snapshot %v to %v via proxy %v to %v", snapshotName, backupName, c.ServiceURL, serviceAddress)
	}

	return recv.BackupId, recv.Replica, nil
}

func (c *ProxyClient) SnapshotBackupStatus(serviceAddress, backupName, replicaAddress string) (status *SnapshotBackupStatus, err error) {
	if serviceAddress == "" || backupName == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup status via proxy", backupName)

	req := &rpc.EngineSnapshotBackupStatusRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		BackupName:     backupName,
		ReplicaAddress: replicaAddress,
	}
	recv, err := c.service.SnapshotBackupStatus(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %v backup status via proxy %v to %v", backupName, c.ServiceURL, serviceAddress)
	}

	status = &SnapshotBackupStatus{
		Progress:       int(recv.Progress),
		BackupURL:      recv.BackupUrl,
		Error:          recv.Error,
		SnapshotName:   recv.SnapshotName,
		State:          recv.State,
		ReplicaAddress: recv.ReplicaAddress,
	}
	return status, nil
}

func (c *ProxyClient) BackupRestore(serviceAddress, url, target, volumeName string, envs []string) error {
	if serviceAddress == "" || url == "" || target == "" || volumeName == "" {
		return errors.Wrapf(ErrParameter, "failed to restore backup to volume")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Restoring %v backup to %v via proxy", url, volumeName)

	req := &rpc.EngineBackupRestoreRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		Envs:       envs,
		Url:        url,
		Target:     target,
		VolumeName: volumeName,
	}
	recv, err := c.service.BackupRestore(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to restore backup %v to %v via proxy %v to %v", url, volumeName, c.ServiceURL, serviceAddress)
	}

	if recv.TaskError != nil {
		var taskErr TaskError
		if jsonErr := json.Unmarshal(recv.TaskError, &taskErr); jsonErr != nil {
			return errors.Wrapf(jsonErr, "Cannot unmarshal the restore error, maybe it's not caused by the replica restore failure")
		}

		return taskErr
	}

	return nil
}

func (c *ProxyClient) BackupRestoreStatus(serviceAddress string) (status map[string]*BackupRestoreStatus, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup restore status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting backup restore status via proxy")

	req := &rpc.ProxyEngineRequest{
		Address: serviceAddress,
	}
	recv, err := c.service.BackupRestoreStatus(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get backup restore status via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	status = map[string]*BackupRestoreStatus{}
	for k, v := range recv.Status {
		status[k] = &BackupRestoreStatus{
			IsRestoring:            v.IsRestoring,
			LastRestored:           v.LastRestored,
			CurrentRestoringBackup: v.CurrentRestoringBackup,
			Progress:               int(v.Progress),
			Error:                  v.Error,
			Filename:               v.Filename,
			State:                  v.State,
			BackupURL:              v.BackupUrl,
		}
	}
	return status, nil
}

func (c *ProxyClient) BackupGet(destURL string, envs []string) (info *EngineBackupInfo, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup via proxy", destURL)

	req := &rpc.EngineBackupGetRequest{
		Envs:    envs,
		DestUrl: destURL,
	}
	recv, err := c.service.BackupGet(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %v backup via proxy %v", destURL, c.ServiceURL)
	}

	return parseBackup(recv.Backup), nil
}

func (c *ProxyClient) BackupVolumeGet(destURL string, envs []string) (info *EngineBackupVolumeInfo, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup volume")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup volume via proxy", destURL)

	req := &rpc.EngineBackupVolumeGetRequest{
		Envs:    envs,
		DestUrl: destURL,
	}
	recv, err := c.service.BackupVolumeGet(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %v backup volume via proxy %v", destURL, c.ServiceURL)
	}

	info = &EngineBackupVolumeInfo{
		Name:                 recv.Volume.Name,
		Size:                 recv.Volume.Size,
		Labels:               recv.Volume.Labels,
		Created:              recv.Volume.Created,
		LastBackupName:       recv.Volume.LastBackupName,
		LastBackupAt:         recv.Volume.LastBackupAt,
		DataStored:           recv.Volume.DataStored,
		Messages:             recv.Volume.Messages,
		Backups:              parseBackups(recv.Volume.Backups),
		BackingImageName:     recv.Volume.BackingImageName,
		BackingImageChecksum: recv.Volume.BackingImageChecksum,
	}
	return info, nil
}

func (c *ProxyClient) BackupVolumeList(destURL, volumeName string, volumeOnly bool, envs []string) (info map[string]*EngineBackupVolumeInfo, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to list backup volumes")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	if volumeName != "" {
		log = log.WithField("volume", volumeName)
	}
	log.Debugf("Listing %v backup volumes via proxy", destURL)

	req := &rpc.EngineBackupVolumeListRequest{
		Envs:       envs,
		DestUrl:    destURL,
		VolumeName: volumeName,
		VolumeOnly: volumeOnly,
	}
	recv, err := c.service.BackupVolumeList(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list %v backup volumes via proxy %v", destURL, c.ServiceURL)
	}

	info = map[string]*EngineBackupVolumeInfo{}
	for k, v := range recv.Volumes {
		info[k] = &EngineBackupVolumeInfo{
			Name:                 v.Name,
			Size:                 v.Size,
			Labels:               v.Labels,
			Created:              v.Created,
			LastBackupName:       v.LastBackupName,
			LastBackupAt:         v.LastBackupAt,
			DataStored:           v.DataStored,
			Messages:             v.Messages,
			Backups:              parseBackups(v.Backups),
			BackingImageName:     v.BackingImageName,
			BackingImageChecksum: v.BackingImageChecksum,
		}
	}
	return info, nil
}

func parseBackups(in map[string]*rpc.EngineBackupInfo) (out map[string]*EngineBackupInfo) {
	out = map[string]*EngineBackupInfo{}
	for k, v := range in {
		out[k] = parseBackup(v)
	}
	return out
}

func parseBackup(in *rpc.EngineBackupInfo) (out *EngineBackupInfo) {
	return &EngineBackupInfo{
		Name:                   in.Name,
		URL:                    in.Url,
		SnapshotName:           in.SnapshotName,
		SnapshotCreated:        in.SnapshotCreated,
		Created:                in.Created,
		Size:                   in.Size,
		Labels:                 in.Labels,
		IsIncremental:          in.IsIncremental,
		VolumeName:             in.VolumeName,
		VolumeSize:             in.VolumeSize,
		VolumeCreated:          in.VolumeCreated,
		VolumeBackingImageName: in.VolumeBackingImageName,
		Messages:               in.Messages,
	}
}

func (c *ProxyClient) BackupConfigMetaGet(destURL string, envs []string) (meta *backupstore.ConfigMetadata, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup config metadata")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup config metadata via proxy", destURL)

	req := &rpc.EngineBackupConfigMetaGetRequest{
		Envs:    envs,
		DestUrl: destURL,
	}
	recv, err := c.service.BackupConfigMetaGet(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get %v backup config metadata via proxy %v", destURL, c.ServiceURL)
	}

	ts, err := ptypes.Timestamp(recv.ModificationTime)
	if err != nil {
		return nil, errors.Wrapf(err, "failed convert protobuf timestamp %v", recv.ModificationTime)
	}

	return &backupstore.ConfigMetadata{
		ModificationTime: ts,
	}, nil
}

func (c *ProxyClient) BackupRemove(destURL, volumeName string, envs []string) (err error) {
	if destURL == "" {
		return errors.Wrapf(ErrParameter, "failed to remove backup")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	if volumeName != "" {
		log = log.WithField("volume", volumeName)
	}
	log.Debugf("Removing %v backup via proxy", destURL)

	req := &rpc.EngineBackupRemoveRequest{
		Envs:       envs,
		DestUrl:    destURL,
		VolumeName: volumeName,
	}
	_, err = c.service.BackupRemove(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to remove %v backup via proxy %v", destURL, c.ServiceURL)
	}

	return nil
}
