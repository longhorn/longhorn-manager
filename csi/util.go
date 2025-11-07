package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	"k8s.io/mount-utils"

	utilexec "k8s.io/utils/exec"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	// defaultStaleReplicaTimeout set to 48 hours (2880 minutes)
	defaultStaleReplicaTimeout                         = 2880
	defaultStorageClassDisableRevisionCounterParameter = true

	defaultForceUmountTimeout = 30 * time.Second

	tempTestMountPointValidStatusFile = ".longhorn-volume-mount-point-test.tmp"

	nodeTopologyKey = "kubernetes.io/hostname"
)

// NewForcedParamsExec creates a osExecutor that allows for adding additional params to later occurring Run calls
func NewForcedParamsExec(cmdParamMapping map[string]string) utilexec.Interface {
	return &forcedParamsOsExec{
		exec:            utilexec.New(),
		cmdParamMapping: cmdParamMapping,
	}
}

type forcedParamsOsExec struct {
	exec            utilexec.Interface
	cmdParamMapping map[string]string
}

type volumeFilesystemStatistics struct {
	availableBytes int64
	totalBytes     int64
	usedBytes      int64

	availableInodes int64
	totalInodes     int64
	usedInodes      int64
}

func (e *forcedParamsOsExec) Command(cmd string, args ...string) utilexec.Cmd {
	var params []string
	if value := e.cmdParamMapping[cmd]; value != "" {
		// we prepend the user params, since options are conventionally before the final args
		// command [-option(s)] [argument(s)]
		params = append(params, strings.Split(value, " ")...)
	}
	params = append(params, args...)
	return e.exec.Command(cmd, params...)
}

func (e *forcedParamsOsExec) CommandContext(ctx context.Context, cmd string, args ...string) utilexec.Cmd {
	return e.exec.CommandContext(ctx, cmd, args...)
}

func (e *forcedParamsOsExec) LookPath(file string) (string, error) {
	return e.exec.LookPath(file)
}

func updateVolumeParamsForBackingImage(volumeParameters map[string]string, backingImageParameters map[string]string) {
	BackingImageInfoFields := []string{
		longhorn.BackingImageParameterName,
		longhorn.BackingImageParameterDataSourceType,
		longhorn.BackingImageParameterChecksum,
	}
	for _, v := range BackingImageInfoFields {
		volumeParameters[v] = backingImageParameters[v]
		delete(backingImageParameters, v)
	}
	backingImageParametersStr, _ := json.Marshal(backingImageParameters)
	volumeParameters[longhorn.BackingImageParameterDataSourceParameters] = string(backingImageParametersStr)
}

func getVolumeOptions(volumeID string, volOptions map[string]string) (*longhornclient.Volume, error) {
	vol := &longhornclient.Volume{}

	if staleReplicaTimeout, ok := volOptions["staleReplicaTimeout"]; ok {
		srt, err := strconv.Atoi(staleReplicaTimeout)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter staleReplicaTimeout")
		}
		vol.StaleReplicaTimeout = int64(srt)
	}
	if vol.StaleReplicaTimeout <= 0 {
		vol.StaleReplicaTimeout = defaultStaleReplicaTimeout
	}

	if share, ok := volOptions["share"]; ok {
		isShared, err := strconv.ParseBool(share)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter share")
		}

		if isShared {
			vol.AccessMode = string(longhorn.AccessModeReadWriteMany)
		}
	}

	if exclusive, ok := volOptions["exclusive"]; ok {
		isExclusive, err := strconv.ParseBool(exclusive)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter exclusive")
		}
		if isExclusive && vol.AccessMode == string(longhorn.AccessModeReadWriteMany) {
			return nil, errors.New("cannot set both share and exclusive to true")
		}
		if isExclusive {
			vol.AccessMode = string(longhorn.AccessModeReadWriteOncePod)
		}
	}

	if vol.AccessMode == "" {
		vol.AccessMode = string(longhorn.AccessModeReadWriteOnce)
	}

	if migratable, ok := volOptions["migratable"]; ok {
		isMigratable, err := strconv.ParseBool(migratable)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter migratable")
		}

		if isMigratable && vol.AccessMode != string(longhorn.AccessModeReadWriteMany) {
			logrus.Infof("Cannot mark volume %v as migratable, "+
				"since access mode is not RWX proceeding with RWO non migratable volume creation", volumeID)
			volOptions["migratable"] = strconv.FormatBool(false)
			isMigratable = false
		}
		vol.Migratable = isMigratable
	}

	if encrypted, ok := volOptions["encrypted"]; ok {
		isEncrypted, err := strconv.ParseBool(encrypted)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter encrypted")
		}
		vol.Encrypted = isEncrypted
	}

	if numberOfReplicas, ok := volOptions["numberOfReplicas"]; ok {
		nor, err := strconv.Atoi(numberOfReplicas)
		if err != nil || nor < 0 {
			return nil, errors.Wrap(err, "invalid parameter numberOfReplicas")
		}
		vol.NumberOfReplicas = int64(nor)
	}

	if ublkNumberOfQueue, ok := volOptions["ublkNumberOfQueue"]; ok {
		noq, err := strconv.Atoi(ublkNumberOfQueue)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter ublkNumberOfQueue")
		}
		vol.UblkNumberOfQueue = int64(noq)
	}

	if ublkQueueDepth, ok := volOptions["ublkQueueDepth"]; ok {
		depth, err := strconv.Atoi(ublkQueueDepth)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter ublkQueueDepth")
		}
		vol.UblkQueueDepth = int64(depth)
	}

	if replicaAutoBalance, ok := volOptions["replicaAutoBalance"]; ok {
		err := types.ValidateReplicaAutoBalance(longhorn.ReplicaAutoBalance(replicaAutoBalance))
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter replicaAutoBalance")
		}
		vol.ReplicaAutoBalance = replicaAutoBalance
	}

	if locality, ok := volOptions["dataLocality"]; ok {
		if err := types.ValidateDataLocality(longhorn.DataLocality(locality)); err != nil {
			return nil, errors.Wrap(err, "invalid parameter dataLocality")
		}
		vol.DataLocality = locality
	}

	if revisionCounterDisabled, ok := volOptions["disableRevisionCounter"]; ok {
		revCounterDisabled, err := strconv.ParseBool(revisionCounterDisabled)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter disableRevisionCounter")
		}
		vol.RevisionCounterDisabled = revCounterDisabled
	} else {
		vol.RevisionCounterDisabled = defaultStorageClassDisableRevisionCounterParameter
	}

	if unmapMarkSnapChainRemoved, ok := volOptions["unmapMarkSnapChainRemoved"]; ok {
		if err := types.ValidateUnmapMarkSnapChainRemoved(longhorn.DataEngineType(vol.DataEngine), longhorn.UnmapMarkSnapChainRemoved(unmapMarkSnapChainRemoved)); err != nil {
			return nil, errors.Wrap(err, "invalid parameter unmapMarkSnapChainRemoved")
		}
		vol.UnmapMarkSnapChainRemoved = unmapMarkSnapChainRemoved
	}

	if replicaSoftAntiAffinity, ok := volOptions["replicaSoftAntiAffinity"]; ok {
		if err := types.ValidateReplicaSoftAntiAffinity(longhorn.ReplicaSoftAntiAffinity(replicaSoftAntiAffinity)); err != nil {
			return nil, errors.Wrap(err, "invalid parameter replicaSoftAntiAffinity")
		}
		vol.ReplicaSoftAntiAffinity = replicaSoftAntiAffinity
	}

	if replicaZoneSoftAntiAffinity, ok := volOptions["replicaZoneSoftAntiAffinity"]; ok {
		if err := types.ValidateReplicaZoneSoftAntiAffinity(longhorn.ReplicaZoneSoftAntiAffinity(replicaZoneSoftAntiAffinity)); err != nil {
			return nil, errors.Wrap(err, "invalid parameter replicaZoneSoftAntiAffinity")
		}
		vol.ReplicaZoneSoftAntiAffinity = replicaZoneSoftAntiAffinity
	}

	if replicaDiskSoftAntiAffinity, ok := volOptions["replicaDiskSoftAntiAffinity"]; ok {
		if err := types.ValidateReplicaDiskSoftAntiAffinity(longhorn.ReplicaDiskSoftAntiAffinity(replicaDiskSoftAntiAffinity)); err != nil {
			return nil, errors.Wrap(err, "invalid parameter replicaDiskSoftAntiAffinity")
		}
		vol.ReplicaDiskSoftAntiAffinity = replicaDiskSoftAntiAffinity
	}

	if fromBackup, ok := volOptions["fromBackup"]; ok {
		vol.FromBackup = fromBackup
	}

	if backupTargetName, ok := volOptions["backupTargetName"]; ok {
		vol.BackupTargetName = backupTargetName
	}

	if backupBlockSize, ok := volOptions["backupBlockSize"]; ok {
		blockSize, err := util.ConvertSize(backupBlockSize)
		if err != nil {
			return nil, errors.Wrap(err, "invalid parameter backupBlockSize")
		}
		vol.BackupBlockSize = strconv.FormatInt(blockSize, 10)
	}

	if dataSource, ok := volOptions["dataSource"]; ok {
		vol.DataSource = dataSource
		vol.CloneMode = volOptions["cloneMode"]
	}

	if backingImage, ok := volOptions[longhorn.BackingImageParameterName]; ok {
		vol.BackingImage = backingImage
	}

	recurringJobSelector := []longhornclient.VolumeRecurringJob{}
	if jsonRecurringJobSelector, ok := volOptions["recurringJobSelector"]; ok {
		err := json.Unmarshal([]byte(jsonRecurringJobSelector), &recurringJobSelector)
		if err != nil {
			return nil, errors.Wrap(err, "invalid json format of recurringJobSelector")
		}
		vol.RecurringJobSelector = recurringJobSelector
	}

	if diskSelector, ok := volOptions["diskSelector"]; ok {
		vol.DiskSelector = strings.Split(diskSelector, ",")
	}

	if nodeSelector, ok := volOptions["nodeSelector"]; ok {
		vol.NodeSelector = strings.Split(nodeSelector, ",")
	}

	vol.DataEngine = string(longhorn.DataEngineTypeV1)
	if driver, ok := volOptions["dataEngine"]; ok {
		vol.DataEngine = driver
	}

	if freezeFilesystemForSnapshot, ok := volOptions["freezeFilesystemForSnapshot"]; ok {
		if err := types.ValidateFreezeFilesystemForSnapshot(longhorn.FreezeFilesystemForSnapshot(freezeFilesystemForSnapshot)); err != nil {
			return nil, errors.Wrap(err, "invalid parameter freezeFilesystemForSnapshot")
		}
		vol.FreezeFilesystemForSnapshot = freezeFilesystemForSnapshot
	}

	vol.Frontend = volOptions["frontend"]

	return vol, nil
}

func syncMountPointDirectory(targetPath string) error {
	d, err := os.OpenFile(targetPath, os.O_SYNC, 0750)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := d.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warnf("Failed to close mount point %v", targetPath)
		}
	}()

	if _, err := d.Readdirnames(1); err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
	}

	// it would not always return `Input/Output Error` or `read-only file system` errors if we only use ReadDir() or Readdirnames() without an I/O operation
	// an I/O operation will make the targetPath mount point invalid immediately
	tempFile := path.Join(targetPath, tempTestMountPointValidStatusFile)
	f, err := os.OpenFile(tempFile, os.O_CREATE|os.O_RDWR|os.O_SYNC|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			logrus.WithError(err).Warnf("Failed to close %v", tempFile)
		}
		if err := os.Remove(tempFile); err != nil {
			logrus.WithError(err).Warnf("Failed to remove %v", tempFile)
		}
	}()

	if _, err := f.WriteString(tempFile); err != nil {
		return err
	}

	return nil
}

func checkBindMountFileAccessible(targetPath string) error {
	d, openErr := os.OpenFile(targetPath, os.O_RDONLY|unix.O_NONBLOCK, 0750)
	if openErr != nil {
		return openErr
	}
	if closeErr := d.Close(); closeErr != nil {
		logrus.WithError(closeErr).Warnf("Failed to close bind mount point %v", targetPath)
	}
	return nil
}

// ensureMountPoint evaluates whether a path is a valid mountPoint
// in case the path does not exists it will create a path and return false
// in case where the mount point exists but is corrupt, the mount point will be cleaned up and a error is returned
func ensureMountPoint(path string, mounter mount.Interface) (bool, error) {
	logrus.Infof("Trying to ensure mount point %v", path)
	isMnt, err := mounter.IsMountPoint(path)
	if os.IsNotExist(err) {
		return false, os.MkdirAll(path, 0750)
	}

	IsCorruptedMnt := mount.IsCorruptedMnt(err)
	if !IsCorruptedMnt {
		logrus.Infof("Mount point %v try opening and syncing dir to make sure it's healthy", path)
		if err := syncMountPointDirectory(path); err != nil {
			logrus.WithError(err).Warnf("Mount point %v was identified as corrupt by opening and syncing", path)
			IsCorruptedMnt = true
		}
	}

	if IsCorruptedMnt {
		unmountErr := unmount(path, mounter)
		if unmountErr != nil {
			return false, fmt.Errorf("failed to unmount corrupt mount point %v umount error: %v eval error: %v",
				path, unmountErr, err)
		}

		logrus.Infof("Unmounted existing corrupt mount point %v", path)
		return false, nil
	}
	return isMnt, err
}

// ensureBindMountPoint evaluates whether a path is a valid mount point for bind mount.
// In case the path is not exist, a regular file will be created, and returns false.
// In case the path exists, but is not an unmounted regular file, it will clean up the path and recreate a new regular file, and returns false.
// In case the path is a healthy mount, nothing happens and returns true.
func ensureBindMountPoint(path string, mounter mount.Interface) (bool, error) {
	logrus.Infof("Trying to ensure bind mount point %v", path)

	checkMountPointIsValid := func(path string, mounter mount.Interface) (isValid, isMounted bool, err error) {
		// check the mount status first to avoid busy device blocking
		isMounted, checkMountErr := mounter.IsMountPoint(path)
		if os.IsNotExist(checkMountErr) {
			return false, false, nil
		}
		if mount.IsCorruptedMnt(checkMountErr) {
			return false, isMounted, nil
		}
		if checkMountErr != nil {
			return false, isMounted, errors.Wrapf(checkMountErr, "failed to check mounting status of bind mount point %v", path)
		}
		if isMounted {
			logrus.Infof("Bind mount point %v try opening to make sure it's healthy", path)
			if err := checkBindMountFileAccessible(path); err != nil {
				logrus.WithError(err).Warnf("Bind mount point %v is identified as corrupt", path)
				return false, true, nil
			}
			return true, true, nil
		}

		// file is exist and is not mounted, check the file type
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return false, false, errors.Wrapf(statErr, "failed to check file type of bind mount point %v", path)
		}
		return info.Mode().IsRegular(), false, nil
	}

	isValidBindMountPoint, isMounted, checkErr := checkMountPointIsValid(path, mounter)
	if checkErr != nil {
		return isMounted, checkErr
	} else if isValidBindMountPoint {
		return isMounted, nil
	}

	// invalid mounted target
	logrus.Infof("Recreating bind mount point %v", path)

	// clean up the target mount point for recreation
	if cleanupErr := unmountAndCleanupMountPoint(path, mounter); cleanupErr != nil {
		return true, errors.Wrapf(cleanupErr, "failed to remove corrupt bind mount point at %s", path)
	}

	// create regular file for bind mount
	if makeFileErr := makeFile(path); makeFileErr != nil {
		return false, errors.Wrapf(makeFileErr, "failed to create regular file for bind mount at %s", path)
	}
	return false, nil
}

// ensureDirectory checks if a folder exists at the specified path.
// If not, it creates the folder and returns true, otherwise returns false.
// If the path exists but is not a folder, it returns an error.
func ensureDirectory(path string) (bool, error) {
	fileInfo, err := os.Stat(path)

	if os.IsNotExist(err) {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return false, err
		}
		return true, nil
	} else if err != nil {
		return false, err
	}

	if fileInfo.IsDir() {
		return true, nil
	}

	return false, fmt.Errorf("path %v exists but is not a folder", path)
}

func lazyUnmount(path string, exec utilexec.Interface) (err error) {
	lazyUnmountCmd := exec.Command("umount", "-l", path)
	output, err := lazyUnmountCmd.CombinedOutput()
	if isFileNotExistError(err) || isNotMountedError(err) {
		logrus.WithError(err).Infof("No need for lazy unmount: not a mount point %v", path)
		return nil
	}
	return errors.Wrapf(err, "failed to lazy unmount a mount point %v, output: %s", path, output)
}

func unmount(path string, mounter mount.Interface) (err error) {
	forceUnmounter, ok := mounter.(mount.MounterForceUnmounter)
	if ok {
		logrus.Infof("Trying to force unmount potential mount point %v", path)
		err = forceUnmounter.UnmountWithForce(path, defaultForceUmountTimeout)
	} else {
		logrus.Infof("Trying to unmount potential mount point %v", path)
		err = mounter.Unmount(path)
	}
	if err == nil {
		return nil
	} else if isFileNotExistError(err) || isNotMountedError(err) {
		logrus.WithError(err).Infof("No need for unmount not a mount point %v", path)
		return nil
	}
	logrus.WithError(err).Warnf("Failed to unmount %v, attempting lazy unmount to release mount point", path)
	return lazyUnmount(path, utilexec.New())
}

// unmountAndCleanupMountPoint ensures all mount layers for the path are unmounted and the mount point is removed
func unmountAndCleanupMountPoint(path string, mounter mount.Interface) error {
	// we just try to unmount since the path check would get stuck for nfs mounts
	logrus.Infof("Trying to umount mount point %v", path)
	if err := unmount(path, mounter); err != nil {
		logrus.WithError(err).Warnf("Failed to unmount %v during cleanup", path)
		return err
	}

	// ensure the file is removed even when it is a broken symbolic link
	logrus.Infof("Trying to clean up mount point %v", path)
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to stat mount point %v", path)
	}
	logrus.WithField("fileMode", info.Mode().String()).Debugf("Mount point %v exists, removing the file", path)
	if err := os.Remove(path); err != nil {
		return errors.Wrapf(err, "failed to remove mount point %v", path)
	}
	return nil
}

// isBlockDevice return true if volumePath file is a block device, false otherwise.
func isBlockDevice(volumePath string) (bool, error) {
	var stat unix.Stat_t
	// See https://man7.org/linux/man-pages/man2/stat.2.html for details
	err := unix.Stat(volumePath, &stat)
	if err != nil {
		return false, err
	}

	// See https://man7.org/linux/man-pages/man7/inode.7.html for detail
	if (stat.Mode & unix.S_IFMT) == unix.S_IFBLK {
		return true, nil
	}

	return false, nil
}

func getDiskFormat(devicePath string) (string, error) {
	m := mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
	return m.GetDiskFormat(devicePath)
}

func getFilesystemStatistics(volumePath string) (*volumeFilesystemStatistics, error) {
	var statfs unix.Statfs_t
	// See http://man7.org/linux/man-pages/man2/statfs.2.html for details.
	err := unix.Statfs(volumePath, &statfs)
	if err != nil {
		return nil, err
	}

	volStats := &volumeFilesystemStatistics{
		availableBytes: int64(statfs.Bavail) * int64(statfs.Bsize),
		totalBytes:     int64(statfs.Blocks) * int64(statfs.Bsize),
		usedBytes:      (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize),

		availableInodes: int64(statfs.Ffree),
		totalInodes:     int64(statfs.Files),
		usedInodes:      int64(statfs.Files) - int64(statfs.Ffree),
	}

	return volStats, nil
}

// makeFile creates an empty regular file.
// If pathname already exists, whether a file or directory, no error is returned.
func makeFile(pathname string) error {
	f, err := os.OpenFile(pathname, os.O_CREATE, os.FileMode(0644))
	if f != nil {
		err = f.Close()
		return err
	}
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

// requiresSharedAccess checks if the volume is requested to be multi node capable
// a volume that is already in shared access mode, must be used via shared access
// even if single node access is requested.
func requiresSharedAccess(vol *longhornclient.Volume, cap *csi.VolumeCapability) bool {
	isSharedVolume := false
	if vol != nil {
		isSharedVolume = vol.AccessMode == string(longhorn.AccessModeReadWriteMany) || vol.Migratable
	}

	mode := csi.VolumeCapability_AccessMode_UNKNOWN
	if cap != nil {
		mode = cap.AccessMode.Mode
	}

	return isSharedVolume ||
		mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
		mode == csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER ||
		mode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
}

func requireExclusiveAccess(vol *longhornclient.Volume, capability *csi.VolumeCapability) bool {
	isExclusive := false
	if vol != nil {
		isExclusive = vol.AccessMode == string(longhorn.AccessModeReadWriteOncePod)
	}

	mode := csi.VolumeCapability_AccessMode_UNKNOWN
	if capability != nil {
		mode = capability.AccessMode.Mode
	}

	return isExclusive || mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER
}

func getStageBlockVolumePath(stagingTargetPath, volumeID string) string {
	return filepath.Join(stagingTargetPath, volumeID)
}

func parseNodeID(topology *csi.Topology) (string, error) {
	if topology == nil || topology.Segments == nil {
		return "", fmt.Errorf("missing accessible topology request parameter")
	}
	nodeId, ok := topology.Segments[nodeTopologyKey]
	if !ok {
		return "", fmt.Errorf("accessible topology request parameter is missing %s key", nodeTopologyKey)
	}
	return nodeId, nil
}

func isFileNotExistError(err error) bool {
	if err == nil {
		return false
	}
	if os.IsNotExist(err) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such file")
}

func isNotMountedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not mounted") ||
		strings.Contains(err.Error(), "no mount point specified")
}
