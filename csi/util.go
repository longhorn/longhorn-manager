package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"

	"golang.org/x/sys/unix"

	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	// defaultStaleReplicaTimeout set to 48 hours (2880 minutes)
	defaultStaleReplicaTimeout = 2880

	defaultForceUmountTimeout = 30 * time.Second
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

func getVolumeOptions(volOptions map[string]string) (*longhornclient.Volume, error) {
	vol := &longhornclient.Volume{}

	if staleReplicaTimeout, ok := volOptions["staleReplicaTimeout"]; ok {
		srt, err := strconv.Atoi(staleReplicaTimeout)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter staleReplicaTimeout")
		}
		vol.StaleReplicaTimeout = int64(srt)
	}
	if vol.StaleReplicaTimeout <= 0 {
		vol.StaleReplicaTimeout = defaultStaleReplicaTimeout
	}

	if share, ok := volOptions["share"]; ok {
		isShared, err := strconv.ParseBool(share)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter share")
		}

		if isShared {
			vol.AccessMode = string(longhorn.AccessModeReadWriteMany)
		} else {
			vol.AccessMode = string(longhorn.AccessModeReadWriteOnce)
		}
	}

	if migratable, ok := volOptions["migratable"]; ok {
		isMigratable, err := strconv.ParseBool(migratable)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter migratable")
		}

		if isMigratable && vol.AccessMode != string(longhorn.AccessModeReadWriteMany) {
			logrus.Infof("Cannot mark volume %v as migratable, "+
				"since access mode is not RWX proceeding with RWO non migratable volume creation.", vol.Name)
			volOptions["migratable"] = strconv.FormatBool(false)
			isMigratable = false
		}
		vol.Migratable = isMigratable
	}

	if encrypted, ok := volOptions["encrypted"]; ok {
		isEncrypted, err := strconv.ParseBool(encrypted)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter encrypted")
		}
		vol.Encrypted = isEncrypted
	}

	if numberOfReplicas, ok := volOptions["numberOfReplicas"]; ok {
		nor, err := strconv.Atoi(numberOfReplicas)
		if err != nil || nor < 0 {
			return nil, errors.Wrap(err, "Invalid parameter numberOfReplicas")
		}
		vol.NumberOfReplicas = int64(nor)
	}

	if replicaAutoBalance, ok := volOptions["replicaAutoBalance"]; ok {
		err := types.ValidateReplicaAutoBalance(longhorn.ReplicaAutoBalance(replicaAutoBalance))
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter replicaAutoBalance")
		}
		vol.ReplicaAutoBalance = replicaAutoBalance
	}

	if locality, ok := volOptions["dataLocality"]; ok {
		if err := types.ValidateDataLocality(longhorn.DataLocality(locality)); err != nil {
			return nil, errors.Wrap(err, "Invalid parameter dataLocality")
		}
		vol.DataLocality = locality
	}

	if revisionCounterDisabled, ok := volOptions["disableRevisionCounter"]; ok {
		revCounterDisabled, err := strconv.ParseBool(revisionCounterDisabled)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter disableRevisionCounter")
		}
		vol.RevisionCounterDisabled = revCounterDisabled
	}

	if unmapMarkSnapChainRemoved, ok := volOptions["unmapMarkSnapChainRemoved"]; ok {
		if err := types.ValidateUnmapMarkSnapChainRemoved(longhorn.UnmapMarkSnapChainRemoved(unmapMarkSnapChainRemoved)); err != nil {
			return nil, errors.Wrap(err, "Invalid parameter unmapMarkSnapChainRemoved")
		}
		vol.UnmapMarkSnapChainRemoved = unmapMarkSnapChainRemoved
	}

	if replicaSoftAntiAffinity, ok := volOptions["replicaSoftAntiAffinity"]; ok {
		if err := types.ValidateReplicaSoftAntiAffinity(longhorn.ReplicaSoftAntiAffinity(replicaSoftAntiAffinity)); err != nil {
			return nil, errors.Wrap(err, "Invalid parameter replicaSoftAntiAffinity")
		}
		vol.ReplicaSoftAntiAffinity = replicaSoftAntiAffinity
	}

	if replicaZoneSoftAntiAffinity, ok := volOptions["replicaZoneSoftAntiAffinity"]; ok {
		if err := types.ValidateReplicaZoneSoftAntiAffinity(longhorn.ReplicaZoneSoftAntiAffinity(replicaZoneSoftAntiAffinity)); err != nil {
			return nil, errors.Wrap(err, "Invalid parameter replicaZoneSoftAntiAffinity")
		}
		vol.ReplicaZoneSoftAntiAffinity = replicaZoneSoftAntiAffinity
	}

	if fromBackup, ok := volOptions["fromBackup"]; ok {
		vol.FromBackup = fromBackup
	}

	if dataSource, ok := volOptions["dataSource"]; ok {
		vol.DataSource = dataSource
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

	vol.BackendStoreDriver = string(longhorn.BackendStoreDriverTypeLonghorn)
	if driver, ok := volOptions["backendStoreDriver"]; ok {
		vol.BackendStoreDriver = driver
	}
	return vol, nil
}

func parseJSONRecurringJobs(jsonRecurringJobs string) ([]longhornclient.RecurringJob, error) {
	recurringJobs := []longhornclient.RecurringJob{}
	err := json.Unmarshal([]byte(jsonRecurringJobs), &recurringJobs)
	if err != nil {
		return nil, fmt.Errorf("invalid json format of recurringJobs: %v  %v", jsonRecurringJobs, err)
	}
	for _, recurringJob := range recurringJobs {
		if _, err := cron.ParseStandard(recurringJob.Cron); err != nil {
			return nil, fmt.Errorf("invalid cron format(%v): %v", recurringJob.Cron, err)
		}
	}
	return recurringJobs, nil
}

// ensureMountPoint evaluates whether a path is a valid mountPoint
// in case the targetPath does not exists it will create a path and return false
// in case where the mount point exists but is corrupt, the mount point will be cleaned up and a error is returned
// the underlying implementation utilizes mounter.IsLikelyNotMountPoint so it cannot detect bind mounts
func ensureMountPoint(targetPath string, mounter mount.Interface) (bool, error) {
	logrus.Debugf("trying to ensure mount point %v", targetPath)
	notMnt, err := mount.IsNotMountPoint(mounter, targetPath)
	if os.IsNotExist(err) {
		return false, os.MkdirAll(targetPath, 0750)
	}

	IsCorruptedMnt := mount.IsCorruptedMnt(err)
	if !IsCorruptedMnt {
		logrus.Debugf("mount point %v try reading dir to make sure it's healthy", targetPath)
		if _, err := os.ReadDir(targetPath); err != nil {
			logrus.Debugf("mount point %v was identified as corrupt by ReadDir", targetPath)
			IsCorruptedMnt = true
		}
	}

	if IsCorruptedMnt {
		unmountErr := unmount(targetPath, mounter)
		if unmountErr != nil {
			return false, fmt.Errorf("failed to unmount corrupt mount point %v umount error: %v eval error: %v",
				targetPath, unmountErr, err)
		}

		return false, fmt.Errorf("unmounted existing corrupt mount point %v", targetPath)
	}

	return !notMnt, err
}

func unmount(targetPath string, mounter mount.Interface) error {
	var err error

	forceUnmounter, ok := mounter.(mount.MounterForceUnmounter)
	if ok {
		logrus.Debugf("Trying to force unmount potential mount point %v", targetPath)
		err = forceUnmounter.UnmountWithForce(targetPath, defaultForceUmountTimeout)
	} else {
		logrus.Debugf("Trying to unmount potential mount point %v", targetPath)
		err = mounter.Unmount(targetPath)
	}
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "not mounted") ||
		strings.Contains(err.Error(), "no mount point specified") {
		logrus.Infof("no need for unmount not a mount point %v", targetPath)
		return nil
	}

	return err
}

// cleanupMountPoint ensures all mount layers for the targetPath are unmounted and the mount directory is removed
func cleanupMountPoint(targetPath string, mounter mount.Interface) error {
	// we just try to unmount since the path check would get stuck for nfs mounts
	logrus.Infof("trying to cleanup mount point %v", targetPath)
	if err := unmount(targetPath, mounter); err != nil {
		logrus.Debugf("failed to unmount during cleanup error: %v", err)
		return err
	}

	logrus.Infof("cleaned up mount point %v", targetPath)
	return mount.CleanupMountPoint(targetPath, mounter, true)
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

// makeFile creates an empty file.
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
