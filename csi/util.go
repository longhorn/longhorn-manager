package csi

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"

	"golang.org/x/sys/unix"

	"k8s.io/kubernetes/pkg/util/mount"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	// defaultStaleReplicaTimeout set to 48 hours (2880 minutes)
	defaultStaleReplicaTimeout = 2880
)

// NewForcedParamsOsExec creates a osExecutor that allows for adding additional params to later occurring Run calls
func NewForcedParamsOsExec(cmdParamMapping map[string]string) mount.Exec {
	return &forcedParamsOsExec{
		osExec:          mount.NewOSExec(),
		cmdParamMapping: cmdParamMapping,
	}
}

type forcedParamsOsExec struct {
	osExec          mount.Exec
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

func (e *forcedParamsOsExec) Run(cmd string, args ...string) ([]byte, error) {
	var params []string
	if param := e.cmdParamMapping[cmd]; param != "" {
		// we prepend the user params, since options are conventionally before the final args
		// command [-option(s)] [argument(s)]
		params = append(params, param)
	}
	params = append(params, args...)
	return e.osExec.Run(cmd, params...)
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
			vol.AccessMode = string(types.AccessModeReadWriteMany)
		} else {
			vol.AccessMode = string(types.AccessModeReadWriteOnce)
		}
	}

	if migratable, ok := volOptions["migratable"]; ok {
		isMigratable, err := strconv.ParseBool(migratable)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter migratable")
		}

		if isMigratable && vol.AccessMode != string(types.AccessModeReadWriteMany) {
			logrus.Infof("Cannot mark volume %v as migratable, "+
				"since access mode is not RWX proceeding with RWO non migratable volume creation.", vol.Name)
			volOptions["migratable"] = strconv.FormatBool(false)
			isMigratable = false
		}
		vol.Migratable = isMigratable
	}

	if numberOfReplicas, ok := volOptions["numberOfReplicas"]; ok {
		nor, err := strconv.Atoi(numberOfReplicas)
		if err != nil || nor < 0 {
			return nil, errors.Wrap(err, "Invalid parameter numberOfReplicas")
		}
		vol.NumberOfReplicas = int64(nor)
	}

	if locality, ok := volOptions["dataLocality"]; ok {
		if err := types.ValidateDataLocality(types.DataLocality(locality)); err != nil {
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

	if fromBackup, ok := volOptions["fromBackup"]; ok {
		vol.FromBackup = fromBackup
	}

	if backingImage, ok := volOptions["backingImage"]; ok {
		vol.BackingImage = backingImage
	}

	if jsonRecurringJobs, ok := volOptions["recurringJobs"]; ok {
		recurringJobs, err := parseJSONRecurringJobs(jsonRecurringJobs)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid parameter recurringJobs")
		}
		vol.RecurringJobs = recurringJobs
	}

	if diskSelector, ok := volOptions["diskSelector"]; ok {
		vol.DiskSelector = strings.Split(diskSelector, ",")
	}

	if nodeSelector, ok := volOptions["nodeSelector"]; ok {
		vol.NodeSelector = strings.Split(nodeSelector, ",")
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
	notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
	if os.IsNotExist(err) {
		return false, os.MkdirAll(targetPath, 0750)
	}

	if mount.IsCorruptedMnt(err) {
		cleanupErr := cleanupMountPoint(targetPath, mounter)
		if cleanupErr != nil {
			return true, fmt.Errorf("failed to cleanup corrupt mount point %v cleanup error %v", targetPath, err)
		}

		return true, fmt.Errorf("cleaned up existing corrupt mount point %v", targetPath)
	}

	return !notMnt, err
}

// cleanupMountPoint ensures all mount layers for the targetPath are unmounted and the mount directory is removed
// the underlying implementation utilizes mounter.IsLikelyNotMountPoint so it cannot detect multiple layers of bind mounts
func cleanupMountPoint(targetPath string, mounter mount.Interface) error {
	for {
		if err := mounter.Unmount(targetPath); err != nil {
			if strings.Contains(err.Error(), "not mounted") ||
				strings.Contains(err.Error(), "no mount point specified") {
				break
			}
			return err
		}
		notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			return err
		}

		if notMnt {
			break
		}
	}

	return mount.CleanupMountPoint(targetPath, mounter, false)
}

func isLikelyNotMountPointAttach(targetpath string) (bool, error) {
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetpath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(targetpath, 0750)
			if err == nil {
				notMnt = true
			}
		}
	}
	return notMnt, err
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

// makeDir creates a new directory.
// If pathname already exists as a directory, no error is returned.
// If pathname already exists as a file, an error is returned.
func makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

// makeFile creates an empty file.
// If pathname already exists, whether a file or directory, no error is returned.
func makeFile(pathname string) error {
	f, err := os.OpenFile(pathname, os.O_CREATE, os.FileMode(0644))
	if f != nil {
		f.Close()
	}
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

//requiresSharedAccess checks if the volume is requested to be multi node capable
// a volume that is already in shared access mode, must be used via shared access
// even if single node access is requested.
func requiresSharedAccess(vol *longhornclient.Volume, cap *csi.VolumeCapability) bool {
	isSharedVolume := false
	if vol != nil {
		isSharedVolume = vol.AccessMode == string(types.AccessModeReadWriteMany) || vol.Migratable
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
