package csi

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/robfig/cron"

	"k8s.io/kubernetes/pkg/util/mount"

	longhornclient "github.com/longhorn/longhorn-manager/client"
)

const (
	// defaultStaleReplicaTimeout set to 48 hours (2880 minutes)
	defaultStaleReplicaTimeout = 2880
)

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

	if numberOfReplicas, ok := volOptions["numberOfReplicas"]; ok {
		nor, err := strconv.Atoi(numberOfReplicas)
		if err != nil || nor < 0 {
			return nil, errors.Wrap(err, "Invalid parameter numberOfReplicas")
		}
		vol.NumberOfReplicas = int64(nor)
	}

	if fromBackup, ok := volOptions["fromBackup"]; ok {
		vol.FromBackup = fromBackup
	}

	if baseImage, ok := volOptions["baseImage"]; ok {
		vol.BaseImage = baseImage
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

func isLikelyNotMountPointDetach(targetpath string) (bool, error) {
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetpath)
	if err != nil {
		if os.IsNotExist(err) {
			return notMnt, fmt.Errorf("targetpath not found")
		}
	}
	return notMnt, err
}

// Should be similar to the detect function in `util` package
// For csi plugins, util.DetectFileSystem is not available since we cannot use NSExecutor in the workloads
func detectFileSystem(devicePath string) (string, error) {
	mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOsExec()}
	output, err := mounter.Run("blkid", devicePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the file system info from device %v, maybe there is no Linux file system on the volume", devicePath)
	}
	items := strings.Split(string(output), " ")
	if len(items) < 3 {
		return "", fmt.Errorf("failed to detect the file system from device %v, invalid output of command blkid", devicePath)
	}
	return strings.Trim(strings.TrimPrefix(strings.TrimSpace(items[2]), "TYPE="), "\""), nil
}
