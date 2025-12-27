package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	commonns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/go-spdk-helper/pkg/types"
)

const (
	lsblkBinary    = "lsblk"
	blkidBinary    = "blkid"
	BlockdevBinary = "blockdev"
)

type BlockDevice struct {
	Name   string `json:"name"`
	Major  int    `json:"maj"`
	Minor  int    `json:"min"`
	MajMin string `json:"maj:min"`
}

type BlockDevices struct {
	Devices []BlockDevice `json:"blockdevices"`
}

type LonghornBlockDevice struct {
	Source BlockDevice
	Export BlockDevice
}

// RemoveDevice removes the given device
func RemoveDevice(devPath string) error {
	if _, err := os.Stat(devPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
	}

	// Still try to remove the device node
	return remove(devPath)
}

// GetKnownDevices returns the path of the device with the given major and minor numbers
func GetKnownDevices(executor *commonns.Executor) (map[string]*LonghornBlockDevice, error) {
	knownDevices := make(map[string]*LonghornBlockDevice)

	// Example command output
	//   $ lsblk -l -n -o NAME,MAJ:MIN
	//   sda           8:0
	//   sdb           8:16
	//   sdc           8:32
	//   nvme0n1     259:0
	//   nvme0n1p1   259:1
	//   nvme0n1p128 259:2
	//   nvme1n1     259:3

	opts := []string{
		"-l", "-n", "-o", "NAME,MAJ:MIN",
	}

	output, err := executor.Execute(nil, lsblkBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return knownDevices, err
	}

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		f := strings.Fields(line)
		if len(f) == 2 {
			dev := &LonghornBlockDevice{
				Source: BlockDevice{
					Name: f[0],
				},
			}
			if _, err := fmt.Sscanf(f[1], "%d:%d", &dev.Source.Major, &dev.Source.Minor); err != nil {
				return nil, fmt.Errorf("invalid major:minor %s for NVMe device %s", dev.Source.Name, f[1])
			}
			knownDevices[dev.Source.Name] = dev
		}
	}

	return knownDevices, nil
}

// DetectDevice detects the device with the given path
func DetectDevice(path string, executor *commonns.Executor) (*BlockDevice, error) {
	// Example command output
	// $ lsblk -l -n <Device Path> -o NAME,MAJ:MIN
	// nvme1n1     259:3

	opts := []string{
		path, "-n", "-o", "NAME,MAJ:MIN", "--nodeps",
	}

	output, err := executor.Execute(nil, lsblkBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}

	var dev *BlockDevice
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		f := strings.Fields(line)
		if len(f) == 2 {
			dev = &BlockDevice{
				Name: f[0],
			}
			_, err = fmt.Sscanf(f[1], "%d:%d", &dev.Major, &dev.Minor)
			if err != nil {
				return nil, fmt.Errorf("invalid major:minor %s for device %s with path %s", dev.Name, f[1], path)
			}
		}
		break // nolint:staticcheck
	}
	if dev == nil {
		return nil, fmt.Errorf("failed to get device with path %s", path)
	}

	return dev, nil
}

func parseMajorMinorFromJSON(jsonStr string) (int, int, error) {
	var data BlockDevices
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to parse JSON")
	}

	if len(data.Devices) != 1 {
		return 0, 0, fmt.Errorf("number of devices is not 1")
	}

	majMinParts := splitMajMin(data.Devices[0].MajMin)

	if len(majMinParts) != 2 {
		return 0, 0, fmt.Errorf("invalid maj:min format: %s", data.Devices[0].MajMin)
	}

	major, err := parseNumber(majMinParts[0])
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to parse major number")
	}

	minor, err := parseNumber(majMinParts[1])
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to parse minor number")
	}

	return major, minor, nil
}

func splitMajMin(majMin string) []string {
	return splitIgnoreEmpty(majMin, ":")
}

func splitIgnoreEmpty(str string, sep string) []string {
	parts := []string{}
	for _, part := range strings.Split(str, sep) {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func parseNumber(str string) (int, error) {
	return strconv.Atoi(strings.TrimSpace(str))
}

// GetDeviceSectorSize returns the sector size of the given device
func GetDeviceSectorSize(devPath string, executor *commonns.Executor) (int64, error) {
	opts := []string{
		"--getsz", devPath,
	}

	output, err := executor.Execute(nil, BlockdevBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(strings.TrimSpace(output), 10, 64)
}

// GetDeviceNumbers returns the major and minor numbers of the given device
func GetDeviceNumbers(devPath string, executor *commonns.Executor) (int, int, error) {
	opts := []string{
		"-l", "-J", "-n", "-o", "MAJ:MIN", devPath,
	}
	output, err := executor.Execute(nil, lsblkBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return -1, -1, err
	}

	return parseMajorMinorFromJSON(output)
}

// DuplicateDevice creates a device node for the given device
func DuplicateDevice(dev *LonghornBlockDevice, dest string) error {
	if dev == nil {
		return fmt.Errorf("found nil device for device duplication")
	}
	if dest == "" {
		return fmt.Errorf("found empty destination for device duplication")
	}
	dir := filepath.Dir(dest)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			logrus.WithError(err).Fatalf("device %v: Failed to create directory for %v", dev.Source.Name, dest)
		}
	}
	if err := mknod(dest, dev.Export.Major, dev.Export.Minor); err != nil {
		return errors.Wrapf(err, "cannot create device node %s for device %s", dest, dev.Source.Name)
	}
	if err := os.Chmod(dest, 0660); err != nil {
		return errors.Wrapf(err, "cannot change permission of the device %s", dest)
	}
	// We use the group 6 by default because this is common group for disks
	// See more at https://github.com/longhorn/longhorn/issues/8088#issuecomment-1982300242
	if err := os.Chown(dest, 0, 6); err != nil {
		return errors.Wrapf(err, "cannot change ownership of the device %s", dest)
	}
	return nil
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0660
	fileMode |= unix.S_IFBLK
	dev := int(unix.Mkdev(uint32(major), uint32(minor)))

	logrus.Infof("Creating device %s %d:%d", device, major, minor)
	return unix.Mknod(device, uint32(fileMode), dev)
}

func removeAsync(path string, done chan<- error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.WithError(err).Errorf("Failed to remove %v", path)
		done <- err
	}
	done <- nil
}

func remove(path string) error {
	done := make(chan error)
	go removeAsync(path, done)
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout trying to delete %s", path)
	}
}

// IsBlockDevice returns true if the given path is a block device
func IsBlockDevice(path string) (bool, error) {
	var st unix.Stat_t
	err := unix.Stat(path, &st)
	if err != nil {
		return false, err
	}

	return (st.Mode & unix.S_IFMT) == unix.S_IFBLK, nil
}

// IsBlockDeviceInUse returns true if the given block device has a filesystem
// or partition table detected by blkid, indicating that the device is already
// in use or contains existing data.
func IsBlockDeviceInUse(devPath string, executor *commonns.Executor) bool {
	opts := []string{devPath}
	_, err := executor.Execute(nil, blkidBinary, opts, types.ExecuteTimeout)
	return err == nil
}
