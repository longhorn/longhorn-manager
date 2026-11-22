package types

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"

	lhns "github.com/longhorn/go-common-libs/ns"
)

func GetDeviceTypeOf(mountPath string) (string, error) {
	devicePath, err := lhns.FindBlockDeviceForPath(mountPath)
	if err != nil {
		return "", err
	}

	return GetBlockDeviceType(devicePath)
}

func GetBlockDeviceType(devicePath string) (string, error) {
	// Check if device rotational file exist
	deviceID := filepath.Base(devicePath)
	rotationalPath := fmt.Sprintf("/sys/block/%s/queue/rotational", deviceID)
	_, err := os.Stat(rotationalPath)
	if err != nil {
		if os.IsNotExist(err) {
			deviceName := filepath.Base(devicePath)
			linkPath := fmt.Sprintf("/sys/class/block/%s/..", deviceName)
			// Use filepath.EvalSymlinks() to resolve the symbolic link
			// Returned parentPath looks like /sys/devices/pci0000:00/0000:00:1d.0/0000:3d:00.0/nvme/nvme0/nvme0n1
			parentDevicePath, err := filepath.EvalSymlinks(linkPath)
			if err != nil {
				return "", err
			}

			// Try to get the device type of the parent device
			return GetBlockDeviceType(parentDevicePath)

		}
		return "", errors.Wrapf(err, "failed to get %v file information", rotationalPath)
	}

	// Read the contents of the partition rational file.
	contents, err := os.ReadFile(rotationalPath)
	if err != nil {
		return "", err
	}

	// Convert the byte slice to a string and trim any whitespace.
	content := strings.TrimSpace(string(contents))
	switch content {
	case "0":
		if strings.HasPrefix(strings.ToLower(deviceID), "nvme") {
			return "NVMe", nil
		}
		return "SSD", nil
	case "1":
		return "HDD", nil
	default:
		return "", fmt.Errorf("unexpected content in %v: %v", rotationalPath, content)
	}
}
