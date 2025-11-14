package sys

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FindBlockDeviceForMount returns the block device associated with a given mount path.
func FindBlockDeviceForMount(mountPath string) (string, error) {
	return findBlockDeviceForMountWithFile(mountPath, "/proc/mounts")
}

// findBlockDeviceForMountWithFile returns the block device associated with a given mount path
// by reading the specified mounts file.
//
// This function allows dependency injection for unit testing.
func findBlockDeviceForMountWithFile(mountPath, mountsFile string) (string, error) {
	f, err := os.Open(mountsFile)
	if err != nil {
		return "", fmt.Errorf("failed to open %s: %v", mountsFile, err)
	}
	defer f.Close()

	mountPath = filepath.Clean(mountPath)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[1] == mountPath {
			return fields[0], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading %s: %v", mountsFile, err)
	}
	return "", fmt.Errorf("mount path %s not found", mountPath)
}

// isNVMeController checks if the given device name is an NVMe controller (nvmeX)
// rather than an NVMe namespace (nvmeXnY).
// Controller pattern: nvme0, nvme1, etc. (nvme followed by digits only)
// Namespace pattern: nvme0n1, nvme1n2, etc. (nvme followed by digits, then 'n', then more digits)
func isNVMeController(devName string) bool {
	if !strings.HasPrefix(devName, "nvme") {
		return false
	}
	// Remove the "nvme" prefix and check if remaining part contains 'n'
	// If it doesn't contain 'n', it's a controller (e.g., nvme0, nvme1)
	// If it contains 'n', it's a namespace (e.g., nvme0n1, nvme1n2)
	return !strings.Contains(strings.TrimPrefix(devName, "nvme"), "n")
}

// ResolveBlockDeviceToPhysicalDevice returns the physical block device
// (e.g., /dev/nvme0 for NVMe, /dev/sda for SATA) corresponding to the given
// block device (e.g., /dev/nvme0n1p2, /dev/nvme0n).
func ResolveBlockDeviceToPhysicalDevice(
	blockDevice string,
) (string, error) {
	return resolveBlockDeviceToPhysicalDeviceWithDeps(
		blockDevice,
		filepath.EvalSymlinks,
		os.Readlink,
		os.Stat,
	)
}

// resolveBlockDeviceToPhysicalDeviceWithDeps resolves a block device to its
// underlying physical device.
//
// For regular SATA/SAS drives: /dev/sda1 -> /dev/sda
// For NVMe drives: /dev/nvme0n1p2 -> /dev/nvme0 (the controller)
//
// This function allows dependency injection for unit testing.
func resolveBlockDeviceToPhysicalDeviceWithDeps(
	blockDevice string,
	evalSymlinks func(string) (string, error),
	readlink func(string) (string, error),
	statFn func(string) (os.FileInfo, error),
) (string, error) {
	// Resolve symlinks to the actual block device
	blockDevice, err := evalSymlinks(blockDevice)
	if err != nil {
		return "", fmt.Errorf("failed to resolve symlink for %s: %v", blockDevice, err)
	}

	// Extract the device name
	devName := filepath.Base(blockDevice)
	sysPath := filepath.Join("/sys/class/block", devName)

	// Walk up until no "partition" file exists
	for {
		partitionFile := filepath.Join(sysPath, "partition")
		if _, err := statFn(partitionFile); os.IsNotExist(err) {
			break
		}

		// If "device" symlink exists, walk to parent
		deviceLink := filepath.Join(sysPath, "device")
		parentLink, err := readlink(deviceLink)
		if err != nil || parentLink == "" {
			break
		}

		devName = filepath.Base(parentLink)
		sysPath = filepath.Join("/sys/class/block", devName)
	}

	// Special handling for NVMe: ensure we return the controller (nvmeX)
	// Resolve the sysfs path to get the real device hierarchy
	realSysPath, err := evalSymlinks(sysPath)
	if err == nil {
		// Walk up the real sysfs path to find the NVMe controller
		// For example: /sys/devices/.../nvme/nvme0/nvme0n1 -> walk up to nvme0
		pathParts := strings.Split(realSysPath, "/")
		for i := len(pathParts) - 1; i >= 0; i-- {
			part := pathParts[i]
			if isNVMeController(part) {
				devName = part
				break
			}
		}
	}

	return "/dev/" + devName, nil
}

// ResolveMountPathToPhysicalDevice returns the physical block device (e.g., /dev/nvme0)
// for the given mount path (e.g., /var/lib/longhorn)
func ResolveMountPathToPhysicalDevice(mountPath string) (string, error) {
	return resolveMountPathToPhysicalDeviceWithDeps(
		mountPath,
		"/proc/mounts",
		filepath.EvalSymlinks,
		os.Readlink,
		os.Stat,
	)
}

// resolveMountPathToPhysicalDeviceWithDeps returns the top-level physical device
// (e.g., /dev/nvme0 for NVMe, /dev/sda for SATA) corresponding to the given
// mount path (e.g., /var/lib/longhorn).
//
// This function allows dependency injection for unit testing.
func resolveMountPathToPhysicalDeviceWithDeps(
	mountPath string,
	mountsFile string,
	evalSymlinks func(string) (string, error),
	readlink func(string) (string, error),
	statFn func(string) (os.FileInfo, error),
) (string, error) {
	blockDevice, err := findBlockDeviceForMountWithFile(mountPath, mountsFile)
	if err != nil {
		return "", err
	}

	return resolveBlockDeviceToPhysicalDeviceWithDeps(blockDevice, evalSymlinks, readlink, statFn)
}
