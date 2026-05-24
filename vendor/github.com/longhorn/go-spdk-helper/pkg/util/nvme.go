package util

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	DevPath        = "/dev"
	LonghornDevDir = "/longhorn"

	DefaultNVMeNamespaceID = 1
)

func GetNvmeDevicePath(name string) string {
	return filepath.Join(DevPath, name)
}

func GetLonghornDevicePath(name string) string {
	return filepath.Join(DevPath, LonghornDevDir, name)
}

func GetNvmeNamespaceNameFromControllerName(controllerName string, nsID int) string {
	return fmt.Sprintf("%sn%d", controllerName, nsID)
}

func GetNvmeControllerNameFromNamespaceName(nsName string) string {
	reg := regexp.MustCompile(`([^"]*)n\d+$`)
	return reg.ReplaceAllString(nsName, "${1}")
}

// NormalizeNvmeAddr strips surrounding brackets from an IPv6 address if present.
// Both nvme-cli (-a flag) and SPDK expect bare IPv6 (e.g., "fd00::1" not "[fd00::1]").
func NormalizeNvmeAddr(ip string) string {
	ip = strings.TrimSpace(ip)
	if strings.HasPrefix(ip, "[") && strings.HasSuffix(ip, "]") {
		ip = ip[1 : len(ip)-1]
	}
	return ip
}
