package util

import (
	"fmt"
	"net"
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

// IsSameNvmeAddr reports whether two NVMe transport addresses refer to the same host.
// An IPv6 address has several valid textual forms, so the kernel may report a
// controller address that differs from the requested one byte-wise while naming the
// same host. Compare the parsed IPs to avoid missing such a match.
func IsSameNvmeAddr(a, b string) bool {
	a, b = NormalizeNvmeAddr(a), NormalizeNvmeAddr(b)
	if a == b {
		return true
	}

	ipA, ipB := net.ParseIP(a), net.ParseIP(b)
	return ipA != nil && ipB != nil && ipA.Equal(ipB)
}
