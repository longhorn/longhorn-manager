package util

import (
	"fmt"
	"path/filepath"
	"regexp"
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
