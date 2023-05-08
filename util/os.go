package util

import (
	"fmt"

	iscsiutil "github.com/longhorn/go-iscsi-helper/util"
)

// GetHostKernelRelease retrieves the kernel release version of the host.
func GetHostKernelRelease() (string, error) {
	initiatorNSPath := iscsiutil.GetHostNamespacePath(HostProcPath)
	mountPath := fmt.Sprintf("--mount=%s/mnt", initiatorNSPath)
	output, err := Execute([]string{}, "nsenter", mountPath, "uname", "-r")
	if err != nil {
		return "", err
	}
	return RemoveNewlines(output), nil
}
