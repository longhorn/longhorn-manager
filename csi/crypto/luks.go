package crypto

import (
	iscsi_util "github.com/longhorn/go-iscsi-helper/util"
)

const HostProcPath = "/proc" // we use hostPID for the csi plugin

func luksOpen(volume, devicePath, passphrase string) (stdout string, err error) {
	return cryptSetupWithPassphrase(passphrase,
		"luksOpen", devicePath, volume, "-d", "/dev/stdin")
}

func luksClose(volume string) (stdout string, err error) {
	return cryptSetup("luksClose", volume)
}

func luksFormat(devicePath, passphrase string) (stdout string, err error) {
	return cryptSetupWithPassphrase(passphrase,
		"-q", "luksFormat", "--type", "luks2", "--hash", "sha256",
		devicePath, "-d", "/dev/stdin")
}

func luksStatus(volume string) (stdout string, err error) {
	return cryptSetup("status", volume)
}

func cryptSetup(args ...string) (stdout string, err error) {
	return cryptSetupWithPassphrase("", args...)
}

// cryptSetupWithPassphrase runs cryptsetup via nsenter inside of the host namespaces
// cryptsetup returns 0 on success and a non-zero value on error.
// 1 wrong parameters, 2 no permission (bad passphrase),
// 3 out of memory, 4 wrong device specified,
// 5 device already exists or device is busy.
func cryptSetupWithPassphrase(passphrase string, args ...string) (stdout string, err error) {
	initiatorNSPath := iscsi_util.GetHostNamespacePath(HostProcPath)
	ne, err := iscsi_util.NewNamespaceExecutor(initiatorNSPath)
	if err != nil {
		return "", err
	}

	if len(passphrase) > 0 {
		return ne.ExecuteWithStdin("cryptsetup", args, passphrase)

	}
	return ne.Execute("cryptsetup", args)
}
