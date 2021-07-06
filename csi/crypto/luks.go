package crypto

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

func luksOpen(volume, devicePath, passphrase string) (stdout, stderr []byte, err error) {
	return cryptSetupWithPassphrase(&passphrase,
		"luksOpen", devicePath, volume, "-d", "/dev/stdin")
}

func luksClose(volume string) (stdout, stderr []byte, err error) {
	return cryptSetup("luksClose", volume)
}

func luksFormat(devicePath, passphrase string) (stdout, stderr []byte, err error) {
	return cryptSetupWithPassphrase(&passphrase,
		"-q", "luksFormat", "--type", "luks2", "--hash", "sha256",
		devicePath, "-d", "/dev/stdin")
}

func luksStatus(volume string) (stdout, stderr []byte, err error) {
	return cryptSetup("status", volume)
}

func cryptSetup(args ...string) (stdout, stderr []byte, err error) {
	return cryptSetupWithPassphrase(nil, args...)
}

// cryptSetupWithPassphrase runs cryptsetup via nsenter inside of the host namespaces
// cryptsetup returns 0 on success and a non-zero value on error.
// 1 wrong parameters, 2 no permission (bad passphrase),
// 3 out of memory, 4 wrong device specified,
// 5 device already exists or device is busy.
func cryptSetupWithPassphrase(passphrase *string, args ...string) (stdout, stderr []byte, err error) {
	nsenterArgs := []string{"-t 1", "--all", "cryptsetup"}
	nsenterArgs = append(nsenterArgs, args...)
	cmd := exec.Command("nsenter", nsenterArgs...)

	var stdoutBuf bytes.Buffer
	var stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	if passphrase != nil {
		cmd.Stdin = strings.NewReader(*passphrase)
	}

	if err := cmd.Run(); err != nil {
		return stdoutBuf.Bytes(), stderrBuf.Bytes(), fmt.Errorf("failed to run cryptsetup args: %v error: %v", args, err)
	}

	return stdoutBuf.Bytes(), nil, nil
}
