package nfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"k8s.io/utils/exec"
)

const (
	/*
		 * overview of the different namespace types
		 * https://man7.org/linux/man-pages/man1/nsenter.1.html
		/proc/pid/ns/mnt    the mount namespace
		/proc/pid/ns/uts    the UTS namespace
		/proc/pid/ns/ipc    the IPC namespace
		/proc/pid/ns/net    the network namespace
		/proc/pid/ns/pid    the PID namespace
		/proc/pid/ns/user   the user namespace
		/proc/pid/ns/cgroup the cgroup namespace
		/proc/pid/ns/time   the time namespace
		/proc/pid/root      the root directory
		/proc/pid/cwd       the working directory respectively
	*/
	nsenterPath = "nsenter"
	mntNsPath   = "/proc/1/ns/mnt"
	netNsPath   = "/proc/1/ns/net"
	utsNsPath   = "/proc/1/ns/uts"
	ipcNsPath   = "/proc/1/ns/ipc"
	pidNsPath   = "/proc/1/ns/pid"

	userNsPath   = "/proc/1/ns/user"
	cgroupNsPath = "/proc/1/ns/cgroup"

	// TODO: remove this once we replaced direct syscalls with namespaced stat command invocation
	// we can then remove the host mount from the csi plugin deployment
	defaultHostRootFsPath = "/rootfs"
)

func NewNsEnter() (*NsEnter, error) {
	ne := &NsEnter{
		executor:       exec.New(),
		hostRootFsPath: defaultHostRootFsPath,
	}

	if err := ne.initPaths(); err != nil {
		return nil, err
	}
	return ne, nil
}

type NsEnter struct {
	executor       exec.Interface
	hostRootFsPath string
	paths          map[string]string
}

func (ne *NsEnter) initPaths() error {
	ne.paths = map[string]string{}
	binaries := []string{
		"mount",
		"findmnt",
		"umount",
		"systemd-run",
		"stat",
		"touch",
		"mkdir",
		"sh",
		"chmod",
		"realpath",
	}

	// ensure required binaries are available in the host mount namespace
	for _, binary := range binaries {
		for _, path := range []string{"/", "/bin", "/usr/sbin", "/usr/bin"} {
			binPath := filepath.Join(path, binary)
			hostPath := filepath.Join(ne.hostRootFsPath, binPath)

			// TODO: replace this with namespaced stat invocation
			// first need to find stat, or assume it's available on path
			// we use lstat since it's sufficient if there is symbolic link available
			if _, err := os.Lstat(hostPath); err != nil {
				continue
			}
			ne.paths[binary] = binPath
			break
		}
		// systemd-run is optional, bailout if we don't find any of the other binaries
		if ne.paths[binary] == "" && binary != "systemd-run" {
			return fmt.Errorf("unable to find required binary %v on host", binary)
		}
	}
	return nil
}

// ContainerPath TODO: remove this once the direct syscalls of stat are replaced with invocations of the namespaced stat command
func (ne *NsEnter) ContainerPath(pathname string) string {
	return filepath.Join(ne.hostRootFsPath, pathname)
}

// SupportsSystemd checks whether command systemd-run exists
func (ne *NsEnter) SupportsSystemd() (string, bool) {
	systemdRunPath, ok := ne.paths["systemd-run"]
	return systemdRunPath, ok && systemdRunPath != ""
}

// AbsHostPath returns the absolute runnable path for a specified command
func (ne *NsEnter) AbsHostPath(command string) string {
	path, ok := ne.paths[command]
	if !ok {
		return command
	}
	return path
}

// EvalSymlinks returns the path name on the host after evaluating symlinks on the
// host.
// mustExist makes EvalSymlinks to return error when the path does not
// exist. When it's false, it evaluates symlinks of the existing part and
// blindly adds the non-existing part:
// pathname: /mnt/volume/non/existing/directory
//     /mnt/volume exists
//                non/existing/directory does not exist
// -> It resolves symlinks in /mnt/volume to say /mnt/foo and returns
//    /mnt/foo/non/existing/directory.
//
// BEWARE! EvalSymlinks is not able to detect symlink looks with mustExist=false!
// If /tmp/link is symlink to /tmp/link, EvalSymlinks(/tmp/link/foo) returns /tmp/link/foo.
func (ne *NsEnter) EvalSymlinks(pathname string, mustExist bool) (string, error) {
	var args []string
	if mustExist {
		// "realpath -e: all components of the path must exist"
		args = []string{"-e", pathname}
	} else {
		// "realpath -m: no path components need exist or be a directory"
		args = []string{"-m", pathname}
	}
	outBytes, err := ne.Exec("realpath", args).CombinedOutput()
	if err != nil {
		logrus.WithError(err).Errorf("failed to resolve symbolic links on %s", pathname)
		return "", err
	}
	return strings.TrimSpace(string(outBytes)), nil
}

// Exec executes nsenter commands in host mount namespace
func (ne *NsEnter) Exec(cmd string, args []string) exec.Cmd {
	nsEnterArgs := append(ne.makeNsEnterArgs(), "--")
	cmdArgs := append([]string{ne.AbsHostPath(cmd)}, args...)
	fullArgs := append(nsEnterArgs, cmdArgs...)

	logrus.Debugf("Running nsenter command: %v %v", nsenterPath, fullArgs)
	return ne.executor.Command(nsenterPath, fullArgs...)
}

// Command returns a command wrapped with nsenter
func (ne *NsEnter) Command(cmd string, args ...string) exec.Cmd {
	return ne.Exec(cmd, args)
}

func (ne *NsEnter) makeNsEnterArgs() []string {
	nsEnterArgs := []string{
		fmt.Sprintf("--mount=%s", mntNsPath),
		fmt.Sprintf("--net=%s", netNsPath),
		fmt.Sprintf("--uts=%s", utsNsPath),
	}

	return nsEnterArgs
}

// CommandContext returns a CommandContext wrapped with nsenter
func (ne *NsEnter) CommandContext(ctx context.Context, cmd string, args ...string) exec.Cmd {
	nsEnterArgs := append(ne.makeNsEnterArgs(), "--")
	cmdArgs := append([]string{ne.AbsHostPath(cmd)}, args...)
	fullArgs := append(nsEnterArgs, cmdArgs...)

	logrus.Debugf("Running nsenter command: %v %v", nsenterPath, fullArgs)
	return ne.executor.CommandContext(ctx, nsenterPath, fullArgs...)
}

// LookPath returns a LookPath wrapped with nsenter
func (ne *NsEnter) LookPath(file string) (string, error) {
	return "", fmt.Errorf("not implemented, error looking up : %s", file)
}
