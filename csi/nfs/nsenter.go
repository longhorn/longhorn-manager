package nfs

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/utils/exec"
	"k8s.io/utils/nsenter"
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
	nsenterPath       = "nsenter"
	defaultRootFsPath = "/rootfs"

	mntNsPath = "/proc/1/ns/mnt"
	netNsPath = "/proc/1/ns/net"
	utsNsPath = "/proc/1/ns/uts"
	ipcNsPath = "/proc/1/ns/ipc"
	pidNsPath = "/proc/1/ns/pid"

	userNsPath   = "/proc/1/ns/user"
	cgroupNsPath = "/proc/1/ns/cgroup"
)

func NewNsEnter(rootFsPath string) (*NsEnter, error) {
	executor := exec.New()
	ne, err := nsenter.NewNsenter(rootFsPath, executor)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to create nsenter executor, err: %v", err))
	}

	return &NsEnter{
		NSEnter:    ne,
		rootFsPath: rootFsPath,
		executor:   exec.New(),
	}, nil
}

type NsEnter struct {
	*nsenter.NSEnter
	rootFsPath string
	executor   exec.Interface
}

// Exec executes nsenter commands in hostProcMountNsPath mount namespace
func (e *NsEnter) Exec(cmd string, args []string) exec.Cmd {
	nsEnterArgs := append(e.makeNsEnterArgs(), "--")
	cmdArgs := append([]string{e.AbsHostPath(cmd)}, args...)
	fullArgs := append(nsEnterArgs, cmdArgs...)

	logrus.Debugf("Running nsenter command: %v %v", nsenterPath, fullArgs)
	return e.executor.Command(nsenterPath, fullArgs...)
}

// Command returns a command wrapped with nsenter
func (e *NsEnter) Command(cmd string, args ...string) exec.Cmd {
	return e.Exec(cmd, args)
}

func (e *NsEnter) makeNsEnterArgs() []string {
	nsEnterArgs := []string{
		fmt.Sprintf("--mount=%s", filepath.Join(e.rootFsPath, mntNsPath)),
		fmt.Sprintf("--net=%s", filepath.Join(e.rootFsPath, netNsPath)),
		fmt.Sprintf("--uts=%s", filepath.Join(e.rootFsPath, utsNsPath)),
		// fmt.Sprintf("--ipc=%s", filepath.Join(e.rootFsPath, ipcNsPath)),
		// fmt.Sprintf("--pid=%s", filepath.Join(e.rootFsPath, pidNsPath)),

		// fmt.Sprintf("--user=%s", filepath.Join(e.rootFsPath, userNsPath)),
		// fmt.Sprintf("--cgroup=%s", filepath.Join(e.rootFsPath, cgroupNsPath)),
	}

	return nsEnterArgs
}

// CommandContext returns a CommandContext wrapped with nsenter
func (e *NsEnter) CommandContext(ctx context.Context, cmd string, args ...string) exec.Cmd {
	nsEnterArgs := append(e.makeNsEnterArgs(), "--")
	cmdArgs := append([]string{e.AbsHostPath(cmd)}, args...)
	fullArgs := append(nsEnterArgs, cmdArgs...)

	logrus.Debugf("Running nsenter command: %v %v", nsenterPath, fullArgs)
	return e.executor.CommandContext(ctx, nsenterPath, fullArgs...)
}

// LookPath returns a LookPath wrapped with nsenter
func (e *NsEnter) LookPath(file string) (string, error) {
	return "", fmt.Errorf("not implemented, error looking up : %s", file)
}
