package server

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-share-manager/pkg/crypto"
	"github.com/longhorn/longhorn-share-manager/pkg/server/nfs"
	"github.com/longhorn/longhorn-share-manager/pkg/types"
	"github.com/longhorn/longhorn-share-manager/pkg/volume"
)

const healthCheckInterval = time.Second * 10
const configPath = "/tmp/vfs.conf"

type ShareManager struct {
	logger logrus.FieldLogger

	volume      volume.Volume
	mountStatus MountStatus

	context  context.Context
	shutdown context.CancelFunc

	nfsServer *nfs.Server
}

type MountStatus struct {
	State types.ProgressState
	Error string
}

// NewShareManager creates a new ShareManager
func NewShareManager(logger logrus.FieldLogger, volume volume.Volume) (*ShareManager, error) {
	m := &ShareManager{
		volume: volume,
		logger: logger.WithField("volume", volume.Name).WithField("encrypted", volume.IsEncrypted()),
		mountStatus: MountStatus{
			State: types.ProgressStatePending,
			Error: "",
		},
	}

	m.context, m.shutdown = context.WithCancel(context.Background())

	nfsServer, err := nfs.NewServer(logger, configPath, types.ExportPath, volume.Name)
	if err != nil {
		return nil, err
	}
	m.nfsServer = nfsServer
	return m, nil
}

// GetVolumeName returns the name of the volume
func (m *ShareManager) GetVolumeName() string {
	return m.volume.Name
}

// SetMountStatus sets the mount status
func (m *ShareManager) SetMountStatus(state types.ProgressState, err string) {
	m.mountStatus.State = state
	m.mountStatus.Error = err
}

// GetMountStatus returns the mount status
func (m *ShareManager) GetMountStatus() (types.ProgressState, string) {
	return m.mountStatus.State, m.mountStatus.Error
}

// Run checks if the volume is valid, mounts it, and exports it by reloading the NFS server
func (m *ShareManager) Run() error {
	var err error

	defer func() {
		if err != nil {
			m.SetMountStatus(types.ProgressStateError, err.Error())
		}
	}()

	m.SetMountStatus(types.ProgressStateStarting, "")

	vol := m.volume
	err = m.nfsServer.CreateExport(vol.Name)
	if err != nil {
		m.logger.WithError(err).Error("Failed to create NFS export")
		return err
	}

	go func() {
		var err error

		m.SetMountStatus(types.ProgressStateInProgress, "")

		defer func() {
			if err != nil {
				m.SetMountStatus(types.ProgressStateError, err.Error())
			} else {
				m.SetMountStatus(types.ProgressStateComplete, "")
			}
		}()

		mountPath := types.GetMountPath(vol.Name)
		devicePath := types.GetVolumeDevicePath(vol.Name, false)

		if !volume.CheckDeviceValid(devicePath) {
			err = fmt.Errorf("volume %v is not valid", vol.Name)
			return
		}

		m.logger.Infof("Setting up volume")
		devicePath, err = setupDevice(m.logger, vol, devicePath)
		if err != nil {
			return
		}

		err = mountVolume(m.logger, vol, devicePath, mountPath)
		if err != nil {
			m.logger.WithError(err).Warn("Failed to mount volume")
			return
		}

		err = resizeVolume(m.logger, devicePath, mountPath)
		if err != nil {
			m.logger.WithError(err).Warn("Failed to resize volume after mount")
			return
		}

		err = volume.SetPermissions(mountPath, 0777)
		if err != nil {
			m.logger.WithError(err).Error("Failed to set permissions for volume")
			return
		}

		// Reload the config to pick up the new export
		err = m.nfsServer.ReloadExports()
		if err != nil {
			m.logger.WithError(err).Error("Failed to reload NFS exports")
			return
		}

		go m.runHealthCheck()
	}()

	return nil
}

// StartNfsServer starts the NFS server and blocks until it exits.
func (m *ShareManager) StartNfsServer() error {
	m.logger.Info("Starting NFS server")

	defer func() {
		mountPath := types.GetMountPath(m.GetVolumeName())

		m.logger.Info("Unmounting volume")
		if err := volume.UnmountVolume(mountPath); err != nil {
			m.logger.WithError(err).Error("Failed to unmount volume")
		}

		m.logger.Info("Tearing down device")
		if err := tearDownDevice(m.logger, m.volume); err != nil {
			m.logger.WithError(err).Error("Failed to tear down device")
		}
	}()

	err := m.nfsServer.Run(m.context)
	if err != nil {
		m.logger.WithError(err).Error("NFS server exited with error")
	}
	return err
}

// StopNfsServer stops the NFS server.
func (m *ShareManager) StopNfsServer() error {
	m.logger.Info("Stopping NFS server")

	cmd := m.nfsServer.GetCommand()
	if cmd == nil {
		return nil
	}

	m.logger.Info("Sending SIGTERM to NFS server")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		m.logger.WithError(err).Error("Failed to send SIGTERM to NFS server, sending SIGKILL instead")
		if err := cmd.Process.Kill(); err != nil {
			m.logger.WithError(err).Error("Failed to send SIGKILL to NFS server")
		}
	}

	_, err := cmd.Process.Wait()
	return err
}

// setupDevice will return a path where the device file can be found
// for encrypted volumes, it will try formatting the volume on first use
// then open it and expose a crypto device at the returned path
func setupDevice(logger logrus.FieldLogger, vol volume.Volume, devicePath string) (string, error) {
	diskFormat, err := volume.GetDiskFormat(devicePath)
	if err != nil {
		return "", errors.Wrap(err, "failed to determine filesystem format")
	}
	logger.Infof("Volume %v device %v contains filesystem of format %v", vol.Name, devicePath, diskFormat)

	if vol.IsEncrypted() || diskFormat == "luks" {
		if vol.Passphrase == "" {
			return "", fmt.Errorf("missing passphrase for encrypted volume %v", vol.Name)
		}

		// initial setup of longhorn device for crypto
		if diskFormat == "" {
			logger.Info("Encrypting new volume before first use")
			if err := crypto.EncryptVolume(devicePath, vol.Passphrase); err != nil {
				return "", errors.Wrapf(err, "failed to encrypt volume %v", vol.Name)
			}
		}

		cryptoDevice := types.GetVolumeDevicePath(vol.Name, true)
		logger.Infof("Volume %s requires crypto device %s", vol.Name, cryptoDevice)
		if err := crypto.OpenVolume(vol.Name, devicePath, vol.Passphrase); err != nil {
			logger.WithError(err).Error("Failed to open encrypted volume")
			return "", err
		}

		// update the device path to point to the new crypto device
		return cryptoDevice, nil
	}

	return devicePath, nil
}

func tearDownDevice(logger logrus.FieldLogger, vol volume.Volume) error {
	// close any matching crypto device for this volume
	cryptoDevice := types.GetVolumeDevicePath(vol.Name, true)
	if isOpen, err := crypto.IsDeviceOpen(cryptoDevice); err != nil {
		return err
	} else if isOpen {
		logger.Infof("Volume %s has active crypto device %s", vol.Name, cryptoDevice)
		if err := crypto.CloseVolume(vol.Name); err != nil {
			return err
		}
		logger.Infof("Volume %s closed active crypto device %s", vol.Name, cryptoDevice)
	}

	return nil
}

func mountVolume(logger logrus.FieldLogger, vol volume.Volume, devicePath, mountPath string) error {
	fsType := vol.FsType
	mountOptions := vol.MountOptions

	// https://github.com/longhorn/longhorn/issues/2991
	// pre v1.2 we ignored the fsType and always formatted as ext4
	// after v1.2 we include the user specified fsType to be able to
	// mount priorly created volumes we need to switch to the existing fsType
	diskFormat, err := volume.GetDiskFormat(devicePath)
	if err != nil {
		logger.WithError(err).Error("Failed to evaluate disk format")
		return err
	}

	// `unknown data, probably partitions` is used when the disk contains a partition table
	if diskFormat != "" && !strings.Contains(diskFormat, "unknown data") && fsType != diskFormat {
		logger.Warnf("Disk is already formatted to %v but user requested fs is %v using existing device fs type for mount", diskFormat, fsType)
		fsType = diskFormat
	}

	return volume.MountVolume(devicePath, mountPath, fsType, mountOptions)
}

func resizeVolume(logger logrus.FieldLogger, devicePath, mountPath string) error {
	if resized, err := volume.ResizeVolume(devicePath, mountPath); err != nil {
		logger.WithError(err).Error("Failed to resize filesystem for volume")
		return err
	} else if resized {
		logger.Info("Resized filesystem for volume after mount")
	}

	return nil
}

func (m *ShareManager) runHealthCheck() {
	m.logger.Info("Starting health check for volume")
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.context.Done():
			m.logger.Info("Stopping health check since NFS server is shut down")
			return
		case <-ticker.C:
			if !m.hasHealthyVolume() {
				m.logger.Error("Volume health check failed, terminating")
				m.Shutdown()
				return
			}
		}
	}
}

func (m *ShareManager) hasHealthyVolume() bool {
	mountPath := types.GetMountPath(m.GetVolumeName())
	err := exec.CommandContext(m.context, "ls", mountPath).Run()
	return err == nil
}

// Shutdown shuts down the NFS server and cancels the share manager context.
func (m *ShareManager) Shutdown() {
	m.logger.Info("Gracefully shutting down NFS server")
	if err := m.StopNfsServer(); err != nil {
		m.logger.WithError(err).Error("Failed to stop NFS server")
	}

	m.logger.Info("Canceling share manager context")
	m.shutdown()
}
