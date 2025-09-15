package client

import (
	"path/filepath"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

// AddDevice adds a device with the given device path, name, and cluster size.
func (c *Client) AddDevice(devicePath, name string, clusterSize uint32) (bdevAioName, lvsName, lvsUUID string, err error) {
	// Use the file name as aio name and lvs name if name is not specified.
	if name == "" {
		name = filepath.Base(devicePath)
	}

	if _, err := c.BdevAioCreate(devicePath, name, 4096); err != nil {
		return "", "", "", err
	}

	lvsList, err := c.BdevLvolGetLvstore("", "")
	if err != nil {
		return "", "", "", err
	}
	lvsCreated := false
	for _, lvsInfo := range lvsList {
		if lvsInfo.BaseBdev == name {
			lvsCreated = true
			lvsUUID = lvsInfo.UUID
			break
		}
	}
	if !lvsCreated {
		if lvsUUID, err = c.BdevLvolCreateLvstore(name, name, clusterSize); err != nil {
			return "", "", "", err
		}
	}

	return name, name, lvsUUID, nil
}

// DeleteDevice deletes the device with the given bdevAioName and lvsName.
func (c *Client) DeleteDevice(bdevAioName, lvsName string) (err error) {
	if _, err := c.BdevLvolDeleteLvstore(lvsName, ""); err != nil {
		return err
	}

	if _, err := c.BdevAioDelete(bdevAioName); err != nil {
		return err
	}

	return nil
}

// StartExposeBdev exposes the bdev with the given nqn, bdevName, nguid, ip, and port.
func (c *Client) StartExposeBdev(nqn, bdevName, nguid, ip, port string) error {
	nvmfTransportList, err := c.NvmfGetTransports("", "")
	if err != nil {
		return err
	}
	if nvmfTransportList != nil && len(nvmfTransportList) == 0 {
		if _, err := c.NvmfCreateTransport(spdktypes.NvmeTransportTypeTCP); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
			return err
		}
	}

	if _, err := c.NvmfCreateSubsystem(nqn); err != nil {
		return err
	}

	if _, err := c.NvmfSubsystemAddNs(nqn, bdevName, nguid); err != nil {
		return err
	}

	if _, err := c.NvmfSubsystemAddListener(nqn, ip, port, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4); err != nil {
		return err
	}

	return nil
}

// StopExposeBdev stops exposing the bdev with the given nqn.
func (c *Client) StopExposeBdev(nqn string) error {
	var subsystem *spdktypes.NvmfSubsystem

	subsystemList, err := c.NvmfGetSubsystems("", "")
	if err != nil {
		return err
	}
	for _, s := range subsystemList {
		if s.Nqn != nqn {
			continue
		}
		subsystem = &s
		break
	}
	if subsystem == nil {
		return nil
	}

	listenerList, err := c.NvmfSubsystemGetListeners(nqn, "")
	if err != nil {
		return err
	}
	for _, l := range listenerList {
		if _, err := c.NvmfSubsystemRemoveListener(nqn, l.Address.Traddr, l.Address.Trsvcid, l.Address.Trtype, l.Address.Adrfam); err != nil {
			return err
		}
	}

	for _, ns := range subsystem.Namespaces {
		if _, err := c.NvmfSubsystemRemoveNs(nqn, ns.Nsid); err != nil {
			return err
		}
	}

	if _, err := c.NvmfDeleteSubsystem(nqn, ""); err != nil {
		return err
	}

	return nil
}
