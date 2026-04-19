package client

import (
	"net"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	spdkutil "github.com/longhorn/go-spdk-helper/pkg/util"
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

// DetectAddressFamily returns the NVMe address family for the given IP.
// Exported so downstream repos (longhorn-spdk-engine) can reuse it.
func DetectAddressFamily(ip string) spdktypes.NvmeAddressFamily {
	if ip == "" {
		return spdktypes.NvmeAddressFamilyIPv4
	}

	normalized := spdkutil.NormalizeNvmeAddr(ip)
	parsedIP := net.ParseIP(normalized)
	if parsedIP == nil {
		logrus.Warnf("Failed to parse IP %q, defaulting to IPv4", ip)
		return spdktypes.NvmeAddressFamilyIPv4
	}

	if parsedIP.To4() == nil {
		return spdktypes.NvmeAddressFamilyIPv6
	}
	return spdktypes.NvmeAddressFamilyIPv4
}

// StartExposeBdev exposes the bdev with the given nqn, bdevName, nguid, ip, and port.
func (c *Client) StartExposeBdev(nqn, bdevName, nguid, ip, port string) error {
	ip = spdkutil.NormalizeNvmeAddr(ip)

	logrus.Infof("Exposing bdev with nqn %v, bdevName %v, nguid %v, ip %v, port %v", nqn, bdevName, nguid, ip, port)

	nvmfTransportList, err := c.NvmfGetTransports("", "")
	if err != nil {
		return err
	}
	if nvmfTransportList != nil && len(nvmfTransportList) == 0 {
		logrus.Infof("Creating transport with type %v", spdktypes.NvmeTransportTypeTCP)
		if _, err := c.NvmfCreateTransport(spdktypes.NvmeTransportTypeTCP); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
			return err
		}
	}

	logrus.Infof("Creating subsystem with nqn %v", nqn)
	if _, err := c.NvmfCreateSubsystem(nqn); err != nil {
		return err
	}

	logrus.Infof("Adding NVMe namespace with bdev name %v and nguid %v to subsystem with nqn %v", bdevName, nguid, nqn)
	if _, err := c.NvmfSubsystemAddNs(nqn, bdevName, nguid); err != nil {
		return err
	}

	adrfam := DetectAddressFamily(ip)

	logrus.Infof("Adding listener with transport address %v, transport service id %v, transport type %v, address family %v to subsystem with nqn %v", ip, port, spdktypes.NvmeTransportTypeTCP, adrfam, nqn)
	if _, err := c.NvmfSubsystemAddListener(nqn, ip, port, spdktypes.NvmeTransportTypeTCP, adrfam); err != nil {
		return err
	}

	return nil
}

// StartExposeBdevWithANAState exposes the bdev with the given nqn, bdevName,
// nguid, nsUUID, ip, port, initial ANA state, and optional CNTLID range.
// nsUUID sets a stable namespace UUID so the Linux kernel can aggregate
// controllers into the same NVMe multipath group. minCntlid/maxCntlid assign
// a unique controller-ID range per engine to avoid "Duplicate cntlid" errors
// when multiple targets share one subsystem NQN. Pass 0 for defaults.
func (c *Client) StartExposeBdevWithANAState(nqn, bdevName, nguid, nsUUID, ip, port string, anaState spdktypes.NvmfSubsystemListenerAnaState, minCntlid, maxCntlid uint16) error {
	ip = spdkutil.NormalizeNvmeAddr(ip)

	logrus.Infof("Exposing bdev with nqn %v, bdevName %v, nguid %v, nsUUID %v, ip %v, port %v, anaState %v, minCntlid %v, maxCntlid %v",
		nqn, bdevName, nguid, nsUUID, ip, port, anaState, minCntlid, maxCntlid)

	nvmfTransportList, err := c.NvmfGetTransports("", "")
	if err != nil {
		return err
	}
	if nvmfTransportList != nil && len(nvmfTransportList) == 0 {
		logrus.Infof("Creating transport with type %v", spdktypes.NvmeTransportTypeTCP)
		if _, err := c.NvmfCreateTransport(spdktypes.NvmeTransportTypeTCP); err != nil && !jsonrpc.IsJSONRPCRespErrorTransportTypeAlreadyExists(err) {
			return err
		}
	}

	logrus.Infof("Creating subsystem with nqn %v, minCntlid %v, maxCntlid %v", nqn, minCntlid, maxCntlid)
	if _, err := c.NvmfCreateSubsystemWithCntlid(nqn, minCntlid, maxCntlid); err != nil {
		return err
	}

	logrus.Infof("Adding NVMe namespace with bdev name %v, nguid %v, uuid %v to subsystem with nqn %v", bdevName, nguid, nsUUID, nqn)
	if _, err := c.NvmfSubsystemAddNsWithUUID(nqn, bdevName, nguid, nsUUID); err != nil {
		return err
	}

	adrfam := DetectAddressFamily(ip)

	logrus.Infof("Adding listener with transport address %v, transport service id %v, transport type %v, address family %v to subsystem with nqn %v", ip, port, spdktypes.NvmeTransportTypeTCP, adrfam, nqn)
	if _, err := c.NvmfSubsystemAddListener(nqn, ip, port, spdktypes.NvmeTransportTypeTCP, adrfam); err != nil {
		return err
	}

	logrus.Infof("Setting listener ANA state to %v for subsystem with nqn %v", anaState, nqn)
	if _, err := c.NvmfSubsystemListenerSetANAState(nqn, ip, port, spdktypes.NvmeTransportTypeTCP,
		adrfam, anaState, spdktypes.DefaultNvmfANAGroupID); err != nil {
		return err
	}

	return nil
}

// StopExposeBdev stops exposing the bdev with the given nqn.
func (c *Client) StopExposeBdev(nqn string) error {
	logrus.Infof("Stopping exposing bdev with nqn %v", nqn)

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
		logrus.Infof("Removing listener with transport address %v, transport service id %v, transport type %v, address family %v", l.Address.Traddr, l.Address.Trsvcid, l.Address.Trtype, l.Address.Adrfam)
		if _, err := c.NvmfSubsystemRemoveListener(nqn, l.Address.Traddr, l.Address.Trsvcid, l.Address.Trtype, l.Address.Adrfam); err != nil {
			return err
		}
	}

	for _, ns := range subsystem.Namespaces {
		logrus.Infof("Removing namespace with NSID %v", ns.Nsid)
		if _, err := c.NvmfSubsystemRemoveNs(nqn, ns.Nsid); err != nil {
			return err
		}
	}

	logrus.Infof("Deleting subsystem with nqn %v", nqn)
	if _, err := c.NvmfDeleteSubsystem(nqn, ""); err != nil {
		return err
	}

	return nil
}
