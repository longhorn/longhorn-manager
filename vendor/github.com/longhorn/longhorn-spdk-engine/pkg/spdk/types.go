package spdk

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

const (
	DiskTypeFilesystem = "filesystem"
	DiskTypeBlock      = "block"

	ReplicaRebuildingLvolSuffix  = "rebuilding"
	ReplicaExpiredLvolSuffix     = "expired"
	ReplicaCloningLvolSuffix     = "cloning"
	RebuildingSnapshotNamePrefix = "rebuild"

	SyncTimeout = 60 * time.Minute

	nvmeNguidLength = 32

	maxNumRetries = 15
	retryInterval = 1 * time.Second

	MaxShallowCopyWaitTime   = 72 * time.Hour
	ShallowCopyCheckInterval = 3 * time.Second

	MaxSnapshotCloneWaitTime         = 72 * time.Hour
	SnapshotCloneStatusCheckInterval = 3 * time.Second
)

const (
	// Timeouts for RAID base bdev (replica)
	// The ctrlr_loss_timeout_sec setting applies to the base bdev's NVMe controller
	// and defines the timeout duration (30 seconds) for SPDK to attempt reconnecting to the controller
	// after losing connection.
	//
	// When an instance manager containing a replica is deleted, SPDK starts to reconnect to the base bdev's controller.
	// If the connection cannot be reestablished within the ctrlr_loss_timeout_sec period, the base bdev is removed from the RAID bdev.
	//
	// Because the ctrl-loss-tmo for the NVMe-oF initiator connecting to the RAID target is also set to 30 seconds,
	// replicaCtrlrLossTimeoutSec and replicaFastIOFailTimeoutSec are set to 15 seconds and 10 seconds, respectively.
	//
	// If an I/O operation to a replica (base bdev) is unresponsive within 10 seconds, an I/O error is returned,
	// and the base bdev is deleted after 5 seconds.
	replicaCtrlrLossTimeoutSec  = 15
	replicaReconnectDelaySec    = 2
	replicaFastIOFailTimeoutSec = 10
	replicaTransportAckTimeout  = 10
	replicaKeepAliveTimeoutMs   = 10000
	replicaMultipath            = "disable"
)

type Lvol struct {
	sync.RWMutex

	Name       string
	UUID       string
	Alias      string
	SpecSize   uint64
	ActualSize uint64
	// Parent is the snapshot lvol name. <snapshot lvol name> consists of `<replica name>-snap-<snapshot name>`
	Parent string
	// Children is map[<snapshot lvol name>] rather than map[<snapshot name>]. <snapshot lvol name> consists of `<replica name>-snap-<snapshot name>`
	Children          map[string]*Lvol
	CreationTime      string
	UserCreated       bool
	SnapshotTimestamp string
	SnapshotChecksum  string
}

func ServiceBackingImageLvolToProtoBackingImageLvol(lvol *Lvol) *spdkrpc.Lvol {
	lvol.RLock()
	defer lvol.RUnlock()

	res := &spdkrpc.Lvol{
		Uuid:       lvol.UUID,
		Name:       lvol.Name,
		SpecSize:   lvol.SpecSize,
		ActualSize: lvol.ActualSize,
		// BackingImage has no parent
		Parent:       "",
		Children:     map[string]bool{},
		CreationTime: lvol.CreationTime,
		UserCreated:  false,
		// Use creation time instead
		SnapshotTimestamp: "",
	}

	for childLvolName := range lvol.Children {
		// For backing image, the children is map[<snapshot lvol name>]
		res.Children[childLvolName] = true
	}

	return res
}

func ServiceLvolToProtoLvol(replicaName string, lvol *Lvol) *spdkrpc.Lvol {
	if lvol == nil {
		return nil
	}
	res := &spdkrpc.Lvol{
		Uuid:              lvol.UUID,
		SpecSize:          lvol.SpecSize,
		ActualSize:        lvol.ActualSize,
		Parent:            GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, lvol.Parent),
		Children:          map[string]bool{},
		CreationTime:      lvol.CreationTime,
		UserCreated:       lvol.UserCreated,
		SnapshotTimestamp: lvol.SnapshotTimestamp,
		SnapshotChecksum:  lvol.SnapshotChecksum,
	}

	if lvol.Name == replicaName {
		res.Name = types.VolumeHead
	} else {
		res.Name = GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, lvol.Name)
	}

	for childLvolName := range lvol.Children {
		// spdkrpc.Lvol.Children is map[<snapshot name>] rather than map[<snapshot lvol name>]
		if childLvolName == replicaName {
			res.Children[types.VolumeHead] = true
		} else {
			res.Children[GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, childLvolName)] = true
		}
	}

	return res
}

func BdevLvolInfoToServiceLvol(bdev *spdktypes.BdevInfo) *Lvol {
	svcLvol := &Lvol{
		Name:              spdktypes.GetLvolNameFromAlias(bdev.Aliases[0]),
		Alias:             bdev.Aliases[0],
		UUID:              bdev.UUID,
		SpecSize:          bdev.NumBlocks * uint64(bdev.BlockSize),
		ActualSize:        bdev.DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize,
		Parent:            bdev.DriverSpecific.Lvol.BaseSnapshot,
		Children:          map[string]*Lvol{},
		CreationTime:      bdev.CreationTime,
		UserCreated:       bdev.DriverSpecific.Lvol.Xattrs[spdkclient.UserCreated] == strconv.FormatBool(true),
		SnapshotTimestamp: bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotTimestamp],
		SnapshotChecksum:  bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotChecksum],
	}

	// Need to further update this separately
	for _, childLvolName := range bdev.DriverSpecific.Lvol.Clones {
		svcLvol.Children[childLvolName] = nil
	}

	return svcLvol
}

func IsProbablyReplicaName(name string) bool {
	matched, _ := regexp.MatchString("^.+-r-[a-zA-Z0-9]{8}$", name)
	return matched
}

func GetBackingImageSnapLvolName(backingImageName string, lvsUUID string) string {
	return fmt.Sprintf("bi-%s-disk-%s", backingImageName, lvsUUID)
}

func GetBackingImageTempHeadLvolName(backingImageName string, lvsUUID string) string {
	return fmt.Sprintf("bi-%s-disk-%s-temp-head", backingImageName, lvsUUID)
}

func GetReplicaSnapshotLvolNamePrefix(replicaName string) string {
	return fmt.Sprintf("%s-snap-", replicaName)
}

func GetReplicaSnapshotLvolName(replicaName, snapshotName string) string {
	return fmt.Sprintf("%s%s", GetReplicaSnapshotLvolNamePrefix(replicaName), snapshotName)
}

func GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, snapLvolName string) string {
	return strings.TrimPrefix(snapLvolName, GetReplicaSnapshotLvolNamePrefix(replicaName))
}

func IsReplicaLvol(replicaName, lvolName string) bool {
	return strings.HasPrefix(lvolName, fmt.Sprintf("%s-", replicaName)) || lvolName == replicaName
}

func IsReplicaSnapshotLvol(replicaName, lvolName string) bool {
	return strings.HasPrefix(lvolName, GetReplicaSnapshotLvolNamePrefix(replicaName))
}

func GenerateRebuildingSnapshotName() string {
	return fmt.Sprintf("%s-%s", RebuildingSnapshotNamePrefix, util.UUID()[:8])
}

func GenerateReplicaExpiredLvolName(replicaName string) string {
	return fmt.Sprintf("%s-%s-%s", replicaName, ReplicaExpiredLvolSuffix, util.UUID()[:8])
}

func GetReplicaRebuildingLvolName(replicaName string) string {
	return fmt.Sprintf("%s-%s", replicaName, ReplicaRebuildingLvolSuffix)
}

func IsRebuildingLvol(lvolName string) bool {
	return strings.HasSuffix(lvolName, ReplicaRebuildingLvolSuffix)
}

func IsReplicaExpiredLvol(replicaName, lvolName string) bool {
	return strings.HasPrefix(lvolName, fmt.Sprintf("%s-%s", replicaName, ReplicaExpiredLvolSuffix))
}

func GetReplicaNameFromRebuildingLvolName(lvolName string) string {
	return strings.TrimSuffix(lvolName, fmt.Sprintf("-%s", ReplicaRebuildingLvolSuffix))
}

func GetReplicaCloningLvolName(replicaName string) string {
	return fmt.Sprintf("%s-%s", replicaName, ReplicaCloningLvolSuffix)
}

func IsCloningLvol(lvolName string) bool {
	return strings.HasSuffix(lvolName, ReplicaCloningLvolSuffix)
}

func GetReplicaNameFromCloningLvolName(lvolName string) string {
	return strings.TrimSuffix(lvolName, fmt.Sprintf("-%s", ReplicaCloningLvolSuffix))
}

func GetTmpSnapNameForCloningLvol(replicaName string) string {
	return fmt.Sprintf("%s-%s-tmp", replicaName, ReplicaCloningLvolSuffix)
}

func GetNvmfEndpoint(nqn, ip string, port int32) string {
	return fmt.Sprintf("nvmf://%s:%d/%s", ip, port, nqn)
}

func GetServiceClient(address string) (*client.SPDKClient, error) {
	ip, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	// TODO: Can we use the fixed port
	addr := net.JoinHostPort(ip, strconv.Itoa(types.SPDKServicePort))

	// TODO: Can we share the clients in the whole server?
	return client.NewSPDKClient(addr)
}

func GetBdevMap(cli *spdkclient.Client) (map[string]*spdktypes.BdevInfo, error) {
	bdevList, err := cli.BdevGetBdevs("", 0)
	if err != nil {
		return nil, err
	}

	bdevMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		bdevType := spdktypes.GetBdevType(bdev)

		switch bdevType {
		case spdktypes.BdevTypeLvol:
			if len(bdev.Aliases) != 1 {
				continue
			}
			bdevMap[bdev.Aliases[0]] = bdev
		case spdktypes.BdevTypeRaid:
			fallthrough
		default:
			bdevMap[bdev.Name] = bdev
		}
	}

	return bdevMap, nil
}

func GetBdevLvolMap(cli *spdkclient.Client) (map[string]*spdktypes.BdevInfo, error) {
	return GetBdevLvolMapWithFilter(cli, func(*spdktypes.BdevInfo) bool { return true })
}

func GetBdevLvolMapWithFilter(cli *spdkclient.Client, filter func(*spdktypes.BdevInfo) bool) (map[string]*spdktypes.BdevInfo, error) {
	bdevList, err := cli.BdevLvolGetWithFilter("", 0, filter)
	if err != nil {
		return nil, err
	}

	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		bdevType := spdktypes.GetBdevType(bdev)
		if bdevType != spdktypes.BdevTypeLvol {
			continue
		}
		if len(bdev.Aliases) != 1 {
			continue
		}
		lvolName := spdktypes.GetLvolNameFromAlias(bdev.Aliases[0])
		bdevLvolMap[lvolName] = bdev
	}

	return bdevLvolMap, nil
}

func GetNvmfSubsystemMap(cli *spdkclient.Client) (map[string]*spdktypes.NvmfSubsystem, error) {
	subsystemList, err := cli.NvmfGetSubsystems("", "")
	if err != nil {
		return nil, err
	}

	subsystemMap := map[string]*spdktypes.NvmfSubsystem{}
	for idx := range subsystemList {
		subsystem := &subsystemList[idx]
		subsystemMap[subsystem.Nqn] = subsystem
	}

	return subsystemMap, nil
}

type BackupCreateInfo struct {
	BackupName     string
	IsIncremental  bool
	ReplicaAddress string
}
