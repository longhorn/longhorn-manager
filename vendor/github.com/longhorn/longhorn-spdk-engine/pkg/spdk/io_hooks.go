package spdk

import (
	"os"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"

	commonnet "github.com/longhorn/go-common-libs/net"
	commonns "github.com/longhorn/go-common-libs/ns"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	lhclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"
)

type nvmeInitiator interface {
	StartNvmeTCPInitiator(transportAddress, transportServiceID string, dmDeviceAndEndpointCleanupRequired bool, stop bool) (bool, error)
	Stop(spdkClient *spdkclient.Client, dmDeviceAndEndpointCleanupRequired, deferDmDeviceCleanup, returnErrorForBusyDevice bool) (bool, error)
	Endpoint() string
}

type realNVMeInitiator struct {
	*initiator.Initiator
}

func (i *realNVMeInitiator) Endpoint() string {
	return i.Initiator.Endpoint
}

type backingImageServiceClient interface {
	BackingImageExpose(name, lvsUUID string) (string, error)
	BackingImageUnexpose(name, lvsUUID string) error
	Close() error
}

// NOTE: These package-level hook variables are replaced in tests.
// They are NOT safe for parallel test execution.
var (
	openFile = os.OpenFile

	newNVMeTCPInitiator = func(name string, nvmeTCPInfo *initiator.NVMeTCPInfo) (nvmeInitiator, error) {
		i, err := initiator.NewInitiator(name, initiator.HostProc, nvmeTCPInfo, nil)
		if err != nil {
			return nil, err
		}
		return &realNVMeInitiator{Initiator: i}, nil
	}

	backupNewFragmap = func(b *Backup) (*Fragmap, error) {
		return b.newFragmap()
	}
	backupExposeSnapshotLvolBdev = exposeSnapshotLvolBdev
	backupStopExposeBdev         = func(cli *spdkclient.Client, nqn string) error { return cli.StopExposeBdev(nqn) }

	restoreExposeSnapshotLvolBdev = exposeSnapshotLvolBdev
	restoreStopExposeBdev         = func(cli *spdkclient.Client, nqn string) error { return cli.StopExposeBdev(nqn) }

	backingImageGetIPForPod = func() (string, error) { return commonnet.GetIPForPodByNetworkAndFamily(commonnet.IPFamilyUnspecified) }
	backingImageNewExecutor = func(hostProc string) (*commonns.Executor, error) {
		return helperutil.NewExecutor(hostProc)
	}
	backingImageGetLvsNameByUUID = GetLvsNameByUUID
	backingImageBdevLvolCreate   = func(cli *spdkclient.Client, lvstoreName, lvstoreUUID, lvolName string, sizeInMib uint64, clearMethod spdktypes.BdevLvolClearMethod, thinProvision bool) (string, error) {
		return cli.BdevLvolCreate(lvstoreName, lvstoreUUID, lvolName, sizeInMib, clearMethod, thinProvision)
	}
	backingImageBdevLvolGet = func(cli *spdkclient.Client, name string, timeout uint64) ([]spdktypes.BdevInfo, error) {
		return cli.BdevLvolGet(name, timeout)
	}
	backingImageExposeSnapshotLvolBdev       = exposeSnapshotLvolBdev
	backingImageStopExposeBdev               = func(cli *spdkclient.Client, nqn string) error { return cli.StopExposeBdev(nqn) }
	backingImageDiscoverAndConnectNVMeTarget = discoverAndConnectNVMeTarget

	engineBdevRaidCreate = func(cli *spdkclient.Client, name string, baseBdevs []string, uuid string, opts spdktypes.BdevRaidCreateOptions) (bool, error) {
		return cli.BdevRaidCreateWithOptions(name, spdktypes.BdevRaidLevel1, 0, baseBdevs, uuid, opts)
	}
	expansionReplicaExpand = func(cli *lhclient.SPDKClient, name string, size uint64) error {
		return cli.ReplicaExpand(name, size)
	}
	expansionBdevRaidGet = func(cli *spdkclient.Client, name string) ([]spdktypes.BdevInfo, error) {
		return cli.BdevRaidGet(name, 0)
	}
	expansionBdevRaidRemoveBaseBdev = func(cli *spdkclient.Client, name string) (bool, error) {
		return cli.BdevRaidRemoveBaseBdev(name)
	}
	expansionBdevGetBdevs = func(cli *spdkclient.Client, name string) ([]spdktypes.BdevInfo, error) {
		return cli.BdevGetBdevs(name, 0)
	}
	expansionBdevRaidResize = func(cli *spdkclient.Client, name string, expectedSizeInMib uint64) (spdktypes.BdevRaidResizeResponse, error) {
		return cli.BdevRaidResize(name, expectedSizeInMib)
	}
)

var _ backingImageServiceClient = (*lhclient.SPDKClient)(nil)

func getDeviceEndpoint(i nvmeInitiator, fh *os.File) string {
	if i != nil {
		return i.Endpoint()
	}
	if fh != nil {
		return fh.Name()
	}
	return ""
}
