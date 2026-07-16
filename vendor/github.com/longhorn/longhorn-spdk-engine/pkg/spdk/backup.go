package spdk

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/0xPolygon/polygon-edge/consensus/polybft/bitmap"
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/go-spdk-helper/pkg/initiator"

	btypes "github.com/longhorn/backupstore/types"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

type Fragmap struct {
	Map         bitmap.Bitmap
	ClusterSize uint64
	NumClusters uint64
}

type Backup struct {
	sync.Mutex

	spdkClient    *spdkclient.Client
	portAllocator *commonbitmap.Bitmap

	Name           string
	VolumeName     string
	SnapshotName   string
	replica        *Replica
	replicaAddress string
	fragmap        *Fragmap
	IP             string
	Port           int32
	IsIncremental  bool

	BackupURL string
	State     btypes.ProgressState
	Progress  int
	Error     string

	subsystemNQN   string
	controllerName string
	initiator      nvmeInitiator
	devFh          *os.File
	executor       *commonns.Executor

	// onTerminal is invoked (in a new goroutine) exactly once when the
	// backup first reaches a terminal state (Complete or Error) AND
	// CloseSnapshot has finished releasing all heavy resources.
	// The server uses this hook to trigger pruning of retained backups.
	onTerminal func()
	// terminalSeen guards onTerminal so it fires at most once.
	terminalSeen bool
	// terminalAt records when the backup entered a terminal state,
	// used by the pruning logic to evict the oldest entries first.
	terminalAt time.Time

	log logrus.FieldLogger
}

var _ backupstore.DeltaBlockBackupOperations = (*Backup)(nil)

// NewBackup creates a new backup instance
func NewBackup(spdkClient *spdkclient.Client, backupName, volumeName, snapshotName string, replica *Replica, superiorPortAllocator *commonbitmap.Bitmap, onTerminal func()) (*Backup, error) {
	log := logrus.WithFields(logrus.Fields{
		"backupName":   backupName,
		"volumeName":   volumeName,
		"snapshotName": snapshotName,
	})

	log.Info("Initializing backup")

	replicaAddress := ""
	if replica != nil {
		replicaAddress = replica.GetAddress()
	}

	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return nil, err
	}

	port, _, err := superiorPortAllocator.AllocateRange(1)
	if err != nil {
		return nil, err
	}

	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		if releaseErr := superiorPortAllocator.ReleaseRange(port, port); releaseErr != nil {
			log.WithError(releaseErr).Warnf("Failed to release port %v after executor creation failure", port)
		}
		return nil, errors.Wrapf(err, "failed to create executor")
	}

	return &Backup{
		spdkClient:     spdkClient,
		portAllocator:  superiorPortAllocator,
		Name:           backupName,
		VolumeName:     volumeName,
		SnapshotName:   snapshotName,
		replica:        replica,
		replicaAddress: replicaAddress,
		IP:             podIP,
		Port:           port,
		State:          btypes.ProgressStateInProgress,
		log:            log,
		executor:       executor,
		onTerminal:     onTerminal,
	}, nil
}

// BackupCreate creates the backup
func (b *Backup) BackupCreate(config *backupstore.DeltaBackupConfig) error {
	b.log.Info("Creating backup")

	isIncremental, err := backupstore.CreateDeltaBlockBackup(b.Name, config)
	if err != nil {
		return err
	}

	b.IsIncremental = isIncremental
	return nil
}

// HasSnapshot checks if the snapshot exists
func (b *Backup) HasSnapshot(snapshotName, volumeName string) bool {
	b.log.Info("Checking if snapshot exists")

	b.Lock()
	defer b.Unlock()

	if b.VolumeName != volumeName {
		b.log.Warnf("Invalid state volume [%s] are open, not [%s]", b.VolumeName, volumeName)
		return false
	}

	return b.findIndex(GetReplicaSnapshotLvolName(b.replica.Name, snapshotName)) >= 0
}

// OpenSnapshot opens the snapshot lvol for backup
func (b *Backup) OpenSnapshot(snapshotName, volumeName string) (err error) {
	b.Lock()
	defer b.Unlock()

	b.log.Info("Preparing snapshot lvol bdev for backup")
	frgmap, err := backupNewFragmap(b)
	if err != nil {
		return err
	}
	b.fragmap = frgmap

	lvolName := GetReplicaSnapshotLvolName(b.replica.Name, snapshotName)
	exposed := false
	defer func() {
		if err == nil {
			return
		}

		if b.devFh != nil {
			if errClose := b.devFh.Close(); errClose != nil {
				b.log.WithError(errClose).Warnf("Failed to close NVMe device %v during backup open cleanup", b.devFh.Name())
			} else {
				b.devFh = nil
			}
		}

		if b.initiator != nil {
			if _, stopErr := b.initiator.Stop(nil, true, true, true); stopErr != nil {
				b.log.WithError(stopErr).Warnf("Failed to stop NVMe initiator for snapshot lvol bdev %v during backup open cleanup", lvolName)
			} else {
				b.initiator = nil
			}
		}

		if exposed {
			if stopErr := backupStopExposeBdev(b.spdkClient, helpertypes.GetNQN(lvolName)); stopErr != nil {
				b.log.WithError(stopErr).Warnf("Failed to unexpose snapshot lvol bdev %v during backup open cleanup", lvolName)
			} else {
				b.subsystemNQN = ""
				b.controllerName = ""
			}
		}

		b.fragmap = nil
	}()

	b.replica.Lock()
	defer b.replica.Unlock()

	b.log.Infof("Exposing snapshot lvol bdev %v", lvolName)
	subsystemNQN, controllerName, err := backupExposeSnapshotLvolBdev(b.spdkClient, b.replica.LvsName, lvolName, b.IP, b.Port, b.executor)
	if err != nil {
		b.log.WithError(err).Errorf("Failed to expose snapshot lvol bdev %v", lvolName)
		return errors.Wrapf(err, "failed to expose snapshot lvol bdev %v", lvolName)
	}
	exposed = true
	b.subsystemNQN = subsystemNQN
	b.controllerName = controllerName

	b.log.Infof("Creating NVMe initiator for snapshot lvol bdev %v", lvolName)
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: helpertypes.GetNQN(lvolName),
	}
	i, err := newNVMeTCPInitiator(lvolName, nvmeTCPInfo)
	if err != nil {
		return errors.Wrapf(err, "failed to create NVMe initiator for snapshot lvol bdev %v", lvolName)
	}
	if _, err := i.StartNvmeTCPInitiator(b.IP, strconv.Itoa(int(b.Port)), false, true); err != nil {
		if _, stopErr := i.Stop(nil, true, true, true); stopErr != nil {
			b.log.WithError(stopErr).Warnf("Failed to stop partially-started NVMe initiator for snapshot lvol bdev %v", lvolName)
		}
		return errors.Wrapf(err, "failed to start NVMe initiator for snapshot lvol bdev %v", lvolName)
	}
	b.initiator = i

	b.log.Infof("Opening NVMe device %v", b.initiator.Endpoint())
	devFh, err := openFile(b.initiator.Endpoint(), os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to open NVMe device %v for snapshot lvol bdev %v", b.initiator.Endpoint(), lvolName)
	}
	b.devFh = devFh

	return nil
}

// CompareSnapshot compares the data between two snapshots and returns the mappings
func (b *Backup) CompareSnapshot(snapshotName, compareSnapshotName, volumeName string, blockSize int64) (*btypes.Mappings, error) {
	b.log.Infof("Comparing snapshots from %v to %v", snapshotName, compareSnapshotName)

	lvolName := GetReplicaSnapshotLvolName(b.replica.Name, snapshotName)

	compareLvolName := ""
	if compareSnapshotName != "" {
		compareLvolName = GetReplicaSnapshotLvolName(b.replica.Name, compareSnapshotName)
	}

	b.replica.Lock()
	defer b.replica.Unlock()

	from, to, err := b.findSnapshotRange(lvolName, compareLvolName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find snapshot lvol range %v (%v) and %v (%v)",
			lvolName, from, compareLvolName, to)
	}

	// Overlay the fragments of snapshots and store the result in the b.fragmap.Map
	b.log.Infof("Constructing fragment map for snapshot lvols from %v (%v) to %v (%v)", lvolName, from, compareLvolName, to)
	if err := b.constructFragmap(from, to); err != nil {
		return nil, errors.Wrapf(err, "failed to construct fragment map for snapshot lvols from %v (%v) to %v (%v)",
			lvolName, from, compareLvolName, to)
	}

	return b.constructMappings(blockSize), nil
}

// ReadSnapshot reads the data from the block device exposed by NVMe/TCP TCP.
// It checks replica health before reading so that the backup fails naturally
// through backupstore's error path when the replica is no longer running,
// matching v1 behavior where killing the replica process stops the backup.
func (b *Backup) ReadSnapshot(snapshotName, volumeName string, offset int64, data []byte) error {
	b.replica.RLock()
	replicaState := b.replica.State
	replicaName := b.replica.Name
	b.replica.RUnlock()

	if replicaState != types.InstanceStateRunning {
		return fmt.Errorf("replica %s is in %s state", replicaName, replicaState)
	}

	b.Lock()
	defer b.Unlock()

	_, err := b.devFh.ReadAt(data, offset)

	return err
}

func (b *Backup) CloseSnapshot(snapshotName, volumeName string) error {
	b.Lock()
	defer b.Unlock()

	var errs []error
	endpoint := getDeviceEndpoint(b.initiator, b.devFh)

	var lvolName string
	if b.replica != nil {
		lvolName = GetReplicaSnapshotLvolName(b.replica.Name, snapshotName)
	}

	if b.devFh != nil {
		if endpoint != "" {
			b.log.Infof("Closing NVMe device %v", endpoint)
		} else {
			b.log.Info("Closing NVMe device")
		}
		if err := b.devFh.Close(); err != nil {
			if endpoint != "" {
				errs = append(errs, errors.Wrapf(err, "failed to close NVMe device %v", endpoint))
			} else {
				errs = append(errs, errors.Wrap(err, "failed to close NVMe device"))
			}
		} else {
			b.devFh = nil
		}
	}

	if b.initiator != nil {
		b.log.Info("Stopping NVMe initiator")
		if _, err := b.initiator.Stop(nil, true, true, true); err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to stop NVMe initiator"))
		} else {
			b.initiator = nil
		}
	}

	if lvolName != "" {
		b.log.Info("Unexposing snapshot lvol bdev")
		if err := backupStopExposeBdev(b.spdkClient, helpertypes.GetNQN(lvolName)); err != nil {
			errs = append(errs, errors.Wrapf(err, "failed to unexpose snapshot lvol bdev %v", lvolName))
		} else {
			b.subsystemNQN = ""
			b.controllerName = ""
		}
	}

	errs = append(errs, b.releaseHeavyResourcesLocked()...)
	if len(errs) == 0 && !b.hasActiveSnapshotResourcesLocked() {
		b.markTerminalHandledLocked()
	}

	if len(errs) > 0 {
		return errors.Errorf("CloseSnapshot encountered %d error(s): %v", len(errs), errs)
	}
	return nil
}

// markTerminalHandledLocked fires the onTerminal callback exactly once
// after all heavy resources have been released by releaseHeavyResourcesLocked.
// It relies on CloseSnapshot always being called (via defer) by the
// backupstore library after the backup goroutine exits.
func (b *Backup) markTerminalHandledLocked() {
	if !isBackupTerminalState(b.State) || b.terminalSeen {
		return
	}

	b.terminalSeen = true
	if b.terminalAt.IsZero() {
		b.terminalAt = time.Now()
	}
	if b.onTerminal != nil {
		go b.onTerminal()
	}
}

func (b *Backup) releaseHeavyResourcesLocked() (errs []error) {
	// Only release the port when all resources that depend on it have been
	// cleaned up. If the NVMe-oF target is still exposed (subsystemNQN != "")
	// or the kernel-side initiator/device is still connected, releasing the
	// port risks reuse while the old target is still active.
	if b.portAllocator != nil && b.Port != 0 {
		if b.subsystemNQN != "" || b.initiator != nil || b.devFh != nil {
			b.log.Warnf("Skipping port %v release: resources still active (subsystemNQN=%q, initiator=%v, devFh=%v)",
				b.Port, b.subsystemNQN, b.initiator != nil, b.devFh != nil)
		} else {
			if err := b.portAllocator.ReleaseRange(b.Port, b.Port); err != nil {
				b.log.WithError(err).Warnf("Failed to release port %v", b.Port)
				errs = append(errs, errors.Wrapf(err, "failed to release backup port %v", b.Port))
			} else {
				b.portAllocator = nil
			}
		}
	}
	b.fragmap = nil
	b.executor = nil
	if !b.hasActiveSnapshotResourcesLocked() {
		b.replica = nil
	}
	return errs
}

func (b *Backup) hasActiveSnapshotResourcesLocked() bool {
	return (b.portAllocator != nil && b.Port != 0) || b.subsystemNQN != "" || b.initiator != nil || b.devFh != nil
}

// UpdateBackupStatus updates the backup status. The state is first-respected, but if
// - The errString is not empty, the state will be set to error.
// - The progress is 100, the state will be set to complete.
func (b *Backup) UpdateBackupStatus(snapshotName, volumeName string, state string, progress int, url string, errString string) error {
	b.Lock()
	defer b.Unlock()

	if isBackupTerminalState(b.State) {
		b.log.Warnf("Backup %s for volume %s is already terminal in state %s, skipping status update", b.Name, b.VolumeName, b.State)
		return nil
	}

	b.State = btypes.ProgressState(state)
	b.Progress = progress
	b.BackupURL = url
	b.Error = errString

	if b.Error != "" {
		b.State = btypes.ProgressStateError
	} else if b.Progress == 100 {
		b.State = btypes.ProgressStateComplete
	}

	if isBackupTerminalState(b.State) && b.terminalAt.IsZero() {
		b.terminalAt = time.Now()
	}

	return nil
}

func isBackupTerminalState(state btypes.ProgressState) bool {
	return state == btypes.ProgressStateComplete || state == btypes.ProgressStateError
}

func (b *Backup) newFragmap() (*Fragmap, error) {
	lvsList, err := b.spdkClient.BdevLvolGetLvstore(b.replica.LvsName, "")
	if err != nil {
		return nil, err
	}
	if len(lvsList) == 0 {
		return nil, errors.Errorf("cannot find lvs %v for volume %v backup creation", b.replica.LvsName, b.VolumeName)
	}
	lvs := lvsList[0]

	if lvs.ClusterSize == 0 || lvs.BlockSize == 0 {
		return nil, errors.Errorf("invalid cluster size %v block size %v lvs %v", lvs.ClusterSize, lvs.BlockSize, b.replica.LvsName)
	}

	if (b.replica.SpecSize % lvs.ClusterSize) != 0 {
		return nil, errors.Errorf("replica size %v is not multiple of cluster size %v", b.replica.SpecSize, lvs.ClusterSize)
	}

	numClusters := b.replica.SpecSize / lvs.ClusterSize

	return &Fragmap{
		ClusterSize: lvs.ClusterSize,
		NumClusters: numClusters,
		// Calculate the number of bytes in the fragmap required considering 8 bits per byte
		Map: make([]byte, (numClusters+7)/8),
	}, nil
}

func (b *Backup) overlayFragmap(fragmap []byte, offset, size uint64) error {
	b.log.Debugf("Overlaying fragment map for offset %v size %v", offset, size)

	startBytes := int((offset / b.fragmap.ClusterSize) / 8)
	if startBytes+len(fragmap) > len(b.fragmap.Map) {
		return fmt.Errorf("invalid start bytes %v and fragmap length %v", startBytes, len(fragmap))
	}

	for i := 0; i < len(fragmap); i++ {
		b.fragmap.Map[startBytes+i] |= fragmap[i]
	}
	return nil
}

func (b *Backup) overlayFragmaps(lvol *Lvol) error {
	// Cluster size is 1 MiB by default, so each byte in fragmap represents 8 clusters.
	// Process 256 bytes at a time to reduce the number of calls.
	batchSize := 256 * (8 * b.fragmap.ClusterSize)

	// Old snapshots remain smaller after expansion; cap fragmap to the lvol size.
	// E.g. a 1Gi snapshot stays 1Gi after expanding the volume to 2Gi.
	effectiveSize := min(b.replica.SpecSize, lvol.SpecSize)

	offset := uint64(0)
	for {
		if offset >= effectiveSize {
			return nil
		}

		size := util.Min(batchSize, effectiveSize-offset)

		result, err := b.spdkClient.BdevLvolGetFragmap(lvol.UUID, uint64(offset), size)
		if err != nil {
			return err
		}

		fragmap, err := base64.StdEncoding.DecodeString(result.Fragmap)
		if err != nil {
			return err
		}

		err = b.overlayFragmap(fragmap, offset, size)
		if err != nil {
			return err
		}

		offset += size
	}
}

func (b *Backup) constructFragmap(from, to int) error {
	for i := from; i > to; i-- {
		lvol := b.replica.ActiveChain[i]
		if lvol != nil {
			b.log.Infof("Overlaying snapshot lvol bdev %v", lvol.Name)
			err := b.overlayFragmaps(lvol)
			if err != nil {
				return errors.Wrapf(err, "failed to overlay fragment map for snapshot lvol bdev %v", lvol.Name)
			}
		}
	}
	return nil
}

func (b *Backup) findSnapshotRange(lvolName, compareLvolName string) (from, to int, err error) {
	from = b.findIndex(lvolName)
	if from < 0 {
		return 0, 0, fmt.Errorf("failed to find snapshot %s in chain", lvolName)
	}

	to = b.findIndex(compareLvolName)
	if to < 0 {
		return 0, 0, fmt.Errorf("failed to find snapshot %s in chain", compareLvolName)
	}

	if from <= to {
		b.log.Warnf("Last backup snapshot %s is not an ancestor of current snapshot %s; performing full backup instead",
			compareLvolName, lvolName)
		to = 0
	}

	if from > len(b.replica.ActiveChain)-1 {
		return 0, 0, fmt.Errorf("invalid to index %v which is greater than the length of active chain %v",
			to, len(b.replica.ActiveChain)-1)
	}

	return from, to, nil
}

func (b *Backup) constructMappings(blockSize int64) *btypes.Mappings {
	b.log.Info("Constructing mappings")

	mappings := &btypes.Mappings{
		BlockSize: blockSize,
	}

	mapping := btypes.Mapping{
		Offset: -1,
	}

	i := uint64(0)
	for i = 0; i < b.fragmap.NumClusters; i++ {
		if b.fragmap.Map.IsSet(uint64(i)) {
			offset := int64(i) * int64(b.fragmap.ClusterSize)
			offset -= (offset % blockSize)
			if mapping.Offset != offset {
				mapping = btypes.Mapping{
					Offset: offset,
					Size:   blockSize,
				}
				mappings.Mappings = append(mappings.Mappings, mapping)
			}
		}
	}

	b.log.Info("Constructed mappings")

	return mappings
}

func (b *Backup) findIndex(lvolName string) int {
	if lvolName == "" {
		// Note that, 0 can be a backing image if ActiveChanin[0] is not nil.
		// Caller should handle this case
		return 0
	}

	for i, lvol := range b.replica.ActiveChain {
		if i == 0 {
			continue
		}
		if lvol.Name == lvolName {
			return i
		}
	}

	return -1
}
