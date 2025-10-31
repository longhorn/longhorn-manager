package spdk

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	commontypes "github.com/longhorn/go-common-libs/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

type BackingImage struct {
	sync.RWMutex

	ctx context.Context

	// Name of the BackingImage, e.g. "parrot"
	Name             string
	BackingImageUUID string
	LvsName          string
	LvsUUID          string
	Size             uint64
	ProcessedSize    int64
	ExpectedChecksum string

	IsExposed bool
	Port      int32

	// We need to create a snapshot from the BackingImage lvol so we can create the replica from that snapshot.
	Snapshot *Lvol
	// This is the lvol alias for the snapshot lvol, e.g. "n1v2disk/bi-parrot-disk-${lvsuuid}"
	Alias string

	Progress        int32
	State           types.BackingImageState
	CurrentChecksum string
	ErrorMsg        string

	UpdateCh chan interface{}

	log *safelog.SafeLogger
}

func ServiceBackingImageToProtoBackingImage(bi *BackingImage) *spdkrpc.BackingImage {
	res := &spdkrpc.BackingImage{
		Name:             bi.Name,
		BackingImageUuid: bi.BackingImageUUID,
		LvsName:          bi.LvsName,
		LvsUuid:          bi.LvsUUID,
		Size:             bi.Size,
		ExpectedChecksum: bi.ExpectedChecksum,
		Snapshot:         nil,
		Progress:         bi.Progress,
		State:            string(bi.State),
		CurrentChecksum:  bi.CurrentChecksum,
		ErrorMsg:         bi.ErrorMsg,
	}
	if bi.Snapshot != nil {
		res.Snapshot = ServiceBackingImageLvolToProtoBackingImageLvol(bi.Snapshot)
	}
	return res
}

func NewBackingImage(ctx context.Context, backingImageName, backingImageUUID, lvsUUID string, size uint64, checksum string, updateCh chan interface{}) *BackingImage {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"backingImagename": backingImageName,
		"lvsUUID":          lvsUUID,
	})

	return &BackingImage{
		ctx:              ctx,
		Name:             backingImageName,
		BackingImageUUID: backingImageUUID,
		LvsUUID:          lvsUUID,
		Size:             size,
		ExpectedChecksum: checksum,
		State:            types.BackingImageStateStarting,
		UpdateCh:         updateCh,
		log:              safelog.NewSafeLogger(log),
	}
}

// Create initiates the backing image, prepare the lvol, copy the data from the local backing file and create the snapshot.
func (bi *BackingImage) Create(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap, fromAddress string, srcLvsUUID string) (ret *spdkrpc.BackingImage, err error) {
	updateRequired := true

	bi.Lock()
	defer func() {
		bi.Unlock()

		if updateRequired {
			bi.UpdateCh <- nil
		}
	}()
	if bi.State == types.BackingImageStateReady {
		updateRequired = false
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "backing image %v already exists and running", bi.Name)
	}
	if bi.State != types.BackingImageStateStarting {
		updateRequired = false
		return nil, grpcstatus.Errorf(grpccodes.Internal, "invalid state %s for backing image %s creation", bi.State, bi.Name)
	}

	go func() {
		err := bi.prepareBackingImageSnapshot(spdkClient, superiorPortAllocator, fromAddress, srcLvsUUID)
		if err != nil {
			bi.log.WithError(err).Error("Failed to create backing image")
		}
		// update the backing image after preparing
		bi.UpdateCh <- nil
	}()

	return ServiceBackingImageToProtoBackingImage(bi), nil
}

func (bi *BackingImage) Get() (pBackingImage *spdkrpc.BackingImage) {
	bi.RLock()
	defer bi.RUnlock()
	return ServiceBackingImageToProtoBackingImage(bi)
}

func (bi *BackingImage) Delete(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	updateRequired := false

	bi.Lock()
	defer func() {
		if err != nil {
			bi.log.WithError(err).Errorf("Failed to delete backing image")
			bi.State = types.BackingImageStateFailed
			bi.ErrorMsg = err.Error()
			updateRequired = true
		}
		bi.Unlock()

		if updateRequired {
			bi.UpdateCh <- nil
		}
	}()

	// blindly unexpose the lvol bdevs
	backingImageSnapLvolName := GetBackingImageSnapLvolName(bi.Name, bi.LvsUUID)
	if err = spdkClient.StopExposeBdev(helpertypes.GetNQN(backingImageSnapLvolName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to unexpose lvol bdev %v when deleting backing image %v", backingImageSnapLvolName, bi.Name)
	}

	backingImageTempHeadName := GetBackingImageTempHeadLvolName(bi.Name, bi.LvsUUID)
	if err = spdkClient.StopExposeBdev(helpertypes.GetNQN(backingImageTempHeadName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to unexpose lvol bdev %v when deleting backing image %v", backingImageTempHeadName, bi.Name)
	}
	bi.IsExposed = false
	if err := superiorPortAllocator.ReleaseRange(bi.Port, bi.Port); err != nil {
		return errors.Wrapf(err, "failed to release port %v after when deleting backing image %v", bi.Port, bi.Name)
	}
	bi.Port = 0

	biTempHeadAlias := fmt.Sprintf("%s/%s", bi.LvsName, GetBackingImageTempHeadLvolName(bi.Name, bi.LvsUUID))
	if _, err := spdkClient.BdevLvolDelete(biTempHeadAlias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	if _, err := spdkClient.BdevLvolDelete(bi.Alias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	bi.log.Info("Deleted backing image")
	updateRequired = true
	return nil
}

func (bi *BackingImage) ValidateAndUpdate(spdkClient *spdkclient.Client) (err error) {
	updateRequired := false
	bi.Lock()
	defer func() {
		bi.Unlock()

		if updateRequired {
			bi.UpdateCh <- nil
		}
	}()

	// Backing image normal state transition: starting -> in-progress -> ready/failed
	// If backing image is in ready or failed state, that means we are validating a record already cached.
	// We check if the corresponding lvol existence.
	// If backing image is in starting and in-progress state, we don't need to do anything because it is still preparing.
	// For uncache backing image lvol, the state will be pending, and we will need to reconstruct the record.
	if bi.State == types.BackingImageStateReady || bi.State == types.BackingImageStateFailed {
		defer func() {
			if err != nil {
				bi.State = types.BackingImageStateFailed
				bi.ErrorMsg = err.Error()
				updateRequired = true
			}
		}()

		// check if the lvol sill exists
		bdevLvolList, err := spdkClient.BdevLvolGet(bi.Alias, 0)
		if err != nil {
			return err
		}
		if len(bdevLvolList) != 1 {
			return fmt.Errorf("zero or multiple snap lvols with alias %v found when validating", bi.Alias)
		}
		return nil
	}

	if bi.State == types.BackingImageStateInProgress || bi.State == types.BackingImageStateStarting {
		return nil
	}

	biLvolName := GetBackingImageSnapLvolName(bi.Name, bi.LvsUUID)

	lvsName, err := GetLvsNameByUUID(spdkClient, bi.LvsUUID)
	if err != nil {
		return errors.Wrapf(err, "failed to get the lvs name for backing image %v with lvs uuid %v", bi.Name, bi.LvsUUID)
	}

	if errUpdateLogger := bi.log.UpdateLogger(logrus.Fields{
		"backingImagename": bi.Name,
		"lvsName":          bi.LvsName,
		"lvsUUID":          bi.LvsUUID,
	}); errUpdateLogger != nil {
		bi.log.WithError(errUpdateLogger).Warn("Failed to update logger")
	}

	bi.LvsName = lvsName

	bi.Alias = spdktypes.GetLvolAlias(bi.LvsName, biLvolName)
	bdevLvolList, err := spdkClient.BdevLvolGet(bi.Alias, 0)
	if err != nil {
		return err
	}
	if len(bdevLvolList) != 1 {
		return fmt.Errorf("zero or multiple snap lvols with alias %v found after lvol snapshot", bi.Alias)
	}
	snapSvcLvol := BdevLvolInfoToServiceLvol(&bdevLvolList[0])
	bi.Snapshot = snapSvcLvol
	state, err := GetSnapXattr(spdkClient, bi.Alias, types.LonghornBackingImageSnapshotAttrPrepareState)
	if err != nil {
		return errors.Wrapf(err, "failed to get the prepare state for backing image snapshot %v", bi.Name)
	}
	bi.State = types.BackingImageState(state)

	// TODO: recheck the checksum when pick up the backing image snapshot lvol
	bi.CurrentChecksum = bi.ExpectedChecksum
	bi.Progress = 100
	updateRequired = true

	return nil
}

func (bi *BackingImage) BackingImageExpose(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) (exposedSnapshotLvolAddress string, err error) {
	bi.log.Infof("Exposing backing image %v for syncing", bi.Name)

	updateRequired := false

	bi.Lock()
	defer func() {
		bi.Unlock()
		if updateRequired {
			bi.UpdateCh <- nil
		}
	}()

	defer func() {
		if err != nil {
			bi.log.WithError(err).Warnf("Failed to expose the backing image snapshot lvol")

			// We don't need to handle exposed bdev here since it is the last step.
			// If it failed to expose the bdev, we only need to release the port.
			opErr := superiorPortAllocator.ReleaseRange(bi.Port, bi.Port)
			if opErr != nil {
				err = errors.Wrapf(err, "Failed to release port %v with error %v", bi.Port, opErr)
			} else {
				bi.Port = 0
			}
			updateRequired = true
		}
	}()

	if bi.State != types.BackingImageStateReady {
		return "", fmt.Errorf("invalid state %v for backing image %s to be exposed for syncing", bi.State, bi.Name)
	}
	backingImageSnapLvolName := GetBackingImageSnapLvolName(bi.Name, bi.LvsUUID)

	snapLvol := bi.Snapshot
	if snapLvol == nil {
		return "", fmt.Errorf("cannot find snapshot for the backing image %s to be exposed for syncing", bi.Name)
	}

	// Expose the bdev using nvmf
	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return "", err
	}

	if bi.IsExposed {
		exposedSnapshotLvolAddress = net.JoinHostPort(podIP, strconv.Itoa(int(bi.Port)))
		bi.log.Infof("Backing image lvol bdev %v is already exposed with exposedSnapshotLvolAddress %v", backingImageSnapLvolName, exposedSnapshotLvolAddress)
		return exposedSnapshotLvolAddress, nil
	}

	port, _, err := superiorPortAllocator.AllocateRange(int32(types.BackingImagePortCount))
	if err != nil {
		return "", err
	}
	bi.Port = port
	bi.log.Infof("Allocated port %v", port)

	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create executor")
	}

	subsystemNQN, controllerName, err := exposeSnapshotLvolBdev(spdkClient, bi.LvsName, backingImageSnapLvolName, podIP, port, executor)
	if err != nil {
		bi.log.WithError(err).Errorf("Failed to expose lvol bdev")
		return "", err
	}
	bi.IsExposed = true

	exposedSnapshotLvolAddress = net.JoinHostPort(podIP, strconv.Itoa(int(port)))
	bi.log.Infof("Exposed lvol bdev %v, subsystemNQN %v, controllerName %v, exposedSnapshotLvolAddress: %v", backingImageSnapLvolName, subsystemNQN, controllerName, exposedSnapshotLvolAddress)

	updateRequired = true

	return exposedSnapshotLvolAddress, nil
}

func (bi *BackingImage) BackingImageUnexpose(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	bi.log.Infof("Stop exposing backing image %v", bi.Name)

	updateRequired := false

	bi.Lock()
	defer func() {
		bi.Unlock()

		if updateRequired {
			bi.UpdateCh <- nil
		}
	}()

	if !bi.IsExposed {
		return nil
	}

	backingImageSnapLvolName := GetBackingImageSnapLvolName(bi.Name, bi.LvsUUID)
	// Unexpose the bdev
	bi.log.Info("Unexposing lvol bdev")
	if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(backingImageSnapLvolName)); err != nil {
		return errors.Wrapf(err, "Failed to unexpose lvol bdev %v", backingImageSnapLvolName)
	}
	bi.IsExposed = false

	if err := superiorPortAllocator.ReleaseRange(bi.Port, bi.Port); err != nil {
		return errors.Wrapf(err, "Failed to release port %v after failed to create backing image", bi.Port)
	}
	bi.Port = 0
	updateRequired = true

	return
}

func checkIsSourceFromBIM(fromAddress string) bool {
	return strings.HasPrefix(fromAddress, "http")
}

func (bi *BackingImage) UpdateProgress(processedSize int64) {
	bi.updateProgress(processedSize)
	bi.UpdateCh <- nil
}

func (bi *BackingImage) updateProgress(processedSize int64) {
	bi.Lock()
	defer bi.Unlock()

	if bi.State == types.BackingImageStateStarting {
		bi.State = types.BackingImageStateInProgress
	}
	if bi.State == types.BackingImageStateReady {
		return
	}

	bi.ProcessedSize = bi.ProcessedSize + processedSize
	if bi.Size > 0 {
		bi.Progress = int32((float32(bi.ProcessedSize) / float32(bi.Size)) * 100)
	}
}

func (bi *BackingImage) prepareBackingImageSnapshot(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap, fromAddress string, srcLvsUUID string) (err error) {
	// If we already create the temp head lvol, no matter it fails to prepare or not, we should create the snapshot for it.
	// The state will be recorded in the xattr of the snapshot lvol so when the node is rebooted, we can pick it up and reconstruct the record.
	// Controller will handle the failed backing image copy by deleting it and recreating it.
	biTempHeadUUID := ""

	defer func() {
		bi.Lock()
		defer bi.Unlock()

		if bi.IsExposed {
			bi.log.Info("Unexposing lvol bdev")
			backingImageTempHeadName := GetBackingImageTempHeadLvolName(bi.Name, bi.LvsUUID)
			if err = spdkClient.StopExposeBdev(helpertypes.GetNQN(backingImageTempHeadName)); err != nil {
				bi.log.WithError(err).Errorf("Failed to unexpose lvol bdev %v after failed to create backing image", backingImageTempHeadName)
			}
			bi.IsExposed = false
		}
		if bi.Port != 0 {
			if err = superiorPortAllocator.ReleaseRange(bi.Port, bi.Port); err != nil {
				bi.log.WithError(err).Errorf("Failed to release port %v after failed to create backing image", bi.Port)
			}
			bi.Port = 0
		}

		if err == nil {
			if bi.State != types.BackingImageStateInProgress {
				err = fmt.Errorf("invalid state %v for backing image %s creation after processing", bi.State, bi.Name)
			}

			if bi.Size > 0 && bi.ProcessedSize != int64(bi.Size) {
				err = fmt.Errorf("processed data size %v does not match the expected file size %v", bi.ProcessedSize, bi.Size)
			}

			if bi.CurrentChecksum != bi.ExpectedChecksum {
				err = fmt.Errorf("current checksum %v does not match the expected checksum %v", bi.CurrentChecksum, bi.ExpectedChecksum)
			}
		}

		if err != nil {
			bi.log.WithError(err).Errorf("Failed to create backing image %v", bi.Name)
			if bi.State != types.BackingImageStateFailed {
				bi.State = types.BackingImageStateFailed
			}
			bi.ErrorMsg = err.Error()
		} else {
			bi.Progress = 100
			bi.State = types.BackingImageStateReady
			bi.ErrorMsg = ""
		}

		if biTempHeadUUID != "" {
			// Create a snapshot with backing image name from the backing image temp head lvol
			// the snapshot lvol name will be "bi-${biName}-disk-${lvsUUID}"
			// and the alias will be "${lvsName}/bi-${biName}-disk-${lvsUUID}"

			createSnapshotErr := bi.createSnapshotFromTempHead(spdkClient, biTempHeadUUID)
			if createSnapshotErr != nil {
				bi.State = types.BackingImageStateFailed
				bi.Progress = 0
				if bi.ErrorMsg != "" {
					bi.ErrorMsg = fmt.Sprintf("%v; %v", bi.ErrorMsg, createSnapshotErr.Error())
				} else {
					bi.ErrorMsg = createSnapshotErr.Error()
				}
			}

			backingImageTempHeadName := GetBackingImageTempHeadLvolName(bi.Name, bi.LvsUUID)
			if _, opErr := spdkClient.BdevLvolDelete(biTempHeadUUID); opErr != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(opErr) {
				bi.log.WithError(opErr).Errorf("Failed to delete the temp head %v of backing image ", backingImageTempHeadName)
			}
		}
	}()

	bi.Lock()
	lvsName, err := GetLvsNameByUUID(spdkClient, bi.LvsUUID)
	if err != nil {
		return errors.Wrapf(err, "failed to get the lvs name for backing image %v with lvs uuid %v", bi.Name, bi.LvsUUID)
	}
	bi.LvsName = lvsName

	if errUpdateLogger := bi.log.UpdateLogger(logrus.Fields{
		"backingImagename": bi.Name,
		"lvsName":          bi.LvsName,
		"lvsUUID":          bi.LvsUUID,
	}); errUpdateLogger != nil {
		bi.log.WithError(errUpdateLogger).Warn("Failed to update logger")
	}

	bi.Unlock()

	// backingImageTempHeadName will be "bi-${biName}-disk-${lvsUUID}-temp-head"
	backingImageTempHeadName := GetBackingImageTempHeadLvolName(bi.Name, bi.LvsUUID)
	biTempHeadUUID, err = spdkClient.BdevLvolCreate("", bi.LvsUUID, backingImageTempHeadName, util.BytesToMiB(bi.Size), "", true)
	if err != nil {
		return err
	}
	bdevLvolList, err := spdkClient.BdevLvolGet(biTempHeadUUID, 0)
	if err != nil {
		return err
	}
	if len(bdevLvolList) < 1 {
		return fmt.Errorf("cannot find lvol %v after creation", backingImageTempHeadName)
	}
	bi.log.Infof("Created a head lvol %v for the new backing image", backingImageTempHeadName)

	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return err
	}
	port, _, err := superiorPortAllocator.AllocateRange(int32(types.BackingImagePortCount))
	if err != nil {
		return err
	}
	bi.Lock()
	bi.Port = port
	bi.Unlock()
	bi.log.Infof("Allocated port %v", port)

	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return errors.Wrapf(err, "failed to create executor")
	}
	subsystemNQN, controllerName, err := exposeSnapshotLvolBdev(spdkClient, bi.LvsName, backingImageTempHeadName, podIP, port, executor)
	if err != nil {
		bi.log.WithError(err).Errorf("Failed to expose head lvol")
		return err
	}
	bi.Lock()
	bi.IsExposed = true
	bi.Unlock()
	bi.log.Infof("Exposed head lvol %v, subsystemNQN %v, controllerName %v", backingImageTempHeadName, subsystemNQN, controllerName)

	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: helpertypes.GetNQN(backingImageTempHeadName),
	}
	headInitiator, err := initiator.NewInitiator(backingImageTempHeadName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create NVMe initiator for head lvol %v", backingImageTempHeadName)
	}
	if _, err := headInitiator.StartNvmeTCPInitiator(podIP, strconv.Itoa(int(port)), true); err != nil {
		return errors.Wrapf(err, "failed to start NVMe initiator for head lvol %v", backingImageTempHeadName)
	}
	bi.log.Infof("Created NVMe initiator for head lvol %v", backingImageTempHeadName)

	headFh, err := os.OpenFile(headInitiator.Endpoint, os.O_RDWR, 0666)
	defer func() {
		// Stop the initiator
		if errClose := headFh.Close(); errClose != nil {
			bi.log.WithError(errClose).Error("Failed to close the backing image")
		}
		bi.log.Info("Stopping NVMe initiator")
		if _, opErr := headInitiator.Stop(nil, true, true, false); opErr != nil {
			bi.log.WithError(opErr).Error("Failed to stop the backing image head NVMe initiator")
		}
	}()
	if err != nil {
		return errors.Wrapf(err, "failed to open NVMe device %v for lvol bdev %v", headInitiator.Endpoint, backingImageTempHeadName)
	}

	// An SPDK backing image should only be created by downloading it from BIM if it's a first copy,
	// or by syncing it from another SPDK server.
	isSourceFromBIM := checkIsSourceFromBIM(fromAddress)
	if isSourceFromBIM {
		if err := bi.prepareFromURL(headFh, fromAddress); err != nil {
			bi.log.WithError(err).Warnf("Failed to prepare the backing image %v from URL %v", bi.Name, fromAddress)
			return errors.Wrapf(err, "failed to prepare the backing image %v from URL %v", bi.Name, fromAddress)
		}
	} else {
		if err := bi.prepareFromSync(headFh, fromAddress, srcLvsUUID); err != nil {
			bi.log.WithError(err).Warnf("Failed to prepare the backing image %v by syncing from %v and srcLvsUUID %v", bi.Name, fromAddress, srcLvsUUID)
			return errors.Wrapf(err, "failed to prepare the backing image %v by syncing from  %v and srcLvsUUID %v", bi.Name, fromAddress, srcLvsUUID)
		}
	}

	currentChecksum, err := util.GetFileChecksum(headInitiator.Endpoint)
	if err != nil {
		return errors.Wrapf(err, "failed to get the current checksum of the backing image %v target device %v", bi.Name, headInitiator.Endpoint)
	}
	bi.log.Infof("Get the current checksum of the backing image %v: %v", bi.Name, currentChecksum)
	bi.Lock()
	bi.CurrentChecksum = currentChecksum
	bi.Unlock()

	return nil
}

func (bi *BackingImage) createSnapshotFromTempHead(spdkClient *spdkclient.Client, biTempHeadUUID string) (err error) {
	ne, err := helperutil.NewExecutor(commontypes.HostProcDirectory)
	if err != nil {
		bi.log.WithError(err).Warnf("Failed to get the executor for snapshot backing image %v, will skip the sync and continue", bi.Name)
	} else {
		bi.log.Infof("Requesting system sync before snapshot backin image %v", bi.Name)
		// TODO: only sync the device path rather than all filesystems
		if _, err := ne.Execute(nil, "sync", []string{}, SyncTimeout); err != nil {
			// sync should never fail though, so it more like due to the nsenter
			bi.log.WithError(err).Errorf("WARNING: failed to sync for snapshot backing image %v, will skip the sync and continue", bi.Name)
		}
	}

	biSnapLvolName := GetBackingImageSnapLvolName(bi.Name, bi.LvsUUID)
	bi.Alias = spdktypes.GetLvolAlias(bi.LvsName, biSnapLvolName)

	var xattrs []spdkclient.Xattr
	checksum := spdkclient.Xattr{
		Name:  types.LonghornBackingImageSnapshotAttrChecksum,
		Value: bi.ExpectedChecksum,
	}
	xattrs = append(xattrs, checksum)
	backingImageUUID := spdkclient.Xattr{
		Name:  types.LonghornBackingImageSnapshotAttrUUID,
		Value: bi.BackingImageUUID,
	}
	xattrs = append(xattrs, backingImageUUID)
	prepareState := spdkclient.Xattr{
		Name:  types.LonghornBackingImageSnapshotAttrPrepareState,
		Value: string(bi.State),
	}
	xattrs = append(xattrs, prepareState)

	snapUUID, err := spdkClient.BdevLvolSnapshot(biTempHeadUUID, biSnapLvolName, xattrs)
	if err != nil {
		return err
	}

	bdevLvolList, err := spdkClient.BdevLvolGet(snapUUID, 0)
	if err != nil {
		return err
	}

	if len(bdevLvolList) != 1 {
		return fmt.Errorf("zero or multiple snap lvols with UUID %s found after lvol snapshot", snapUUID)
	}

	snapSvcLvol := BdevLvolInfoToServiceLvol(&bdevLvolList[0])
	bi.Snapshot = snapSvcLvol

	return nil
}

func (bi *BackingImage) prepareFromURL(targetFh *os.File, fromAddress string) (err error) {
	httpHandler := util.HTTPHandler{}

	// Parse the base URL into a URL object
	parsedURL, err := url.Parse(fromAddress)
	if err != nil {
		bi.log.WithError(err).Error("Failed to parse the URL")
		return errors.Wrapf(err, "failed to parse the URL %v", fromAddress)
	}
	// Add query parameters
	query := parsedURL.Query()
	query.Set("forV2Creation", "true")
	parsedURL.RawQuery = query.Encode()

	size, err := httpHandler.GetSizeFromURL(parsedURL.String())
	if err != nil {
		return errors.Wrapf(err, "failed to get the file size from %v", fromAddress)
	}
	if size != int64(bi.Size) {
		return errors.Wrapf(err, "download file %v size %v is not the same as the backing image size %v", fromAddress, size, bi.Size)
	}

	if _, err := httpHandler.DownloadFromURL(bi.ctx, parsedURL.String(), targetFh, bi); err != nil {
		return errors.Wrapf(err, "failed to download the file from %v", fromAddress)
	}
	return nil
}

func (bi *BackingImage) prepareFromSync(targetFh *os.File, fromAddress, srcLvsUUID string) (err error) {
	if fromAddress == "" || srcLvsUUID == "" {
		return errors.Wrapf(err, "missing required source backing image service address %v or source lvsUUID %v", fromAddress, srcLvsUUID)
	}
	srcBackingImageServiceCli, err := GetServiceClient(fromAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to init the source backing image spdk service client")
	}
	defer func() {
		if errClose := srcBackingImageServiceCli.Close(); errClose != nil {
			bi.log.WithError(errClose).Error("Failed to close the source backing image spdk service client")
		}
	}()
	exposedSnapshotLvolAddress, err := srcBackingImageServiceCli.BackingImageExpose(bi.Name, srcLvsUUID)
	if err != nil {
		return errors.Wrapf(err, "failed to expose the source backing image %v", bi.Name)
	}
	externalSnapshotLvolName := GetBackingImageSnapLvolName(bi.Name, srcLvsUUID)
	defer func() {
		// Unexpose the source snapshot
		if err := srcBackingImageServiceCli.BackingImageUnexpose(bi.Name, srcLvsUUID); err != nil {
			bi.log.WithError(err).Warnf("failed to unsexpose the source backing image %v", bi.Name)
		}
	}()

	srcIP, srcPort, err := splitHostPort(exposedSnapshotLvolAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split host and port from address %v", exposedSnapshotLvolAddress)
	}
	_, _, err = connectNVMeTarget(srcIP, srcPort, maxNumRetries, retryInterval)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to NVMe target for source backing image %v in lvsUUID %v with address %v", bi.Name, srcLvsUUID, exposedSnapshotLvolAddress)
	}

	bi.log.Info("Creating NVMe initiator for source backing image %v", bi.Name)
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: helpertypes.GetNQN(externalSnapshotLvolName),
	}
	i, err := initiator.NewInitiator(externalSnapshotLvolName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create NVMe initiator for source backing image %v in lvsUUID %v with address %v", bi.Name, srcLvsUUID, exposedSnapshotLvolAddress)
	}
	if _, err := i.StartNvmeTCPInitiator(srcIP, strconv.Itoa(int(srcPort)), true); err != nil {
		return errors.Wrapf(err, "failed to start NVMe initiator for source backing image %v in lvsUUID %v with address %v", bi.Name, srcLvsUUID, exposedSnapshotLvolAddress)
	}

	bi.log.Infof("Opening NVMe device %v", i.Endpoint)
	srcFh, err := os.OpenFile(i.Endpoint, os.O_RDWR, 0666)
	defer func() {
		if errClose := srcFh.Close(); errClose != nil {
			bi.log.WithError(errClose).Error("Failed to close the source backing image")
		}
		// Stop the source initiator
		bi.log.Info("Stopping NVMe initiator")
		if _, err := i.Stop(nil, true, true, false); err != nil {
			bi.log.WithError(err).Warnf("failed to stop NVMe initiator")
		}
	}()
	if err != nil {
		return errors.Wrapf(err, "failed to open NVMe device %v for source backing image %v in lvsUUID %v with address %v", i.Endpoint, bi.Name, srcLvsUUID, exposedSnapshotLvolAddress)
	}

	ctx, cancel := context.WithCancel(bi.ctx)
	defer cancel()
	_, err = util.IdleTimeoutCopy(ctx, cancel, srcFh, targetFh, bi, false)
	if err != nil {
		return errors.Wrapf(err, "failed to copy the source backing image %v in lvsUUID %v with address %v", bi.Name, srcLvsUUID, exposedSnapshotLvolAddress)
	}

	return nil
}

func cleanupOrphanBackingImageTempHead(spdkClient *spdkclient.Client, lvsName, backingImageTempHeadName string) error {
	if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(backingImageTempHeadName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "Failed to unexpose orphan backing image temp head %v", backingImageTempHeadName)
	}

	biTempHeadAlias := fmt.Sprintf("%s/%s", lvsName, backingImageTempHeadName)
	if _, err := spdkClient.BdevLvolDelete(biTempHeadAlias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "Failed to delete orphan backing image temp head %v", backingImageTempHeadName)
	}
	return nil
}
