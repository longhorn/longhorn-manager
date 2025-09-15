package spdk

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	btypes "github.com/longhorn/backupstore/types"
	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/initiator"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

type Restore struct {
	sync.RWMutex

	spdkClient *spdkclient.Client
	replica    *Replica

	Progress  int
	Error     string
	BackupURL string
	State     btypes.ProgressState

	// The snapshot file that stores the restored data in the end.
	LvolName     string
	SnapshotName string

	LastRestored           string
	CurrentRestoringBackup string

	ip             string
	port           int32
	executor       *commonns.Executor
	subsystemNQN   string
	controllerName string
	initiator      *initiator.Initiator

	stopOnce sync.Once
	stopChan chan struct{}

	log logrus.FieldLogger
}

var _ backupstore.DeltaRestoreOperations = (*Restore)(nil)

func NewRestore(spdkClient *spdkclient.Client, lvolName, snapshotName, backupUrl, backupName string, replica *Replica) (*Restore, error) {
	log := logrus.WithFields(logrus.Fields{
		"lvolName":     lvolName,
		"snapshotName": snapshotName,
		"backupUrl":    backupUrl,
		"backupName":   backupName,
	})

	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create executor")
	}

	return &Restore{
		spdkClient:             spdkClient,
		replica:                replica,
		BackupURL:              backupUrl,
		CurrentRestoringBackup: backupName,
		LvolName:               lvolName,
		SnapshotName:           snapshotName,
		ip:                     replica.IP,
		port:                   replica.PortStart,
		executor:               executor,
		State:                  btypes.ProgressStateInProgress,
		Progress:               0,
		stopChan:               make(chan struct{}),
		log:                    log,
	}, nil
}

func (r *Restore) StartNewRestore(backupUrl, currentRestoringBackup, lvolName, snapshotName string, validLastRestoredBackup bool) {
	r.Lock()
	defer r.Unlock()

	r.LvolName = lvolName
	r.SnapshotName = snapshotName

	r.Progress = 0
	r.Error = ""
	r.BackupURL = backupUrl
	r.State = btypes.ProgressStateInProgress
	if !validLastRestoredBackup {
		r.LastRestored = ""
	}
	r.CurrentRestoringBackup = currentRestoringBackup
}

func (r *Restore) DeepCopy() *Restore {
	r.RLock()
	defer r.RUnlock()

	return &Restore{
		LvolName:               r.LvolName,
		SnapshotName:           r.SnapshotName,
		LastRestored:           r.LastRestored,
		BackupURL:              r.BackupURL,
		CurrentRestoringBackup: r.CurrentRestoringBackup,
		State:                  r.State,
		Error:                  r.Error,
		Progress:               r.Progress,
	}
}

func (r *Restore) OpenVolumeDev(volDevName string) (*os.File, string, error) {
	lvolName := r.replica.Name

	r.log.Info("Unexposing lvol bdev before restoration")
	if r.replica.IsExposed {
		err := r.spdkClient.StopExposeBdev(helpertypes.GetNQN(lvolName))
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to unexpose lvol bdev %v", lvolName)
		}
		r.replica.IsExposed = false
	}

	r.log.Info("Exposing snapshot lvol bdev for restore")
	subsystemNQN, controllerName, err := exposeSnapshotLvolBdev(r.spdkClient, r.replica.LvsName, lvolName, r.ip, r.port, r.executor)
	if err != nil {
		r.log.WithError(err).Errorf("Failed to expose lvol bdev")
		return nil, "", err
	}
	r.subsystemNQN = subsystemNQN
	r.controllerName = controllerName
	r.replica.IsExposed = true
	r.log.Infof("Exposed snapshot lvol bdev %v, subsystemNQN=%v, controllerName %v", lvolName, subsystemNQN, controllerName)

	r.log.Info("Creating NVMe initiator for lvol bdev")
	nvmeTCPInfo := &initiator.NVMeTCPInfo{
		SubsystemNQN: helpertypes.GetNQN(lvolName),
	}
	i, err := initiator.NewInitiator(lvolName, initiator.HostProc, nvmeTCPInfo, nil)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to create NVMe initiator for lvol bdev %v", lvolName)
	}
	if _, err := i.StartNvmeTCPInitiator(r.ip, strconv.Itoa(int(r.port)), true); err != nil {
		return nil, "", errors.Wrapf(err, "failed to start NVMe initiator for lvol bdev %v", lvolName)
	}
	r.initiator = i

	r.log.Infof("Opening NVMe device %v", r.initiator.Endpoint)
	fh, err := os.OpenFile(r.initiator.Endpoint, os.O_RDONLY, 0666)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to open NVMe device %v for lvol bdev %v", r.initiator.Endpoint, lvolName)
	}

	return fh, r.initiator.Endpoint, err
}

func (r *Restore) CloseVolumeDev(volDev *os.File) error {
	r.log.Infof("Closing NVMe device %v", r.initiator.Endpoint)
	if err := volDev.Close(); err != nil {
		return errors.Wrapf(err, "failed to close NVMe device %v", r.initiator.Endpoint)
	}

	r.log.Info("Stopping NVMe initiator")
	if _, err := r.initiator.Stop(nil, true, true, false); err != nil {
		return errors.Wrapf(err, "failed to stop NVMe initiator")
	}

	if !r.replica.IsExposed {
		r.log.Info("Unexposing lvol bdev")
		lvolName := r.replica.Name
		err := r.spdkClient.StopExposeBdev(helpertypes.GetNQN(lvolName))
		if err != nil {
			return errors.Wrapf(err, "failed to unexpose lvol bdev %v", lvolName)
		}
		r.replica.IsExposed = false
	}

	return nil
}

func (r *Restore) UpdateRestoreStatus(snapshotLvolName string, progress int, err error) {
	r.Lock()
	defer r.Unlock()

	r.LvolName = snapshotLvolName
	r.Progress = progress

	if err != nil {
		r.CurrentRestoringBackup = ""

		// No need to mark restore as error if it's cancelled.
		// The restoration will be restarted after the engine is restarted.
		if strings.Contains(err.Error(), btypes.ErrorMsgRestoreCancelled) {
			r.log.WithError(err).Warn("Backup restoration is cancelled")
			r.State = btypes.ProgressStateCanceled
		} else {
			r.log.WithError(err).Error("Backup restoration is failed")
			r.State = btypes.ProgressStateError
			if r.Error != "" {
				r.Error = fmt.Sprintf("%v: %v", err.Error(), r.Error)
			} else {
				r.Error = err.Error()
			}
		}
	}
}

func (r *Restore) FinishRestore() {
	r.Lock()
	defer r.Unlock()

	if r.State != btypes.ProgressStateError && r.State != btypes.ProgressStateCanceled {
		r.State = btypes.ProgressStateComplete
		r.LastRestored = r.CurrentRestoringBackup
		r.CurrentRestoringBackup = ""
	}
}

func (r *Restore) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}

func (r *Restore) GetStopChan() chan struct{} {
	return r.stopChan
}

// TODL: implement this
// func (status *RestoreStatus) Revert(previousStatus *RestoreStatus) {
// }
