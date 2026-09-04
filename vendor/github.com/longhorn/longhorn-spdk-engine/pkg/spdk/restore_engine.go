package spdk

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	btypes "github.com/longhorn/backupstore/types"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
)

type EngineRestore struct {
	sync.RWMutex

	spdkClient *spdkclient.Client
	engine     *Engine
	endpoint   string

	Progress  int
	Error     string
	BackupURL string
	State     btypes.ProgressState

	// ErrorSourceReplicaName is the name of the replica responsible for the
	// current restore error. Empty means the error, if any, is not attributed
	// to a specific replica.
	ErrorSourceReplicaName string

	// The snapshot file that stores the restored data in the end.
	SnapshotName string

	superiorPortAllocator *commonbitmap.Bitmap

	LastRestored           string
	CurrentRestoringBackup string

	// finalized is set once the engine has recorded the outcome of the
	// current restore cycle. While set, status reports from backupstore
	// workers are ignored. See FinalizeRestore for why late reports happen.
	finalized bool

	stopChan chan struct{}
	stopOnce sync.Once

	// ctx ends this restore cycle's backupstore goroutines. Canceling it at
	// the end of the cycle frees a block producer left blocked on its
	// bounded channel after the workers have exited. signalStop and
	// FinalizeRestore cancel it; StartNewRestore replaces it for the next
	// cycle.
	ctx       context.Context
	cancelCtx context.CancelFunc

	log logrus.FieldLogger
}

var _ backupstore.DeltaRestoreOperations = (*EngineRestore)(nil)

func NewEngineRestore(spdkClient *spdkclient.Client, backupURL string, backupName string, engine *Engine, superiorPortAllocator *commonbitmap.Bitmap) *EngineRestore {
	log := logrus.WithFields(logrus.Fields{
		"backupURL":  backupURL,
		"backupName": backupName,
	})

	ctx, cancelCtx := context.WithCancel(engine.ctx)

	return &EngineRestore{
		spdkClient:             spdkClient,
		engine:                 engine,
		BackupURL:              backupURL,
		CurrentRestoringBackup: backupName,
		superiorPortAllocator:  superiorPortAllocator,
		State:                  btypes.ProgressStateInProgress,
		Progress:               0,
		stopChan:               make(chan struct{}),
		ctx:                    ctx,
		cancelCtx:              cancelCtx,
		log:                    log,
	}
}

func (r *EngineRestore) StartNewRestore(backupURL string, currentRestoringBackup string, validLastRestoredBackup bool) {
	r.Lock()
	defer r.Unlock()

	r.Progress = 0
	r.Error = ""
	r.ErrorSourceReplicaName = ""
	r.BackupURL = backupURL
	r.State = btypes.ProgressStateInProgress
	// Reset the late-report guard: FinalizeRestore set it for the previous
	// cycle, and the new cycle's own status reports must get through.
	r.finalized = false

	// FinalizeRestore already canceled the previous cycle's context; cancel
	// again for a cycle that never reached it. The new cycle gets its own.
	r.cancelCtx()
	r.ctx, r.cancelCtx = context.WithCancel(r.engine.ctx)

	if !validLastRestoredBackup {
		r.LastRestored = ""
	}

	r.CurrentRestoringBackup = currentRestoringBackup
}

func (r *EngineRestore) DeepCopy() *EngineRestore {
	r.RLock()
	defer r.RUnlock()

	return &EngineRestore{
		BackupURL:              r.BackupURL,
		CurrentRestoringBackup: r.CurrentRestoringBackup,
		LastRestored:           r.LastRestored,
		SnapshotName:           r.SnapshotName,
		superiorPortAllocator:  r.superiorPortAllocator,
		State:                  r.State,
		Error:                  r.Error,
		ErrorSourceReplicaName: r.ErrorSourceReplicaName,
		Progress:               r.Progress,
	}
}

// RecordErrorSource records the name of the replica responsible for the
// current restore error. Only the first name recorded in a restore cycle is
// kept, because follow-up errors are usually consequences of the original
// failure. StartNewRestore clears it.
func (r *EngineRestore) RecordErrorSource(replicaName string) {
	// An empty name means the caller cannot attribute the error to a replica;
	// the error stays unattributed (engine-level).
	if replicaName == "" {
		return
	}

	r.Lock()
	defer r.Unlock()

	if r.ErrorSourceReplicaName != "" {
		if r.ErrorSourceReplicaName != replicaName {
			r.log.Infof("Keeping existing restore error source %v; ignoring later error source %v", r.ErrorSourceReplicaName, replicaName)
		}
		return
	}

	r.log.Infof("Recording restore error source %v", replicaName)
	r.ErrorSourceReplicaName = replicaName
}

func (r *EngineRestore) OpenVolumeDev(_ string) (*os.File, string, error) {
	endpoint := r.endpoint

	r.log.Infof("Opening NVMe device %v", endpoint)
	fh, err := os.OpenFile(endpoint, os.O_RDWR|syscall.O_DIRECT, 0666)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to open NVMe device %v", endpoint)
	}
	return fh, endpoint, nil
}

// CloseVolumeDev flushes the restored data to the NVMe device and closes it.
// A sync failure means the last writes may not have reached the device, so
// it is returned as an error and backupstore records it in the restore
// status; the restore is then reported as failed instead of complete. The
// device is closed even when the sync fails.
func (r *EngineRestore) CloseVolumeDev(volDev *os.File) error {
	var syncErr error
	if err := volDev.Sync(); err != nil {
		syncErr = errors.Wrapf(err, "failed to sync NVMe device %v before close", volDev.Name())
		r.log.WithError(err).Errorf("Failed to sync NVMe device %v before close", volDev.Name())
	}

	r.log.Infof("Closing NVMe device %v", volDev.Name())
	if err := volDev.Close(); err != nil {
		return errors.Join(syncErr, errors.Wrapf(err, "failed to close NVMe device %v", volDev.Name()))
	}

	return syncErr
}

// UpdateRestoreStatus is called by backupstore workers to report progress
// and errors. Reports are ignored once FinalizeRestore has recorded the
// outcome of the cycle.
func (r *EngineRestore) UpdateRestoreStatus(snapshot string, progress int, err error) {
	r.Lock()
	defer r.Unlock()

	if r.finalized {
		r.log.WithError(err).Debugf("Ignoring restore status report at %v%% after the restore outcome was recorded", progress)
		return
	}

	r.Progress = progress

	if err != nil {
		r.recordErrorLocked(err)
	}
}

func (r *EngineRestore) recordErrorLocked(err error) {
	if strings.Contains(err.Error(), btypes.ErrorMsgRestoreCancelled) {
		r.State = btypes.ProgressStateCanceled
		r.Error = err.Error()
		return
	}

	r.State = btypes.ProgressStateError
	if r.Error != "" {
		r.Error = fmt.Sprintf("%v: %v", err.Error(), r.Error)
	} else {
		r.Error = err.Error()
	}
}

// FinalizeRestore records the engine's final word on the restore cycle. A nil
// err marks the restore complete; a non-nil err records it as failed or
// cancelled. Afterwards UpdateRestoreStatus ignores reports until
// StartNewRestore begins a new cycle.
//
// The guard is needed because backupstore workers can outlive the engine's
// watcher. When the watcher aborts an idle restore, a worker may still be
// blocked inside a write to the NVMe device. The write only returns once the
// restore initiator is torn down, and the worker then reports the resulting
// error. Without the guard that late report would replace the error the
// engine recorded, or flip the state to cancelled.
func (r *EngineRestore) FinalizeRestore(err error) {
	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.Progress = 0
		r.recordErrorLocked(err)
	} else if r.State != btypes.ProgressStateError && r.State != btypes.ProgressStateCanceled {
		r.State = btypes.ProgressStateComplete
		r.LastRestored = r.CurrentRestoringBackup
		r.CurrentRestoringBackup = ""
	}

	r.finalized = true

	// The cycle is over. Cancel its context so a block producer still
	// blocked on the bounded channel exits with the workers.
	r.cancelCtx()
}

// Stop cancels the restore on behalf of engine deletion: it stops the
// backupstore workers and records the cancelled outcome.
func (r *EngineRestore) Stop() {
	r.signalStop()

	r.Lock()
	defer r.Unlock()

	if r.finalized {
		return
	}
	r.State = btypes.ProgressStateCanceled
	r.Error = btypes.ErrorMsgRestoreCancelled
	r.Progress = 0
}

// signalStop ends this cycle's backupstore goroutines. Closing the stop
// channel makes workers between blocks exit instead of fetching more.
// Canceling the cycle context makes a producer blocked on the bounded
// channel exit with them. It does not change the restore state; the caller
// records the outcome. Workers blocked inside a write do not see either
// signal until the restore initiator is torn down.
func (r *EngineRestore) signalStop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})

	// StartNewRestore replaces cancelCtx under the lock, so read it under
	// the lock as well.
	r.RLock()
	cancelCtx := r.cancelCtx
	r.RUnlock()
	cancelCtx()
}

func (r *EngineRestore) GetStopChan() chan struct{} {
	return r.stopChan
}

// Context returns the context that ends this restore cycle's backupstore
// goroutines.
func (r *EngineRestore) Context() context.Context {
	r.RLock()
	defer r.RUnlock()
	return r.ctx
}
