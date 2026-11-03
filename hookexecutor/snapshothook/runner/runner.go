// Package runner runs VolumeHookPolicy commands around SnapshotGroup
// snapshots. The pause runs pre-commands before any member snapshot is
// taken. The release runs post-commands once the group ends or its deadline
// passes. The group controller owns the group's lifecycle and snapshots;
// this package only runs the commands and records their outcomes.
package runner

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stypes "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/longhorn/longhorn-manager/hookexecutor/podexec"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// errWaiting marks a step that is waiting on the cluster, such as a pod
// still shutting down, and will be retried. The queue logs it as a wait
// rather than a failure. Wrap it with %w and say what is being waited on.
var errWaiting = errors.New("waiting")

const (
	// DefaultPauseWorkers is how many groups may run pre-commands at once
	// unless the executor command's --pause-workers flag says otherwise. It
	// is also the floor of the release pool.
	DefaultPauseWorkers = 5

	// writeAttempts is how many times one status write retries on Conflict.
	// The group controller writes the same status object, so an occasional
	// Conflict is expected. Repeated Conflicts mean something else is
	// wrong, and the queue's backoff should take over.
	writeAttempts = 3
)

// Runner runs the hook commands for hooked SnapshotGroups. One process
// holds the leader lease. Within it, the pause queue runs pre-commands and
// the release queue runs post-commands. Once a group's release is
// triggered, its pause work stops and can never resume. That block lives
// in memory and is keyed by UID, so it does not carry over to a new group
// created under the same name.
type Runner struct {
	logger       logrus.FieldLogger
	store        Store
	podExec      podexec.Executor
	recorder     record.EventRecorder
	now          func() time.Time
	pauseWorkers int
	// releaseWorkers is max(pauseWorkers, DefaultPauseWorkers): it grows
	// with the pause pool but never shrinks below the default. The pool is
	// separate so a release never waits behind pause work.
	releaseWorkers int

	pauseQueue   workqueue.TypedRateLimitingInterface[string]
	releaseQueue workqueue.TypedRateLimitingInterface[string]

	mu sync.Mutex
	// releaseGate holds the UIDs of groups whose release has been triggered.
	// The pause path checks it at entry and before each write.
	releaseGate map[k8stypes.UID]bool
	// pauseCancels holds, by group name, the cancel of a pause in flight.
	pauseCancels map[string]context.CancelFunc
	// groupLocks holds one lock per group name. The pause and the release
	// of a group both take it, so they never run at the same time: a
	// release that cancels a pause waits for that pause to finish first.
	// The lock is keyed by name, not UID, so this holds even when the
	// group is deleted and recreated under the same name in between.
	groupLocks map[string]*groupLock
}

type groupLock struct {
	mu   sync.Mutex
	refs int
}

// New builds a Runner. pauseWorkers bounds how many groups run
// pre-commands at once; a value below 1 falls back to DefaultPauseWorkers.
func New(logger logrus.FieldLogger, store Store, podExec podexec.Executor, recorder record.EventRecorder, pauseWorkers int) *Runner {
	if pauseWorkers < 1 {
		pauseWorkers = DefaultPauseWorkers
	}
	return &Runner{
		logger:         logger,
		store:          store,
		podExec:        podExec,
		recorder:       recorder,
		now:            time.Now,
		pauseWorkers:   pauseWorkers,
		releaseWorkers: max(pauseWorkers, DefaultPauseWorkers),
		pauseQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, snapshothook.MaxReleaseBackoff),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "snapshot-hook-pause"}),
		releaseQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Second, snapshothook.MaxReleaseBackoff),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "hook-release"}),
		releaseGate:  map[k8stypes.UID]bool{},
		pauseCancels: map[string]context.CancelFunc{},
		groupLocks:   map[string]*groupLock{},
	}
}

// Run subscribes to the SnapshotGroup informer, which the caller must
// start, and runs the workers until ctx ends.
func (r *Runner) Run(ctx context.Context, groupInformer cache.SharedIndexInformer) error {
	defer utilruntime.HandleCrash()
	defer r.pauseQueue.ShutDown()
	defer r.releaseQueue.ShutDown()

	registration, err := groupInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    r.onGroupChange,
		UpdateFunc: func(_, cur any) { r.onGroupChange(cur) },
		DeleteFunc: r.onGroupDelete,
	})
	if err != nil {
		return fmt.Errorf("failed to register snapshot group handler: %w", err)
	}
	defer func() { _ = groupInformer.RemoveEventHandler(registration) }()

	r.logger.Info("Waiting for the snapshot group cache to sync")
	if !cache.WaitForNamedCacheSync("hook-executor", ctx.Done(), groupInformer.HasSynced) {
		return fmt.Errorf("snapshot group cache did not sync")
	}
	r.logger.Info("Starting the hook executor")

	for i := 0; i < r.pauseWorkers; i++ {
		go wait.UntilWithContext(ctx, r.pauseWorker, time.Second)
	}
	for i := 0; i < r.releaseWorkers; i++ {
		go wait.UntilWithContext(ctx, r.releaseWorker, time.Second)
	}
	<-ctx.Done()
	r.logger.Info("Stopping the hook executor")
	return nil
}

// onGroupChange routes a hooked group on every add, update, and cache
// replay. A terminal or deleting group is released; any other has its
// release scheduled for its deadline and, while still RunningPreHooks, is
// paused.
func (r *Runner) onGroupChange(obj any) {
	group, ok := obj.(*longhorn.SnapshotGroup)
	if !ok {
		return
	}
	if !snapshothook.IsHooked(group) {
		return
	}
	if snapshothook.IsTerminal(group) || !group.DeletionTimestamp.IsZero() {
		// Check for recorded pods rather than member phase. A member can be
		// PreFailed with its pod still paused if the process crashed right
		// after the pause; that pod must still be released.
		if len(snapshothook.UnreleasedHookPods(group)) > 0 {
			r.releaseQueue.Add(group.Name)
		}
		return
	}
	deadline := snapshothook.GroupDeadline(group)
	if now := r.now(); now.After(deadline) {
		r.releaseQueue.Add(group.Name)
	} else {
		r.releaseQueue.AddAfter(group.Name, deadline.Sub(now)+time.Second)
	}
	if group.Status.Phase == longhorn.SnapshotGroupPhaseRunningPreHooks && !group.Status.PreHooksDone {
		r.pauseQueue.Add(group.Name)
	}
}

// onGroupDelete drops the group's pending work. An exec already in flight
// runs to completion; the release path accepts that its final status write
// has nothing left to write to. The group lock is not touched here: the
// pause holding it drops it when it returns.
func (r *Runner) onGroupDelete(obj any) {
	group, ok := obj.(*longhorn.SnapshotGroup)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		group, ok = tombstone.Obj.(*longhorn.SnapshotGroup)
		if !ok {
			return
		}
	}
	r.cancelPause(group.Name)
	r.pauseQueue.Forget(group.Name)
	r.releaseQueue.Forget(group.Name)
	r.mu.Lock()
	delete(r.releaseGate, group.UID)
	r.mu.Unlock()
}

func (r *Runner) pauseWorker(ctx context.Context) {
	for r.processNext(ctx, r.pauseQueue, r.pause) {
	}
}

func (r *Runner) releaseWorker(ctx context.Context) {
	for r.processNext(ctx, r.releaseQueue, r.release) {
	}
}

// processNext runs one item. An error or requeue puts it back with
// backoff; otherwise it is done.
func (r *Runner) processNext(ctx context.Context, queue workqueue.TypedRateLimitingInterface[string], handle func(context.Context, string) (requeue bool, err error)) bool {
	name, shutdown := queue.Get()
	if shutdown {
		return false
	}
	defer queue.Done(name)

	requeue, err := handle(ctx, name)
	switch {
	case err != nil:
		log := r.logger.WithField("snapshotGroup", name).WithError(err)
		switch {
		case apierrors.IsConflict(err):
			// The controller and the executor both write the group. A
			// Conflict that outlived the in-step retries is contention,
			// not a fault.
			log.Debug("Snapshot hook step lost a write race; retrying")
		case errors.Is(err, errWaiting):
			log.Debug("Snapshot hook step is waiting; retrying")
		default:
			log.Warn("Snapshot hook step failed; retrying")
		}
		queue.AddRateLimited(name)
	case requeue:
		queue.AddRateLimited(name)
	default:
		queue.Forget(name)
	}
	return true
}

// lockGroup takes the lock for the named group and returns the function
// that releases it. A lock entry is created on first use and removed when
// its last holder releases it.
func (r *Runner) lockGroup(name string) (unlock func()) {
	r.mu.Lock()
	lock, ok := r.groupLocks[name]
	if !ok {
		lock = &groupLock{}
		r.groupLocks[name] = lock
	}
	lock.refs++
	r.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		r.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(r.groupLocks, name)
		}
		r.mu.Unlock()
	}
}

func (r *Runner) isReleaseGated(uid k8stypes.UID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.releaseGate[uid]
}

// gateRelease marks the group released and cancels any pause in flight.
// It reports whether this call set the gate and whether a pause was
// cancelled. It does not wait for the pause to finish; callers wait by
// taking the group lock.
func (r *Runner) gateRelease(name string, uid k8stypes.UID) (newlyGated, pauseRunning bool) {
	r.mu.Lock()
	newlyGated = !r.releaseGate[uid]
	r.releaseGate[uid] = true
	cancel, pauseRunning := r.pauseCancels[name]
	r.mu.Unlock()
	if pauseRunning {
		cancel()
	}
	return newlyGated, pauseRunning
}

// beginPause wraps ctx in a cancellable context for the group's pause and
// registers its cancel so gateRelease can stop the pause. It returns the
// context and a cleanup that cancels and unregisters it.
func (r *Runner) beginPause(ctx context.Context, name string) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.pauseCancels[name] = cancel
	r.mu.Unlock()
	return ctx, func() {
		cancel()
		r.mu.Lock()
		if r.pauseCancels[name] != nil {
			delete(r.pauseCancels, name)
		}
		r.mu.Unlock()
	}
}

func (r *Runner) cancelPause(name string) {
	r.mu.Lock()
	cancel := r.pauseCancels[name]
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// getGroup reads the group from the cluster. A group that no longer exists
// comes back as nil with no error: there is nothing left to do, so callers
// stop rather than retry.
func (r *Runner) getGroup(ctx context.Context, name string) (*longhorn.SnapshotGroup, error) {
	group, err := r.store.GetSnapshotGroup(ctx, name)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return group, err
}

// writeStatus persists group status, refreshing the HOOKS printer column
// first.
func (r *Runner) writeStatus(ctx context.Context, group *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error) {
	refreshHookProgress(group)
	return r.store.UpdateSnapshotGroupStatus(ctx, group)
}

// writeFunc is a status write. The pause path passes one that gives up
// once the release has taken over the group.
type writeFunc func(context.Context, *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error)

// writeOutcomes persists the outcomes of one command round for one member.
// The commands have already run, so a failed write must not fall back to
// the queue: a retry could not tell which commands ran. On Conflict the
// group is re-read and the outcomes reapplied, but only if the member's
// record still matches `before`, the one the commands ran against. Any
// other change means another writer touched the record, and the round
// fails rather than guessing.
func (r *Runner) writeOutcomes(ctx context.Context, group *longhorn.SnapshotGroup, before *longhorn.HookStatus, write writeFunc) (*longhorn.SnapshotGroup, error) {
	after := hookStatusFor(group, before.SnapshotName).DeepCopy()
	for attempt := 1; ; attempt++ {
		written, err := write(ctx, group)
		if err == nil || !apierrors.IsConflict(err) || attempt >= writeAttempts {
			return written, err
		}
		fresh, err := r.store.GetSnapshotGroup(ctx, group.Name)
		if err != nil {
			return nil, err
		}
		current := snapshothook.FindHookStatus(fresh, before.SnapshotName)
		if current == nil || !reflect.DeepEqual(current, before) {
			return nil, fmt.Errorf("hook record of member %v changed underneath the command round", before.SnapshotName)
		}
		*current = *after.DeepCopy()
		group = fresh
	}
}

func (r *Runner) event(group *longhorn.SnapshotGroup, eventType, reason, format string, args ...any) {
	r.recorder.Eventf(group, eventType, reason, format, args...)
}

func (r *Runner) warn(group *longhorn.SnapshotGroup, reason, format string, args ...any) {
	r.event(group, corev1.EventTypeWarning, reason, format, args...)
}
