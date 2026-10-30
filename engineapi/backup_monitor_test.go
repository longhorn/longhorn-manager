package engineapi

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// fakeBackupStatusProxy answers SnapshotBackupStatus from a list of responses, repeating the last one.
// Any other EngineClientProxy call panics on the nil embedded interface, so a test that expects
// no SnapshotBackup call fails loudly if one is made.
type fakeBackupStatusProxy struct {
	EngineClientProxy

	lock      sync.Mutex
	calls     int
	responses []fakeBackupStatusResponse
	release   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

type fakeBackupStatusResponse struct {
	status *longhorn.EngineBackupStatus
	err    error
}

func newFakeBackupStatusProxy(release chan struct{}, responses ...fakeBackupStatusResponse) *fakeBackupStatusProxy {
	return &fakeBackupStatusProxy{responses: responses, release: release, closed: make(chan struct{})}
}

func (p *fakeBackupStatusProxy) SnapshotBackupStatus(obj DataEngineObject, backupName, replicaAddress, replicaName string) (*longhorn.EngineBackupStatus, error) {
	if p.release != nil {
		select {
		case <-p.release:
		case <-p.closed:
			return nil, fmt.Errorf("proxy closed")
		}
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	i := p.calls
	if i >= len(p.responses) {
		i = len(p.responses) - 1
	}
	p.calls++
	return p.responses[i].status, p.responses[i].err
}

func (p *fakeBackupStatusProxy) Close() {
	p.closeOnce.Do(func() { close(p.closed) })
}

func (p *fakeBackupStatusProxy) callCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.calls
}

func newInProgressBackup() *longhorn.Backup {
	return &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-1", Namespace: "longhorn-system"},
		Spec:       longhorn.BackupSpec{SnapshotName: "snap-1"},
		Status: longhorn.BackupStatus{
			State:          longhorn.BackupStateInProgress,
			Progress:       40,
			URL:            "s3://bucket@region/?backup=backup-1&volume=vol-1",
			SnapshotName:   "snap-1",
			ReplicaAddress: "tcp://10.0.0.1:10000",
			Messages:       map[string]string{"info": "in progress"},
		},
	}
}

func newTestBackupMonitor(t *testing.T, backup *longhorn.Backup, proxy EngineClientProxy, callback func(string)) *BackupMonitor {
	if callback == nil {
		callback = func(string) {}
	}
	m, err := NewBackupMonitor(logrus.New(), nil, backup, &longhorn.Volume{}, nil, "", "", 0, "",
		&longhorn.Engine{}, proxy, callback)
	require.NoError(t, err)
	t.Cleanup(m.Close)
	return m
}

// A monitor re-created for an in-progress backup (for example after a status-update conflict or
// a manager restart) must expose the last known status at once. Before its first poll, the
// controller copies this status into the Backup CR, and the recurring job fails if the snapshot
// name in it is empty (longhorn/longhorn#14226).
func TestNewBackupMonitorKeepsInProgressStatus(t *testing.T) {
	backup := newInProgressBackup()
	expected := *backup.Status.DeepCopy()

	// Block the engine status call so the test only sees the constructor's status.
	proxy := newFakeBackupStatusProxy(make(chan struct{}),
		fakeBackupStatusResponse{err: fmt.Errorf("unexpected status call")})
	m := newTestBackupMonitor(t, backup, proxy, nil)

	require.Equal(t, expected, m.GetBackupStatus())

	// The monitor must own its copy: later changes to the CR object must not leak into it.
	backup.Status.Messages["info"] = "changed"
	backup.Status.SnapshotName = "changed"
	require.Equal(t, expected, m.GetBackupStatus())
}

// After the engine reports a final state, the monitor must keep it. The controller keeps the
// monitor until the final state is persisted, and a later poll failure must not replace
// Completed with Error, or the post-backup cleanup would never run.
func TestBackupMonitorStopsPollingAfterFinalState(t *testing.T) {
	backup := newInProgressBackup()
	completed := &longhorn.EngineBackupStatus{
		Progress:       100,
		BackupURL:      backup.Status.URL,
		SnapshotName:   backup.Spec.SnapshotName,
		State:          backupStateComplete,
		ReplicaAddress: backup.Status.ReplicaAddress,
	}
	proxy := newFakeBackupStatusProxy(nil,
		fakeBackupStatusResponse{status: completed},
		fakeBackupStatusResponse{err: fmt.Errorf("transient engine error")})

	synced := make(chan struct{}, 10)
	m := newTestBackupMonitor(t, backup, proxy, func(string) { synced <- struct{}{} })

	select {
	case <-synced:
	case <-time.After(3 * BackupMonitorSyncPeriod):
		t.Fatal("timed out waiting for the first backup status poll")
	}
	require.Equal(t, longhorn.BackupStateCompleted, m.GetBackupStatus().State)

	// Wait past the next poll period: no further poll may happen, and the state must stay Completed.
	time.Sleep(BackupMonitorSyncPeriod + BackupMonitorSyncPeriod/2)
	require.Equal(t, 1, proxy.callCount())
	require.Equal(t, longhorn.BackupStateCompleted, m.GetBackupStatus().State)
}
