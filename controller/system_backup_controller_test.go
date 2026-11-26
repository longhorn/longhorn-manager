package controller

import (
	"archive/zip"
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"

	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/backupstore"

	systembackupstore "github.com/longhorn/backupstore/systembackup"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

type SystemBackupTestCase struct {
	state longhorn.SystemBackupState

	controllerID string

	notExist            bool
	isDeleting          bool
	isExistInRemote     bool
	systemBackupName    string
	systemBackupVersion string
	volumeBackupPolicy  longhorn.SystemBackupCreateVolumeBackupPolicy

	existPersistentVolumes map[SystemRolloutCRName]*corev1.PersistentVolume
	existVolumes           map[SystemRolloutCRName]*longhorn.Volume
	existBackingImages     map[SystemRolloutCRName]*longhorn.BackingImage
	existBackups           map[string]*longhorn.Backup
	existSnapshots         map[string]*longhorn.Snapshot

	expectError                 bool
	expectErrorConditionMessage string
	expectState                 longhorn.SystemBackupState
	expectRemove                bool
	expectNewVolumBackupCount   int

	// Runs after the volume backups are marked Completed and before
	// WaitForVolumeBackupToComplete, so a case can change the volume or
	// backup state that the wait loop will read.
	prepareVolumeBackupWait     volumeBackupWaitHook
	expectVolumeBackupWaitError bool
	// If set, exactly one warning containing this text must be logged while
	// WaitForVolumeBackupToComplete runs.
	expectVolumeBackupWaitWarnOnce string
}

type volumeBackupWaitHook func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, informerFactory lhinformers.SharedInformerFactory)

const (
	testBackupSnapshotTime = "2026-10-23T07:00:00Z"
	testBackupCreatedTime  = "2026-10-23T07:01:00Z"
	testEarlierTime        = "2026-10-23T06:00:00Z"
	testLaterTime          = "2026-10-23T08:00:00Z"
)

// waitForVolumeBackupsWithLogs runs WaitForVolumeBackupToComplete and
// captures the log entries it writes. Capture is removed from the shared
// logger on return so later tests are unaffected.
func waitForVolumeBackupsWithLogs(sbc *SystemBackupController, backups map[string]*longhorn.Backup, systemBackup *longhorn.SystemBackup) (*test.Hook, error) {
	logger := logrus.StandardLogger()
	capturedLogs := test.NewLocal(logger)
	defer logger.ReplaceHooks(make(logrus.LevelHooks))
	return capturedLogs, sbc.WaitForVolumeBackupToComplete(backups, systemBackup)
}

func countLogEntries(logs *test.Hook, level logrus.Level, substr string) int {
	n := 0
	for _, e := range logs.AllEntries() {
		if e.Level == level && strings.Contains(e.Message, substr) {
			n++
		}
	}
	return n
}

// otherVolumeBackup builds a backup of volumeName that the system backup did
// not create. Test cases set it as volume.Status.LastBackup to simulate the
// backup store reporting a different backup as the volume's last backup.
func otherVolumeBackup(name, volumeName string, state longhorn.BackupState, snapshotCreatedAt, backupCreatedAt string) *longhorn.Backup {
	b := newBackup(name)
	b.Labels = map[string]string{types.LonghornLabelBackupVolume: volumeName}
	b.Status = longhorn.BackupStatus{
		State:             state,
		VolumeName:        volumeName,
		SnapshotCreatedAt: snapshotCreatedAt,
		BackupCreatedAt:   backupCreatedAt,
	}
	return b
}

// backupOfVolume returns the system backup's own backup of volumeName.
func backupOfVolume(c *C, backups map[string]*longhorn.Backup, volumeName string) *longhorn.Backup {
	for _, b := range backups {
		if b.Status.VolumeName == volumeName {
			return b
		}
	}
	c.Fatalf("no backup of volume %v in %v", volumeName, backups)
	return nil
}

// setVolumeLastBackup sets volume.Status.LastBackup and LastBackupAt, which
// is what the backup store would report as the volume's last backup.
func setVolumeLastBackup(volumes map[SystemRolloutCRName]*longhorn.Volume, volumeName, lastBackup, lastBackupAt string) {
	volume := volumes[SystemRolloutCRName(volumeName)]
	volume.Status.LastBackup = lastBackup
	volume.Status.LastBackupAt = lastBackupAt
}

func (s *TestSuite) TestReconcileSystemBackup(c *C) {
	datastore.SkipListerCheck = true
	datastore.SystemBackupTimeout = 10 * time.Second
	datastore.VolumeBackupTimeout = 10 * time.Second

	rolloutOwnerID := TestNode1

	tempDirs := []string{}
	defer func() {
		for _, dir := range tempDirs {
			err := os.RemoveAll(dir)
			c.Assert(err, IsNil)
		}
	}()

	testCases := map[string]SystemBackupTestCase{
		"system backup create": {
			state:       longhorn.SystemBackupStateNone,
			expectState: longhorn.SystemBackupStateVolumeBackup,
		},
		"system backup create list backup failed": {
			systemBackupName: TestSystemBackupNameListFailed,
			state:            longhorn.SystemBackupStateNone,
			expectState:      longhorn.SystemBackupStateError,
		},
		"system backup create volume backup if-not-present": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "",
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		// Test cases named "system backup wait for volume backup:" cover
		// WaitForVolumeBackupToComplete. They use the if-not-present policy on a
		// volume with no backup so BackupVolumes creates exactly one backup; the
		// policy itself is not under test. That backup has already completed, and
		// each case then changes the volume status so a different backup shows up
		// as the volume's last backup.
		"system backup wait for volume backup: volume status already names our backup": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup wait for volume backup: another backup with a newer snapshot is last": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				setVolumeLastBackup(volumes, TestVolumeName, "other", testLaterTime)
			},
		},
		"system backup wait for volume backup: another backup of the same snapshot time is last": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				setVolumeLastBackup(volumes, TestVolumeName, "other", testBackupSnapshotTime)
			},
		},
		"system backup wait for volume backup: snapshot CR was deleted before the check": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, informerFactory lhinformers.SharedInformerFactory) {
				backup := backupOfVolume(c, backups, TestVolumeName)
				indexer := informerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
				snapshot, exists, err := indexer.GetByKey(TestNamespace + "/" + backup.Status.SnapshotName)
				c.Assert(err, IsNil)
				c.Assert(exists, Equals, true)
				c.Assert(indexer.Delete(snapshot), IsNil)
			},
		},
		"system backup wait for volume backup: older backup that finished after ours is last": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 2, // own backup plus the injected "older" one
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				setVolumeLastBackup(volumes, TestVolumeName, "older", testEarlierTime)
				backups["older"] = otherVolumeBackup("older", TestVolumeName, longhorn.BackupStateCompleted, testEarlierTime, testLaterTime)
			},
			expectVolumeBackupWaitWarnOnce: "as synced",
		},
		"system backup wait for volume backup: older backup that finished before ours is last": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateError,
			expectNewVolumBackupCount: 2, // own backup plus the injected "older" one
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				// Stale volume status: the next sync will name our backup, so keep waiting.
				setVolumeLastBackup(volumes, TestVolumeName, "older", testEarlierTime)
				backups["older"] = otherVolumeBackup("older", TestVolumeName, longhorn.BackupStateCompleted, testEarlierTime, testEarlierTime)
			},
			expectVolumeBackupWaitError: true,
		},
		"system backup wait for volume backup: older backup is last but its CR is missing": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateError,
			expectNewVolumBackupCount: 1,
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				setVolumeLastBackup(volumes, TestVolumeName, "older", testEarlierTime)
			},
			expectVolumeBackupWaitError: true,
		},
		"system backup wait for volume backup: older backup is last but still in progress": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateError,
			expectNewVolumBackupCount: 2, // own backup plus the injected "older" one
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				setVolumeLastBackup(volumes, TestVolumeName, "older", testEarlierTime)
				backups["older"] = otherVolumeBackup("older", TestVolumeName, longhorn.BackupStateInProgress, testEarlierTime, "")
			},
			expectVolumeBackupWaitError: true,
		},
		"system backup wait for volume backup: volume status has no lastBackupAt yet": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateError,
			expectNewVolumBackupCount: 1,
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				setVolumeLastBackup(volumes, TestVolumeName, "other", "")
			},
			expectVolumeBackupWaitError: true,
		},
		"system backup wait for volume backup: backup status has no volumeName": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				backupOfVolume(c, backups, TestVolumeName).Status.VolumeName = ""
			},
		},
		"system backup wait for volume backup: older backup accepted once while another volume never syncs": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName):        {},
				SystemRolloutCRName(TestVolumeName + "-2"): {},
			},
			expectState:               longhorn.SystemBackupStateError,
			expectNewVolumBackupCount: 3, // two own backups plus the injected "older" one
			prepareVolumeBackupWait: func(c *C, backups map[string]*longhorn.Backup, volumes map[SystemRolloutCRName]*longhorn.Volume, _ lhinformers.SharedInformerFactory) {
				setVolumeLastBackup(volumes, TestVolumeName, "older", testEarlierTime)
				backups["older"] = otherVolumeBackup("older", TestVolumeName, longhorn.BackupStateCompleted, testEarlierTime, testLaterTime)
				setVolumeLastBackup(volumes, TestVolumeName+"-2", "other", "")
			},
			expectVolumeBackupWaitError:    true,
			expectVolumeBackupWaitWarnOnce: "as synced",
		},
		"system backup create volume backup if-not-present when backup exists": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 0,
		},
		"system backup create volume backup if-not-present when snapshot creationTime is not set": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse: true,
						// CreationTime is not set
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup create volume backup if-not-present when snapshot creationTime is null": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse:   true,
						CreationTime: "null",
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup create volume backup if-not-present when an older snapshot has no new data": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse:   true,
						Size:         1,
						CreationTime: metav1.Now().Format(time.RFC3339),
					},
				},
				"older": {
					ObjectMeta: metav1.ObjectMeta{Name: "older"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse:   true,
						Size:         1,
						CreationTime: metav1.NewTime(time.Now().Add(-time.Hour)).Format(time.RFC3339),
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 0,
		},
		"system backup create volume backup if-not-present when a snapshot outside the last backup chain has no creationTime": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse:   true,
						Size:         1,
						CreationTime: metav1.Now().Format(time.RFC3339),
					},
				},
				"leftover": {
					ObjectMeta: metav1.ObjectMeta{Name: "leftover"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse: true,
						Size:       1,
						// CreationTime is not set
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup skip volume backup if-not-present when only an ancestor of the last backup snapshot has no creationTime": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						Parent:       "restored",
						ReadyToUse:   true,
						Size:         1,
						CreationTime: metav1.Now().Format(time.RFC3339),
					},
				},
				"restored": {
					ObjectMeta: metav1.ObjectMeta{Name: "restored"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse: true,
						Size:       1,
						// CreationTime is not set, as for the first snapshot of a restored or cloned V1 volume
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 0,
		},
		"system backup skip volume backup if-not-present when an ancestor two snapshots up the chain has no creationTime": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						Parent:       "middle",
						ReadyToUse:   true,
						Size:         1,
						CreationTime: metav1.Now().Format(time.RFC3339),
					},
				},
				"middle": {
					ObjectMeta: metav1.ObjectMeta{Name: "middle"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						Parent:       "restored",
						ReadyToUse:   true,
						Size:         1,
						CreationTime: metav1.NewTime(time.Now().Add(-time.Hour)).Format(time.RFC3339),
					},
				},
				"restored": {
					ObjectMeta: metav1.ObjectMeta{Name: "restored"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse: true,
						Size:       1,
						// CreationTime is not set
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 0,
		},
		"system backup create volume backup always": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyAlways,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup create backingimage backup": {
			state:       longhorn.SystemBackupStateBackingImageBackup,
			expectState: longhorn.SystemBackupStateGenerating,
		},
		"system backup generate": {
			state:       longhorn.SystemBackupStateGenerating,
			expectState: longhorn.SystemBackupStateUploading,
		},
		"system backup upload": {
			state:       longhorn.SystemBackupStateUploading,
			expectState: longhorn.SystemBackupStateReady,
		},
		"system backup upload file exceed timeout": {
			systemBackupName: TestSystemBackupNameUploadExceedTimeout,
			state:            longhorn.SystemBackupStateUploading,
			expectState:      longhorn.SystemBackupStateReady,
		},
		"system backup upload file failed": {
			systemBackupName:            TestSystemBackupNameUploadFailed,
			state:                       longhorn.SystemBackupStateUploading,
			expectState:                 longhorn.SystemBackupStateError,
			expectErrorConditionMessage: fmt.Sprintf("%v:", SystemBackupErrTimeoutUpload),
		},
		"system backup upload get config failed": {
			systemBackupName:            TestSystemBackupNameGetConfigFailed,
			state:                       longhorn.SystemBackupStateUploading,
			expectState:                 longhorn.SystemBackupStateError,
			expectErrorConditionMessage: fmt.Sprintf("%v: %v:", SystemBackupErrTimeoutUpload, SystemBackupErrGetConfig),
		},
		"system backup ready": {
			state: longhorn.SystemBackupStateReady,
		},
		"system backup error": {
			state: longhorn.SystemBackupStateError,
		},
		"system backup with deletion timestamp": {
			state:       longhorn.SystemBackupStateReady,
			isDeleting:  true,
			expectState: longhorn.SystemBackupStateDeleting,
		},
		"system backup delete": {
			state:        longhorn.SystemBackupStateDeleting,
			isDeleting:   true,
			expectRemove: true,
		},
		"system backup from backup target": {
			state:               longhorn.SystemBackupStateNone,
			expectState:         longhorn.SystemBackupStateSyncing,
			systemBackupVersion: TestSystemBackupLonghornVersion,
		},
		"system backup syncing": {
			state:               longhorn.SystemBackupStateSyncing,
			expectState:         longhorn.SystemBackupStateReady,
			systemBackupVersion: TestSystemBackupLonghornVersion,
		},
		"system backup exist in remote": {
			state:           longhorn.SystemBackupStateNone,
			expectState:     longhorn.SystemBackupStateNone,
			isExistInRemote: true,
		},
		"system backup PersistentVolume source not from CSI": {
			state:       longhorn.SystemBackupStateGenerating,
			expectState: longhorn.SystemBackupStateUploading,

			existPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName,
							Namespace: TestNamespace,
						},
						StorageClassName: TestStorageClassName,
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/fake",
							},
						},
					},
				},
			},
		},
	}

	for name, tc := range testCases {
		if tc.systemBackupName == "" {
			tc.systemBackupName = TestSystemBackupName
		}

		if tc.expectState == "" {
			tc.expectState = tc.state
		}

		if tc.controllerID == "" {
			tc.controllerID = rolloutOwnerID
		}

		backupTargetClient := &FakeSystemBackupTargetClient{
			name: tc.systemBackupName,
		}
		if tc.isExistInRemote {
			backupTargetClient.version = TestSystemBackupLonghornVersion
		}

		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutNamespace(c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutSettingDefaultEngineImage(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutStorageClassesDefault(c, informerFactories.KubeInformerFactory, kubeClient)

		fakeSystemRolloutVolumes(tc.existVolumes, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackups(tc.existBackups, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackingImages(tc.existBackingImages, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutPersistentVolumes(tc.existPersistentVolumes, c, informerFactories.KubeInformerFactory, kubeClient)

		systemBackupController, err := newFakeSystemBackupController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(err, IsNil)

		systemBackup := fakeSystemBackup(tc.systemBackupName, rolloutOwnerID, tc.systemBackupVersion, tc.isDeleting, tc.volumeBackupPolicy, tc.state, c, informerFactories.LhInformerFactory, lhClient)
		if tc.notExist {
			systemBackup = fakeSystemBackup("none", rolloutOwnerID, tc.systemBackupVersion, tc.isDeleting, tc.volumeBackupPolicy, tc.state, c, informerFactories.LhInformerFactory, lhClient)
		}

		systemBackupTempDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("*-%v", TestSystemBackupName))
		c.Assert(err, IsNil)
		tempDirs = append(tempDirs, systemBackupTempDir)

		archievePath := filepath.Join(systemBackupTempDir, tc.systemBackupName+".zip")
		tempDir := filepath.Join(systemBackupTempDir, tc.systemBackupName)

		switch systemBackup.Status.State {
		case longhorn.SystemBackupStateVolumeBackup:
			if tc.existBackups != nil {
				existBackupSnapshots := make(map[string]*longhorn.Snapshot)
				if tc.existSnapshots == nil {
					for _, backup := range tc.existBackups {
						existBackupSnapshots[backup.Status.SnapshotName] = &longhorn.Snapshot{
							ObjectMeta: metav1.ObjectMeta{Name: backup.Status.SnapshotName},
							Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
							Status: longhorn.SnapshotStatus{
								ReadyToUse:   true,
								CreationTime: metav1.Now().Format(time.RFC3339),
							},
						}
					}
				} else {
					existBackupSnapshots = tc.existSnapshots
				}

				for _, existBackupSnap := range existBackupSnapshots {
					fakeSystemRolloutSnapshot(existBackupSnap, c, informerFactories.LhInformerFactory, lhClient)
				}
			}
			backups, err := systemBackupController.BackupVolumes(systemBackup)
			if tc.expectError {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
			}

			for _, backup := range backups {
				snapshot, err := lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), backup.Spec.SnapshotName, metav1.GetOptions{})
				c.Assert(err, IsNil)
				backup.Labels = map[string]string{types.LonghornLabelBackupVolume: snapshot.Spec.Volume}
				backup.Status.State = longhorn.BackupStateCompleted
				backup.Status.SnapshotName = backup.Spec.SnapshotName
				backup.Status.VolumeName = snapshot.Spec.Volume
				backup.Status.SnapshotCreatedAt = testBackupSnapshotTime
				backup.Status.BackupCreatedAt = testBackupCreatedTime
				setVolumeLastBackup(tc.existVolumes, snapshot.Spec.Volume, backup.Name, testBackupSnapshotTime)
				fakeSystemRolloutSnapshot(snapshot, c, informerFactories.LhInformerFactory, lhClient)
			}
			waitBackups := maps.Clone(backups)
			// Keep this after the loop above. The loop looks up each volume through
			// its Snapshot CR, and the "snapshot CR was deleted" case removes that CR
			// from the lister to prove the wait loop no longer depends on it.
			if tc.prepareVolumeBackupWait != nil {
				tc.prepareVolumeBackupWait(c, backups, tc.existVolumes, informerFactories.LhInformerFactory)
			}
			fakeSystemRolloutVolumes(tc.existVolumes, c, informerFactories.LhInformerFactory, lhClient)
			fakeSystemRolloutBackups(backups, c, informerFactories.LhInformerFactory, lhClient)

			capturedLogs, err := waitForVolumeBackupsWithLogs(systemBackupController, waitBackups, systemBackup)
			if tc.expectVolumeBackupWaitError {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
			}
			if tc.expectVolumeBackupWaitWarnOnce != "" {
				c.Assert(countLogEntries(capturedLogs, logrus.WarnLevel, tc.expectVolumeBackupWaitWarnOnce), Equals, 1)
			}

		case longhorn.SystemBackupStateBackingImageBackup:
			backupBackingImages, _ := systemBackupController.BackupBackingImage()
			for _, backupBackingImage := range backupBackingImages {
				backupBackingImage.Status.State = longhorn.BackupStateCompleted
			}
			fakeSystemRolloutBackupBackingImages(backupBackingImages, c, informerFactories.LhInformerFactory, lhClient)
			systemBackupController.WaitForBackingImageBackupToComplete(backupBackingImages, systemBackup)

		case longhorn.SystemBackupStateGenerating:
			systemBackupController.GenerateSystemBackup(systemBackup, archievePath, tempDir)

		case longhorn.SystemBackupStateUploading:
			systemBackupController.UploadSystemBackup(systemBackup, archievePath, tempDir, backupTargetClient)

		default:
			defaultBackupTarget, err := lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
			c.Assert(err, IsNil)
			err = systemBackupController.reconcile(tc.systemBackupName, backupTargetClient, defaultBackupTarget)
			if tc.expectError {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
			}
		}

		systemBackup, err = lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), tc.systemBackupName, metav1.GetOptions{})
		if tc.notExist {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(systemBackup.Status.State, Equals, tc.expectState)
		}

		checkFinalizer := !util.FinalizerExists(longhornFinalizerKey, systemBackup) == (tc.expectRemove || tc.notExist)
		c.Assert(checkFinalizer, Equals, true)

		if tc.expectErrorConditionMessage != "" {
			errCondition := types.GetCondition(systemBackup.Status.Conditions, longhorn.SystemBackupConditionTypeError)
			c.Assert(errCondition.Status, Equals, longhorn.ConditionStatusTrue)
			c.Assert(strings.HasPrefix(errCondition.Message, tc.expectErrorConditionMessage), Equals, true)
		}

		if tc.isExistInRemote {
			c.Assert(systemBackup.Labels[types.GetVersionLabelKey()], NotNil)
		}

		volumeBackups, err := lhClient.LonghornV1beta2().Backups(TestNamespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(len(volumeBackups.Items), Equals, tc.expectNewVolumBackupCount)
	}
}

func newFakeSystemBackupController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*SystemBackupController, error) {
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	c, err := NewSystemBackupController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID, TestManagerImage)
	if err != nil {
		return nil, err
	}
	c.eventRecorder = record.NewFakeRecorder(100)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	return c, nil
}

func fakeSystemBackup(name, currentOwnerID, longhornVersion string, isDeleting bool,
	volumeBackupPolicy longhorn.SystemBackupCreateVolumeBackupPolicy,
	state longhorn.SystemBackupState, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) *longhorn.SystemBackup {
	if volumeBackupPolicy == "" {
		volumeBackupPolicy = longhorn.SystemBackupCreateVolumeBackupPolicyDisabled
	}
	systemBackup := newSystemBackup(name, currentOwnerID, longhornVersion, volumeBackupPolicy, state)

	err := util.AddFinalizer(longhornFinalizerKey, systemBackup)
	c.Assert(err, IsNil)

	if isDeleting {
		now := metav1.NewTime(time.Now())
		systemBackup.DeletionTimestamp = &now
	}

	systemBackup, err = client.LonghornV1beta2().SystemBackups(TestNamespace).Create(context.TODO(), systemBackup, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	indexer := informerFactory.Longhorn().V1beta2().SystemBackups().Informer().GetIndexer()
	err = indexer.Add(systemBackup)
	c.Assert(err, IsNil)

	return systemBackup
}

func fakeSystemRolloutNamespace(c *C, informerFactory informers.SharedInformerFactory, kubeClient *fake.Clientset) {
	nsIndexer := informerFactory.Core().V1().Namespaces().Informer().GetIndexer()
	namespace, err := kubeClient.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestNamespace,
		},
	}, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = nsIndexer.Add(namespace)
	c.Assert(err, IsNil)
}

type systemBackupChildrenTestEnv struct {
	lhClient          *lhfake.Clientset
	informerFactories *util.InformerFactories
	controller        *SystemBackupController
	volumes           map[SystemRolloutCRName]*longhorn.Volume
}

func newSystemBackupChildrenTestEnv(c *C, autoCleanupSnapshot string) *systemBackupChildrenTestEnv {
	datastore.SkipListerCheck = true
	datastore.SystemBackupTimeout = 10 * time.Second
	datastore.VolumeBackupTimeout = 10 * time.Second

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	fakeSystemRolloutNamespace(c, informerFactories.KubeInformerFactory, kubeClient)
	fakeSystemRolloutSettingDefaultEngineImage(c, informerFactories.LhInformerFactory, lhClient)
	fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)
	// Cleanup deletes the archive of a deleting SystemBackup only when the
	// backup target is available.
	backupTarget, err := lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	backupTarget.Spec.BackupTargetURL = TestBackupTarget
	backupTarget.Status.Available = true
	backupTarget, err = lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Update(context.TODO(), backupTarget, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(informerFactories.LhInformerFactory.Longhorn().V1beta2().BackupTargets().Informer().GetIndexer().Update(backupTarget), IsNil)
	fakeSystemRolloutStorageClassesDefault(c, informerFactories.KubeInformerFactory, kubeClient)
	fakeSystemRolloutSettings(map[SystemRolloutCRName]*longhorn.Setting{
		SystemRolloutCRName(types.SettingNameAutoCleanupRecurringJobBackupSnapshot): {
			Value: autoCleanupSnapshot,
		},
	}, c, informerFactories.LhInformerFactory, lhClient)

	volumes := map[SystemRolloutCRName]*longhorn.Volume{
		SystemRolloutCRName(TestVolumeName): {
			Status: longhorn.VolumeStatus{},
		},
	}
	fakeSystemRolloutVolumes(volumes, c, informerFactories.LhInformerFactory, lhClient)

	systemBackupController, err := newFakeSystemBackupController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	return &systemBackupChildrenTestEnv{
		lhClient:          lhClient,
		informerFactories: informerFactories,
		controller:        systemBackupController,
		volumes:           volumes,
	}
}

func (env *systemBackupChildrenTestEnv) addSystemBackup(c *C, name string, policy longhorn.SystemBackupCreateVolumeBackupPolicy, state longhorn.SystemBackupState) *longhorn.SystemBackup {
	return fakeSystemBackup(name, TestNode1, "", state == longhorn.SystemBackupStateDeleting, policy, state, c, env.informerFactories.LhInformerFactory, env.lhClient)
}

// addSnapshot adds a snapshot of TestVolumeName. A snapshot with owners is
// labeled as created by a SystemBackup.
func (env *systemBackupChildrenTestEnv) addSnapshot(c *C, name, creationTime string, owners ...string) {
	snapshot := newSnapshot(name)
	snapshot.Labels = types.GetVolumeLabels(TestVolumeName)
	if len(owners) > 0 {
		maps.Copy(snapshot.Labels, types.GetSystemBackupLabels())
	}
	snapshot.Spec.Volume = TestVolumeName
	snapshot.Status.ReadyToUse = true
	snapshot.Status.CreationTime = creationTime
	snapshot.Status.Owners = owners
	fakeSystemRolloutSnapshotWithMetadata(snapshot, c, env.informerFactories.LhInformerFactory, env.lhClient)
}

// addBackup adds a completed backup of TestVolumeName from the snapshot with
// the same name. A backup with owners is labeled as created by a SystemBackup.
func (env *systemBackupChildrenTestEnv) addBackup(c *C, name string, owners ...string) {
	backup := newBackup(name)
	backup.Labels = map[string]string{types.LonghornLabelBackupVolume: TestVolumeName}
	if len(owners) > 0 {
		maps.Copy(backup.Labels, types.GetSystemBackupLabels())
	}
	backup.Spec.SnapshotName = name
	backup.Status.SnapshotName = name
	backup.Status.VolumeName = TestVolumeName
	backup.Status.State = longhorn.BackupStateCompleted
	backup.Status.Owners = owners
	fakeSystemRolloutBackupWithMetadata(backup, c, env.informerFactories.LhInformerFactory, env.lhClient)
}

// syncCache copies the Backup and Snapshot CRs from the client to the informer
// cache, which no informer updates in these tests.
func (env *systemBackupChildrenTestEnv) syncCache(c *C) {
	backups, err := env.lhClient.LonghornV1beta2().Backups(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	backupIndexer := env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer()
	for i := range backups.Items {
		c.Assert(backupIndexer.Update(&backups.Items[i]), IsNil)
	}

	snapshots, err := env.lhClient.LonghornV1beta2().Snapshots(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	snapshotIndexer := env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
	for i := range snapshots.Items {
		c.Assert(snapshotIndexer.Update(&snapshots.Items[i]), IsNil)
	}
}

func (env *systemBackupChildrenTestEnv) getSnapshot(name string) (*longhorn.Snapshot, error) {
	return env.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (env *systemBackupChildrenTestEnv) getBackup(name string) (*longhorn.Backup, error) {
	return env.lhClient.LonghornV1beta2().Backups(TestNamespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// setVolumeFromBackup writes Spec.FromBackup on TestVolumeName. The shared
// fakeSystemRolloutVolumes helper copies Status only.
func (env *systemBackupChildrenTestEnv) setVolumeFromBackup(c *C, fromBackup string) {
	volume, err := env.lhClient.LonghornV1beta2().Volumes(TestNamespace).Get(context.TODO(), TestVolumeName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	volume.Spec.FromBackup = fromBackup
	volume, err = env.lhClient.LonghornV1beta2().Volumes(TestNamespace).Update(context.TODO(), volume, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer().Update(volume), IsNil)
}

func (s *TestSuite) TestSystemBackupCreatedChildrenHaveSystemBackupOwner(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateVolumeBackup)

	backups, err := env.controller.BackupVolumes(systemBackup)
	c.Assert(err, IsNil)
	c.Assert(len(backups), Equals, 1)

	for _, created := range backups {
		backup, err := env.getBackup(created.Name)
		c.Assert(err, IsNil)
		c.Assert(types.IsCreatedBySystemBackup(backup.Labels), Equals, true)
		c.Assert(backup.Status.Owners, DeepEquals, []string{systemBackup.Name})

		snapshot, err := env.getSnapshot(backup.Spec.SnapshotName)
		c.Assert(err, IsNil)
		c.Assert(types.IsCreatedBySystemBackup(snapshot.Labels), Equals, true)
		c.Assert(snapshot.Status.Owners, DeepEquals, []string{systemBackup.Name})
	}
}

func (s *TestSuite) TestSystemBackupNameLongerThanLabelValueLimit(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	longName := strings.Repeat("a", 100)
	systemBackup := env.addSystemBackup(c, longName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateVolumeBackup)

	backups, err := env.controller.BackupVolumes(systemBackup)
	c.Assert(err, IsNil)
	c.Assert(len(backups), Equals, 1)
	for _, created := range backups {
		backup, err := env.getBackup(created.Name)
		c.Assert(err, IsNil)
		c.Assert(backup.Status.Owners, DeepEquals, []string{longName})
	}
}

func (s *TestSuite) TestSystemBackupWaitForVolumeBackupToCompleteAutoCleanup(c *C) {
	testCases := map[string]struct {
		autoCleanupSnapshot     string
		expectOlderSnapshotKept bool
	}{
		"auto cleanup enabled": {
			autoCleanupSnapshot:     "true",
			expectOlderSnapshotKept: false,
		},
		"auto cleanup disabled": {
			autoCleanupSnapshot:     "false",
			expectOlderSnapshotKept: true,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		env := newSystemBackupChildrenTestEnv(c, tc.autoCleanupSnapshot)
		systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateVolumeBackup)

		env.addSnapshot(c, "system-backup-older", testEarlierTime, "older-system-backup")
		env.addSnapshot(c, "user-snapshot", testEarlierTime)

		backups, err := env.controller.BackupVolumes(systemBackup)
		c.Assert(err, IsNil)
		c.Assert(len(backups), Equals, 1)

		var latestSnapshotName string
		for _, backup := range backups {
			latestSnapshotName = backup.Spec.SnapshotName
			backup.Status.State = longhorn.BackupStateCompleted
			backup.Status.SnapshotName = backup.Spec.SnapshotName
			backup.Status.VolumeName = TestVolumeName
			env.addSnapshot(c, latestSnapshotName, testBackupSnapshotTime, systemBackup.Name)
			env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = backup.Name
		}
		fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)
		fakeSystemRolloutBackups(backups, c, env.informerFactories.LhInformerFactory, env.lhClient)
		env.addBackup(c, "system-backup-older", "older-system-backup")
		// Another SystemBackup has not created the backup of this snapshot yet.
		env.addSnapshot(c, "system-backup-pending", testEarlierTime, "pending-system-backup")

		err = env.controller.WaitForVolumeBackupToComplete(backups, systemBackup)
		c.Assert(err, IsNil)

		_, err = env.getSnapshot(latestSnapshotName)
		c.Assert(err, IsNil, Commentf("the snapshot of the latest backup is the if-not-present baseline"))

		_, err = env.getSnapshot("user-snapshot")
		c.Assert(err, IsNil)

		_, err = env.getSnapshot("system-backup-pending")
		c.Assert(err, IsNil, Commentf("a snapshot without a finished backup is kept"))

		_, err = env.getSnapshot("system-backup-older")
		if tc.expectOlderSnapshotKept {
			c.Assert(err, IsNil)
		} else {
			c.Assert(apierrors.IsNotFound(err), Equals, true)
		}
	}
}

func (s *TestSuite) TestSystemBackupIfNotPresentReusesBackup(c *C) {
	testCases := map[string]struct {
		autoCleanupSnapshot  string
		expectProbeSnapshots int
	}{
		"auto cleanup enabled": {
			autoCleanupSnapshot:  "true",
			expectProbeSnapshots: 0,
		},
		"auto cleanup disabled": {
			autoCleanupSnapshot:  "false",
			expectProbeSnapshots: 2,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		env := newSystemBackupChildrenTestEnv(c, tc.autoCleanupSnapshot)

		creator := "creator-system-backup"
		env.addSnapshot(c, "system-backup-reused", testBackupSnapshotTime, creator)
		env.addBackup(c, "system-backup-reused", creator)
		env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = "system-backup-reused"
		fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)

		// The second run checks that the volume is still detected as unchanged
		// after the first run cleaned up its snapshot.
		reusers := []string{"reuser-system-backup-1", "reuser-system-backup-2"}
		for _, reuser := range reusers {
			systemBackup := env.addSystemBackup(c, reuser, longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent, longhorn.SystemBackupStateVolumeBackup)
			backups, err := env.controller.BackupVolumes(systemBackup)
			c.Assert(err, IsNil)
			c.Assert(len(backups), Equals, 0)
			env.syncCache(c)
		}

		backup, err := env.getBackup("system-backup-reused")
		c.Assert(err, IsNil)
		c.Assert(backup.Status.Owners, DeepEquals, append([]string{creator}, reusers...))

		snapshot, err := env.getSnapshot("system-backup-reused")
		c.Assert(err, IsNil)
		c.Assert(snapshot.Status.Owners, DeepEquals, append([]string{creator}, reusers...))

		snapshots, err := env.lhClient.LonghornV1beta2().Snapshots(TestNamespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(len(snapshots.Items), Equals, 1+tc.expectProbeSnapshots)
	}
}

func (s *TestSuite) TestSystemBackupDeletingStateCleansAssociatedBackupAndSnapshotCRs(c *C) {
	testCases := map[string]struct {
		owners            []string
		deleteCROnly      bool
		expectDeleted     bool
		expectOwnersAfter []string
	}{
		"last owner deletes the backup and snapshot": {
			owners:        []string{TestSystemBackupName},
			expectDeleted: true,
		},
		"shared owner keeps the backup and snapshot": {
			owners:            []string{TestSystemBackupName, "newer-system-backup"},
			expectOwnersAfter: []string{"newer-system-backup"},
		},
		"not an owner keeps the backup and snapshot": {
			owners:            []string{"other-system-backup"},
			expectOwnersAfter: []string{"other-system-backup"},
		},
		"custom resource only deletion keeps the backup and snapshot": {
			owners:            []string{TestSystemBackupName},
			deleteCROnly:      true,
			expectOwnersAfter: []string{TestSystemBackupName},
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		env := newSystemBackupChildrenTestEnv(c, "true")
		systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
		if tc.deleteCROnly {
			systemBackup.Labels = map[string]string{types.GetLonghornLabelKey(types.DeleteCustomResourceOnly): "true"}
			var err error
			systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Update(context.TODO(), systemBackup, metav1.UpdateOptions{})
			c.Assert(err, IsNil)
			err = env.informerFactories.LhInformerFactory.Longhorn().V1beta2().SystemBackups().Informer().GetIndexer().Update(systemBackup)
			c.Assert(err, IsNil)
		}

		env.addSnapshot(c, "system-backup-child", testBackupSnapshotTime, tc.owners...)
		env.addBackup(c, "system-backup-child", tc.owners...)

		defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: systemBackup.Name}, defaultBackupTarget)
		c.Assert(err, IsNil)

		systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, false)

		backup, backupErr := env.getBackup("system-backup-child")
		snapshot, snapshotErr := env.getSnapshot("system-backup-child")
		if tc.expectDeleted {
			c.Assert(apierrors.IsNotFound(backupErr), Equals, true)
			c.Assert(apierrors.IsNotFound(snapshotErr), Equals, true)
			continue
		}
		c.Assert(backupErr, IsNil)
		c.Assert(snapshotErr, IsNil)
		c.Assert(backup.Status.Owners, DeepEquals, tc.expectOwnersAfter)
		c.Assert(snapshot.Status.Owners, DeepEquals, tc.expectOwnersAfter)
	}
}

func (s *TestSuite) TestSystemBackupArchiveAddsLastBackupOwners(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")

	creator := "creator-system-backup"
	env.addSnapshot(c, "system-backup-last", testBackupSnapshotTime, creator)
	env.addBackup(c, "system-backup-last", creator)
	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = "system-backup-last"
	fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)

	// The disabled policy creates no volume backup, but the archive still
	// references the volume's last backup.
	archiver := env.addSystemBackup(c, "archiver-system-backup", longhorn.SystemBackupCreateVolumeBackupPolicyDisabled, longhorn.SystemBackupStateGenerating)
	_, err := env.controller.getVolumesAndAddLastBackupOwners(archiver.Name)
	c.Assert(err, IsNil)

	backup, err := env.getBackup("system-backup-last")
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{creator, archiver.Name})

	snapshot, err := env.getSnapshot("system-backup-last")
	c.Assert(err, IsNil)
	c.Assert(snapshot.Status.Owners, DeepEquals, []string{creator, archiver.Name})
}

func (s *TestSuite) TestSystemBackupArchiveFailsOnMissingLastBackup(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyDisabled, longhorn.SystemBackupStateGenerating)

	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = "system-backup-missing"
	fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)

	_, err := env.controller.getVolumesAndAddLastBackupOwners(TestSystemBackupName)
	c.Assert(err, NotNil)
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

func (s *TestSuite) TestSystemBackupArchiveReadsLastBackupMissingFromCache(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyDisabled, longhorn.SystemBackupStateGenerating)

	creator := "creator-system-backup"
	backup := newBackup("system-backup-uncached")
	backup.Labels = types.GetSystemBackupLabels()
	backup.Status.Owners = []string{creator}
	_, err := env.lhClient.LonghornV1beta2().Backups(TestNamespace).Create(context.TODO(), backup, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = backup.Name
	fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)

	_, err = env.controller.getVolumesAndAddLastBackupOwners(TestSystemBackupName)
	c.Assert(err, IsNil)

	backup, err = env.getBackup(backup.Name)
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{creator, TestSystemBackupName})
}

func (s *TestSuite) TestSystemBackupArchiveSkipsDeletingLastBackupSnapshot(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyDisabled, longhorn.SystemBackupStateGenerating)

	creator := "creator-system-backup"
	env.addBackup(c, "system-backup-last", creator)
	// The snapshot is being deleted, for example by
	// auto-cleanup-snapshot-after-on-demand-backup-completed.
	snapshot := newSnapshot("system-backup-last")
	snapshot.Labels = types.GetSystemBackupLabels()
	snapshot.Status.Owners = []string{creator}
	now := metav1.Now()
	snapshot.DeletionTimestamp = &now
	snapshot, err := env.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(context.TODO(), snapshot, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer().Add(snapshot), IsNil)
	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = "system-backup-last"
	fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)

	_, err = env.controller.getVolumesAndAddLastBackupOwners(TestSystemBackupName)
	c.Assert(err, IsNil)

	backup, err := env.getBackup("system-backup-last")
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{creator, TestSystemBackupName})

	snapshot, err = env.getSnapshot("system-backup-last")
	c.Assert(err, IsNil)
	c.Assert(snapshot.Status.Owners, DeepEquals, []string{creator})
}

func (s *TestSuite) TestSystemBackupAddOwnerRejectsDeletingBackup(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")

	backup := newBackup("system-backup-deleting")
	now := metav1.Now()
	backup.DeletionTimestamp = &now
	err := env.controller.addBackupOwner(backup, TestSystemBackupName)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "is being deleted"), Equals, true)
}

func (s *TestSuite) TestSystemBackupAddOwnerReturnsReloadErrorAfterConflict(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")

	backup := newBackup("system-backup-conflict")
	env.lhClient.PrependReactor("update", "backups", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(longhorn.Resource("backups"), backup.Name, fmt.Errorf("conflict"))
	})

	// The backup is not in the API server, so the uncached reload after the
	// conflict fails.
	err := env.controller.addBackupOwner(backup, TestSystemBackupName)
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

func (s *TestSuite) TestSystemBackupAddOwnerReloadsFromAPIServerAfterConflict(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")

	backup := newBackup("system-backup-conflict-reload")
	backup.Labels = types.GetSystemBackupLabels()
	backup.Status.Owners = []string{"creator-system-backup"}
	backup, err := env.lhClient.LonghornV1beta2().Backups(TestNamespace).Create(context.TODO(), backup, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	// The cache keeps a stale copy that will not be used after the conflict.
	stale := backup.DeepCopy()
	stale.ResourceVersion = "stale"
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer().Add(stale), IsNil)

	updates := 0
	env.lhClient.PrependReactor("update", "backups", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(longhorn.Resource("backups"), backup.Name, fmt.Errorf("conflict"))
		}
		return false, nil, nil
	})

	err = env.controller.addBackupOwner(stale, TestSystemBackupName)
	c.Assert(err, IsNil)
	c.Assert(updates >= 2, Equals, true)

	backup, err = env.getBackup(backup.Name)
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{"creator-system-backup", TestSystemBackupName})
}

func (s *TestSuite) TestSystemBackupAddSnapshotOwnerReloadsFromAPIServerAfterConflict(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")

	snapshot := newSnapshot("system-backup-conflict-reload")
	snapshot.Labels = types.GetSystemBackupLabels()
	snapshot.Status.Owners = []string{"creator-system-backup"}
	snapshot, err := env.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(context.TODO(), snapshot, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	stale := snapshot.DeepCopy()
	stale.ResourceVersion = "stale"
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer().Add(stale), IsNil)

	updates := 0
	env.lhClient.PrependReactor("update", "snapshots", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(longhorn.Resource("snapshots"), snapshot.Name, fmt.Errorf("conflict"))
		}
		return false, nil, nil
	})

	err = env.controller.addSnapshotOwner(stale, TestSystemBackupName)
	c.Assert(err, IsNil)
	c.Assert(updates >= 2, Equals, true)

	snapshot, err = env.getSnapshot(snapshot.Name)
	c.Assert(err, IsNil)
	c.Assert(snapshot.Status.Owners, DeepEquals, []string{"creator-system-backup", TestSystemBackupName})
}

func (s *TestSuite) TestSystemBackupDeletingStateUsesCreatorAnnotation(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)

	// The creator was never written to Status.Owners, for example because the
	// manager restarted right after creating the backup.
	backup := newBackup("system-backup-unrecorded")
	backup.Labels = types.GetSystemBackupLabels()
	backup.Annotations = map[string]string{types.SystemBackupAnnotationCreator: systemBackup.Name}
	backup.Status.State = longhorn.BackupStateCompleted
	fakeSystemRolloutBackupWithMetadata(backup, c, env.informerFactories.LhInformerFactory, env.lhClient)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: systemBackup.Name}, defaultBackupTarget)
	c.Assert(err, IsNil)

	_, err = env.getBackup(backup.Name)
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

func (s *TestSuite) TestSystemBackupArchiveStopsWhenSystemBackupIsDeleting(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyDisabled, longhorn.SystemBackupStateDeleting)

	creator := "creator-system-backup"
	env.addBackup(c, "system-backup-last", creator)
	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = "system-backup-last"
	fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)

	_, err := env.controller.getVolumesAndAddLastBackupOwners(systemBackup.Name)
	c.Assert(err, NotNil)

	backup, err := env.getBackup("system-backup-last")
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{creator})
}

func (s *TestSuite) TestSystemBackupArchiveAddsFromBackupOwners(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")

	creator := "creator-system-backup"
	env.addSnapshot(c, "system-backup-from", testBackupSnapshotTime, creator)
	env.addBackup(c, "system-backup-from", creator)
	// Restore preserves Spec.FromBackup for standby volumes even when
	// Status.LastBackup is set. fakeSystemRolloutVolumes copies Status only,
	// so Spec.FromBackup is written afterwards.
	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = "system-backup-other"
	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.IsStandby = true
	fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)
	env.setVolumeFromBackup(c, backupstore.EncodeBackupURL("system-backup-from", TestVolumeName, TestBackupTarget))

	archiver := env.addSystemBackup(c, "archiver-system-backup", longhorn.SystemBackupCreateVolumeBackupPolicyDisabled, longhorn.SystemBackupStateGenerating)
	_, err := env.controller.getVolumesAndAddLastBackupOwners(archiver.Name)
	c.Assert(err, IsNil)

	backup, err := env.getBackup("system-backup-from")
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{creator, archiver.Name})
}

func (s *TestSuite) TestSystemBackupArchiveFailsOnInvalidFromBackup(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyDisabled, longhorn.SystemBackupStateGenerating)

	env.volumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = ""
	fakeSystemRolloutVolumes(env.volumes, c, env.informerFactories.LhInformerFactory, env.lhClient)
	env.setVolumeFromBackup(c, "://bad")

	_, err := env.controller.getVolumesAndAddLastBackupOwners(TestSystemBackupName)
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestSystemBackupDeletingStateStopsSnapshotCleanupOnBackupError(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	env.addSnapshot(c, "system-backup-child", testBackupSnapshotTime, systemBackup.Name)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	env.lhClient.PrependReactor("delete", "backups", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("delete failed")
	})

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: systemBackup.Name}, defaultBackupTarget)
	c.Assert(err, NotNil)

	_, err = env.getSnapshot("system-backup-child")
	c.Assert(err, IsNil, Commentf("snapshot cleanup must not run after backup cleanup fails"))
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)
}

func (s *TestSuite) TestSystemBackupDeletingStateWaitsForLastOwnerBackup(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	env.addSnapshot(c, "system-backup-child", testBackupSnapshotTime, systemBackup.Name)

	// Simulate a Backup DELETE that has been accepted but not finalized yet.
	backup := newBackup("system-backup-child")
	backup.Labels = types.GetSystemBackupLabels()
	backup.Status.Owners = []string{systemBackup.Name}
	backup.Status.State = longhorn.BackupStateCompleted
	now := metav1.Now()
	backup.DeletionTimestamp = &now
	backup, err := env.lhClient.LonghornV1beta2().Backups(TestNamespace).Create(context.TODO(), backup, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	// The fake client clears DeletionTimestamp on Create, so restore it in the
	// API object and the cache used by the lister.
	backup.DeletionTimestamp = &now
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer().Add(backup), IsNil)
	env.lhClient.PrependReactor("get", "backups", func(action k8stesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(k8stesting.GetAction)
		if !ok || getAction.GetName() != backup.Name {
			return false, nil, nil
		}
		return true, backup.DeepCopy(), nil
	})

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	backupTargetClient := &FakeSystemBackupTargetClient{name: systemBackup.Name}
	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "waiting for last-owner backups"), Equals, true)

	_, err = env.getSnapshot("system-backup-child")
	c.Assert(err, IsNil, Commentf("the snapshot is kept while its backup is still being deleted"))
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)

	c.Assert(env.lhClient.LonghornV1beta2().Backups(TestNamespace).Delete(context.TODO(), backup.Name, metav1.DeleteOptions{}), IsNil)
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer().Delete(backup), IsNil)

	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, IsNil)
	_, err = env.getSnapshot("system-backup-child")
	c.Assert(apierrors.IsNotFound(err), Equals, true)
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, false)
}

func (s *TestSuite) TestSystemBackupDeletingStateWaitsForGeneration(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	backupTargetClient := &FakeSystemBackupTargetClient{name: systemBackup.Name}

	env.controller.generatingSystemBackups.Store(systemBackup.Name, struct{}{})
	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, IsNil)

	_, err = env.getBackup("system-backup-child")
	c.Assert(err, IsNil, Commentf("the references are kept while generation is running"))
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)

	env.controller.generatingSystemBackups.Delete(systemBackup.Name)
	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, IsNil)

	_, err = env.getBackup("system-backup-child")
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

func (s *TestSuite) TestSystemBackupDeletingStateKeepsBackupReferencedByUntrackedArchive(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	// A SystemBackup created by this cluster tracks its references with owners,
	// so its archive is not downloaded.
	localSystemBackup := env.addSystemBackup(c, "local-system-backup", longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateReady)

	env.addSnapshot(c, "system-backup-referenced", testBackupSnapshotTime, systemBackup.Name)
	env.addBackup(c, "system-backup-referenced", systemBackup.Name)
	env.addBackup(c, "system-backup-unreferenced", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	backupTargetClient := &fakeArchiveSystemBackupTargetClient{
		archives: map[string][]string{
			systemBackup.Name:      nil,
			localSystemBackup.Name: nil,
			// An archive of another cluster sharing the backup target, or
			// one kept by a CR-only deletion.
			"untracked-system-backup": {"system-backup-referenced"},
		},
	}
	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, IsNil)

	backup, err := env.getBackup("system-backup-referenced")
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{"untracked-system-backup"})

	_, err = env.getBackup("system-backup-unreferenced")
	c.Assert(apierrors.IsNotFound(err), Equals, true)

	_, err = env.getSnapshot("system-backup-referenced")
	c.Assert(apierrors.IsNotFound(err), Equals, true, Commentf("restore reads only the backup"))
}

func (s *TestSuite) TestSystemBackupDeletingStateWaitsForUpload(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	backupTargetClient := &FakeSystemBackupTargetClient{name: systemBackup.Name}

	env.controller.uploadingSystemBackups.Store(systemBackup.Name, struct{}{})
	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, IsNil)

	_, err = env.getBackup("system-backup-child")
	c.Assert(err, IsNil, Commentf("the references are kept while upload is running"))
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)

	env.controller.uploadingSystemBackups.Delete(systemBackup.Name)
	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, IsNil)

	_, err = env.getBackup("system-backup-child")
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

func (s *TestSuite) TestSystemBackupDeletingStateWaitsForUploadOnAnotherManager(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	systemBackup.Annotations = map[string]string{
		types.SystemBackupAnnotationUploading: systemBackupUploadingAnnotationValue("other-manager", time.Now()),
	}
	systemBackup, err := env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Update(context.TODO(), systemBackup, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().SystemBackups().Informer().GetIndexer().Update(systemBackup), IsNil)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: systemBackup.Name}, defaultBackupTarget)
	c.Assert(err, IsNil)

	_, err = env.getBackup("system-backup-child")
	c.Assert(err, IsNil, Commentf("the references are kept while another manager is uploading"))
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)
}

func (s *TestSuite) TestSystemBackupDeletingStateRetriesWhileBackupTargetUnavailable(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	defaultBackupTarget.Status.Available = false
	defaultBackupTarget, err = env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).UpdateStatus(context.TODO(), defaultBackupTarget, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().BackupTargets().Informer().GetIndexer().Update(defaultBackupTarget), IsNil)

	err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: systemBackup.Name}, defaultBackupTarget)
	c.Assert(err, NotNil)

	backup, err := env.getBackup("system-backup-child")
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{systemBackup.Name})
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)
}

func (s *TestSuite) TestSystemBackupDeletingStateDropsFinalizerWhenBackupTargetIsNotConfigured(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	defaultBackupTarget.Spec.BackupTargetURL = ""
	defaultBackupTarget, err = env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Update(context.TODO(), defaultBackupTarget, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().BackupTargets().Informer().GetIndexer().Update(defaultBackupTarget), IsNil)

	err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: systemBackup.Name}, defaultBackupTarget)
	c.Assert(err, IsNil)

	backup, err := env.getBackup("system-backup-child")
	c.Assert(err, IsNil, Commentf("the backup is kept when the archives cannot be checked"))
	c.Assert(backup.Status.Owners, DeepEquals, []string{systemBackup.Name})
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, false)
}

func (s *TestSuite) TestSystemBackupDeletingStateIgnoresSnapshotAlreadyBeingDeleted(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)

	snapshot := newSnapshot("system-backup-child")
	snapshot.Labels = types.GetSystemBackupLabels()
	snapshot.Status.Owners = []string{systemBackup.Name}
	snapshot, err := env.lhClient.LonghornV1beta2().Snapshots(TestNamespace).Create(context.TODO(), snapshot, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	now := metav1.Now()
	snapshot.DeletionTimestamp = &now
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer().Add(snapshot), IsNil)
	env.lhClient.PrependReactor("list", "snapshots", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &longhorn.SnapshotList{Items: []longhorn.Snapshot{*snapshot.DeepCopy()}}, nil
	})

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: systemBackup.Name}, defaultBackupTarget)
	c.Assert(err, IsNil)

	_, err = env.getSnapshot(snapshot.Name)
	c.Assert(err, IsNil, Commentf("SnapshotController finishes deleting the snapshot"))
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, false)
}

func (s *TestSuite) TestSystemBackupUploadStopsWhenSystemBackupIsDeleting(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)

	// The archive is not in the backup target yet.
	backupTargetClient := &countingUploadSystemBackupTargetClient{
		FakeSystemBackupTargetClient: FakeSystemBackupTargetClient{
			name:    systemBackup.Name,
			deleted: map[string]bool{systemBackup.Name: true},
		},
	}
	env.controller.uploadingSystemBackups.Store(systemBackup.Name, struct{}{})
	env.controller.UploadSystemBackup(systemBackup, "", "", backupTargetClient)

	c.Assert(backupTargetClient.uploads, Equals, 0)
	_, running := env.controller.uploadingSystemBackups.Load(systemBackup.Name)
	c.Assert(running, Equals, false)
}

func (s *TestSuite) TestSystemBackupDeletingStateRetriesUnconfirmedArchiveDeletion(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	backupTargetClient := &failingDeleteSystemBackupTargetClient{
		FakeSystemBackupTargetClient: FakeSystemBackupTargetClient{name: systemBackup.Name},
	}
	err = env.controller.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
	c.Assert(err, NotNil)

	_, err = env.getBackup("system-backup-child")
	c.Assert(err, IsNil, Commentf("the references are kept while the archive exists"))
	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)
}

func (s *TestSuite) TestSystemBackupCROnlyDeletionRetriesFailedArchivePresenceCheck(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")
	systemBackup := env.addSystemBackup(c, TestSystemBackupName, longhorn.SystemBackupCreateVolumeBackupPolicyAlways, longhorn.SystemBackupStateDeleting)
	systemBackup.Labels = map[string]string{types.GetLonghornLabelKey(types.DeleteCustomResourceOnly): "true"}
	systemBackup, err := env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Update(context.TODO(), systemBackup, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.informerFactories.LhInformerFactory.Longhorn().V1beta2().SystemBackups().Informer().GetIndexer().Update(systemBackup), IsNil)
	env.addBackup(c, "system-backup-child", systemBackup.Name)

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	err = env.controller.reconcile(systemBackup.Name, &FakeSystemBackupTargetClient{name: TestSystemBackupNameListFailed}, defaultBackupTarget)
	c.Assert(err, NotNil)

	systemBackup, err = env.lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(util.FinalizerExists(longhornFinalizerKey, systemBackup), Equals, true)
	backup, err := env.getBackup("system-backup-child")
	c.Assert(err, IsNil)
	c.Assert(backup.Status.Owners, DeepEquals, []string{systemBackup.Name})
}

func (s *TestSuite) TestSystemBackupArchiveReferenceCacheValidatesChecksum(c *C) {
	env := newSystemBackupChildrenTestEnv(c, "true")

	defaultBackupTarget, err := env.lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	archiveName := "untracked-system-backup"
	backupTargetClient := &fakeArchiveSystemBackupTargetClient{
		archives:  map[string][]string{archiveName: {"system-backup-old"}},
		checksums: map[string]string{archiveName: "checksum-1"},
	}

	for range 2 {
		references, err := env.controller.getUntrackedArchiveBackupReferences(backupTargetClient, defaultBackupTarget, TestSystemBackupName)
		c.Assert(err, IsNil)
		c.Assert(references, DeepEquals, map[string][]string{"system-backup-old": {archiveName}})
	}
	c.Assert(backupTargetClient.downloads, Equals, 1, Commentf("the unchanged archive is read from the cache"))

	// The archive is recreated with the same name and version.
	backupTargetClient.archives[archiveName] = []string{"system-backup-new"}
	backupTargetClient.checksums[archiveName] = "checksum-2"
	references, err := env.controller.getUntrackedArchiveBackupReferences(backupTargetClient, defaultBackupTarget, TestSystemBackupName)
	c.Assert(err, IsNil)
	c.Assert(references, DeepEquals, map[string][]string{"system-backup-new": {archiveName}})
	c.Assert(backupTargetClient.downloads, Equals, 2)
}

type failingDeleteSystemBackupTargetClient struct {
	FakeSystemBackupTargetClient
}

func (c *failingDeleteSystemBackupTargetClient) DeleteSystemBackup(systemBackup *longhorn.SystemBackup) (string, error) {
	return "", fmt.Errorf("failed to delete %v", systemBackup.Name)
}

type countingUploadSystemBackupTargetClient struct {
	FakeSystemBackupTargetClient
	uploads int
}

func (c *countingUploadSystemBackupTargetClient) UploadSystemBackup(name, localFile, longhornVersion, longhornGitCommit, managerImage, engineImage string) (string, error) {
	c.uploads++
	return "", nil
}

// fakeArchiveSystemBackupTargetClient serves archives that reference the given
// volume last backups. Downloading an archive without last backups fails.
type fakeArchiveSystemBackupTargetClient struct {
	FakeSystemBackupTargetClient
	archives  map[string][]string
	checksums map[string]string
	downloads int
}

func (c *fakeArchiveSystemBackupTargetClient) ListSystemBackup() (systembackupstore.SystemBackups, error) {
	systemBackups := systembackupstore.SystemBackups{}
	for name := range c.archives {
		if c.deleted[name] {
			continue
		}
		systemBackups[systembackupstore.Name(name)] = systembackupstore.URI(fmt.Sprintf(TestSystemBackupURIFmt, TestSystemBackupLonghornVersion, name))
	}
	return systemBackups, nil
}

func (c *fakeArchiveSystemBackupTargetClient) GetSystemBackupConfig(name, version string) (*systembackupstore.Config, error) {
	if _, exists := c.archives[name]; !exists || c.deleted[name] {
		return nil, fmt.Errorf("cannot find %v", name)
	}
	return &systembackupstore.Config{Name: name, LonghornVersion: version, Checksum: c.checksums[name]}, nil
}

func (c *fakeArchiveSystemBackupTargetClient) DownloadSystemBackup(name, version, downloadPath string) error {
	c.downloads++
	lastBackups := c.archives[name]
	if len(lastBackups) == 0 {
		return fmt.Errorf("unexpected download of system backup %v", name)
	}

	volumeList := &longhorn.VolumeList{TypeMeta: metav1.TypeMeta{APIVersion: longhorn.SchemeGroupVersion.String(), Kind: types.LonghornKindVolumeList}}
	for i, lastBackup := range lastBackups {
		volume := longhorn.Volume{
			TypeMeta:   metav1.TypeMeta{APIVersion: longhorn.SchemeGroupVersion.String(), Kind: types.LonghornKindVolume},
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("volume-%d", i)},
		}
		volume.Status.LastBackup = lastBackup
		volumeList.Items = append(volumeList.Items, volume)
	}

	f, err := os.Create(downloadPath)
	if err != nil {
		return err
	}
	defer f.Close() // nolint: errcheck
	writer := zip.NewWriter(f)
	entry, err := writer.Create(name + "/yamls/longhorn/volumes.yaml")
	if err != nil {
		return err
	}
	if err := (&printers.YAMLPrinter{}).PrintObj(volumeList, entry); err != nil {
		return err
	}
	return writer.Close()
}

func fakeSystemRolloutBackupWithMetadata(fakeObj *longhorn.Backup, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer()
	clientInterface := client.LonghornV1beta2().Backups(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		if exist.Name != fakeObj.Name {
			continue
		}

		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	backup := newBackup(fakeObj.Name)
	backup.Labels = fakeObj.Labels
	backup.Annotations = fakeObj.Annotations
	backup.Spec = fakeObj.Spec
	backup.Status = fakeObj.Status

	exist, err := clientInterface.Create(context.TODO(), backup, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = indexer.Add(exist)
	c.Assert(err, IsNil)
}

func fakeSystemRolloutSnapshotWithMetadata(fakeObj *longhorn.Snapshot, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
	clientInterface := client.LonghornV1beta2().Snapshots(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		if exist.Name != fakeObj.Name {
			continue
		}

		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	snapshot := newSnapshot(fakeObj.Name)
	snapshot.Labels = fakeObj.Labels
	snapshot.Annotations = fakeObj.Annotations
	snapshot.Spec = fakeObj.Spec
	snapshot.Status = fakeObj.Status

	exist, err := clientInterface.Create(context.TODO(), snapshot, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = indexer.Add(exist)
	c.Assert(err, IsNil)
}
