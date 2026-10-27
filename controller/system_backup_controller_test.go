package controller

import (
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

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

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
