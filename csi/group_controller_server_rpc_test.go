package csi

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"

	grpcstatus "google.golang.org/grpc/status"

	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testRPCNamespace = "longhorn-system"
	testRPCGroupUID  = "11111111-2222-3333-4444-555555555555"
	testRPCTime      = "2026-07-30T00:00:30Z"
)

// testRPCMemberSnapshotName returns a deterministic, format-valid member
// name for fixtures: production names are random, so tests stamp their own.
func testRPCMemberSnapshotName(groupName, volumeName string) string {
	return fmt.Sprintf("%s-%08x", groupName, crc32.ChecksumIEEE([]byte(volumeName)))
}

// newTestGroupControllerServer serves the RPCs against a fake clientset. The
// tests drive the retry paths on pre-seeded groups: a cold create cannot turn
// Ready without the SnapshotGroup controller.
func newTestGroupControllerServer(objects ...runtime.Object) *GroupControllerServer {
	return &GroupControllerServer{
		enabled:     true,
		lhClient:    lhfake.NewSimpleClientset(objects...), // nolint: staticcheck
		lhNamespace: testRPCNamespace,
		log:         logrus.StandardLogger().WithField("component", "csi-group-controller-server-test"),
	}
}

// newRPCTestGroup returns a group as the CSI create path and the controller
// leave it: type label, parameters record, resolved members, terminal phase.
func newRPCTestGroup(name, csiSnapshotType string, phase longhorn.SnapshotGroupPhase, volumes ...string) *longhorn.SnapshotGroup {
	members := make([]longhorn.SnapshotGroupMember, 0, len(volumes))
	memberStatuses := make([]longhorn.SnapshotGroupMemberStatus, 0, len(volumes))
	for _, volume := range volumes {
		snapshotName := testRPCMemberSnapshotName(name, volume)
		members = append(members, longhorn.SnapshotGroupMember{VolumeName: volume, SnapshotName: snapshotName})
		memberStatuses = append(memberStatuses, longhorn.SnapshotGroupMemberStatus{
			VolumeName: volume, SnapshotName: snapshotName, ReadyToUse: true, CreationTime: testRPCTime,
		})
	}
	return &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testRPCNamespace,
			UID:       testRPCGroupUID,
			Labels: map[string]string{
				types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType): csiSnapshotType,
			},
			Annotations: map[string]string{
				types.SnapshotGroupAnnotationCSIParameters: `{"backupMode":"incremental"}`,
			},
		},
		Spec: longhorn.SnapshotGroupSpec{
			Volumes:         volumes,
			Members:         members,
			DeadlineSeconds: 300,
		},
		Status: longhorn.SnapshotGroupStatus{
			Phase:        phase,
			ReadyToUse:   phase == longhorn.SnapshotGroupPhaseReady,
			CreationTime: testRPCTime,
			Members:      memberStatuses,
		},
	}
}

// newRPCTestBackup returns a member backup as the fan-out creates it.
func newRPCTestBackup(name, groupName, volumeName string, state longhorn.BackupState) *longhorn.Backup {
	return &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testRPCNamespace,
			Labels: map[string]string{
				types.LonghornLabelBackupTarget:                                "default",
				types.LonghornLabelBackupVolume:                                volumeName,
				types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroup):    groupName,
				types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupUID): testRPCGroupUID,
			},
		},
		Spec: longhorn.BackupSpec{
			SnapshotName: testRPCMemberSnapshotName(groupName, volumeName),
			BackupMode:   longhorn.BackupModeIncremental,
		},
		Status: longhorn.BackupStatus{State: state},
	}
}

func newRPCTestVolume(name string) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testRPCNamespace},
		Spec:       longhorn.VolumeSpec{BackupTargetName: "default"},
	}
}

func requireRPCCode(t *testing.T, err error, expected codes.Code) {
	t.Helper()
	if grpcstatus.Code(err) != expected {
		t.Fatalf("expected code %v, got %v (error: %v)", expected, grpcstatus.Code(err), err)
	}
}

// TestPollUntilKeepsContextCode verifies that a poll failing because its
// request was canceled reports the context's code, not the poll's Internal.
func TestPollUntilKeepsContextCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pollUntil(ctx, time.Minute, time.Minute, "the test condition", func() (bool, error) {
		return false, grpcstatus.Error(codes.Internal, "wrapped: context canceled")
	})
	requireRPCCode(t, err, codes.Canceled)
}

func TestCreateVolumeGroupSnapshotSnapRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("retry on a ready group is idempotent", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup("group-a", csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1", "vol-2"))
		response, err := srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name:            "group-a",
			SourceVolumeIds: []string{"vol-1", "vol-2"},
			Parameters:      map[string]string{"type": "snap"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response.GroupSnapshot.GroupSnapshotId != "snap://group-a" {
			t.Errorf("unexpected handle: %v", response.GroupSnapshot.GroupSnapshotId)
		}
		if !response.GroupSnapshot.ReadyToUse {
			t.Error("expected the ready group to be reported ready")
		}
		if len(response.GroupSnapshot.Snapshots) != 2 {
			t.Fatalf("unexpected member count: %v", len(response.GroupSnapshot.Snapshots))
		}
		groups, err := srv.snapshotGroups().List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(groups.Items) != 1 {
			t.Errorf("expected the retry to create no group, found %v", len(groups.Items))
		}
	})

	t.Run("duplicated source volume ids are rejected", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup("group-a", csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1"))
		_, err := srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name:            "group-a",
			SourceVolumeIds: []string{"vol-1", "vol-1"},
			Parameters:      map[string]string{"type": "snap"},
		})
		requireRPCCode(t, err, codes.AlreadyExists)
	})

	t.Run("different source volumes are rejected", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup("group-a", csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1"))
		_, err := srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name:            "group-a",
			SourceVolumeIds: []string{"vol-1", "vol-2"},
			Parameters:      map[string]string{"type": "snap"},
		})
		requireRPCCode(t, err, codes.AlreadyExists)
	})

	t.Run("different type is rejected", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup("group-a", csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1"))
		_, err := srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name:            "group-a",
			SourceVolumeIds: []string{"vol-1"},
			Parameters:      map[string]string{"type": "bak"},
		})
		requireRPCCode(t, err, codes.AlreadyExists)
	})

	t.Run("different snapshot labels are rejected", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup("group-a", csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1"))
		_, err := srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name:            "group-a",
			SourceVolumeIds: []string{"vol-1"},
			Parameters:      map[string]string{"type": "snap", "team": "web"},
		})
		requireRPCCode(t, err, codes.AlreadyExists)
	})

	t.Run("invalid arguments are rejected", func(t *testing.T) {
		srv := newTestGroupControllerServer()
		_, err := srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			SourceVolumeIds: []string{"vol-1"}, Parameters: map[string]string{"type": "snap"},
		})
		requireRPCCode(t, err, codes.InvalidArgument)
		_, err = srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name: "group-a", Parameters: map[string]string{"type": "snap"},
		})
		requireRPCCode(t, err, codes.InvalidArgument)
		_, err = srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name: "group-a", SourceVolumeIds: []string{"vol-1"}, Parameters: map[string]string{"type": "unknown"},
		})
		requireRPCCode(t, err, codes.InvalidArgument)
	})
}

func TestCreateVolumeGroupSnapshotBak(t *testing.T) {
	ctx := context.Background()

	t.Run("fan-out creates one backup per member and adopts on retry", func(t *testing.T) {
		srv := newTestGroupControllerServer(
			newRPCTestGroup("group-a", csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseReady, "vol-1", "vol-2"),
			newRPCTestVolume("vol-1"), newRPCTestVolume("vol-2"),
		)
		request := &csi.CreateVolumeGroupSnapshotRequest{
			Name:            "group-a",
			SourceVolumeIds: []string{"vol-1", "vol-2"},
			Parameters:      map[string]string{"type": "bak"},
		}
		response, err := srv.CreateVolumeGroupSnapshot(ctx, request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response.GroupSnapshot.ReadyToUse {
			t.Error("expected the group to not be ready while uploads run")
		}
		backups, err := srv.backups().List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(backups.Items) != 2 {
			t.Fatalf("expected one backup per member, found %v", len(backups.Items))
		}
		for _, backup := range backups.Items {
			if backup.Spec.BackupMode != longhorn.BackupModeIncremental {
				t.Errorf("unexpected backup mode: %v", backup.Spec.BackupMode)
			}
			if backup.Labels[types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupUID)] != testRPCGroupUID {
				t.Error("expected the backup to carry the group UID label")
			}
		}

		if _, err = srv.CreateVolumeGroupSnapshot(ctx, request); err != nil {
			t.Fatalf("unexpected retry error: %v", err)
		}
		backups, err = srv.backups().List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(backups.Items) != 2 {
			t.Errorf("expected the retry to adopt the backups, found %v", len(backups.Items))
		}
	})

	t.Run("failed group is deleted before the error returns", func(t *testing.T) {
		failedGroup := newRPCTestGroup("group-a", csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseFailed, "vol-1")
		failedGroup.Status.Error = "deadline exceeded before every member snapshot was taken"
		srv := newTestGroupControllerServer(failedGroup)

		_, err := srv.CreateVolumeGroupSnapshot(ctx, &csi.CreateVolumeGroupSnapshotRequest{
			Name:            "group-a",
			SourceVolumeIds: []string{"vol-1"},
			Parameters:      map[string]string{"type": "bak"},
		})
		requireRPCCode(t, err, codes.Internal)
		if !strings.Contains(err.Error(), "deadline exceeded") {
			t.Errorf("expected the failure reason in the error, got: %v", err)
		}
		if _, err := srv.snapshotGroups().Get(ctx, "group-a", metav1.GetOptions{}); err == nil {
			t.Error("expected the failed group to be deleted")
		}
	})
}

func TestGetVolumeGroupSnapshotBak(t *testing.T) {
	ctx := context.Background()
	groupName := "group-a"
	memberSnapshot := testRPCMemberSnapshotName(groupName, "vol-1")

	t.Run("completion is stamped and survives backup deletion", func(t *testing.T) {
		srv := newTestGroupControllerServer(
			newRPCTestGroup(groupName, csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseReady, "vol-1"),
			newRPCTestBackup("backup-1", groupName, "vol-1", longhorn.BackupStateCompleted),
		)
		request := &csi.GetVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://" + groupName}

		response, err := srv.GetVolumeGroupSnapshot(ctx, request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !response.GroupSnapshot.ReadyToUse {
			t.Error("expected the completed group to be ready")
		}
		if response.GroupSnapshot.Snapshots[0].SnapshotId != "bak://vol-1/backup-1" {
			t.Errorf("unexpected member handle: %v", response.GroupSnapshot.Snapshots[0].SnapshotId)
		}
		group, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if recordedSnapshotGroupMemberBackupNames(group)[memberSnapshot] != "backup-1" {
			t.Error("expected completion to be recorded on the group")
		}

		// The recorded handle survives the backup's deletion.
		if err := srv.backups().Delete(ctx, "backup-1", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		response, err = srv.GetVolumeGroupSnapshot(ctx, request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !response.GroupSnapshot.ReadyToUse {
			t.Error("expected the recorded group to stay ready")
		}
		if response.GroupSnapshot.Snapshots[0].SnapshotId != "bak://vol-1/backup-1" {
			t.Errorf("expected the recorded member handle to survive, got: %v", response.GroupSnapshot.Snapshots[0].SnapshotId)
		}
	})

	t.Run("failed upload surfaces and its deletion recreates the backup", func(t *testing.T) {
		failedBackup := newRPCTestBackup("backup-1", groupName, "vol-1", longhorn.BackupStateError)
		failedBackup.Status.Error = "upload failed"
		srv := newTestGroupControllerServer(
			newRPCTestGroup(groupName, csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseReady, "vol-1"),
			failedBackup,
			newRPCTestVolume("vol-1"),
		)
		request := &csi.GetVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://" + groupName}

		_, err := srv.GetVolumeGroupSnapshot(ctx, request)
		requireRPCCode(t, err, codes.Internal)
		if !strings.Contains(err.Error(), "delete and recreate the failed backups") {
			t.Errorf("expected the recovery instruction in the error, got: %v", err)
		}

		// The documented recovery: delete the failed backup; the next poll
		// creates a replacement.
		if err := srv.backups().Delete(ctx, "backup-1", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		response, err := srv.GetVolumeGroupSnapshot(ctx, request)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response.GroupSnapshot.ReadyToUse {
			t.Error("expected the group to not be ready while the replacement uploads")
		}
		backups, err := srv.backups().List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(backups.Items) != 1 || backups.Items[0].Name == "backup-1" {
			t.Errorf("expected a replacement backup, found %v", len(backups.Items))
		}
	})

	t.Run("terminating completed backup is not stamped", func(t *testing.T) {
		terminating := newRPCTestBackup("backup-1", groupName, "vol-1", longhorn.BackupStateCompleted)
		deletionTime := metav1.Now()
		terminating.DeletionTimestamp = &deletionTime
		terminating.Finalizers = []string{"longhorn.io"}
		srv := newTestGroupControllerServer(
			newRPCTestGroup(groupName, csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseReady, "vol-1"),
			terminating,
			newRPCTestVolume("vol-1"),
		)

		response, err := srv.GetVolumeGroupSnapshot(ctx, &csi.GetVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://" + groupName})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response.GroupSnapshot.ReadyToUse {
			t.Error("expected the group to not be ready with a terminating backup")
		}
		group, err := srv.snapshotGroups().Get(ctx, groupName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if snapshotGroupBackupsRecorded(group) {
			t.Error("expected no completion record with a terminating backup")
		}
	})

	t.Run("failed group returns its recorded failure", func(t *testing.T) {
		failedGroup := newRPCTestGroup(groupName, csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseFailed, "vol-1")
		failedGroup.Status.Error = "deadline exceeded before every member snapshot was taken"
		srv := newTestGroupControllerServer(failedGroup)

		_, err := srv.GetVolumeGroupSnapshot(ctx, &csi.GetVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://" + groupName})
		requireRPCCode(t, err, codes.Internal)
		if !strings.Contains(err.Error(), "deadline exceeded") {
			t.Errorf("expected the failure reason in the error, got: %v", err)
		}
	})

	t.Run("unready group without backups lists no members", func(t *testing.T) {
		srv := newTestGroupControllerServer(
			newRPCTestGroup(groupName, csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseInProgress, "vol-1"),
		)
		response, err := srv.GetVolumeGroupSnapshot(ctx, &csi.GetVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://" + groupName})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if response.GroupSnapshot.ReadyToUse {
			t.Error("expected the unready group to not be ready")
		}
		if len(response.GroupSnapshot.Snapshots) != 0 {
			t.Errorf("expected no member handles before any backup exists, got %v", response.GroupSnapshot.Snapshots)
		}
	})

	t.Run("missing and mismatched groups return not found", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup(groupName, csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1"))
		_, err := srv.GetVolumeGroupSnapshot(ctx, &csi.GetVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://" + groupName})
		requireRPCCode(t, err, codes.NotFound)
		_, err = srv.GetVolumeGroupSnapshot(ctx, &csi.GetVolumeGroupSnapshotRequest{GroupSnapshotId: "snap://missing"})
		requireRPCCode(t, err, codes.NotFound)
	})
}

func TestDeleteVolumeGroupSnapshot(t *testing.T) {
	ctx := context.Background()

	t.Run("bak deletion sweeps the backups and deletes the group", func(t *testing.T) {
		srv := newTestGroupControllerServer(
			newRPCTestGroup("group-a", csiSnapshotTypeLonghornBackup, longhorn.SnapshotGroupPhaseReady, "vol-1"),
			newRPCTestBackup("backup-1", "group-a", "vol-1", longhorn.BackupStateCompleted),
		)
		if _, err := srv.DeleteVolumeGroupSnapshot(ctx, &csi.DeleteVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://group-a"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		backups, err := srv.backups().List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(backups.Items) != 0 {
			t.Errorf("expected the backups to be swept, found %v", len(backups.Items))
		}
		if _, err := srv.snapshotGroups().Get(ctx, "group-a", metav1.GetOptions{}); err == nil {
			t.Error("expected the group to be deleted")
		}
	})

	t.Run("undecodable handle deletes nothing and succeeds", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup("group-a", csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1"))
		if _, err := srv.DeleteVolumeGroupSnapshot(ctx, &csi.DeleteVolumeGroupSnapshotRequest{GroupSnapshotId: "group-a"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := srv.snapshotGroups().Get(ctx, "group-a", metav1.GetOptions{}); err != nil {
			t.Error("expected the group to survive an undecodable handle")
		}
	})

	t.Run("type mismatch is rejected", func(t *testing.T) {
		srv := newTestGroupControllerServer(newRPCTestGroup("group-a", csiSnapshotTypeLonghornSnapshot, longhorn.SnapshotGroupPhaseReady, "vol-1"))
		_, err := srv.DeleteVolumeGroupSnapshot(ctx, &csi.DeleteVolumeGroupSnapshotRequest{GroupSnapshotId: "bak://group-a"})
		requireRPCCode(t, err, codes.InvalidArgument)
	})
}
