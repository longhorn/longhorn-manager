package csi

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestToCSIVolumeGroupSnapshot(t *testing.T) {
	creationTime := time.Date(2026, time.July, 30, 0, 0, 30, 0, time.UTC)

	newGroup := func(phase longhorn.SnapshotGroupPhase, readyToUse bool, members ...longhorn.SnapshotGroupMemberStatus) *longhorn.SnapshotGroup {
		specMembers := make([]longhorn.SnapshotGroupMember, 0, len(members))
		for _, member := range members {
			specMembers = append(specMembers, longhorn.SnapshotGroupMember{VolumeName: member.VolumeName, SnapshotName: member.SnapshotName})
		}
		return &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-group"},
			Spec:       longhorn.SnapshotGroupSpec{Members: specMembers},
			Status: longhorn.SnapshotGroupStatus{
				Phase:        phase,
				ReadyToUse:   readyToUse,
				CreationTime: creationTime.Format(time.RFC3339),
				Members:      members,
			},
		}
	}

	t.Run("ready group maps handles and member snapshot IDs", func(t *testing.T) {
		group := newGroup(longhorn.SnapshotGroupPhaseReady, true,
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
		)
		csiGroup := toCSIVolumeGroupSnapshot(group)

		if csiGroup.GroupSnapshotId != "snap://demo-group" {
			t.Errorf("unexpected group snapshot handle: %v", csiGroup.GroupSnapshotId)
		}
		if !csiGroup.ReadyToUse {
			t.Error("expected group to be ready to use")
		}
		if csiGroup.CreationTime.AsTime() != creationTime {
			t.Errorf("unexpected group creation time: %v", csiGroup.CreationTime.AsTime())
		}
		if len(csiGroup.Snapshots) != 1 {
			t.Fatalf("unexpected member count: %v", len(csiGroup.Snapshots))
		}
		member := csiGroup.Snapshots[0]
		if member.SnapshotId != "snap://vol-1/snap-1" {
			t.Errorf("unexpected member snapshot ID: %v", member.SnapshotId)
		}
		if member.SourceVolumeId != "vol-1" {
			t.Errorf("unexpected member source volume: %v", member.SourceVolumeId)
		}
		if member.GroupSnapshotId != csiGroup.GroupSnapshotId {
			t.Errorf("unexpected member group snapshot handle: %v", member.GroupSnapshotId)
		}
		if !member.ReadyToUse {
			t.Error("expected member to be ready to use")
		}
	})

	t.Run("degraded ready group still reports group and members ready", func(t *testing.T) {
		// A ready group with a lost member keeps status.readyToUse true: the
		// controller only flips the member mirror and the Degraded condition.
		// CSI must keep reporting the group and the member ready - the group
		// was provisioned, and the member snapshot was taken.
		group := newGroup(longhorn.SnapshotGroupPhaseReady, true,
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: false, CreationTime: creationTime.Format(time.RFC3339)},
		)
		csiGroup := toCSIVolumeGroupSnapshot(group)

		if !csiGroup.ReadyToUse {
			t.Error("expected degraded ready group to stay ready to use")
		}
		if !csiGroup.Snapshots[0].ReadyToUse {
			t.Error("expected member of a ready group to be reported ready")
		}
	})

	t.Run("in-progress group mirrors member readiness", func(t *testing.T) {
		group := newGroup(longhorn.SnapshotGroupPhaseInProgress, false,
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-2", SnapshotName: "snap-2"},
		)
		group.Status.CreationTime = ""
		csiGroup := toCSIVolumeGroupSnapshot(group)

		if csiGroup.ReadyToUse {
			t.Error("expected in-progress group to not be ready to use")
		}
		if csiGroup.CreationTime != nil {
			t.Errorf("expected no group creation time before the group is ready, got %v", csiGroup.CreationTime.AsTime())
		}
		if !csiGroup.Snapshots[0].ReadyToUse {
			t.Error("expected member whose snapshot was taken to be reported ready")
		}
		if csiGroup.Snapshots[1].ReadyToUse {
			t.Error("expected member whose snapshot is not yet taken to not be reported ready")
		}
		if csiGroup.Snapshots[1].CreationTime != nil {
			t.Errorf("expected no creation time for a member whose snapshot is not yet taken, got %v", csiGroup.Snapshots[1].CreationTime.AsTime())
		}
	})

	t.Run("restored group with stripped status lists every member", func(t *testing.T) {
		// A restored group carries its members only in the spec until the
		// controller rebuilds the status.
		group := newGroup(longhorn.SnapshotGroupPhaseReady, true,
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1"},
		)
		group.Status.Members = nil
		csiGroup := toCSIVolumeGroupSnapshot(group)

		if len(csiGroup.Snapshots) != 1 {
			t.Fatalf("expected the member list from the spec, got %v members", len(csiGroup.Snapshots))
		}
		if csiGroup.Snapshots[0].SnapshotId != "snap://vol-1/snap-1" {
			t.Errorf("unexpected member snapshot ID: %v", csiGroup.Snapshots[0].SnapshotId)
		}
		if !csiGroup.Snapshots[0].ReadyToUse {
			t.Error("expected the member of a restored ready group to be reported ready")
		}
	})
}

func TestToCSIVolumeGroupSnapshotBackup(t *testing.T) {
	creationTime := time.Date(2026, time.July, 30, 0, 0, 30, 0, time.UTC)

	newReadyGroup := func(members ...longhorn.SnapshotGroupMemberStatus) *longhorn.SnapshotGroup {
		specMembers := make([]longhorn.SnapshotGroupMember, 0, len(members))
		for _, member := range members {
			specMembers = append(specMembers, longhorn.SnapshotGroupMember{VolumeName: member.VolumeName, SnapshotName: member.SnapshotName})
		}
		return &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-group"},
			Spec:       longhorn.SnapshotGroupSpec{Members: specMembers},
			Status: longhorn.SnapshotGroupStatus{
				Phase:        longhorn.SnapshotGroupPhaseReady,
				ReadyToUse:   true,
				CreationTime: creationTime.Format(time.RFC3339),
				Members:      members,
			},
		}
	}
	newBackup := func(name, snapshotName string, state longhorn.BackupState) *longhorn.Backup {
		return &longhorn.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       longhorn.BackupSpec{SnapshotName: snapshotName},
			Status:     longhorn.BackupStatus{State: state},
		}
	}

	t.Run("upload straggler reports not ready with live member aggregation", func(t *testing.T) {
		group := newReadyGroup(
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-2", SnapshotName: "snap-2", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
		)
		backups := map[string]*longhorn.Backup{
			"snap-1": newBackup("backup-1", "snap-1", longhorn.BackupStateCompleted),
			"snap-2": newBackup("backup-2", "snap-2", longhorn.BackupStateInProgress),
		}
		csiGroup := toCSIVolumeGroupSnapshotBackup(group, backups)

		if csiGroup.GroupSnapshotId != "bak://demo-group" {
			t.Errorf("unexpected group snapshot handle: %v", csiGroup.GroupSnapshotId)
		}
		if csiGroup.ReadyToUse {
			t.Error("expected group with an in-progress upload to not be ready to use")
		}
		if csiGroup.CreationTime.AsTime() != creationTime {
			t.Errorf("unexpected group creation time: %v", csiGroup.CreationTime.AsTime())
		}
		if csiGroup.Snapshots[0].SnapshotId != "bak://vol-1/backup-1" {
			t.Errorf("unexpected member snapshot ID: %v", csiGroup.Snapshots[0].SnapshotId)
		}
		if !csiGroup.Snapshots[0].ReadyToUse {
			t.Error("expected member with a completed upload to be ready")
		}
		if csiGroup.Snapshots[1].ReadyToUse {
			t.Error("expected member with an in-progress upload to not be ready")
		}
		if csiGroup.Snapshots[1].CreationTime.AsTime() != creationTime {
			t.Errorf("expected member creation time to be the snapshot creation time, not an upload time, got %v", csiGroup.Snapshots[1].CreationTime.AsTime())
		}
	})

	t.Run("all uploads completed reports ready before the annotation is stamped", func(t *testing.T) {
		group := newReadyGroup(
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-2", SnapshotName: "snap-2", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
		)
		backups := map[string]*longhorn.Backup{
			"snap-1": newBackup("backup-1", "snap-1", longhorn.BackupStateCompleted),
			"snap-2": newBackup("backup-2", "snap-2", longhorn.BackupStateCompleted),
		}
		csiGroup := toCSIVolumeGroupSnapshotBackup(group, backups)

		if !csiGroup.ReadyToUse {
			t.Error("expected group with all uploads completed to be ready without the annotation")
		}
	})

	t.Run("backups-completed annotation freezes ready true", func(t *testing.T) {
		group := newReadyGroup(
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
		)
		group.Annotations = map[string]string{types.SnapshotGroupAnnotationBackupsCompleted: `{"snap-1":"backup-1"}`}
		// After completion is frozen, live backup state no longer matters.
		backups := map[string]*longhorn.Backup{
			"snap-1": newBackup("backup-1", "snap-1", longhorn.BackupStateInProgress),
		}
		csiGroup := toCSIVolumeGroupSnapshotBackup(group, backups)

		if !csiGroup.ReadyToUse {
			t.Error("expected annotated group to be frozen ready to use")
		}
		if !csiGroup.Snapshots[0].ReadyToUse {
			t.Error("expected member of an annotated group to be frozen ready")
		}
	})

	t.Run("annotation that does not name every member does not freeze", func(t *testing.T) {
		group := newReadyGroup(
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: creationTime.Format(time.RFC3339)},
		)
		group.Annotations = map[string]string{types.SnapshotGroupAnnotationBackupsCompleted: "true"}
		backups := map[string]*longhorn.Backup{
			"snap-1": newBackup("backup-1", "snap-1", longhorn.BackupStateInProgress),
		}
		csiGroup := toCSIVolumeGroupSnapshotBackup(group, backups)

		if csiGroup.ReadyToUse {
			t.Error("expected group with an invalid completion annotation to not be ready")
		}
	})

	t.Run("restored recorded group lists every member with recorded handles", func(t *testing.T) {
		// A restored group carries its members only in the spec until the
		// controller rebuilds the status; the recorded names keep the
		// handles serveable.
		group := newReadyGroup(
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1"},
		)
		group.Status.Members = nil
		group.Annotations = map[string]string{
			types.SnapshotGroupAnnotationBackupsCompleted: `{"snap-1":"backup-1"}`,
		}
		csiGroup := toCSIVolumeGroupSnapshotBackup(group, nil)

		if !csiGroup.ReadyToUse {
			t.Error("expected the recorded group to be ready")
		}
		if len(csiGroup.Snapshots) != 1 {
			t.Fatalf("expected the member list from the spec, got %v members", len(csiGroup.Snapshots))
		}
		if csiGroup.Snapshots[0].SnapshotId != "bak://vol-1/backup-1" {
			t.Errorf("unexpected member handle: %v", csiGroup.Snapshots[0].SnapshotId)
		}
	})
}

func TestFailedSnapshotGroupMemberBackupVolumes(t *testing.T) {
	group := &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-group"},
		Spec: longhorn.SnapshotGroupSpec{Members: []longhorn.SnapshotGroupMember{
			{VolumeName: "vol-1", SnapshotName: "snap-1"},
			{VolumeName: "vol-2", SnapshotName: "snap-2"},
			{VolumeName: "vol-3", SnapshotName: "snap-3"},
		}},
	}
	backups := map[string]*longhorn.Backup{
		"snap-1": {Status: longhorn.BackupStatus{State: longhorn.BackupStateCompleted}},
		"snap-2": {Status: longhorn.BackupStatus{State: longhorn.BackupStateError, Error: "upload failed"}},
		"snap-3": {Status: longhorn.BackupStatus{State: longhorn.BackupStateInProgress}},
	}

	failedVolumes := failedSnapshotGroupMemberBackupVolumes(group, backups)
	if len(failedVolumes) != 1 || failedVolumes[0] != "vol-2" {
		t.Errorf("expected only vol-2 to be reported failed, got %v", failedVolumes)
	}
	if allSnapshotGroupMemberBackupsCompleted(group, backups) {
		t.Error("expected group with an in-progress upload to not be completed")
	}

	backups["snap-2"] = &longhorn.Backup{Status: longhorn.BackupStatus{State: longhorn.BackupStateCompleted}}
	backups["snap-3"] = &longhorn.Backup{Status: longhorn.BackupStatus{State: longhorn.BackupStateCompleted}}
	if !allSnapshotGroupMemberBackupsCompleted(group, backups) {
		t.Error("expected group with all uploads completed to be completed")
	}
}

func TestEncodeSnapshotGroupID(t *testing.T) {
	if id := encodeSnapshotGroupID(csiSnapshotTypeLonghornSnapshot, "demo-group"); id != "snap://demo-group" {
		t.Errorf("unexpected group snapshot handle: %v", id)
	}
	if id := encodeSnapshotGroupID(csiSnapshotTypeLonghornBackup, "demo-group"); id != "bak://demo-group" {
		t.Errorf("unexpected group snapshot handle: %v", id)
	}
}

func TestDecodeSnapshotGroupID(t *testing.T) {
	testCases := []struct {
		name            string
		groupSnapshotID string
		expectedType    string
		expectedGroup   string
	}{
		{"snap round trip", encodeSnapshotGroupID(csiSnapshotTypeLonghornSnapshot, "demo-group"), "snap", "demo-group"},
		{"bak round trip", encodeSnapshotGroupID(csiSnapshotTypeLonghornBackup, "demo-group"), "bak", "demo-group"},
		{"deprecated bs handle normalizes to bak", "bs://demo-group", "bak", "demo-group"},
		{"bare name is not a handle", "demo-group", "", ""},
		{"empty", "", "", ""},
		{"unsupported snapshot type", "bi://demo-group", "", ""},
		{"per-volume member handle is not a group handle", "snap://volume/snapshot", "", ""},
		{"per-volume backup handle is not a group handle", "bak://volume/backup", "", ""},
	}
	for _, tc := range testCases {
		csiSnapshotType, group := decodeSnapshotGroupID(tc.groupSnapshotID)
		if csiSnapshotType != tc.expectedType || group != tc.expectedGroup {
			t.Errorf("%v: decodeSnapshotGroupID(%q) = (%q, %q), expected (%q, %q)", tc.name, tc.groupSnapshotID, csiSnapshotType, group, tc.expectedType, tc.expectedGroup)
		}
	}
}

// TestSnapshotGroupLockTable verifies mutual exclusion per group and that a
// group's entry is dropped once the last holder releases it.
func TestSnapshotGroupLockTable(t *testing.T) {
	var table snapshotGroupLockTable

	first := table.lock("group-a")
	secondAcquired := make(chan struct{})
	secondReleased := make(chan struct{})
	go func() {
		second := table.lock("group-a")
		close(secondAcquired)
		second.Unlock()
		close(secondReleased)
	}()

	select {
	case <-secondAcquired:
		t.Fatal("lock acquired twice for the same group")
	case <-time.After(50 * time.Millisecond):
	}

	first.Unlock()
	<-secondReleased

	table.mu.Lock()
	remaining := len(table.entries)
	table.mu.Unlock()
	if remaining != 0 {
		t.Errorf("lock table holds %v entries after every lock is released", remaining)
	}
}

// TestPreferredMemberBackup verifies the duplicate winner: a recorded backup
// wins outright, then the healthier state, then the greater name.
func TestPreferredMemberBackup(t *testing.T) {
	backup := func(name string, state longhorn.BackupState) *longhorn.Backup {
		return &longhorn.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       longhorn.BackupSpec{SnapshotName: "snap-1"},
			Status:     longhorn.BackupStatus{State: state},
		}
	}
	recorded := map[string]string{"snap-1": "backup-recorded"}

	testCases := []struct {
		name     string
		a, b     *longhorn.Backup
		recorded map[string]string
		winner   string
	}{
		{"recorded wins over a completed rival", backup("backup-recorded", longhorn.BackupStateInProgress), backup("backup-zzz", longhorn.BackupStateCompleted), recorded, "backup-recorded"},
		{"completed wins over in-progress", backup("backup-zzz", longhorn.BackupStateInProgress), backup("backup-aaa", longhorn.BackupStateCompleted), nil, "backup-aaa"},
		{"in-progress wins over failed", backup("backup-zzz", longhorn.BackupStateError), backup("backup-aaa", longhorn.BackupStateInProgress), nil, "backup-aaa"},
		{"equal states fall back to name order", backup("backup-aaa", longhorn.BackupStateCompleted), backup("backup-bbb", longhorn.BackupStateCompleted), nil, "backup-bbb"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if winner := preferredMemberBackup(tc.a, tc.b, tc.recorded); winner.Name != tc.winner {
				t.Errorf("picked %v, want %v", winner.Name, tc.winner)
			}
			// The pick must not depend on the argument order.
			if winner := preferredMemberBackup(tc.b, tc.a, tc.recorded); winner.Name != tc.winner {
				t.Errorf("picked %v with swapped arguments, want %v", winner.Name, tc.winner)
			}
		})
	}
}

// TestSnapshotGroupParametersMismatch verifies the create-retry comparison:
// snapshot labels against the immutable spec, backup mode against the record
// stamped at creation.
func TestSnapshotGroupParametersMismatch(t *testing.T) {
	newGroup := func(record string, specLabels map[string]string) *longhorn.SnapshotGroup {
		group := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "group-a"},
			Spec:       longhorn.SnapshotGroupSpec{Labels: specLabels},
		}
		if record != "" {
			group.Annotations = map[string]string{types.SnapshotGroupAnnotationCSIParameters: record}
		}
		return group
	}

	testCases := []struct {
		name             string
		record           string
		specLabels       map[string]string
		params           *volumeGroupSnapshotParameters
		expectedMismatch string
	}{
		{
			name:             "matching parameters",
			record:           `{"backupMode":"incremental"}`,
			specLabels:       map[string]string{"team": "db"},
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornBackup, backupMode: longhorn.BackupModeIncremental, snapshotLabels: map[string]string{"team": "db"}},
			expectedMismatch: "",
		},
		{
			name:             "missing record on a bak group is incompatible",
			record:           "",
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornBackup, backupMode: longhorn.BackupModeFull},
			expectedMismatch: "no readable record",
		},
		{
			name:             "damaged record on a bak group is incompatible",
			record:           "{not json",
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornBackup, backupMode: longhorn.BackupModeFull},
			expectedMismatch: "no readable record",
		},
		{
			name:             "missing record on a snap group is accepted",
			record:           "",
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornSnapshot, backupMode: longhorn.BackupModeFull},
			expectedMismatch: "",
		},
		{
			name:             "different backup mode on a bak group",
			record:           `{"backupMode":"incremental"}`,
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornBackup, backupMode: longhorn.BackupModeFull},
			expectedMismatch: "backup mode",
		},
		{
			name:             "different backup mode on a snap group is ignored",
			record:           `{"backupMode":"incremental"}`,
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornSnapshot, backupMode: longhorn.BackupModeFull},
			expectedMismatch: "",
		},
		{
			name:             "different snapshot labels without any record",
			record:           "",
			specLabels:       map[string]string{"team": "db"},
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornSnapshot, snapshotLabels: map[string]string{"team": "web"}},
			expectedMismatch: "snapshot labels",
		},
		{
			name:             "empty and absent labels are equal",
			record:           `{"backupMode":"incremental"}`,
			params:           &volumeGroupSnapshotParameters{csiSnapshotType: csiSnapshotTypeLonghornBackup, backupMode: longhorn.BackupModeIncremental, snapshotLabels: map[string]string{}},
			expectedMismatch: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mismatch := snapshotGroupParametersMismatch(newGroup(tc.record, tc.specLabels), tc.params)
			if tc.expectedMismatch == "" && mismatch != "" {
				t.Errorf("expected a match, got mismatch %q", mismatch)
			}
			if tc.expectedMismatch != "" && !strings.Contains(mismatch, tc.expectedMismatch) {
				t.Errorf("mismatch %q does not name %q", mismatch, tc.expectedMismatch)
			}
		})
	}
}

// TestAllSnapshotGroupMemberBackupsCompleted verifies completion requires
// every member backup completed and not being deleted.
func TestAllSnapshotGroupMemberBackupsCompleted(t *testing.T) {
	group := &longhorn.SnapshotGroup{
		Spec: longhorn.SnapshotGroupSpec{Members: []longhorn.SnapshotGroupMember{
			{VolumeName: "vol-1", SnapshotName: "snap-1"},
			{VolumeName: "vol-2", SnapshotName: "snap-2"},
		}},
	}
	completed := func() *longhorn.Backup {
		return &longhorn.Backup{Status: longhorn.BackupStatus{State: longhorn.BackupStateCompleted}}
	}
	deletionTime := metav1.Now()
	terminating := completed()
	terminating.DeletionTimestamp = &deletionTime

	testCases := []struct {
		name     string
		backups  map[string]*longhorn.Backup
		expected bool
	}{
		{"all completed", map[string]*longhorn.Backup{"snap-1": completed(), "snap-2": completed()}, true},
		{"one completed backup terminating", map[string]*longhorn.Backup{"snap-1": completed(), "snap-2": terminating}, false},
		{"one missing", map[string]*longhorn.Backup{"snap-1": completed()}, false},
		{"one unfinished", map[string]*longhorn.Backup{"snap-1": completed(), "snap-2": {Status: longhorn.BackupStatus{State: longhorn.BackupStateInProgress}}}, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := allSnapshotGroupMemberBackupsCompleted(group, tc.backups); got != tc.expected {
				t.Errorf("allSnapshotGroupMemberBackupsCompleted = %v, expected %v", got, tc.expected)
			}
		})
	}
}

// TestRecordedSnapshotGroupBackupMode verifies the mode Get hands to the
// fan-out: the recorded mode when readable, the parser default otherwise.
func TestRecordedSnapshotGroupBackupMode(t *testing.T) {
	testCases := []struct {
		name         string
		record       string
		expectedMode longhorn.BackupMode
		expectError  bool
	}{
		{"recorded full mode", `{"backupMode":"full"}`, longhorn.BackupModeFull, false},
		{"recorded incremental mode", `{"backupMode":"incremental"}`, longhorn.BackupModeIncremental, false},
		{"no record is an error", "", "", true},
		{"damaged record is an error", "{not json", "", true},
		{"empty mode in the record is an error", `{"otherField":true}`, "", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			group := &longhorn.SnapshotGroup{ObjectMeta: metav1.ObjectMeta{Name: "group-a"}}
			if tc.record != "" {
				group.Annotations = map[string]string{types.SnapshotGroupAnnotationCSIParameters: tc.record}
			}
			mode, err := recordedSnapshotGroupBackupMode(group)
			if tc.expectError != (err != nil) {
				t.Fatalf("expected error %v, got error %v", tc.expectError, err)
			}
			if mode != tc.expectedMode {
				t.Errorf("recordedSnapshotGroupBackupMode = %v, expected %v", mode, tc.expectedMode)
			}
		})
	}
}
