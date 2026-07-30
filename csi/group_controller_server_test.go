package csi

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestToCSIVolumeGroupSnapshot(t *testing.T) {
	cutTime := time.Date(2026, time.July, 30, 0, 0, 30, 0, time.UTC)

	newGroup := func(phase longhorn.SnapshotGroupPhase, readyToUse bool, members ...longhorn.SnapshotGroupMemberStatus) *longhorn.SnapshotGroup {
		return &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-group"},
			Status: longhorn.SnapshotGroupStatus{
				Phase:        phase,
				ReadyToUse:   readyToUse,
				CreationTime: cutTime.Format(time.RFC3339),
				Members:      members,
			},
		}
	}

	t.Run("ready group maps handles and member snapshot IDs", func(t *testing.T) {
		group := newGroup(longhorn.SnapshotGroupPhaseReady, true,
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: cutTime.Format(time.RFC3339)},
		)
		csiGroup := toCSIVolumeGroupSnapshot(group)

		if csiGroup.GroupSnapshotId != "snap://demo-group" {
			t.Errorf("unexpected group snapshot handle: %v", csiGroup.GroupSnapshotId)
		}
		if !csiGroup.ReadyToUse {
			t.Error("expected group to be ready to use")
		}
		if csiGroup.CreationTime.AsTime() != cutTime {
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

	t.Run("degraded ready group still reports members ready", func(t *testing.T) {
		// A ready group with a lost member: the controller flips the member's
		// mirror to false as a Longhorn-side loss marker, but CSI must not
		// report the member as still in progress - the snapshot was taken.
		group := newGroup(longhorn.SnapshotGroupPhaseReady, false,
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: false, CreationTime: cutTime.Format(time.RFC3339)},
		)
		csiGroup := toCSIVolumeGroupSnapshot(group)

		if csiGroup.ReadyToUse {
			t.Error("expected degraded group to not be ready to use")
		}
		if !csiGroup.Snapshots[0].ReadyToUse {
			t.Error("expected member of a ready group to be reported ready")
		}
	})

	t.Run("in-progress group mirrors member readiness", func(t *testing.T) {
		group := newGroup(longhorn.SnapshotGroupPhaseInProgress, false,
			longhorn.SnapshotGroupMemberStatus{VolumeName: "vol-1", SnapshotName: "snap-1", ReadyToUse: true, CreationTime: cutTime.Format(time.RFC3339)},
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
}

func TestEncodeSnapshotGroupID(t *testing.T) {
	if id := encodeSnapshotGroupID("demo-group"); id != "snap://demo-group" {
		t.Errorf("unexpected group snapshot handle: %v", id)
	}
}

func TestDecodeSnapshotGroupID(t *testing.T) {
	testCases := []struct {
		name            string
		groupSnapshotID string
		expectedGroup   string
	}{
		{"round trip", encodeSnapshotGroupID("demo-group"), "demo-group"},
		{"snap handle", "snap://demo-group", "demo-group"},
		{"bare name is not a handle", "demo-group", ""},
		{"empty", "", ""},
		{"unsupported snapshot type", "bak://demo-group", ""},
		{"per-volume member handle is not a group handle", "snap://volume/snapshot", ""},
	}
	for _, tc := range testCases {
		if group := decodeSnapshotGroupID(tc.groupSnapshotID); group != tc.expectedGroup {
			t.Errorf("%v: decodeSnapshotGroupID(%q) = %q, expected %q", tc.name, tc.groupSnapshotID, group, tc.expectedGroup)
		}
	}
}
