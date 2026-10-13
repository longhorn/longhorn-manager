package api

import (
	"testing"

	"github.com/stretchr/testify/assert"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestToSnapshotGroupMembers(t *testing.T) {
	policyRef := &longhorn.VolumeHookPolicyRef{Namespace: "app", Name: "mongo", Generation: 3}
	group := &longhorn.SnapshotGroup{
		Spec: longhorn.SnapshotGroupSpec{Members: []longhorn.SnapshotGroupMember{
			{VolumeName: "vol-a", SnapshotName: "snap-a", HookPolicyRef: policyRef},
			{VolumeName: "vol-b", SnapshotName: "snap-b"},
		}},
		Status: longhorn.SnapshotGroupStatus{Members: []longhorn.SnapshotGroupMemberStatus{
			{VolumeName: "vol-a", SnapshotName: "snap-a", ReadyToUse: true, CreationTime: "2026-01-01T00:00:00Z"},
			{VolumeName: "vol-b", SnapshotName: "snap-b", Error: "member snapshot deleted"},
		}},
	}
	assert.Equal(t, []SnapshotGroupMember{
		{VolumeName: "vol-a", SnapshotName: "snap-a", ReadyToUse: true, CreationTime: "2026-01-01T00:00:00Z", HookPolicyRef: policyRef},
		{VolumeName: "vol-b", SnapshotName: "snap-b", Error: "member snapshot deleted"},
	}, toSnapshotGroupMembers(group))
}
