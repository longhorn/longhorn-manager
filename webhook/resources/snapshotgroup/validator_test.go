package snapshotgroup

import (
	"fmt"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// testMemberSnapshotName returns a deterministic, format-valid member name
// for fixtures: production names are random, so tests stamp their own.
func testMemberSnapshotName(groupName, volumeName string) string {
	return fmt.Sprintf("%s-%08x", groupName, crc32.ChecksumIEEE([]byte(volumeName)))
}

// validGroup returns a group that passes every Create check: a valid name,
// exactly one selection mode, an in-range deadline, and a member set whose
// snapshot names have the member name format. Cases mutate one aspect at a
// time.
func validGroup() *longhorn.SnapshotGroup {
	name := "analytics-db-snap"
	volumes := []string{
		"pvc-6c30f5a5-aaaa-bbbb-cccc-111111111111",
		"pvc-8a1e22b7-dddd-eeee-ffff-222222222222",
	}
	members := make([]longhorn.SnapshotGroupMember, 0, len(volumes))
	for _, volume := range volumes {
		members = append(members, longhorn.SnapshotGroupMember{
			VolumeName:   volume,
			SnapshotName: testMemberSnapshotName(name, volume),
		})
	}
	return &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: longhorn.SnapshotGroupSpec{
			Volumes:         volumes,
			DeadlineSeconds: types.SnapshotGroupDefaultDeadlineSeconds,
			Members:         members,
		},
	}
}

func TestSnapshotGroupValidatorCreate(t *testing.T) {
	v := &snapshotGroupValidator{}

	cases := []struct {
		name      string
		mutate    func(sg *longhorn.SnapshotGroup)
		expectErr bool
	}{
		{"valid explicit-volumes group", func(sg *longhorn.SnapshotGroup) {}, false},
		{"name at the 54-character cap", func(sg *longhorn.SnapshotGroup) {
			sg.Name = strings.Repeat("a", types.SnapshotGroupNameMaxLength)
			for i := range sg.Spec.Members {
				sg.Spec.Members[i].SnapshotName = testMemberSnapshotName(sg.Name, sg.Spec.Members[i].VolumeName)
			}
		}, false},
		{"name over the 54-character cap", func(sg *longhorn.SnapshotGroup) {
			sg.Name = strings.Repeat("a", types.SnapshotGroupNameMaxLength+1)
		}, true},
		{"name not a valid label value", func(sg *longhorn.SnapshotGroup) {
			sg.Name = "trailing-dash-"
		}, true},
		{"both volumes and volumeSelector set", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.VolumeSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app-group": "analytics-db"}}
		}, true},
		{"neither volumes nor volumeSelector set", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Volumes = nil
		}, true},
		{"invalid engine snapshot label", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Labels = map[string]string{"key": "a=b"}
		}, true},
		{"reserved recurring-job label key", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Labels = map[string]string{types.RecurringJobLabel: "job"}
		}, true},
		{"deadline below the minimum", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.DeadlineSeconds = types.SnapshotGroupMinDeadlineSeconds - 1
		}, true},
		{"deadline above the maximum", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.DeadlineSeconds = types.SnapshotGroupMaxDeadlineSeconds + 1
		}, true},
		{"deadline unset (mutator default missing)", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.DeadlineSeconds = 0
		}, true},
		{"empty member set", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Members = nil
		}, true},
		{"member count over the cap", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Volumes = nil
			sg.Spec.Members = nil
			for i := 0; i <= types.SnapshotGroupMaxMemberCount; i++ {
				volume := "pvc-" + strings.Repeat("0", 8) + "-" + string(rune('a'+i%26)) + strings.Repeat("x", i/26)
				sg.Spec.Volumes = append(sg.Spec.Volumes, volume)
				sg.Spec.Members = append(sg.Spec.Members, longhorn.SnapshotGroupMember{
					VolumeName:   volume,
					SnapshotName: testMemberSnapshotName(sg.Name, volume),
				})
			}
		}, true},
		{"member without a volume name", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Members[0].VolumeName = ""
		}, true},
		{"duplicate member volume", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Members[1] = sg.Spec.Members[0]
		}, true},
		{"member snapshot name with a wrong-length suffix", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Members[0].SnapshotName = sg.Name + "-short"
		}, true},
		{"member snapshot name without the group name prefix", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Members[0].SnapshotName = "other-" + strings.Repeat("a", types.SnapshotGroupMemberSnapshotNameSuffixLength)
		}, true},
		{"members not covering the explicit volumes", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Volumes = sg.Spec.Volumes[:1]
		}, true},
		{"duplicate volumes are rejected", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Volumes = []string{sg.Spec.Volumes[0], sg.Spec.Volumes[0]}
		}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sg := validGroup()
			tc.mutate(sg)
			err := v.Create(nil, sg)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestSnapshotGroupValidatorRejectsMemberSnapshotNameCollision verifies that
// Create rejects a group where two members share one snapshot name.
func TestSnapshotGroupValidatorRejectsMemberSnapshotNameCollision(t *testing.T) {
	v := &snapshotGroupValidator{}

	sg := validGroup()
	sharedName := testMemberSnapshotName(sg.Name, "vol-shared")
	for i := range sg.Spec.Members {
		sg.Spec.Members[i].SnapshotName = sharedName
	}

	err := v.Create(nil, sg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "share the same member snapshot name")
}

func TestSnapshotGroupValidatorUpdate(t *testing.T) {
	v := &snapshotGroupValidator{}

	t.Run("identical spec is allowed", func(t *testing.T) {
		assert.NoError(t, v.Update(nil, validGroup(), validGroup()))
	})

	t.Run("status-only change is allowed", func(t *testing.T) {
		newObj := validGroup()
		newObj.Status.Phase = longhorn.SnapshotGroupPhaseInProgress
		assert.NoError(t, v.Update(nil, validGroup(), newObj))
	})

	t.Run("any spec change is rejected", func(t *testing.T) {
		newObj := validGroup()
		newObj.Spec.DeadlineSeconds++
		assert.Error(t, v.Update(nil, validGroup(), newObj))
	})

	t.Run("dropping spec.members is rejected", func(t *testing.T) {
		newObj := validGroup()
		newObj.Spec.Members = nil
		assert.Error(t, v.Update(nil, validGroup(), newObj))
	})

	t.Run("unchanged CSI type label is allowed", func(t *testing.T) {
		csiTypeLabelKey := types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType)
		oldObj := validGroup()
		oldObj.Labels = map[string]string{csiTypeLabelKey: "snap"}
		newObj := validGroup()
		newObj.Labels = map[string]string{csiTypeLabelKey: "snap"}
		assert.NoError(t, v.Update(nil, oldObj, newObj))
	})

	t.Run("changing, removing, or adding the CSI type label is rejected", func(t *testing.T) {
		csiTypeLabelKey := types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType)
		withLabel := func(csiSnapshotType string) *longhorn.SnapshotGroup {
			group := validGroup()
			if csiSnapshotType != "" {
				group.Labels = map[string]string{csiTypeLabelKey: csiSnapshotType}
			}
			return group
		}
		assert.Error(t, v.Update(nil, withLabel("snap"), withLabel("bak")))
		assert.Error(t, v.Update(nil, withLabel("snap"), withLabel("")))
		assert.Error(t, v.Update(nil, withLabel(""), withLabel("snap")))
	})
}

// TestSnapshotGroupMemberSnapshotNameFormatNeverChanges pins the name format
// as a persistence contract: the group name prefix ties a member to its
// group, and the suffix length backs the SnapshotGroupNameMaxLength math.
// Existing groups carry these names in spec.members, engine snapshots, and
// replica on-disk filenames across upgrades, so the format must never
// change. The suffix itself is random: it must not repeat when a group name
// is reused.
func TestSnapshotGroupMemberSnapshotNameFormatNeverChanges(t *testing.T) {
	groupName := strings.Repeat("a", types.SnapshotGroupNameMaxLength)

	name := types.GenerateSnapshotGroupMemberSnapshotName(groupName)
	assert.True(t, strings.HasPrefix(name, groupName+"-"))
	assert.Len(t, name, len(groupName)+1+types.SnapshotGroupMemberSnapshotNameSuffixLength)
	assert.LessOrEqual(t, len(name), 63)
	assert.NotEqual(t, name, types.GenerateSnapshotGroupMemberSnapshotName(groupName))
}
