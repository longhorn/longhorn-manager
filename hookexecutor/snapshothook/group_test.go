package snapshothook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func hookedGroup(phase longhorn.SnapshotGroupPhase, podPhases ...longhorn.HookPhase) *longhorn.SnapshotGroup {
	group := &longhorn.SnapshotGroup{
		Spec: longhorn.SnapshotGroupSpec{
			Hooks:   longhorn.SnapshotGroupHooksCommand,
			Members: []longhorn.SnapshotGroupMember{{VolumeName: "vol-a", SnapshotName: "g-00000001"}},
		},
		Status: longhorn.SnapshotGroupStatus{Phase: phase},
	}
	if len(podPhases) > 0 {
		member := longhorn.HookStatus{SnapshotName: "g-00000001"}
		for i, podPhase := range podPhases {
			member.Pods = append(member.Pods, longhorn.HookPodStatus{
				Namespace: "app", Name: "pod-" + string(rune('a'+i)), Phase: podPhase,
			})
		}
		group.Status.Hooks = []longhorn.HookStatus{member}
	}
	return group
}

func TestMembersInPauseOrder(t *testing.T) {
	members := []longhorn.SnapshotGroupMember{
		{VolumeName: "vol-a", SnapshotName: "g-1"},
		{VolumeName: "vol-b", SnapshotName: "g-2"},
		{VolumeName: "vol-c", SnapshotName: "g-3"},
	}
	volumeNames := func(ordered []longhorn.SnapshotGroupMember) []string {
		names := make([]string, 0, len(ordered))
		for _, m := range ordered {
			names = append(names, m.VolumeName)
		}
		return names
	}

	t.Run("selector group keeps volume-name order", func(t *testing.T) {
		group := &longhorn.SnapshotGroup{Spec: longhorn.SnapshotGroupSpec{Members: members}}
		assert.Equal(t, []string{"vol-a", "vol-b", "vol-c"}, volumeNames(MembersInPauseOrder(group)))
	})
	t.Run("explicit list orders by spec.volumes index", func(t *testing.T) {
		group := &longhorn.SnapshotGroup{Spec: longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-c", "vol-a", "vol-b"}, Members: members,
		}}
		assert.Equal(t, []string{"vol-c", "vol-a", "vol-b"}, volumeNames(MembersInPauseOrder(group)))
		assert.Equal(t, "vol-a", members[0].VolumeName, "spec.members is left untouched")
	})
}

func TestIsHooked(t *testing.T) {
	cases := []struct {
		name   string
		hooks  longhorn.SnapshotGroupHooks
		hooked bool
	}{
		{"empty is the pre-hooks default and reads as none", "", false},
		{"none is not hooked", longhorn.SnapshotGroupHooksNone, false},
		{"command is hooked", longhorn.SnapshotGroupHooksCommand, true},
		{"a future sibling mechanism is hooked", longhorn.SnapshotGroupHooks("external"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.hooked, HooksEnabled(c.hooks))
			group := &longhorn.SnapshotGroup{Spec: longhorn.SnapshotGroupSpec{Hooks: c.hooks}}
			assert.Equal(t, c.hooked, IsHooked(group))
		})
	}
}

func TestIsGroupUnreleased(t *testing.T) {
	cases := []struct {
		name       string
		group      *longhorn.SnapshotGroup
		unreleased bool
	}{
		{"hooks none is never unreleased, even while in progress", func() *longhorn.SnapshotGroup {
			g := hookedGroup(longhorn.SnapshotGroupPhaseInProgress)
			g.Spec.Hooks = longhorn.SnapshotGroupHooksNone
			return g
		}(), false},
		{"unreconciled hooked group is unreleased", hookedGroup(""), true},
		{"RunningPreHooks is unreleased", hookedGroup(longhorn.SnapshotGroupPhaseRunningPreHooks), true},
		{"InProgress is unreleased", hookedGroup(longhorn.SnapshotGroupPhaseInProgress), true},
		{"Ready with nothing recorded is released", hookedGroup(longhorn.SnapshotGroupPhaseReady), false},
		{"Ready with a PreDone pod is unreleased", hookedGroup(longhorn.SnapshotGroupPhaseReady, longhorn.HookPhasePreDone), true},
		{"Ready with a PostRunning pod is unreleased", hookedGroup(longhorn.SnapshotGroupPhaseReady, longhorn.HookPhasePostRunning), true},
		{"Ready with every pod PostDone is released", hookedGroup(longhorn.SnapshotGroupPhaseReady, longhorn.HookPhasePostDone, longhorn.HookPhasePostDone), false},
		{"Failed with a PostFailed pod is released", hookedGroup(longhorn.SnapshotGroupPhaseFailed, longhorn.HookPhasePostFailed), false},
		{"Failed with one pod still PreDone is unreleased", hookedGroup(longhorn.SnapshotGroupPhaseFailed, longhorn.HookPhasePostDone, longhorn.HookPhasePreDone), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.unreleased, IsGroupUnreleased(c.group))
			assert.Equal(t, c.unreleased, GroupHasUnreleasedMember(c.group, "vol-a"))
			assert.False(t, GroupHasUnreleasedMember(c.group, "vol-other"))
		})
	}
}

func TestGroupHasUnreleasedPod(t *testing.T) {
	group := hookedGroup(longhorn.SnapshotGroupPhaseInProgress, longhorn.HookPhasePreDone, longhorn.HookPhasePostDone)
	group.Status.Hooks[0].Pods[0].UID = "uid-a"
	group.Status.Hooks[0].Pods[1].UID = "uid-b"

	assert.True(t, GroupHasUnreleasedPod(group, "app", "pod-a", "uid-a"))
	assert.True(t, GroupHasUnreleasedPod(group, "app", "pod-a", ""), "an empty UID matches by name alone")
	assert.False(t, GroupHasUnreleasedPod(group, "app", "pod-a", "uid-replacement"), "a same-name pod with a new UID never ran the pre-command")
	assert.False(t, GroupHasUnreleasedPod(group, "other", "pod-a", "uid-a"), "the namespace must match too")
	assert.False(t, GroupHasUnreleasedPod(group, "app", "pod-b", "uid-b"), "a released pod holds nothing")
}

func TestUnreleasedHookPods(t *testing.T) {
	group := hookedGroup(longhorn.SnapshotGroupPhaseFailed,
		longhorn.HookPhasePostDone, longhorn.HookPhasePreDone, longhorn.HookPhasePostFailed, longhorn.HookPhasePostRunning)
	pods := UnreleasedHookPods(group)
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	assert.Equal(t, []string{"pod-b", "pod-d"}, names)
	assert.Empty(t, UnreleasedHookPods(hookedGroup(longhorn.SnapshotGroupPhaseReady)))
}

func TestReleaseStarted(t *testing.T) {
	cases := []struct {
		name    string
		group   *longhorn.SnapshotGroup
		started bool
	}{
		{"nothing recorded", hookedGroup(longhorn.SnapshotGroupPhaseRunningPreHooks), false},
		{"pods still in the pre phases", hookedGroup(longhorn.SnapshotGroupPhaseInProgress, longhorn.HookPhasePreRunning, longhorn.HookPhasePreDone, longhorn.HookPhasePreFailed), false},
		{"one pod PostRunning", hookedGroup(longhorn.SnapshotGroupPhaseInProgress, longhorn.HookPhasePreDone, longhorn.HookPhasePostRunning), true},
		{"one pod PostDone", hookedGroup(longhorn.SnapshotGroupPhaseReady, longhorn.HookPhasePostDone), true},
		{"one pod PostFailed", hookedGroup(longhorn.SnapshotGroupPhaseFailed, longhorn.HookPhasePostFailed), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.started, ReleaseStarted(c.group))
		})
	}
}

func TestPostFailedHookPodWarnings(t *testing.T) {
	abandoned := hookedGroup(longhorn.SnapshotGroupPhaseFailed,
		longhorn.HookPhasePostDone, longhorn.HookPhasePostFailed, longhorn.HookPhasePostFailed)
	abandoned.Name = "abandoned"
	groups := []*longhorn.SnapshotGroup{
		abandoned,
		hookedGroup(longhorn.SnapshotGroupPhaseReady, longhorn.HookPhasePostDone),
		hookedGroup(longhorn.SnapshotGroupPhaseInProgress),
	}

	warnings := PostFailedHookPodWarnings(groups, "vol-a")
	assert.Len(t, warnings, 2, "one per PostFailed pod; PostDone pods and groups with nothing recorded are silent")
	assert.Contains(t, warnings[0], "pod app/pod-b on member volume vol-a")
	assert.Contains(t, warnings[0], "snapshot group abandoned")
	assert.Contains(t, warnings[1], "pod app/pod-c")

	assert.Empty(t, PostFailedHookPodWarnings(groups, "vol-other"), "only the named volume's member counts")
}

func TestRunningWorkloadPods(t *testing.T) {
	volume := &longhorn.Volume{}
	assert.Empty(t, RunningWorkloadPods(volume))

	volume.Status.KubernetesStatus.WorkloadsStatus = []longhorn.WorkloadStatus{
		{PodName: "db-0", PodStatus: string(corev1.PodRunning)},
		{PodName: "db-1", PodStatus: string(corev1.PodPending)},
		{PodName: "db-2", PodStatus: string(corev1.PodRunning)},
	}
	assert.Equal(t, []string{"db-0", "db-2"}, RunningWorkloadPods(volume))

	// A stamped LastPodRefAt marks the whole list as history.
	volume.Status.KubernetesStatus.LastPodRefAt = "2026-01-01T00:00:00Z"
	assert.Empty(t, RunningWorkloadPods(volume))
}
