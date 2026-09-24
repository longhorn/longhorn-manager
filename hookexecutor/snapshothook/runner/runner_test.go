package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/exec"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	testGroupName         = "group-a"
	testLonghornNamespace = "longhorn-system"
	testVolume            = "vol-a"
	testSnapshot          = "snap-a"
	testNamespace         = "app"
	testPVC               = "data"
	testPod               = "mongo-0"
	testPodUID            = "pod-uid-1"
	testPolicyName        = "shared"
	testPolicyUID         = "policy-uid-1"
)

var groupResource = schema.GroupResource{Group: "longhorn.io", Resource: "snapshotgroups"}

// fakeStore is an in-memory Store. Groups are keyed by name; status writes
// bump resourceVersion and fail with Conflict when stale.
type fakeStore struct {
	mu       sync.Mutex
	groups   map[string]*longhorn.SnapshotGroup
	volumes  map[string]*longhorn.Volume
	policies map[string]*longhorn.VolumeHookPolicy
	pods     map[string]*corev1.Pod
	settings map[types.SettingName]int64
	writes   int
	// policyErr and findPodErr, when set, are returned by the policy GET
	// and the pod read instead of the normal result.
	policyErr  error
	findPodErr error
	// beforeWrite, when set, is called before each status write, with the
	// number of writes that have succeeded so far and the stored object.
	// Tests use it to simulate the group controller writing status
	// concurrently.
	beforeWrite func(writes int, stored *longhorn.SnapshotGroup)
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		groups:   map[string]*longhorn.SnapshotGroup{},
		volumes:  map[string]*longhorn.Volume{},
		policies: map[string]*longhorn.VolumeHookPolicy{},
		pods:     map[string]*corev1.Pod{},
		settings: map[types.SettingName]int64{types.SettingNameHookReleaseAttempts: 3, types.SettingNameHookReleaseTimeout: 60},
	}
}

func (s *fakeStore) GetSnapshotGroup(_ context.Context, name string) (*longhorn.SnapshotGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	group, ok := s.groups[name]
	if !ok {
		return nil, apierrors.NewNotFound(groupResource, name)
	}
	return group.DeepCopy(), nil
}

func (s *fakeStore) UpdateSnapshotGroupStatus(_ context.Context, group *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.groups[group.Name]
	if !ok {
		return nil, apierrors.NewNotFound(groupResource, group.Name)
	}
	if s.beforeWrite != nil {
		s.beforeWrite(s.writes, stored)
	}
	if stored.ResourceVersion != group.ResourceVersion {
		return nil, apierrors.NewConflict(groupResource, group.Name, errors.New("stale"))
	}
	updated := stored.DeepCopy()
	updated.Status = *group.Status.DeepCopy()
	updated.ResourceVersion = stored.ResourceVersion + "1"
	s.groups[group.Name] = updated
	s.writes++
	return updated.DeepCopy(), nil
}

func (s *fakeStore) ListSnapshotGroupsRO() ([]*longhorn.SnapshotGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var groups []*longhorn.SnapshotGroup
	for _, group := range s.groups {
		groups = append(groups, group.DeepCopy())
	}
	return groups, nil
}

func (s *fakeStore) GetVolumeRO(name string) (*longhorn.Volume, error) {
	volume, ok := s.volumes[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "longhorn.io", Resource: "volumes"}, name)
	}
	return volume.DeepCopy(), nil
}

func (s *fakeStore) GetVolumeHookPolicy(_ context.Context, namespace, name string) (*longhorn.VolumeHookPolicy, error) {
	if s.policyErr != nil {
		return nil, s.policyErr
	}
	policy, ok := s.policies[namespace+"/"+name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "longhorn.io", Resource: "volumehookpolicies"}, name)
	}
	return policy.DeepCopy(), nil
}

func (s *fakeStore) ListPods(_ context.Context, namespace string) ([]corev1.Pod, error) {
	var pods []corev1.Pod
	for _, pod := range s.pods {
		if pod.Namespace == namespace {
			pods = append(pods, *pod.DeepCopy())
		}
	}
	return pods, nil
}

func (s *fakeStore) FindPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	if s.findPodErr != nil {
		return nil, s.findPodErr
	}
	pod, ok := s.pods[namespace+"/"+name]
	if !ok {
		return nil, nil
	}
	return pod.DeepCopy(), nil
}

func (s *fakeStore) Namespace() string {
	return testLonghornNamespace
}

func (s *fakeStore) GetSettingAsInt(name types.SettingName) (int64, error) {
	return s.settings[name], nil
}

func (s *fakeStore) group(name string) *longhorn.SnapshotGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groups[name].DeepCopy()
}

// fakeExec records every call and answers from a script keyed by pod name.
type fakeExec struct {
	mu      sync.Mutex
	calls   []runTarget
	results map[string][]error
}

func (f *fakeExec) Exec(_ context.Context, namespace, pod, container string, command []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, runTarget{namespace: namespace, pod: pod, container: container, command: command})
	queue := f.results[pod]
	if len(queue) == 0 {
		return nil
	}
	f.results[pod] = queue[1:]
	return queue[0]
}

func (f *fakeExec) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type fixture struct {
	store    *fakeStore
	exec     *fakeExec
	recorder *record.FakeRecorder
	runner   *Runner
	now      time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		store:    newFakeStore(),
		exec:     &fakeExec{results: map[string][]error{}},
		recorder: record.NewFakeRecorder(32),
		now:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	f.runner = New(logger, f.store, f.exec, f.recorder, DefaultPauseWorkers)
	f.runner.now = func() time.Time { return f.now }
	return f
}

func (f *fixture) events() []string {
	var events []string
	for {
		select {
		case event := <-f.recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

func (f *fixture) policy(generation int64, hooks ...longhorn.VolumeHookPolicyHook) {
	if len(hooks) == 0 {
		hooks = []longhorn.VolumeHookPolicyHook{{Exec: &longhorn.VolumeHookPolicyExec{PreCommand: []string{"fsfreeze", "-f"}, PostCommand: []string{"fsfreeze", "-u"}}}}
	}
	f.store.policies[testNamespace+"/"+testPolicyName] = &longhorn.VolumeHookPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testPolicyName, UID: testPolicyUID, Generation: generation},
		Spec:       longhorn.VolumeHookPolicySpec{Hooks: hooks},
	}
}

// testPolicyRef is the reference admission stamps for the fixture policy at
// generation 1.
func testPolicyRef() *longhorn.VolumeHookPolicyRef {
	return &longhorn.VolumeHookPolicyRef{Namespace: testNamespace, Name: testPolicyName, UID: testPolicyUID, Generation: 1}
}

func testPVCRef() *longhorn.PVCRef {
	return &longhorn.PVCRef{Namespace: testNamespace, Name: testPVC}
}

func (f *fixture) volume() {
	f.volumeWithPVC(testVolume, testPVC)
}

func (f *fixture) volumeWithPVC(volumeName, pvcName string) {
	f.store.volumes[volumeName] = &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: volumeName},
		Status: longhorn.VolumeStatus{KubernetesStatus: longhorn.KubernetesStatus{
			Namespace: testNamespace, PVCName: pvcName,
		}},
	}
}

func (f *fixture) runningPod(name, uid string, containers ...string) {
	f.runningPodMounting(name, uid, testPVC, containers...)
}

func (f *fixture) runningPodMounting(name, uid, pvcName string, containers ...string) {
	if len(containers) == 0 {
		containers = []string{"mongod"}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: name, UID: k8stypes.UID(uid)},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
		}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for i, container := range containers {
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: container})
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: container, ContainerID: "containerd://c" + string(rune('1'+i))})
	}
	f.store.pods[testNamespace+"/"+name] = pod
}

func (f *fixture) newGroup(phase longhorn.SnapshotGroupPhase) *longhorn.SnapshotGroup {
	return &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testLonghornNamespace, Name: testGroupName, UID: "group-uid-a", ResourceVersion: "1",
			CreationTimestamp: metav1.NewTime(f.now.Add(-5 * time.Second)),
		},
		Spec: longhorn.SnapshotGroupSpec{
			Hooks:           longhorn.SnapshotGroupHooksCommand,
			HookOperation:   longhorn.HookOperationSnapshot,
			DeadlineSeconds: 60,
			Members:         []longhorn.SnapshotGroupMember{{VolumeName: testVolume, SnapshotName: testSnapshot, HookPolicyRef: testPolicyRef()}},
		},
		Status: longhorn.SnapshotGroupStatus{Phase: phase},
	}
}

// pausedGroup is a group after a successful pause: Ready, with one recorded
// PreDone pod.
func (f *fixture) pausedGroup(phase longhorn.SnapshotGroupPhase) *longhorn.SnapshotGroup {
	group := f.newGroup(phase)
	preDoneAt := metav1.NewTime(f.now.Add(-3 * time.Second))
	group.Status.PreHooksDone = true
	group.Status.PreHooksDoneAt = &preDoneAt
	group.Status.Hooks = []longhorn.HookStatus{{
		SnapshotName: testSnapshot, Phase: longhorn.HookPhasePreDone, PVCRef: testPVCRef(),
		Pods: []longhorn.HookPodStatus{{
			Namespace: testNamespace, Name: testPod, UID: testPodUID, ContainerIDs: []string{"containerd://c1"},
			HookName: "default", Phase: longhorn.HookPhasePreDone,
		}},
	}}
	return group
}

func (f *fixture) putGroup(group *longhorn.SnapshotGroup) {
	f.store.groups[group.Name] = group
}

// controllerWritesBefore makes the fake store act as the group controller
// writing a condition once, right before the Nth successful write: that
// write then fails with Conflict, and the fresh object carries the
// condition.
func (f *fixture) controllerWritesBefore(write int) {
	f.store.beforeWrite = func(writes int, stored *longhorn.SnapshotGroup) {
		if writes != write {
			return
		}
		f.store.beforeWrite = nil
		stored.ResourceVersion += "c"
		stored.Status.Conditions = types.SetCondition(stored.Status.Conditions,
			longhorn.SnapshotGroupConditionTypeDegraded, longhorn.ConditionStatusFalse, "", "")
	}
}

func exitError(code int) error {
	return exec.CodeExitError{Err: errors.New("exit"), Code: code}
}

func TestPauseRunsPreCommandAndMarksDone(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))

	requeue, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)

	group := f.store.group(testGroupName)
	require.Len(t, group.Status.Hooks, 1)
	hook := group.Status.Hooks[0]
	assert.Equal(t, longhorn.HookPhasePreDone, hook.Phase)
	assert.Equal(t, testPVCRef(), hook.PVCRef)
	require.Len(t, hook.Pods, 1)
	assert.Equal(t, longhorn.HookPhasePreDone, hook.Pods[0].Phase)
	assert.Equal(t, testPodUID, hook.Pods[0].UID)
	assert.Equal(t, []string{"containerd://c1"}, hook.Pods[0].ContainerIDs)
	assert.Equal(t, "default", hook.Pods[0].HookName)
	assert.True(t, group.Status.PreHooksDone)
	assert.NotNil(t, group.Status.PreHooksDoneAt)
	assert.NotNil(t, group.Status.HooksStartedAt)
	assert.Equal(t, "pre done", group.Status.HookProgress)

	require.Len(t, f.exec.calls, 1)
	assert.Equal(t, runTarget{namespace: testNamespace, pod: testPod, container: "mongod", command: []string{"fsfreeze", "-f"}}, f.exec.calls[0])
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "PreHooksSucceeded")
}

// A release that takes the group over after the last outcome write leaves
// the pre-hooks-done write to it: nothing more is written, and no
// PreHooksSucceeded is reported for a pause that did not complete.
func TestPauseGatedBeforeDoneWriteReportsNothing(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
	f.putGroup(group)
	f.store.beforeWrite = func(writes int, _ *longhorn.SnapshotGroup) {
		if writes == 1 {
			// The deadline passed during the outcome write; the release
			// gates the group before the pause's final write.
			f.runner.gateRelease(group.Name, group.UID)
		}
	}

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)

	stored := f.store.group(testGroupName)
	assert.Equal(t, longhorn.HookPhasePreDone, stored.Status.Hooks[0].Phase)
	assert.False(t, stored.Status.PreHooksDone)
	assert.Empty(t, f.events())
}

// An explicit-list group pauses members in spec.volumes order and releases
// them in reverse, even though spec.members is stored sorted by volume name.
func TestPauseAndReleaseFollowSpecVolumesOrder(t *testing.T) {
	const (
		coordinatorVolume = "vol-b"
		coordinatorPVC    = "config"
		coordinatorPod    = "config-0"
	)
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.volumeWithPVC(coordinatorVolume, coordinatorPVC)
	f.runningPod(testPod, testPodUID)
	f.runningPodMounting(coordinatorPod, "pod-uid-2", coordinatorPVC)

	group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
	group.Spec.Volumes = []string{coordinatorVolume, testVolume}
	group.Spec.Members = append(group.Spec.Members, longhorn.SnapshotGroupMember{
		VolumeName: coordinatorVolume, SnapshotName: "snap-b",
		HookPolicyRef: testPolicyRef(),
	})
	f.putGroup(group)

	requeue, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	require.Len(t, f.exec.calls, 2)
	assert.Equal(t, coordinatorPod, f.exec.calls[0].pod, "coordinator pauses first")
	assert.Equal(t, testPod, f.exec.calls[1].pod)

	group = f.store.group(testGroupName)
	require.True(t, group.Status.PreHooksDone)
	group.Status.Phase = longhorn.SnapshotGroupPhaseReady
	f.putGroup(group)

	requeue, err = f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	require.Len(t, f.exec.calls, 4)
	assert.Equal(t, testPod, f.exec.calls[2].pod)
	assert.Equal(t, coordinatorPod, f.exec.calls[3].pod, "coordinator releases last")
	for _, hook := range f.store.group(testGroupName).Status.Hooks {
		assert.Equal(t, longhorn.HookPhasePostDone, hook.Phase, hook.SnapshotName)
	}
}

func TestPauseMemberLevelFailures(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(f *fixture, group *longhorn.SnapshotGroup)
		reason longhorn.HookReason
	}{
		{"volume in an older unreleased group", func(f *fixture, _ *longhorn.SnapshotGroup) {
			f.policy(1)
			f.volume()
			f.runningPod(testPod, testPodUID)
			older := f.newGroup(longhorn.SnapshotGroupPhaseInProgress)
			older.Name = "group-0"
			older.UID = "group-uid-0"
			older.CreationTimestamp = metav1.NewTime(f.now.Add(-time.Minute))
			f.putGroup(older)
		}, longhorn.HookReasonVolumeConflict},
		{"pod paused by another group through a different volume", func(f *fixture, _ *longhorn.SnapshotGroup) {
			// The pod mounts this member's PVC and another group's PVC. The
			// groups share no volume, so only the pod record names the
			// conflict.
			f.policy(1)
			f.volume()
			f.runningPod(testPod, testPodUID)
			holder := f.pausedGroup(longhorn.SnapshotGroupPhaseInProgress)
			holder.Name = "group-b"
			holder.UID = "group-uid-b"
			holder.Spec.Members = []longhorn.SnapshotGroupMember{{VolumeName: "vol-b", SnapshotName: "snap-b", HookPolicyRef: testPolicyRef()}}
			holder.Status.Hooks[0].SnapshotName = "snap-b"
			f.putGroup(holder)
		}, longhorn.HookReasonPodConflict},
		{"member without a policy reference", func(f *fixture, group *longhorn.SnapshotGroup) {
			f.policy(1)
			f.volume()
			f.runningPod(testPod, testPodUID)
			group.Spec.Members[0].HookPolicyRef = nil
		}, longhorn.HookReasonPolicyNotFound},
		{"policy missing", func(f *fixture, _ *longhorn.SnapshotGroup) {
			f.volume()
			f.runningPod(testPod, testPodUID)
		}, longhorn.HookReasonPolicyNotFound},
		{"policy changed", func(f *fixture, _ *longhorn.SnapshotGroup) {
			f.policy(2)
			f.volume()
			f.runningPod(testPod, testPodUID)
		}, longhorn.HookReasonPolicyChanged},
		{"policy recreated with the same name and generation", func(f *fixture, _ *longhorn.SnapshotGroup) {
			// Deleting and recreating a policy restarts its generation at 1,
			// so only the UID tells the replacement apart.
			f.policy(1)
			f.store.policies[testNamespace+"/"+testPolicyName].UID = testPolicyUID + "-recreated"
			f.volume()
			f.runningPod(testPod, testPodUID)
		}, longhorn.HookReasonPolicyChanged},
		{"pods in the Longhorn namespace", func(f *fixture, _ *longhorn.SnapshotGroup) {
			f.policy(1)
			f.volume()
			f.store.volumes[testVolume].Status.KubernetesStatus.Namespace = testLonghornNamespace
			f.runningPod(testPod, testPodUID)
		}, longhorn.HookReasonPolicyNotFound},
		{"no running pod", func(f *fixture, _ *longhorn.SnapshotGroup) {
			f.policy(1)
			f.volume()
		}, longhorn.HookReasonPodNotFound},
		{"multi-container pod without a named container", func(f *fixture, _ *longhorn.SnapshotGroup) {
			f.policy(1)
			f.volume()
			f.runningPod(testPod, testPodUID, "mongod", "exporter")
		}, longhorn.HookReasonHookNotMatched},
		{"policy hook runs for another operation only", func(f *fixture, _ *longhorn.SnapshotGroup) {
			f.policy(1, longhorn.VolumeHookPolicyHook{
				Operations: []longhorn.HookOperation{longhorn.HookOperationBackup},
				Exec:       &longhorn.VolumeHookPolicyExec{PreCommand: []string{"fsfreeze", "-f"}, PostCommand: []string{"fsfreeze", "-u"}},
			})
			f.volume()
			f.runningPod(testPod, testPodUID)
		}, longhorn.HookReasonHookNotMatched},
		{"policy changed is decided before the hook match", func(f *fixture, _ *longhorn.SnapshotGroup) {
			// The generation check runs before any pod is matched, so a
			// changed policy is reported as such even when its hooks no
			// longer fit the pod.
			f.policy(2, longhorn.VolumeHookPolicyHook{
				Operations: []longhorn.HookOperation{longhorn.HookOperationBackup},
				Exec:       &longhorn.VolumeHookPolicyExec{PreCommand: []string{"fsfreeze", "-f"}, PostCommand: []string{"fsfreeze", "-u"}},
			})
			f.volume()
			f.runningPod(testPod, testPodUID)
		}, longhorn.HookReasonPolicyChanged},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
			tc.setup(f, group)
			f.putGroup(group)

			_, err := f.runner.pause(context.Background(), testGroupName)
			require.NoError(t, err)

			hook := f.store.group(testGroupName).Status.Hooks[0]
			assert.Equal(t, longhorn.HookPhasePreFailed, hook.Phase)
			assert.Equal(t, tc.reason, hook.Reason)
			assert.Empty(t, hook.Pods)
			assert.Equal(t, 0, f.exec.callCount())
			events := f.events()
			require.Len(t, events, 1)
			assert.Contains(t, events[0], "PreHookFailed")
			assert.Contains(t, events[0], string(tc.reason))
		})
	}
}

// A pod shared with another group blocks the pause only while that group
// holds it: once its pods are released, the next group may pause the same
// pod through its own volume.
func TestPauseProceedsWhenSharedPodWasReleased(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	holder := f.pausedGroup(longhorn.SnapshotGroupPhaseReady)
	holder.Name = "group-b"
	holder.UID = "group-uid-b"
	holder.Spec.Members = []longhorn.SnapshotGroupMember{{VolumeName: "vol-b", SnapshotName: "snap-b", HookPolicyRef: testPolicyRef()}}
	holder.Status.Hooks[0].SnapshotName = "snap-b"
	holder.Status.Hooks[0].Phase = longhorn.HookPhasePostDone
	holder.Status.Hooks[0].Pods[0].Phase = longhorn.HookPhasePostDone
	f.putGroup(holder)
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)

	group := f.store.group(testGroupName)
	assert.Equal(t, longhorn.HookPhasePreDone, group.Status.Hooks[0].Phase)
	assert.Equal(t, 1, f.exec.callCount())
}

func TestPauseRecordsCommandFailure(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.exec.results[testPod] = []error{exitError(2)}
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)

	group := f.store.group(testGroupName)
	hook := group.Status.Hooks[0]
	assert.Equal(t, longhorn.HookPhasePreFailed, hook.Phase)
	require.Len(t, hook.Pods, 1)
	assert.Equal(t, longhorn.HookPhasePreFailed, hook.Pods[0].Phase)
	assert.Equal(t, longhorn.HookReasonExitNonZero, hook.Pods[0].Reason)
	assert.Equal(t, int32(2), *hook.Pods[0].ExitCode)
	assert.False(t, group.Status.PreHooksDone)
	assert.Equal(t, "pre failed", group.Status.HookProgress)
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "PreHookFailed")
	assert.Contains(t, events[0], "exit 2")
	assert.NotContains(t, events[0], "fsfreeze")
}

// A pod whose identity cannot be confirmed right before its pre-command gets
// no command: the pod fails as could-not-start rather than being exec'd by
// name.
func TestPausePodReadErrorRunsNoPreCommand(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.store.findPodErr = apierrors.NewInternalError(errors.New("etcd timeout"))
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)

	hook := f.store.group(testGroupName).Status.Hooks[0]
	assert.Equal(t, longhorn.HookPhasePreFailed, hook.Phase)
	require.Len(t, hook.Pods, 1)
	assert.Equal(t, longhorn.HookPhasePreFailed, hook.Pods[0].Phase)
	assert.Equal(t, longhorn.HookReasonExecFailed, hook.Pods[0].Reason)
	assert.Equal(t, 0, f.exec.callCount())
}

func TestPauseSkipsIneligibleGroups(t *testing.T) {
	for name, mutate := range map[string]func(f *fixture, g *longhorn.SnapshotGroup){
		"not RunningPreHooks": func(_ *fixture, g *longhorn.SnapshotGroup) { g.Status.Phase = longhorn.SnapshotGroupPhaseInProgress },
		"already paused":      func(_ *fixture, g *longhorn.SnapshotGroup) { g.Status.PreHooksDone = true },
		"deleting":            func(f *fixture, g *longhorn.SnapshotGroup) { now := metav1.NewTime(f.now); g.DeletionTimestamp = &now },
		"past deadline":       func(f *fixture, g *longhorn.SnapshotGroup) { g.Spec.DeadlineSeconds = 1 },
		"release gated":       func(f *fixture, g *longhorn.SnapshotGroup) { f.runner.gateRelease(g.Name, g.UID) },
		// A release another leader began: no gate here, deadline still ahead
		// on this clock, but a recorded pod is already PostRunning.
		"release recorded by another leader": func(_ *fixture, g *longhorn.SnapshotGroup) {
			g.Status.Hooks = []longhorn.HookStatus{{
				SnapshotName: g.Spec.Members[0].SnapshotName,
				Phase:        longhorn.HookPhasePostRunning,
				Pods:         []longhorn.HookPodStatus{{Namespace: testNamespace, Name: testPod, Phase: longhorn.HookPhasePostRunning}},
			}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.policy(1)
			f.volume()
			f.runningPod(testPod, testPodUID)
			group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
			mutate(f, group)
			f.putGroup(group)

			_, err := f.runner.pause(context.Background(), testGroupName)
			require.NoError(t, err)
			assert.Equal(t, 0, f.store.writes)
			assert.Equal(t, 0, f.exec.callCount())
		})
	}
}

// A member found PreRunning on entry was left by a writer that stopped
// between the exec and the outcome write. The command is not run again; the pods
// fail as interrupted and stay recorded for the release.
func TestPauseFailsInterruptedMember(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
	group.Status.Hooks = []longhorn.HookStatus{{
		SnapshotName: testSnapshot, Phase: longhorn.HookPhasePreRunning, PVCRef: testPVCRef(),
		Pods: []longhorn.HookPodStatus{{Namespace: testNamespace, Name: testPod, UID: testPodUID, HookName: "default", Phase: longhorn.HookPhasePreRunning}},
	}}
	f.putGroup(group)

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.Equal(t, 0, f.exec.callCount())
	group = f.store.group(testGroupName)
	hook := group.Status.Hooks[0]
	assert.Equal(t, longhorn.HookPhasePreFailed, hook.Phase)
	assert.Equal(t, longhorn.HookPhasePreFailed, hook.Pods[0].Phase)
	assert.Equal(t, longhorn.HookReasonInterrupted, hook.Pods[0].Reason)
	assert.False(t, group.Status.PreHooksDone)
	assert.Equal(t, "pre failed", group.Status.HookProgress)
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "PreHookFailed")
	assert.Contains(t, events[0], "Interrupted")
}

// The group controller writes status concurrently. A Conflict on the write
// that follows the exec must not send the member back through the queue,
// where the command would run again: the outcomes are re-applied to the
// fresh object and the controller's fields are kept.
func TestPauseReappliesOutcomesOnConflict(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))
	f.controllerWritesBefore(1) // the write after the exec; the claim is write 0

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.Equal(t, 1, f.exec.callCount())
	group := f.store.group(testGroupName)
	assert.Equal(t, longhorn.HookPhasePreDone, group.Status.Hooks[0].Pods[0].Phase)
	assert.True(t, group.Status.PreHooksDone)
	assert.Len(t, group.Status.Conditions, 1, "the controller's write is kept")
}

// A record that differs from the one the commands ran against is another
// writer's; the round is reported rather than reconciled by guesswork.
func TestPauseRefusesToReapplyOverChangedRecord(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))
	f.store.beforeWrite = func(writes int, stored *longhorn.SnapshotGroup) {
		if writes != 1 {
			return
		}
		f.store.beforeWrite = nil
		stored.ResourceVersion += "c"
		stored.Status.Hooks[0].Pods[0].Phase = longhorn.HookPhasePreFailed
	}

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "changed underneath")
	assert.Equal(t, 1, f.exec.callCount())
}

// A Conflict on the claim write whose re-read shows the member already
// claimed means another writer owns it, which can only be a second leader
// during a lease handover. This caller must stop and write nothing: no
// pre-command runs twice, and the other writer's record is left as it is.
func TestClaimConflictLeavesClaimedMemberToOtherWriter(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))
	otherWriter := longhorn.HookStatus{
		SnapshotName: testSnapshot, Phase: longhorn.HookPhasePreRunning, PVCRef: testPVCRef(),
		Pods: []longhorn.HookPodStatus{{Namespace: testNamespace, Name: testPod, UID: testPodUID, Phase: longhorn.HookPhasePreRunning}},
	}
	f.store.beforeWrite = func(writes int, stored *longhorn.SnapshotGroup) {
		if writes != 0 {
			return
		}
		f.store.beforeWrite = nil
		stored.ResourceVersion += "c"
		stored.Status.Hooks = []longhorn.HookStatus{otherWriter}
	}

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.Equal(t, 0, f.exec.callCount(), "the pre-command has one runner")
	assert.Equal(t, 0, f.store.writes, "nothing is written over the other writer's claim")
	group := f.store.group(testGroupName)
	require.Len(t, group.Status.Hooks, 1)
	assert.Equal(t, otherWriter, group.Status.Hooks[0])
	assert.False(t, group.Status.PreHooksDone)
	assert.Empty(t, f.events())
}

func TestReleaseOfReplacedPodIsANoOp(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, "another-uid")
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	assert.Equal(t, 0, f.exec.callCount())
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostDone, pod.Phase)
	assert.Equal(t, longhorn.HookReasonPodNotFound, pod.Reason)
}

func TestReleaseRunsPostCommand(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)

	group := f.store.group(testGroupName)
	hook := group.Status.Hooks[0]
	assert.Equal(t, longhorn.HookPhasePostDone, hook.Phase)
	assert.Equal(t, longhorn.HookPhasePostDone, hook.Pods[0].Phase)
	assert.Equal(t, int32(1), hook.Pods[0].ReleaseAttempts)
	assert.Empty(t, hook.Pods[0].Reason)
	assert.NotNil(t, group.Status.PostHooksDoneAt)
	assert.Equal(t, "post done", group.Status.HookProgress)
	require.Len(t, f.exec.calls, 1)
	assert.Equal(t, []string{"fsfreeze", "-u"}, f.exec.calls[0].command)
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "PostHooksSucceeded")
	assert.True(t, f.runner.isReleaseGated(group.UID))
}

// transitionToReady wakes both the release and the controller's next
// reconcile, so a Conflict on the outcome write is the common case. It must
// not cost an attempt or a second post-command.
func TestReleaseReappliesOutcomesOnConflict(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))
	f.controllerWritesBefore(1) // the write after the exec; round one is write 0

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	assert.Equal(t, 1, f.exec.callCount())
	group := f.store.group(testGroupName)
	pod := group.Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostDone, pod.Phase)
	assert.Equal(t, int32(1), pod.ReleaseAttempts)
	assert.NotNil(t, group.Status.PostHooksDoneAt)
	assert.Len(t, group.Status.Conditions, 1, "the controller's write is kept")
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "PostHooksSucceeded")
}

func TestReleaseNotDueIsIgnored(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseInProgress))

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	assert.Equal(t, 0, f.exec.callCount())
	assert.Equal(t, 0, f.store.writes)
}

func TestReleaseRanAndFailedConsumesAttemptsUntilCap(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	f.exec.results[testPod] = []error{exitError(1), exitError(1), exitError(1)}
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseFailed))

	for round := 1; round <= 2; round++ {
		requeue, err := f.runner.release(context.Background(), testGroupName)
		require.NoError(t, err)
		assert.True(t, requeue, "round %d", round)
		pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
		assert.Equal(t, longhorn.HookPhasePostRunning, pod.Phase)
		assert.Equal(t, int32(round), pod.ReleaseAttempts)
		assert.Equal(t, longhorn.HookReasonExitNonZero, pod.Reason)
	}
	assert.Empty(t, f.events())

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	group := f.store.group(testGroupName)
	pod := group.Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostFailed, pod.Phase)
	assert.Equal(t, int32(3), pod.ReleaseAttempts)
	assert.Equal(t, longhorn.HookPhasePostFailed, group.Status.Hooks[0].Phase)
	assert.NotNil(t, group.Status.PostHooksDoneAt)
	assert.Equal(t, "post failed 1/1", group.Status.HookProgress)
	assert.Equal(t, 3, f.exec.callCount())
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "PostHookFailed")
	assert.Contains(t, events[0], "container mongod")
	assert.Contains(t, events[0], "kubectl exec")
	assert.NotContains(t, events[0], "fsfreeze")

	// A further round has nothing left to do and does not fire again.
	requeue, err = f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	assert.Equal(t, 3, f.exec.callCount())
	assert.Empty(t, f.events())
}

func TestReleaseCouldNotStartRefundsAttempt(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	f.exec.results[testPod] = []error{apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, testPod, errors.New("no exec"))}
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.True(t, requeue)
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostRunning, pod.Phase)
	assert.Equal(t, int32(0), pod.ReleaseAttempts)

	requeue, err = f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	pod = f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostDone, pod.Phase)
	assert.Equal(t, int32(1), pod.ReleaseAttempts)
}

func TestReleaseForbiddenWarnsHookReleaseBlocked(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	f.exec.results[testPod] = []error{apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, testPod, errors.New("no exec"))}
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.True(t, requeue)
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "HookReleaseBlocked")
	assert.Contains(t, events[0], "policy "+testNamespace+"/"+testPolicyName)
	assert.Contains(t, events[0], "pod "+testNamespace+"/"+testPod)
	assert.Contains(t, events[0], "denied pods/exec in namespace "+testNamespace)
	assert.Contains(t, events[0], "kind: RoleBinding")
	assert.Contains(t, events[0], "name: "+types.HookExecutorRoleBindingName)
	assert.Contains(t, events[0], "namespace: "+testNamespace)
	assert.Contains(t, events[0], "namespace: "+testLonghornNamespace)
}

// A deleted RoleBinding takes the policy and pod reads away with the exec,
// so the release is refused before any command is attempted. The event
// must fire from there too, and the round must leave the record untouched
// so the unbounded retry costs no attempt.
func TestReleaseForbiddenReadsWarnHookReleaseBlocked(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, testPod, errors.New("no binding"))
	cases := []struct {
		name   string
		setup  func(f *fixture)
		denied string
	}{
		{"policy read", func(f *fixture) { f.store.policyErr = forbidden }, "denied get on volumehookpolicies in namespace " + testNamespace},
		{"pod read", func(f *fixture) { f.store.findPodErr = forbidden }, "denied list on pods in namespace " + testNamespace},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.policy(1)
			f.runningPod(testPod, testPodUID)
			tc.setup(f)
			f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

			_, err := f.runner.release(context.Background(), testGroupName)
			require.Error(t, err)
			assert.True(t, apierrors.IsForbidden(err))
			assert.Equal(t, 0, f.exec.callCount())
			pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
			assert.Equal(t, longhorn.HookPhasePreDone, pod.Phase)
			assert.Equal(t, int32(0), pod.ReleaseAttempts)
			events := f.events()
			require.Len(t, events, 1)
			assert.Contains(t, events[0], "HookReleaseBlocked")
			assert.Contains(t, events[0], "policy "+testNamespace+"/"+testPolicyName)
			assert.Contains(t, events[0], tc.denied)
			assert.Contains(t, events[0], "kind: RoleBinding")
			assert.Contains(t, events[0], "namespace: "+testLonghornNamespace)
		})
	}
}

// A pod read that fails for any other reason is no answer about the pod:
// nothing is exec'd by name, and the round is handed back to the queue.
func TestReleasePodReadErrorRunsNothing(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	f.store.findPodErr = apierrors.NewInternalError(errors.New("etcd timeout"))
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	_, err := f.runner.release(context.Background(), testGroupName)
	require.Error(t, err)
	assert.Equal(t, 0, f.exec.callCount())
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePreDone, pod.Phase)
	assert.Equal(t, int32(0), pod.ReleaseAttempts)
	assert.Empty(t, f.events())
}

// TestReleaseCouldNotStartWarnsEachRoundWithoutSpendingAttempts covers the
// could-not-start that no grant fixes: the 500 the API server returns when
// it cannot dial the kubelet. Every round refunds the attempt and says so
// in an event, without the RoleBinding remedy; the recorder aggregates the
// repeats in the cluster.
func TestReleaseCouldNotStartWarnsEachRoundWithoutSpendingAttempts(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	dialFailure := apierrors.NewInternalError(errors.New("error dialing backend"))
	f.exec.results[testPod] = []error{dialFailure, dialFailure}
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	for round := 1; round <= 2; round++ {
		requeue, err := f.runner.release(context.Background(), testGroupName)
		require.NoError(t, err, "round %d", round)
		assert.True(t, requeue, "round %d", round)
		pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
		assert.Equal(t, longhorn.HookPhasePostRunning, pod.Phase, "round %d", round)
		assert.Equal(t, int32(0), pod.ReleaseAttempts, "round %d", round)
		events := f.events()
		require.Len(t, events, 1, "round %d", round)
		assert.Contains(t, events[0], "HookReleaseBlocked")
		assert.Contains(t, events[0], "could not start in pod "+testNamespace+"/"+testPod)
		assert.Contains(t, events[0], "error dialing backend")
		assert.NotContains(t, events[0], "RoleBinding")
	}

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostDone, pod.Phase)
	assert.Equal(t, int32(1), pod.ReleaseAttempts)
	assert.Equal(t, 3, f.exec.callCount())
}

// TestPauseWaitsForShuttingDownPod verifies that a pod being deleted while
// still Running is neither frozen nor ignored: the pause claims nothing and
// asks to run again, and proceeds once the pod is gone.
func TestPauseWaitsForShuttingDownPod(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.runningPod("mongo-1", "pod-uid-2")
	deletedAt := metav1.NewTime(f.now)
	f.store.pods[testNamespace+"/mongo-1"].DeletionTimestamp = &deletedAt
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.ErrorIs(t, err, errWaiting)
	assert.Contains(t, err.Error(), "mongo-1")
	assert.Empty(t, f.store.group(testGroupName).Status.Hooks)
	assert.Equal(t, 0, f.exec.callCount())
	assert.Empty(t, f.events())

	delete(f.store.pods, testNamespace+"/mongo-1")
	_, err = f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)
	hook := f.store.group(testGroupName).Status.Hooks[0]
	assert.Equal(t, longhorn.HookPhasePreDone, hook.Phase)
	require.Len(t, hook.Pods, 1)
	assert.Equal(t, testPod, hook.Pods[0].Name)
}

// TestPauseWaitsForStartingPod verifies that a Pending pod whose init
// container has started is neither frozen past nor ignored: it may already
// write to the claim but cannot take an exec yet, so the pause claims
// nothing and asks to run again, then records the pod once it is Running.
func TestPauseWaitsForStartingPod(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.volume()
	f.runningPod(testPod, testPodUID)
	f.runningPod("mongo-1", "pod-uid-2")
	starting := f.store.pods[testNamespace+"/mongo-1"]
	starting.Status.Phase = corev1.PodPending
	starting.Status.InitContainerStatuses = starting.Status.ContainerStatuses
	starting.Status.ContainerStatuses = nil
	f.putGroup(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))

	_, err := f.runner.pause(context.Background(), testGroupName)
	require.ErrorIs(t, err, errWaiting)
	assert.Contains(t, err.Error(), "mongo-1")
	assert.Empty(t, f.store.group(testGroupName).Status.Hooks)
	assert.Equal(t, 0, f.exec.callCount())

	starting.Status.Phase = corev1.PodRunning
	_, err = f.runner.pause(context.Background(), testGroupName)
	require.NoError(t, err)
	hook := f.store.group(testGroupName).Status.Hooks[0]
	assert.Equal(t, longhorn.HookPhasePreDone, hook.Phase)
	require.Len(t, hook.Pods, 2)
}

// TestOlderUnreleasedGroupTieBreaksOnName verifies that two groups created
// in the same instant order by name, so exactly one of them yields.
func TestOlderUnreleasedGroupTieBreaksOnName(t *testing.T) {
	f := newFixture(t)
	group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
	twin := f.newGroup(longhorn.SnapshotGroupPhaseInProgress)
	twin.Name = "group-0"
	twin.UID = "group-uid-0"
	f.putGroup(twin)

	other, err := f.runner.olderUnreleasedGroup(group, testVolume)
	require.NoError(t, err)
	assert.Equal(t, "group-0", other, "the smaller name goes first")

	twin.Name = "group-b"
	f.store.groups = map[string]*longhorn.SnapshotGroup{}
	f.putGroup(twin)
	other, err = f.runner.olderUnreleasedGroup(group, testVolume)
	require.NoError(t, err)
	assert.Equal(t, "", other, "the larger name yields")
}

func TestReleaseOfGonePodIsANoOp(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostDone, pod.Phase)
	assert.Equal(t, longhorn.HookReasonPodNotFound, pod.Reason)
	assert.Equal(t, int32(0), pod.ReleaseAttempts)
	assert.Equal(t, 0, f.exec.callCount())
}

// TestReleaseOfTerminalPodIsANoOp verifies that a recorded pod that reached
// a terminal phase is treated like a gone pod: its containers have stopped,
// so there is nothing to exec the post-command in.
func TestReleaseOfTerminalPodIsANoOp(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			f := newFixture(t)
			f.policy(1)
			f.runningPod(testPod, testPodUID)
			f.store.pods[testNamespace+"/"+testPod].Status.Phase = phase
			f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

			requeue, err := f.runner.release(context.Background(), testGroupName)
			require.NoError(t, err)
			assert.False(t, requeue)
			pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
			assert.Equal(t, longhorn.HookPhasePostDone, pod.Phase)
			assert.Equal(t, longhorn.HookReasonPodNotFound, pod.Reason)
			assert.Equal(t, 0, f.exec.callCount())
		})
	}
}

func TestReleaseWithoutPolicyFails(t *testing.T) {
	f := newFixture(t)
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostFailed, pod.Phase)
	assert.Equal(t, longhorn.HookReasonPolicyNotFound, pod.Reason)
	assert.Equal(t, 0, f.exec.callCount())
	events := f.events()
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "PostHookFailed")
}

func TestReleaseWithChangedPolicyRunsAndRecords(t *testing.T) {
	f := newFixture(t)
	f.policy(2)
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	_, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostDone, pod.Phase)
	assert.Equal(t, longhorn.HookReasonPolicyChanged, pod.Reason)
	assert.Equal(t, 1, f.exec.callCount())
}

func TestReleaseWhenHookRenamedFails(t *testing.T) {
	f := newFixture(t)
	f.policy(1, longhorn.VolumeHookPolicyHook{Name: "renamed", Exec: &longhorn.VolumeHookPolicyExec{PreCommand: []string{"a"}, PostCommand: []string{"b"}}})
	f.runningPod(testPod, testPodUID)
	f.putGroup(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))

	_, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	pod := f.store.group(testGroupName).Status.Hooks[0].Pods[0]
	assert.Equal(t, longhorn.HookPhasePostFailed, pod.Phase)
	assert.Equal(t, longhorn.HookReasonHookNotFound, pod.Reason)
	assert.Equal(t, 0, f.exec.callCount())
}

func TestReleaseOnDeletedGroupIsDropped(t *testing.T) {
	f := newFixture(t)
	requeue, err := f.runner.release(context.Background(), "missing")
	require.NoError(t, err)
	assert.False(t, requeue)
}

// A pause stopped by the deadline or a failure leaves the members it never
// reached without a record. The release must not invent one for them.
func TestReleaseRecordsNothingForUnclaimedMembers(t *testing.T) {
	f := newFixture(t)
	f.policy(1)
	f.runningPod(testPod, testPodUID)
	group := f.pausedGroup(longhorn.SnapshotGroupPhaseFailed)
	group.Spec.Members = append(group.Spec.Members, longhorn.SnapshotGroupMember{
		VolumeName: "vol-b", SnapshotName: "snap-b",
		HookPolicyRef: testPolicyRef(),
	})
	f.putGroup(group)

	requeue, err := f.runner.release(context.Background(), testGroupName)
	require.NoError(t, err)
	assert.False(t, requeue)
	require.Len(t, f.exec.calls, 1)
	hooks := f.store.group(testGroupName).Status.Hooks
	require.Len(t, hooks, 1, "no record for the member the pause never reached")
	assert.Equal(t, testSnapshot, hooks[0].SnapshotName)
	assert.Equal(t, longhorn.HookPhasePostDone, hooks[0].Phase)
	assert.NotNil(t, f.store.group(testGroupName).Status.PostHooksDoneAt)
}

func TestLockGroupSurvivesDeleteWhileHeld(t *testing.T) {
	f := newFixture(t)
	group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)

	// A pause holds the lock when the group is deleted; the release for a
	// same-named group must still wait behind it.
	unlockPause := f.runner.lockGroup(group.Name)
	f.runner.onGroupDelete(group)

	acquired := make(chan struct{})
	go func() {
		unlock := f.runner.lockGroup(group.Name)
		close(acquired)
		unlock()
	}()
	select {
	case <-acquired:
		t.Fatal("release took the lock while the pause still held it")
	case <-time.After(50 * time.Millisecond):
	}

	unlockPause()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("release did not get the lock after the pause dropped it")
	}

	// Wait for the goroutine's unlock, then the entry is gone.
	require.Eventually(t, func() bool {
		f.runner.mu.Lock()
		defer f.runner.mu.Unlock()
		return len(f.runner.groupLocks) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestOnGroupChangeRoutesWork(t *testing.T) {
	f := newFixture(t)

	f.runner.onGroupChange(f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks))
	assert.Equal(t, 1, f.runner.pauseQueue.Len())
	assert.Equal(t, 0, f.runner.releaseQueue.Len(), "deadline timer is armed, not fired")

	f.runner.onGroupChange(f.pausedGroup(longhorn.SnapshotGroupPhaseReady))
	assert.Equal(t, 1, f.runner.releaseQueue.Len())

	unhooked := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
	unhooked.Name = "plain"
	unhooked.Spec.Hooks = longhorn.SnapshotGroupHooksNone
	f.runner.onGroupChange(unhooked)
	assert.Equal(t, 1, f.runner.pauseQueue.Len())

	late := f.newGroup(longhorn.SnapshotGroupPhaseInProgress)
	late.Name = "late"
	late.Spec.DeadlineSeconds = 1
	f.runner.onGroupChange(late)
	assert.Equal(t, 2, f.runner.releaseQueue.Len(), "past-deadline group releases now")
}

// A group being deleted goes to the release queue when it has a paused pod
// on record, and nowhere when it has none, whatever its phase: deletion
// never queues pause work.
func TestOnGroupChangeRoutesDeletingGroupToRelease(t *testing.T) {
	f := newFixture(t)
	deletionTime := metav1.NewTime(f.now)

	paused := f.pausedGroup(longhorn.SnapshotGroupPhaseInProgress)
	paused.DeletionTimestamp = &deletionTime
	f.runner.onGroupChange(paused)
	assert.Equal(t, 1, f.runner.releaseQueue.Len(), "a recorded pod must be released")
	assert.Equal(t, 0, f.runner.pauseQueue.Len())

	unclaimed := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
	unclaimed.Name = "unclaimed"
	unclaimed.DeletionTimestamp = &deletionTime
	f.runner.onGroupChange(unclaimed)
	assert.Equal(t, 1, f.runner.releaseQueue.Len(), "nothing recorded, nothing to release")
	assert.Equal(t, 0, f.runner.pauseQueue.Len(), "a deleting group is never paused")
}

// The release pool follows the pause pool upward and never drops below the
// default: lowering --pause-workers to freeze fewer applications at once
// must not slow down unfreezing them.
func TestNewSizesReleasePoolFromPauseWorkers(t *testing.T) {
	logger := logrus.New()
	for _, tc := range []struct {
		pauseWorkers, wantPause, wantRelease int
	}{
		{pauseWorkers: 0, wantPause: DefaultPauseWorkers, wantRelease: DefaultPauseWorkers},
		{pauseWorkers: 2, wantPause: 2, wantRelease: DefaultPauseWorkers},
		{pauseWorkers: DefaultPauseWorkers, wantPause: DefaultPauseWorkers, wantRelease: DefaultPauseWorkers},
		{pauseWorkers: 8, wantPause: 8, wantRelease: 8},
	} {
		r := New(logger, newFakeStore(), &fakeExec{}, record.NewFakeRecorder(1), tc.pauseWorkers)
		assert.Equal(t, tc.wantPause, r.pauseWorkers, "pauseWorkers=%d", tc.pauseWorkers)
		assert.Equal(t, tc.wantRelease, r.releaseWorkers, "pauseWorkers=%d", tc.pauseWorkers)
	}
}

func TestRefreshHookProgress(t *testing.T) {
	f := newFixture(t)
	group := f.newGroup(longhorn.SnapshotGroupPhaseRunningPreHooks)
	refreshHookProgress(group)
	assert.Equal(t, "pre 0/1", group.Status.HookProgress)

	paused := f.pausedGroup(longhorn.SnapshotGroupPhaseInProgress)
	refreshHookProgress(paused)
	assert.Equal(t, "pre done", paused.Status.HookProgress)

	releasing := f.pausedGroup(longhorn.SnapshotGroupPhaseReady)
	releasing.Status.Hooks[0].Pods = append(releasing.Status.Hooks[0].Pods, longhorn.HookPodStatus{Name: "mongo-1", Phase: longhorn.HookPhasePostDone})
	refreshHookProgress(releasing)
	assert.Equal(t, "post 1/2", releasing.Status.HookProgress)

	// The deadline release: still InProgress, no pod done yet, but the first
	// post-command is running. That is the post phase, not "pre done".
	deadlineRelease := f.pausedGroup(longhorn.SnapshotGroupPhaseInProgress)
	deadlineRelease.Status.Hooks[0].Pods[0].Phase = longhorn.HookPhasePostRunning
	refreshHookProgress(deadlineRelease)
	assert.Equal(t, "post 0/1", deadlineRelease.Status.HookProgress)
}
