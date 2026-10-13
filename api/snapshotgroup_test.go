package api

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor"

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

func pvcRef(namespace, name string) *longhorn.PVCRef {
	return &longhorn.PVCRef{Namespace: namespace, Name: name}
}

func TestOrderSnapshotGroupCandidatesForPause(t *testing.T) {
	nameSorted := func() []datastore.SnapshotGroupMemberCandidate {
		return []datastore.SnapshotGroupMemberCandidate{
			{VolumeName: "vol-a"}, {VolumeName: "vol-b"}, {VolumeName: "vol-c"},
		}
	}
	volumeNames := func(candidates []datastore.SnapshotGroupMemberCandidate) []string {
		names := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			names = append(names, candidate.VolumeName)
		}
		return names
	}

	t.Run("selector keeps volume-name order", func(t *testing.T) {
		candidates := nameSorted()
		orderSnapshotGroupCandidatesForPause(candidates, nil)
		assert.Equal(t, []string{"vol-a", "vol-b", "vol-c"}, volumeNames(candidates))
	})
	t.Run("explicit list is the pause order", func(t *testing.T) {
		candidates := nameSorted()
		orderSnapshotGroupCandidatesForPause(candidates, []string{"vol-c", "vol-a", "vol-b"})
		assert.Equal(t, []string{"vol-c", "vol-a", "vol-b"}, volumeNames(candidates))
	})
}

func TestDescribeSnapshotGroupPreviewPods(t *testing.T) {
	pods := map[string]*corev1.Pod{
		"mongo-0": {
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-0", Labels: map[string]string{"app": "mongo"}},
			Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{{Name: "init"}, {Name: "sidecar", RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways)}},
				Containers:     []corev1.Container{{Name: "db"}},
			},
		},
	}
	getPod := func(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		assert.Equal(t, "app", namespace)
		pod, ok := pods[name]
		if !ok {
			return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
		}
		return pod, nil
	}
	snapshot := longhorn.HookOperationSnapshot
	policyWithHook := func(container string) *longhorn.VolumeHookPolicy {
		return &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
			{Name: "freeze", Exec: &longhorn.VolumeHookPolicyExec{Container: container}},
		}}}
	}

	t.Run("a readable pod reports its labels and exec targets, native sidecars included and ordinary init containers not", func(t *testing.T) {
		described := describeSnapshotGroupPreviewPods(context.Background(), "app", []string{"mongo-0"}, getPod, nil, snapshot)
		assert.Equal(t, []SnapshotGroupPreviewPod{{
			Name:       "mongo-0",
			Labels:     map[string]string{"app": "mongo"},
			Containers: []string{"db", "sidecar"},
		}}, described)
	})
	t.Run("with a policy, the pod reports the hook and container the run would use", func(t *testing.T) {
		described := describeSnapshotGroupPreviewPods(context.Background(), "app", []string{"mongo-0"}, getPod, policyWithHook("db"), snapshot)
		assert.Equal(t, []SnapshotGroupPreviewPod{{
			Name:       "mongo-0",
			Labels:     map[string]string{"app": "mongo"},
			Containers: []string{"db", "sidecar"},
			HookName:   "freeze",
			Container:  "db",
		}}, described)
	})
	t.Run("a pod no single hook fits reports the match failure and no hook", func(t *testing.T) {
		described := describeSnapshotGroupPreviewPods(context.Background(), "app", []string{"mongo-0"}, getPod, policyWithHook(""), snapshot)
		assert.Len(t, described, 1)
		assert.Empty(t, described[0].HookName)
		assert.Empty(t, described[0].Container)
		assert.Equal(t, "hook freeze names no container and pod /mongo-0 has 2 exec targets [db sidecar]", described[0].MatchFailure)
	})
	t.Run("a pod that is gone keeps its name and reports the stale status", func(t *testing.T) {
		described := describeSnapshotGroupPreviewPods(context.Background(), "app", []string{"mongo-0", "mongo-1"}, getPod, policyWithHook("db"), snapshot)
		assert.Len(t, described, 2)
		assert.Equal(t, SnapshotGroupPreviewPod{
			Name:        "mongo-1",
			ReadFailure: "pod no longer exists; the volume's workloadsStatus is stale, retry the preview",
		}, described[1])
	})
	t.Run("a read that cannot be completed is reported as retryable", func(t *testing.T) {
		described := describeSnapshotGroupPreviewPods(context.Background(), "app", []string{"mongo-0"}, func(context.Context, string, string) (*corev1.Pod, error) {
			return nil, errors.New("apiserver unreachable")
		}, nil, snapshot)
		assert.Equal(t, []SnapshotGroupPreviewPod{{Name: "mongo-0", ReadFailure: "apiserver unreachable; retry the preview"}}, described)
	})
	t.Run("an expired deadline fails every remaining pod without reading it", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		described := describeSnapshotGroupPreviewPods(ctx, "app", []string{"mongo-0", "mongo-1"}, getPod, nil, snapshot)
		assert.Equal(t, []SnapshotGroupPreviewPod{
			{Name: "mongo-0", ReadFailure: "context canceled; retry the preview"},
			{Name: "mongo-1", ReadFailure: "context canceled; retry the preview"},
		}, described)
	})
	t.Run("no pods describe nothing", func(t *testing.T) {
		assert.Nil(t, describeSnapshotGroupPreviewPods(context.Background(), "app", nil, getPod, nil, snapshot))
	})
}

func TestSnapshotGroupPreviewWarnings(t *testing.T) {
	pods := func(names ...string) []SnapshotGroupPreviewPod {
		var pods []SnapshotGroupPreviewPod
		for _, name := range names {
			pods = append(pods, SnapshotGroupPreviewPod{Name: name})
		}
		return pods
	}
	t.Run("distinct pods warn nothing", func(t *testing.T) {
		warnings := snapshotGroupPreviewWarnings([]SnapshotGroupPreviewMember{
			{VolumeName: "vol-a", PVCRef: pvcRef("app", "data"), WorkloadPods: pods("mongo-0")},
			{VolumeName: "vol-b", PVCRef: pvcRef("app", "config"), WorkloadPods: pods("mongo-1")},
		})
		assert.Empty(t, warnings)
	})
	t.Run("same pod name in two namespaces warns nothing", func(t *testing.T) {
		warnings := snapshotGroupPreviewWarnings([]SnapshotGroupPreviewMember{
			{VolumeName: "vol-a", PVCRef: pvcRef("app", "data"), WorkloadPods: pods("mongo-0")},
			{VolumeName: "vol-b", PVCRef: pvcRef("other", "data"), WorkloadPods: pods("mongo-0")},
		})
		assert.Empty(t, warnings)
	})
	t.Run("one pod under two members warns once, naming both in pause order", func(t *testing.T) {
		warnings := snapshotGroupPreviewWarnings([]SnapshotGroupPreviewMember{
			{VolumeName: "vol-c", PVCRef: pvcRef("app", "config"), WorkloadPods: pods("mongo-0")},
			{VolumeName: "vol-a", PVCRef: pvcRef("app", "data"), WorkloadPods: pods("mongo-0", "mongo-1")},
			{VolumeName: "vol-b", PVCRef: pvcRef("app", "logs"), WorkloadPods: pods("mongo-1")},
		})
		assert.Equal(t, []string{
			"pod app/mongo-0 mounts member volumes vol-c, vol-a; its hooks run once per member, each run counting against the deadline",
			"pod app/mongo-1 mounts member volumes vol-a, vol-b; its hooks run once per member, each run counting against the deadline",
		}, warnings)
	})
	t.Run("members without hooks preview carry no pods and warn nothing", func(t *testing.T) {
		warnings := snapshotGroupPreviewWarnings([]SnapshotGroupPreviewMember{
			{VolumeName: "vol-a"}, {VolumeName: "vol-b"},
		})
		assert.Empty(t, warnings)
	})
	t.Run("a pod no hook fits warns with the run-time failure it would cause", func(t *testing.T) {
		warnings := snapshotGroupPreviewWarnings([]SnapshotGroupPreviewMember{
			{VolumeName: "vol-a", PVCRef: pvcRef("app", "data"), WorkloadPods: []SnapshotGroupPreviewPod{
				{Name: "mongo-0", HookName: "freeze", Container: "db"},
				{Name: "mongo-1", MatchFailure: "no hook for operation snapshot selects pod app/mongo-1"},
			}},
		})
		assert.Equal(t, []string{
			"pod app/mongo-1: no hook for operation snapshot selects pod app/mongo-1; the run would fail member volume vol-a with HookNotMatched",
		}, warnings)
	})
}

func TestRecordHookExecutorAccess(t *testing.T) {
	t.Run("each member namespace is reviewed once and a denial lands on all of its members", func(t *testing.T) {
		denial := &hookexecutor.AccessDenial{
			Reason:   "the hook executor may not list pods in namespace app",
			Manifest: "kind: RoleBinding\n",
		}
		members := []SnapshotGroupPreviewMember{
			{VolumeName: "vol-a", PVCRef: pvcRef("app", "data")},
			{VolumeName: "vol-b", PVCRef: pvcRef("app", "config")},
			{VolumeName: "vol-c", PVCRef: pvcRef("other", "data")},
			{VolumeName: "vol-d", PVCRef: pvcRef("app", "logs"), ValidationFailure: "no running pod"},
			{VolumeName: "vol-e", PVCRef: pvcRef("app", "tmp"), ValidationFailure: "no policy"},
			{VolumeName: "vol-f", ValidationFailure: "no PVC"},
		}
		var reviewed []string
		recordHookExecutorAccess(members, func(namespace string) (*hookexecutor.AccessDenial, error) {
			reviewed = append(reviewed, namespace)
			if namespace == "app" {
				return denial, nil
			}
			return nil, nil
		})
		assert.Equal(t, []string{"app", "other"}, reviewed)
		for _, i := range []int{0, 1} {
			assert.Equal(t, denial.Reason, members[i].AccessFailure)
			assert.Equal(t, denial.Manifest, members[i].AccessFailureManifest)
		}
		assert.Empty(t, members[2].AccessFailure)
		assert.Empty(t, members[2].AccessFailureManifest)
		// Members failing another check are reviewed too, and keep that
		// failure: a namespace without a policy yet shows its missing grant
		// in the same preview.
		for _, i := range []int{3, 4} {
			assert.Equal(t, denial.Reason, members[i].AccessFailure)
			assert.Equal(t, denial.Manifest, members[i].AccessFailureManifest)
		}
		assert.Equal(t, "no running pod", members[3].ValidationFailure)
		assert.Equal(t, "no policy", members[4].ValidationFailure)
		// A member without a PVC has no namespace to review.
		assert.Empty(t, members[5].AccessFailure)
		assert.Empty(t, members[5].AccessFailureManifest)
	})
	t.Run("a review that cannot be completed is reported as retryable, with no manifest", func(t *testing.T) {
		members := []SnapshotGroupPreviewMember{
			{VolumeName: "vol-a", PVCRef: pvcRef("app", "data")},
		}
		recordHookExecutorAccess(members, func(string) (*hookexecutor.AccessDenial, error) {
			return nil, errors.New("apiserver unreachable")
		})
		assert.Equal(t, "apiserver unreachable; retry the preview", members[0].AccessFailure)
		assert.Empty(t, members[0].AccessFailureManifest)
		assert.Empty(t, members[0].ValidationFailure)
	})
}
