package snapshothook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func labeledPod(labels map[string]string, containers ...string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "db-0", Labels: labels}}
	for _, name := range containers {
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: name})
	}
	return pod
}

func TestMatchHookForPod(t *testing.T) {
	single := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
		{Exec: &longhorn.VolumeHookPolicyExec{PreCommand: []string{"pause"}, PostCommand: []string{"resume"}}},
	}}}
	multi := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
		{Name: "primary", Exec: &longhorn.VolumeHookPolicyExec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "primary"}},
			Container:   "mongod",
		}},
		{Name: "any", Exec: &longhorn.VolumeHookPolicyExec{
			PodSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: metav1.LabelSelectorOpExists}}},
		}},
	}}}
	snapshot := longhorn.HookOperationSnapshot

	t.Run("single hook matches every pod and defaults its name", func(t *testing.T) {
		hook, container, err := MatchHookForPod(single, labeledPod(nil, "db"), snapshot)
		require.NoError(t, err)
		assert.Equal(t, longhorn.VolumeHookPolicyDefaultHookName, HookName(hook))
		assert.Equal(t, "db", container)
	})
	t.Run("multi-container pod needs an explicit container", func(t *testing.T) {
		_, _, err := MatchHookForPod(single, labeledPod(nil, "db", "sidecar"), snapshot)
		assert.ErrorContains(t, err, "names no container")
	})
	t.Run("native sidecars count, ordinary init and ephemeral containers do not", func(t *testing.T) {
		pod := labeledPod(nil, "db")
		pod.Spec.InitContainers = []corev1.Container{{Name: "init"}}
		pod.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
		_, container, err := MatchHookForPod(single, pod, snapshot)
		require.NoError(t, err)
		assert.Equal(t, "db", container)

		always := corev1.ContainerRestartPolicyAlways
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{Name: "sidecar", RestartPolicy: &always})
		_, _, err = MatchHookForPod(single, pod, snapshot)
		assert.ErrorContains(t, err, "has 2 exec targets [db sidecar]")
	})
	t.Run("named container must exist", func(t *testing.T) {
		named := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
			{Exec: &longhorn.VolumeHookPolicyExec{Container: "mongod"}},
		}}}
		_, _, err := MatchHookForPod(named, labeledPod(nil, "db"), snapshot)
		assert.ErrorContains(t, err, "does not have")
	})
	t.Run("several hooks selecting one pod is ambiguous", func(t *testing.T) {
		_, _, err := MatchHookForPod(multi, labeledPod(map[string]string{"role": "primary"}, "mongod"), snapshot)
		assert.ErrorContains(t, err, "ensure exactly one hook selects the pod")
	})
	t.Run("exactly one hook selects the pod", func(t *testing.T) {
		hook, container, err := MatchHookForPod(multi, labeledPod(map[string]string{"role": "replica"}, "mongod"), snapshot)
		require.NoError(t, err)
		assert.Equal(t, "any", hook.Name)
		assert.Equal(t, "mongod", container)
	})
	t.Run("no hook selects the pod", func(t *testing.T) {
		_, _, err := MatchHookForPod(multi, labeledPod(nil, "mongod"), snapshot)
		assert.ErrorContains(t, err, "no hook for operation snapshot selects")
	})

	// Two hooks select the same pod but run around different operations, so
	// the operation, not the pod selector, decides between them.
	perOperation := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
		{Name: "quick", Operations: []longhorn.HookOperation{longhorn.HookOperationSnapshot}, Exec: &longhorn.VolumeHookPolicyExec{}},
		{Name: "full", Operations: []longhorn.HookOperation{longhorn.HookOperationBackup}, Exec: &longhorn.VolumeHookPolicyExec{}},
	}}}
	t.Run("only hooks that run for the operation are considered", func(t *testing.T) {
		hook, _, err := MatchHookForPod(perOperation, labeledPod(nil, "db"), longhorn.HookOperationSnapshot)
		require.NoError(t, err)
		assert.Equal(t, "quick", hook.Name)

		hook, _, err = MatchHookForPod(perOperation, labeledPod(nil, "db"), longhorn.HookOperationBackup)
		require.NoError(t, err)
		assert.Equal(t, "full", hook.Name)
	})
	t.Run("a policy with no hook for the operation matches nothing", func(t *testing.T) {
		snapshotOnly := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
			{Operations: []longhorn.HookOperation{longhorn.HookOperationSnapshot}, Exec: &longhorn.VolumeHookPolicyExec{}},
		}}}
		_, _, err := MatchHookForPod(snapshotOnly, labeledPod(nil, "db"), longhorn.HookOperationBackup)
		assert.ErrorContains(t, err, "no hook of the policy runs for operation backup")
	})
	t.Run("a hook with no operations runs for every operation", func(t *testing.T) {
		for _, operation := range []longhorn.HookOperation{longhorn.HookOperationSnapshot, longhorn.HookOperationBackup} {
			hook, _, err := MatchHookForPod(single, labeledPod(nil, "db"), operation)
			require.NoError(t, err)
			assert.Equal(t, longhorn.VolumeHookPolicyDefaultHookName, HookName(hook))
		}
	})
}

func TestPolicyCoversOperation(t *testing.T) {
	policy := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
		{Name: "a", Operations: []longhorn.HookOperation{longhorn.HookOperationSnapshot}},
	}}}
	assert.True(t, PolicyCoversOperation(policy, longhorn.HookOperationSnapshot))
	assert.False(t, PolicyCoversOperation(policy, longhorn.HookOperationBackup))

	policy.Spec.Hooks = append(policy.Spec.Hooks, longhorn.VolumeHookPolicyHook{Name: "b"})
	assert.True(t, PolicyCoversOperation(policy, longhorn.HookOperationBackup))
}

func TestFindHook(t *testing.T) {
	policy := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{
		{Name: "a"}, {Name: "b"},
	}}}
	assert.Equal(t, "b", FindHook(policy, "b").Name)
	assert.Nil(t, FindHook(policy, "c"))

	unnamed := &longhorn.VolumeHookPolicy{Spec: longhorn.VolumeHookPolicySpec{Hooks: []longhorn.VolumeHookPolicyHook{{}}}}
	assert.NotNil(t, FindHook(unnamed, longhorn.VolumeHookPolicyDefaultHookName))
}
