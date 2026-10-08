package volumehookpolicy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

// validPolicy returns a two-hook policy that passes every check. Cases
// mutate one aspect at a time.
func validPolicy() *longhorn.VolumeHookPolicy {
	return &longhorn.VolumeHookPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-db", Namespace: "app"},
		Spec: longhorn.VolumeHookPolicySpec{
			PVCSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "analytics-db"}},
			Hooks: []longhorn.VolumeHookPolicyHook{
				{
					Name:       "primary",
					Operations: []longhorn.HookOperation{longhorn.HookOperationSnapshot, longhorn.HookOperationBackup},
					Exec: &longhorn.VolumeHookPolicyExec{
						PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "primary"}},
						Container:   "postgres",
						PreCommand:  []string{"psql", "-c", "SELECT pg_backup_start('snap')"},
						PostCommand: []string{"psql", "-c", "SELECT pg_backup_stop()"},
					},
				},
				{
					Name: "replica",
					Exec: &longhorn.VolumeHookPolicyExec{
						PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "replica"}},
						PreCommand:  []string{"sync"},
						PostCommand: []string{"true"},
					},
				},
			},
		},
	}
}

func singleHookPolicy() *longhorn.VolumeHookPolicy {
	policy := validPolicy()
	policy.Spec.Hooks = []longhorn.VolumeHookPolicyHook{{Exec: &longhorn.VolumeHookPolicyExec{
		PreCommand:  []string{"fsfreeze", "-f", "/data"},
		PostCommand: []string{"fsfreeze", "-u", "/data"},
	}}}
	return policy
}

func TestVolumeHookPolicyValidatorCreate(t *testing.T) {
	v := &volumeHookPolicyValidator{longhornNamespace: "longhorn-system"}

	cases := []struct {
		name      string
		policy    func() *longhorn.VolumeHookPolicy
		mutate    func(p *longhorn.VolumeHookPolicy)
		expectErr bool
	}{
		{"valid multi-hook policy", validPolicy, func(p *longhorn.VolumeHookPolicy) {}, false},
		{"valid single hook without name or podSelector", singleHookPolicy, func(p *longhorn.VolumeHookPolicy) {}, false},
		{"single hook with a name and podSelector", singleHookPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Name = "freeze"
			p.Spec.Hooks[0].Exec.PodSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"role": "primary"}}
		}, false},
		{"hook limited to one operation", singleHookPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Operations = []longhorn.HookOperation{longhorn.HookOperationBackup}
		}, false},
		{"pvcSelector using matchExpressions only", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "app", Operator: metav1.LabelSelectorOpExists},
			}}
		}, false},
		{"pvcNames instead of pvcSelector", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = nil
			p.Spec.PVCNames = []string{"data-mongo-0", "data-mongo-1"}
		}, false},

		{"empty pvcSelector would match every PVC", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = &metav1.LabelSelector{}
		}, true},
		{"invalid pvcSelector operator", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "app", Operator: "Nope"},
			}}
		}, true},
		{"neither pvcSelector nor pvcNames", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = nil
		}, true},
		{"both pvcSelector and pvcNames", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCNames = []string{"data-mongo-0"}
		}, true},
		{"pvcNames with an empty entry", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = nil
			p.Spec.PVCNames = []string{"data-mongo-0", ""}
		}, true},
		{"pvcNames with a duplicate entry", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = nil
			p.Spec.PVCNames = []string{"data-mongo-0", "data-mongo-0"}
		}, true},
		{"pvcNames entry not a valid object name", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.PVCSelector = nil
			p.Spec.PVCNames = []string{"Data_Mongo"}
		}, true},
		{"no hooks", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks = nil
		}, true},
		{"multi-hook with a missing name", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[1].Name = ""
		}, true},
		{"multi-hook with a missing podSelector", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[1].Exec.PodSelector = nil
		}, true},
		{"multi-hook with an empty podSelector", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[1].Exec.PodSelector = &metav1.LabelSelector{}
		}, true},
		{"single hook with an empty podSelector", singleHookPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Exec.PodSelector = &metav1.LabelSelector{}
		}, true},
		{"hook without exec", singleHookPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Exec = nil
		}, true},
		{"unknown operation", singleHookPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Operations = []longhorn.HookOperation{"restore"}
		}, true},
		{"duplicate operation", singleHookPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Operations = []longhorn.HookOperation{longhorn.HookOperationSnapshot, longhorn.HookOperationSnapshot}
		}, true},
		{"duplicate hook names", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[1].Name = p.Spec.Hooks[0].Name
		}, true},
		{"hook name not a valid label value", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Name = "has space"
		}, true},
		{"invalid container name", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Exec.Container = "Postgres_Main"
		}, true},
		{"empty preCommand", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Exec.PreCommand = nil
		}, true},
		{"empty postCommand", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Exec.PostCommand = []string{}
		}, true},
		{"preCommand with a blank program", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Spec.Hooks[0].Exec.PreCommand = []string{"  ", "-c", "x"}
		}, true},
		{"policy in the Longhorn namespace", validPolicy, func(p *longhorn.VolumeHookPolicy) {
			p.Namespace = "longhorn-system"
		}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			policy := c.policy()
			c.mutate(policy)
			err := v.Create(nil, policy)
			if c.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestVolumeHookPolicyValidatorUpdate(t *testing.T) {
	v := &volumeHookPolicyValidator{longhornNamespace: "longhorn-system"}

	// The spec is mutable; Update applies the same checks as Create to the
	// new object only.
	oldPolicy := validPolicy()
	newPolicy := validPolicy()
	newPolicy.Spec.Hooks[0].Exec.PreCommand = []string{"psql", "-c", "CHECKPOINT"}
	assert.NoError(t, v.Update(nil, oldPolicy, newPolicy))

	broken := validPolicy()
	broken.Spec.Hooks[0].Exec.PostCommand = nil
	assert.Error(t, v.Update(nil, oldPolicy, broken))

	inLonghorn := validPolicy()
	inLonghorn.Namespace = "longhorn-system"
	err := v.Update(nil, inLonghorn, inLonghorn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hooks never run in the Longhorn namespace longhorn-system")
}

func TestVolumeHookPolicyValidatorRejectsWrongType(t *testing.T) {
	v := &volumeHookPolicyValidator{}
	assert.Error(t, v.Create(nil, &longhorn.SnapshotGroup{}))
	assert.Error(t, v.Update(nil, &longhorn.VolumeHookPolicy{}, &longhorn.SnapshotGroup{}))
	assert.Error(t, v.Delete(nil, &longhorn.SnapshotGroup{}))
}

func TestVolumeHookPolicyValidatorDelete(t *testing.T) {
	policy := validPolicy()
	ref := &longhorn.VolumeHookPolicyRef{Namespace: policy.Namespace, Name: policy.Name, Generation: 1}
	otherRef := &longhorn.VolumeHookPolicyRef{Namespace: policy.Namespace, Name: "other", Generation: 1}

	// hookedGroup returns a hooked group in the given phase whose single
	// member was stamped with ref and whose paused pod is in podPhase.
	hookedGroup := func(name string, phase longhorn.SnapshotGroupPhase, ref *longhorn.VolumeHookPolicyRef, podPhase longhorn.HookPhase) *longhorn.SnapshotGroup {
		return &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: longhorn.SnapshotGroupSpec{
				Hooks:   longhorn.SnapshotGroupHooksCommand,
				Members: []longhorn.SnapshotGroupMember{{VolumeName: "vol", SnapshotName: name + "-00000001", HookPolicyRef: ref}},
			},
			Status: longhorn.SnapshotGroupStatus{
				Phase: phase,
				Hooks: []longhorn.HookStatus{{
					SnapshotName: name + "-00000001",
					Pods:         []longhorn.HookPodStatus{{Name: "db-0", Phase: podPhase}},
				}},
			},
		}
	}
	withGroups := func(groups ...*longhorn.SnapshotGroup) *volumeHookPolicyValidator {
		return &volumeHookPolicyValidator{listSnapshotGroups: func() ([]*longhorn.SnapshotGroup, error) { return groups, nil }}
	}

	t.Run("no referencing group admits the delete", func(t *testing.T) {
		v := withGroups(hookedGroup("elsewhere", longhorn.SnapshotGroupPhaseInProgress, otherRef, longhorn.HookPhasePreDone))
		assert.NoError(t, v.Delete(nil, policy))
	})

	t.Run("an in-progress group still referencing the policy refuses the delete, naming it", func(t *testing.T) {
		v := withGroups(
			hookedGroup("nightly", longhorn.SnapshotGroupPhaseInProgress, ref, longhorn.HookPhasePreDone),
			hookedGroup("adhoc", longhorn.SnapshotGroupPhaseRunningPreHooks, ref, longhorn.HookPhasePreRunning),
		)
		err := v.Delete(nil, policy)
		require.Error(t, err)
		assert.Equal(t, int32(422), err.(werror.AdmitError).AsResult().Code)
		assert.Contains(t, err.Error(), "app/analytics-db")
		assert.Contains(t, err.Error(), "adhoc, nightly")
	})

	t.Run("a Ready group whose pod is not yet released still holds the policy", func(t *testing.T) {
		v := withGroups(hookedGroup("nightly", longhorn.SnapshotGroupPhaseReady, ref, longhorn.HookPhasePreDone))
		assert.Error(t, v.Delete(nil, policy))
	})

	t.Run("released, failed-and-released, and unhooked groups do not hold the policy", func(t *testing.T) {
		unhooked := hookedGroup("plain", longhorn.SnapshotGroupPhaseInProgress, ref, longhorn.HookPhasePreDone)
		unhooked.Spec.Hooks = ""
		v := withGroups(
			hookedGroup("done", longhorn.SnapshotGroupPhaseReady, ref, longhorn.HookPhasePostDone),
			hookedGroup("gaveup", longhorn.SnapshotGroupPhaseFailed, ref, longhorn.HookPhasePostFailed),
			unhooked,
		)
		assert.NoError(t, v.Delete(nil, policy))
	})

	t.Run("a group list that fails rejects with a retry", func(t *testing.T) {
		v := &volumeHookPolicyValidator{listSnapshotGroups: func() ([]*longhorn.SnapshotGroup, error) { return nil, errors.New("cache down") }}
		err := v.Delete(nil, policy)
		require.Error(t, err)
		assert.Equal(t, int32(500), err.(werror.AdmitError).AsResult().Code)
		assert.Contains(t, err.Error(), "retry the delete")
	})
}
