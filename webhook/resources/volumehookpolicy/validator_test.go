package volumehookpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
}
