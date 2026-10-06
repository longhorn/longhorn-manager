package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

var (
	testHookPolicyPVCSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mongo"}}
	testHookPolicyPodSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"role": "writer"}}

	// A names policy with a single unnamed hook.
	testHookPolicyPG = &longhorn.VolumeHookPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "pg"},
		Spec: longhorn.VolumeHookPolicySpec{
			PVCNames: []string{"data-pg-0", "data-pg-1"},
			Hooks: []longhorn.VolumeHookPolicyHook{{
				Exec: &longhorn.VolumeHookPolicyExec{Container: "postgres", PreCommand: []string{"pg-freeze"}, PostCommand: []string{"pg-thaw"}},
			}},
		},
	}
	// A selector policy with two named hooks.
	testHookPolicyMongo = &longhorn.VolumeHookPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "mongo"},
		Spec: longhorn.VolumeHookPolicySpec{
			PVCSelector: testHookPolicyPVCSelector,
			Hooks: []longhorn.VolumeHookPolicyHook{
				{Name: "freeze", Operations: []longhorn.HookOperation{longhorn.HookOperationBackup}, Exec: &longhorn.VolumeHookPolicyExec{PodSelector: testHookPolicyPodSelector, PreCommand: []string{"mongo-fsync-lock"}, PostCommand: []string{"mongo-fsync-unlock"}}},
				{Name: "noop", Exec: &longhorn.VolumeHookPolicyExec{PodSelector: &metav1.LabelSelector{}, PreCommand: []string{"noop-pre"}, PostCommand: []string{"noop-post"}}},
			},
		},
	}
)

func TestToVolumeHookPolicyResource(t *testing.T) {
	assert.Equal(t, &VolumeHookPolicy{
		Name:     "pg",
		PVCNames: []string{"data-pg-0", "data-pg-1"},
		Hooks:    []VolumeHookPolicyHook{{Name: longhorn.VolumeHookPolicyDefaultHookName, Container: "postgres"}},
	}, toVolumeHookPolicyResource(testHookPolicyPG))

	assert.Equal(t, &VolumeHookPolicy{
		Name:        "mongo",
		PVCSelector: testHookPolicyPVCSelector,
		Hooks: []VolumeHookPolicyHook{
			{Name: "freeze", Operations: []longhorn.HookOperation{longhorn.HookOperationBackup}, PodSelector: testHookPolicyPodSelector},
			{Name: "noop", PodSelector: &metav1.LabelSelector{}},
		},
	}, toVolumeHookPolicyResource(testHookPolicyMongo))
}

// TestVolumeHookPolicyResourceOmitsCommands pins the invariant that hook
// commands, which may carry an application secret, are never serialised.
// The fixture command words are distinctive so the check cannot collide
// with other field values.
func TestVolumeHookPolicyResourceOmitsCommands(t *testing.T) {
	for _, policy := range []*longhorn.VolumeHookPolicy{testHookPolicyPG, testHookPolicyMongo} {
		body, err := json.Marshal(toVolumeHookPolicyResource(policy))
		assert.NoError(t, err)
		for _, hook := range policy.Spec.Hooks {
			for _, word := range hook.Exec.PreCommand {
				assert.NotContains(t, string(body), word)
			}
			for _, word := range hook.Exec.PostCommand {
				assert.NotContains(t, string(body), word)
			}
		}
		assert.NotContains(t, string(body), "preCommand")
		assert.NotContains(t, string(body), "postCommand")
	}
}

func TestToVolumeHookPolicyResources(t *testing.T) {
	assert.Equal(t, []VolumeHookPolicy{
		*toVolumeHookPolicyResource(testHookPolicyMongo),
		*toVolumeHookPolicyResource(testHookPolicyPG),
	}, toVolumeHookPolicyResources([]*longhorn.VolumeHookPolicy{testHookPolicyPG, testHookPolicyMongo}))

	empty := toVolumeHookPolicyResources(nil)
	assert.NotNil(t, empty)
	assert.Empty(t, empty)
}
