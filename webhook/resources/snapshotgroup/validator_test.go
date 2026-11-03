package snapshotgroup

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
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
		{"CSI group carrying hooks as a label", func(sg *longhorn.SnapshotGroup) {
			sg.Labels = map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType): "snap"}
			sg.Spec.Labels = map[string]string{"hooks": "command"}
		}, true},
		{"CSI group carrying deadlineSeconds as a label", func(sg *longhorn.SnapshotGroup) {
			sg.Labels = map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType): "snap"}
			sg.Spec.Labels = map[string]string{"deadlineSeconds": "120"}
		}, true},
		{"non-CSI group may use hooks as a label", func(sg *longhorn.SnapshotGroup) {
			sg.Spec.Labels = map[string]string{"hooks": "command"}
		}, false},
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

// validHookedGroup returns a hooks: command group that passes every hook
// check: each member has a policy reference, each member volume has a
// Running pod, and no other hooked group holds a member. The returned cache
// provides that cluster state.
func validHookedGroup() (*longhorn.SnapshotGroup, testCache) {
	sg := validGroup()
	sg.Spec.Hooks = longhorn.SnapshotGroupHooksCommand
	sg.Spec.HookOperation = longhorn.HookOperationSnapshot
	sg.Spec.DeadlineSeconds = types.SnapshotGroupHookedDefaultDeadlineSeconds

	cache := testCache{}
	for i := range sg.Spec.Members {
		pvcName := fmt.Sprintf("data-%d", i)
		sg.Spec.Members[i].HookPolicyRef = &longhorn.VolumeHookPolicyRef{Namespace: testAppNamespace, Name: "mongo", Generation: 1}
		cache.volumes = append(cache.volumes, newTestWorkloadVolume(sg.Spec.Members[i].VolumeName, pvcName))
	}
	return sg, cache
}

func TestSnapshotGroupValidatorCreateHooked(t *testing.T) {
	t.Run("stamped members with running pods are admitted", func(t *testing.T) {
		sg, cache := validHookedGroup()
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.NoError(t, v.Create(nil, sg))
	})

	t.Run("a manager rollout still in progress refuses hooked groups, naming the pods", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.managerDS = newTestManagerDaemonSet("longhorn-manager:v2")
		cache.managerPods = []*corev1.Pod{
			newTestManagerPod("longhorn-manager-a", "longhorn-manager:v2"),
			newTestManagerPod("longhorn-manager-b", "longhorn-manager:v1"),
		}
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Equal(t, int32(422), err.(werror.AdmitError).AsResult().Code)
		assert.Contains(t, err.Error(), "rollout completes")
		assert.Contains(t, err.Error(), "longhorn-manager-b")
		assert.NotContains(t, err.Error(), "longhorn-manager-a")

		// Unhooked groups are not held back: the old manager runs them fine.
		plain := validGroup()
		assert.NoError(t, v.Create(nil, plain))

		// Once the last pod is on the new image the group is admitted.
		cache.managerPods[1] = newTestManagerPod("longhorn-manager-b", "longhorn-manager:v2")
		v = &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.NoError(t, v.Create(nil, sg))
	})

	t.Run("a member without a stamped policy is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		sg.Spec.Members[0].HookPolicyRef = nil
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.Error(t, v.Create(nil, sg))
	})

	t.Run("a policy reference without a namespace is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		sg.Spec.Members[0].HookPolicyRef.Namespace = ""
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.Error(t, v.Create(nil, sg))
	})

	t.Run("a member without a stamped generation is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		sg.Spec.Members[0].HookPolicyRef.Generation = 0
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.Error(t, v.Create(nil, sg))
	})

	t.Run("a member without a running pod is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.volumes[1].Status.KubernetesStatus.WorkloadsStatus[0].PodStatus = string(corev1.PodPending)
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no running pod")
		assert.Contains(t, err.Error(), cache.volumes[1].Name)
	})

	t.Run("a member whose pods run in the Longhorn namespace is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.volumes[1].Status.KubernetesStatus.Namespace = testNamespace
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "hooks never run in the Longhorn namespace "+testNamespace)
		assert.Contains(t, err.Error(), cache.volumes[1].Name)
	})

	t.Run("a member whose pods are history is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.volumes[0].Status.KubernetesStatus.LastPodRefAt = "2026-01-01T00:00:00Z"
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.Error(t, v.Create(nil, sg))
	})

	t.Run("a member volume missing from cache is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.volumes = cache.volumes[:1]
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.Error(t, v.Create(nil, sg))
	})

	t.Run("a member of an unreleased hooked group is rejected, naming it", func(t *testing.T) {
		sg, cache := validHookedGroup()
		other := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "earlier", Namespace: testNamespace},
			Spec: longhorn.SnapshotGroupSpec{
				Hooks:   longhorn.SnapshotGroupHooksCommand,
				Members: []longhorn.SnapshotGroupMember{{VolumeName: sg.Spec.Members[1].VolumeName}},
			},
			Status: longhorn.SnapshotGroupStatus{Phase: longhorn.SnapshotGroupPhaseInProgress},
		}
		cache.groups = append(cache.groups, other)
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "member of snapshot group earlier, which has not released it")
	})

	t.Run("a member whose pod another group paused through a different volume is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		// The other group shares no volume with the new one; only its
		// recorded pod, which also mounts the new member's PVC, names the
		// conflict.
		other := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "earlier", Namespace: testNamespace},
			Spec: longhorn.SnapshotGroupSpec{
				Hooks:   longhorn.SnapshotGroupHooksCommand,
				Members: []longhorn.SnapshotGroupMember{{VolumeName: "vol-other", SnapshotName: "earlier-00000001"}},
			},
			Status: longhorn.SnapshotGroupStatus{
				Phase: longhorn.SnapshotGroupPhaseInProgress,
				Hooks: []longhorn.HookStatus{{
					SnapshotName: "earlier-00000001",
					Pods: []longhorn.HookPodStatus{{
						Namespace: testAppNamespace,
						Name:      cache.volumes[1].Status.KubernetesStatus.WorkloadsStatus[0].PodName,
						Phase:     longhorn.HookPhasePreDone,
					}},
				}},
			},
		}
		cache.groups = append(cache.groups, other)
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "still paused by snapshot group earlier")
		assert.Contains(t, err.Error(), cache.volumes[1].Name)
	})

	t.Run("a Ready group with a recorded pod not yet released is still unreleased", func(t *testing.T) {
		sg, cache := validHookedGroup()
		other := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "earlier", Namespace: testNamespace},
			Spec: longhorn.SnapshotGroupSpec{
				Hooks:   longhorn.SnapshotGroupHooksCommand,
				Members: []longhorn.SnapshotGroupMember{{VolumeName: sg.Spec.Members[0].VolumeName, SnapshotName: "earlier-00000001"}},
			},
			Status: longhorn.SnapshotGroupStatus{
				Phase: longhorn.SnapshotGroupPhaseReady,
				Hooks: []longhorn.HookStatus{{
					SnapshotName: "earlier-00000001",
					Pods:         []longhorn.HookPodStatus{{Name: "db-0", Phase: longhorn.HookPhasePreDone}},
				}},
			},
		}
		cache.groups = append(cache.groups, other)
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.Error(t, v.Create(nil, sg))
	})

	t.Run("a released or unhooked group on the same volume is no conflict", func(t *testing.T) {
		sg, cache := validHookedGroup()
		released := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "released", Namespace: testNamespace},
			Spec: longhorn.SnapshotGroupSpec{
				Hooks:   longhorn.SnapshotGroupHooksCommand,
				Members: []longhorn.SnapshotGroupMember{{VolumeName: sg.Spec.Members[0].VolumeName, SnapshotName: "released-00000001"}},
			},
			Status: longhorn.SnapshotGroupStatus{
				Phase: longhorn.SnapshotGroupPhaseReady,
				Hooks: []longhorn.HookStatus{{
					SnapshotName: "released-00000001",
					Pods:         []longhorn.HookPodStatus{{Name: "db-0", Phase: longhorn.HookPhasePostDone}},
				}},
			},
		}
		unhooked := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: testNamespace},
			Spec: longhorn.SnapshotGroupSpec{
				Members: []longhorn.SnapshotGroupMember{{VolumeName: sg.Spec.Members[1].VolumeName}},
			},
			Status: longhorn.SnapshotGroupStatus{Phase: longhorn.SnapshotGroupPhaseInProgress},
		}
		cache.groups = append(cache.groups, released, unhooked)
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		assert.NoError(t, v.Create(nil, sg))
	})

	t.Run("a restored hooked group skips the cluster-state checks", func(t *testing.T) {
		// No volumes or groups in cache and every access review failing:
		// only the format checks can run.
		sg, _ := validHookedGroup()
		sg.Annotations = map[string]string{types.SnapshotGroupAnnotationTerminalPhase: string(longhorn.SnapshotGroupPhaseReady)}
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, testCache{
			executorAccess: func(authorizationv1.ResourceAttributes) (bool, error) { return false, errors.New("unreachable") },
		})}
		assert.NoError(t, v.Create(nil, sg))

		sg.Spec.Members[0].HookPolicyRef.Generation = 0
		assert.Error(t, v.Create(nil, sg))
	})

	t.Run("hooks none rejects a stamped policy", func(t *testing.T) {
		sg := validGroup()
		sg.Spec.Members[0].HookPolicyRef = &longhorn.VolumeHookPolicyRef{Namespace: testAppNamespace, Name: "mongo", Generation: 1}
		v := &snapshotGroupValidator{}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec.hooks is none")
	})

	t.Run("hooks none rejects a hookOperation", func(t *testing.T) {
		sg := validGroup()
		sg.Spec.HookOperation = longhorn.HookOperationSnapshot
		v := &snapshotGroupValidator{}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec.hooks is none")
	})

	t.Run("hooks command needs a known hookOperation", func(t *testing.T) {
		for _, operation := range []longhorn.HookOperation{"", "restore"} {
			sg, cache := validHookedGroup()
			sg.Spec.HookOperation = operation
			v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
			err := v.Create(nil, sg)
			require.Error(t, err, "operation %q", operation)
			assert.Contains(t, err.Error(), "hookOperation")
		}

		sg, cache := validHookedGroup()
		sg.Spec.HookOperation = longhorn.HookOperationBackup
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		request := admission.NewRequest(nil)
		assert.NoError(t, v.Create(request, sg))
	})

	t.Run("a backup hookOperation outside the CSI backup flow is admitted with a warning", func(t *testing.T) {
		sg, cache := validHookedGroup()
		sg.Spec.HookOperation = longhorn.HookOperationBackup
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}

		request := admission.NewRequest(nil)
		require.NoError(t, v.Create(request, sg))
		require.Len(t, request.Warnings(), 1)
		assert.Contains(t, request.Warnings()[0], "hookOperation is backup")
		assert.Contains(t, request.Warnings()[0], "type bak")

		// The CSI driver stamps the type on the groups it backs up: no warning.
		sg.Labels = map[string]string{types.GetLonghornLabelKey(types.LonghornLabelSnapshotGroupCSIType): types.SnapshotGroupCSITypeBackup}
		request = admission.NewRequest(nil)
		require.NoError(t, v.Create(request, sg))
		assert.Empty(t, request.Warnings())

		// A snapshot operation needs no backup to follow: no warning.
		sg.Labels = nil
		sg.Spec.HookOperation = longhorn.HookOperationSnapshot
		request = admission.NewRequest(nil)
		require.NoError(t, v.Create(request, sg))
		assert.Empty(t, request.Warnings())
	})

	t.Run("the executor's grants are reviewed once per member namespace", func(t *testing.T) {
		sg, cache := validHookedGroup()
		var reviewed []authorizationv1.ResourceAttributes
		cache.executorAccess = func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			reviewed = append(reviewed, attributes)
			return true, nil
		}
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		require.NoError(t, v.Create(nil, sg))
		// Both members' PVCs live in one namespace: four grants, reviewed once.
		assert.Equal(t, []authorizationv1.ResourceAttributes{
			{Namespace: testAppNamespace, Verb: "create", Resource: "pods", Subresource: "exec"},
			{Namespace: testAppNamespace, Verb: "get", Resource: "pods", Subresource: "exec"},
			{Namespace: testAppNamespace, Verb: "list", Resource: "pods"},
			{Namespace: testAppNamespace, Group: longhorn.SchemeGroupVersion.Group, Verb: "get", Resource: "volumehookpolicies"},
		}, reviewed)
	})

	t.Run("a member namespace without the exec grant is rejected with the RoleBinding to apply", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.executorAccess = func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			return attributes.Subresource != "exec", nil
		}
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Equal(t, int32(422), err.(werror.AdmitError).AsResult().Code)
		assert.Contains(t, err.Error(), "may not create pods/exec in namespace "+testAppNamespace)
		assert.Contains(t, err.Error(), "kind: RoleBinding")
		assert.Contains(t, err.Error(), "  namespace: "+testAppNamespace+"\n")
		assert.Contains(t, err.Error(), "name: "+types.HookExecutorServiceAccountName+", namespace: "+testNamespace)
	})

	t.Run("a member namespace without the pod list grant is rejected", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.executorAccess = func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			return attributes.Verb != "list", nil
		}
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "may not list pods in namespace "+testAppNamespace)
	})

	t.Run("a member namespace without the policy get grant is rejected", func(t *testing.T) {
		// The release re-reads the policy before it can exec; a binding that
		// grants exec but not the policy read would hold the release on a
		// Forbidden that no event reports.
		sg, cache := validHookedGroup()
		cache.executorAccess = func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			return attributes.Resource != "volumehookpolicies", nil
		}
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "may not get volumehookpolicies in namespace "+testAppNamespace)
		assert.Contains(t, err.Error(), "kind: RoleBinding")
	})

	t.Run("an access review that cannot be completed rejects with a retry, not a manifest", func(t *testing.T) {
		sg, cache := validHookedGroup()
		cache.executorAccess = func(authorizationv1.ResourceAttributes) (bool, error) {
			return false, errors.New("apiserver unreachable")
		}
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		err := v.Create(nil, sg)
		require.Error(t, err)
		assert.Equal(t, int32(500), err.(werror.AdmitError).AsResult().Code)
		assert.Contains(t, err.Error(), "could not verify")
		assert.Contains(t, err.Error(), "retry the create")
		assert.Contains(t, err.Error(), "apiserver unreachable")
		assert.NotContains(t, err.Error(), "kind: RoleBinding")
	})

	t.Run("a PostFailed pod on a member volume is admitted with a warning", func(t *testing.T) {
		sg, cache := validHookedGroup()
		abandoned := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "abandoned", Namespace: testNamespace},
			Spec: longhorn.SnapshotGroupSpec{
				Hooks:   longhorn.SnapshotGroupHooksCommand,
				Members: []longhorn.SnapshotGroupMember{{VolumeName: sg.Spec.Members[0].VolumeName, SnapshotName: "abandoned-00000001"}},
			},
			Status: longhorn.SnapshotGroupStatus{
				Phase: longhorn.SnapshotGroupPhaseFailed,
				Hooks: []longhorn.HookStatus{{
					SnapshotName: "abandoned-00000001",
					Pods: []longhorn.HookPodStatus{
						{Namespace: testAppNamespace, Name: "db-0", Phase: longhorn.HookPhasePostFailed},
						{Namespace: testAppNamespace, Name: "db-1", Phase: longhorn.HookPhasePostDone},
					},
				}},
			},
		}
		cache.groups = append(cache.groups, abandoned)
		v := &snapshotGroupValidator{ds: newHookTestDataStore(t, cache)}
		request := admission.NewRequest(nil)
		require.NoError(t, v.Create(request, sg))
		require.Len(t, request.Warnings(), 1)
		assert.Contains(t, request.Warnings()[0], testAppNamespace+"/db-0")
		assert.Contains(t, request.Warnings()[0], sg.Spec.Members[0].VolumeName)
		assert.Contains(t, request.Warnings()[0], "snapshot group abandoned")
		assert.NotContains(t, request.Warnings()[0], "db-1")
	})
}

func TestSnapshotGroupValidatorUpdate(t *testing.T) {
	v := &snapshotGroupValidator{}

	t.Run("identical spec is allowed", func(t *testing.T) {
		assert.NoError(t, v.Update(nil, validGroup(), validGroup()))
	})

	t.Run("hooks is immutable with the rest of the spec", func(t *testing.T) {
		newObj := validGroup()
		newObj.Spec.Hooks = longhorn.SnapshotGroupHooksCommand
		assert.Error(t, v.Update(nil, validGroup(), newObj))
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

	t.Run("unchanged CSI parameters annotation is allowed", func(t *testing.T) {
		oldObj := validGroup()
		oldObj.Annotations = map[string]string{types.SnapshotGroupAnnotationCSIParameters: `{"backupMode":"full"}`}
		newObj := validGroup()
		newObj.Annotations = map[string]string{types.SnapshotGroupAnnotationCSIParameters: `{"backupMode":"full"}`}
		assert.NoError(t, v.Update(nil, oldObj, newObj))
	})

	t.Run("changing, removing, or adding the CSI parameters annotation is rejected", func(t *testing.T) {
		withAnnotation := func(record string) *longhorn.SnapshotGroup {
			group := validGroup()
			if record != "" {
				group.Annotations = map[string]string{types.SnapshotGroupAnnotationCSIParameters: record}
			}
			return group
		}
		assert.Error(t, v.Update(nil, withAnnotation(`{"backupMode":"full"}`), withAnnotation(`{"backupMode":"incremental"}`)))
		assert.Error(t, v.Update(nil, withAnnotation(`{"backupMode":"full"}`), withAnnotation("")))
		assert.Error(t, v.Update(nil, withAnnotation(""), withAnnotation(`{"backupMode":"full"}`)))
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
