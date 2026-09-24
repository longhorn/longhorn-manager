package snapshotgroup

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const testNamespace = "longhorn-system"

// newTestDataStore returns a datastore whose volume lister serves the given
// volumes, as if the informer had already delivered them.
func newTestDataStore(t *testing.T, volumes ...*longhorn.Volume) *datastore.DataStore {
	return newHookTestDataStore(t, testCache{volumes: volumes})
}

// membersFromPatchOps returns the members carried by the /spec/members patch
// operation, letting tests compare structs instead of raw JSON.
func membersFromPatchOps(t *testing.T, patchOps []string) []longhorn.SnapshotGroupMember {
	t.Helper()
	for _, op := range patchOps {
		var patch struct {
			Path  string          `json:"path"`
			Value json.RawMessage `json:"value"`
		}
		require.NoError(t, json.Unmarshal([]byte(op), &patch))
		if patch.Path != "/spec/members" {
			continue
		}
		var members []longhorn.SnapshotGroupMember
		require.NoError(t, json.Unmarshal(patch.Value, &members))
		return members
	}
	require.FailNow(t, "no /spec/members patch operation")
	return nil
}

// testCache holds the objects admission of a hooked group reads from the
// informer caches: member volumes, their PVCs, the hook policies in the PVC
// namespaces, and the existing groups that may already hold a member volume.
type testCache struct {
	volumes  []*longhorn.Volume
	pvcs     []*corev1.PersistentVolumeClaim
	policies []*longhorn.VolumeHookPolicy
	groups   []*longhorn.SnapshotGroup
	// executorAccess answers the executor's SubjectAccessReviews; nil grants
	// everything.
	executorAccess func(attributes authorizationv1.ResourceAttributes) (allowed bool, err error)
}

func newHookTestDataStore(t *testing.T, seed testCache) *datastore.DataStore {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	kubeClient.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
		allowed := true
		if seed.executorAccess != nil {
			var err error
			if allowed, err = seed.executorAccess(*review.Spec.ResourceAttributes); err != nil {
				return true, nil, err
			}
		}
		review.Status.Allowed = allowed
		return true, review, nil
	})

	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	lh := informerFactories.LhInformerFactory.Longhorn().V1beta2()
	for _, volume := range seed.volumes {
		require.NoError(t, lh.Volumes().Informer().GetIndexer().Add(volume))
	}
	for _, policy := range seed.policies {
		require.NoError(t, lh.VolumeHookPolicies().Informer().GetIndexer().Add(policy))
	}
	for _, group := range seed.groups {
		require.NoError(t, lh.SnapshotGroups().Informer().GetIndexer().Add(group))
	}
	pvcIndexer := informerFactories.KubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
	for _, pvc := range seed.pvcs {
		require.NoError(t, pvcIndexer.Add(pvc))
	}
	return ds
}

func newTestVolume(name string, labels map[string]string) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    labels,
		},
	}
}

const testAppNamespace = "app"

// newTestWorkloadVolume returns a volume bound to a PVC in the application
// namespace with one Running pod, as the PV controller would report it.
func newTestWorkloadVolume(name, pvcName string) *longhorn.Volume {
	volume := newTestVolume(name, nil)
	volume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		Namespace: testAppNamespace,
		PVCName:   pvcName,
		WorkloadsStatus: []longhorn.WorkloadStatus{
			{PodName: pvcName + "-pod", PodStatus: string(corev1.PodRunning)},
		},
	}
	return volume
}

func newTestPVC(name string, labels map[string]string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: testAppNamespace, Name: name, Labels: labels}}
}

func newTestPolicy(name string, generation int64, pvcLabels map[string]string) *longhorn.VolumeHookPolicy {
	return &longhorn.VolumeHookPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: testAppNamespace, Name: name, UID: k8stypes.UID("uid-" + name), Generation: generation},
		Spec: longhorn.VolumeHookPolicySpec{
			PVCSelector: &metav1.LabelSelector{MatchLabels: pvcLabels},
			Hooks:       []longhorn.VolumeHookPolicyHook{{Exec: &longhorn.VolumeHookPolicyExec{PreCommand: []string{"true"}, PostCommand: []string{"true"}}}},
		},
	}
}

func TestSnapshotGroupMutatorCreate(t *testing.T) {
	groupName := "analytics-db-snap"

	newGroup := func(spec longhorn.SnapshotGroupSpec) *longhorn.SnapshotGroup {
		return &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{Name: groupName, Namespace: testNamespace},
			Spec:       spec,
		}
	}

	t.Run("explicit volumes resolve into sorted members with defaults", func(t *testing.T) {
		ds := newTestDataStore(t, newTestVolume("vol-b", nil), newTestVolume("vol-a", nil))
		m := &snapshotGroupMutator{ds: ds}

		patchOps, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-b", "vol-a"},
		}))
		require.NoError(t, err)

		joined := strings.Join(patchOps, "\n")
		// Members are resolved and sorted by volume name; the generated
		// snapshot names carry the group name prefix.
		assert.Contains(t, joined, `"path": "/spec/members"`)
		assert.Contains(t, joined, `"volumeName":"vol-a"`)
		assert.Contains(t, joined, `"volumeName":"vol-b"`)
		assert.Less(t, strings.Index(joined, `"volumeName":"vol-a"`), strings.Index(joined, `"volumeName":"vol-b"`))
		assert.Contains(t, joined, `"snapshotName":"`+groupName+`-`)
		// The deadline default and the finalizer are stamped.
		assert.Contains(t, joined, `"path": "/spec/deadlineSeconds"`)
		assert.Contains(t, joined, "finalizers")
	})

	t.Run("volumeSelector resolves matching volumes only", func(t *testing.T) {
		ds := newTestDataStore(t,
			newTestVolume("vol-a", map[string]string{"group": "db"}),
			newTestVolume("vol-b", map[string]string{"group": "db"}),
			newTestVolume("vol-c", map[string]string{"group": "other"}),
		)
		m := &snapshotGroupMutator{ds: ds}

		patchOps, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			VolumeSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"group": "db"}},
		}))
		require.NoError(t, err)

		joined := strings.Join(patchOps, "\n")
		assert.Contains(t, joined, `"volumeName":"vol-a"`)
		assert.Contains(t, joined, `"volumeName":"vol-b"`)
		assert.NotContains(t, joined, "vol-c")
	})

	t.Run("preset deadline is kept", func(t *testing.T) {
		ds := newTestDataStore(t, newTestVolume("vol-a", nil))
		m := &snapshotGroupMutator{ds: ds}

		patchOps, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes:         []string{"vol-a"},
			DeadlineSeconds: types.SnapshotGroupMinDeadlineSeconds,
		}))
		require.NoError(t, err)
		assert.NotContains(t, strings.Join(patchOps, "\n"), `"path": "/spec/deadlineSeconds"`)
	})

	t.Run("user-set members are rejected", func(t *testing.T) {
		ds := newTestDataStore(t, newTestVolume("vol-a", nil))
		m := &snapshotGroupMutator{ds: ds}

		_, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
			Members: []longhorn.SnapshotGroupMember{{VolumeName: "vol-a", SnapshotName: "user-set"}},
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec.members may only be pre-set on the restore")
	})

	t.Run("restored members with a terminal phase are kept without re-resolution", func(t *testing.T) {
		// The datastore has no volumes: resolving would fail, proving the
		// restore path does not resolve.
		ds := newTestDataStore(t)
		m := &snapshotGroupMutator{ds: ds}

		group := newGroup(longhorn.SnapshotGroupSpec{
			Volumes:         []string{"vol-a"},
			DeadlineSeconds: types.SnapshotGroupMinDeadlineSeconds,
			Members: []longhorn.SnapshotGroupMember{{
				VolumeName:   "vol-a",
				SnapshotName: testMemberSnapshotName(groupName, "vol-a"),
			}},
		})
		group.Annotations = map[string]string{
			types.SnapshotGroupAnnotationTerminalPhase: string(longhorn.SnapshotGroupPhaseReady),
		}

		patchOps, err := m.Create(nil, group)
		require.NoError(t, err)

		joined := strings.Join(patchOps, "\n")
		assert.NotContains(t, joined, `"path": "/spec/members"`)
		assert.Contains(t, joined, "finalizers")
	})

	t.Run("members with an invalid terminal phase are rejected", func(t *testing.T) {
		ds := newTestDataStore(t, newTestVolume("vol-a", nil))
		m := &snapshotGroupMutator{ds: ds}

		group := newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
			Members: []longhorn.SnapshotGroupMember{{
				VolumeName:   "vol-a",
				SnapshotName: testMemberSnapshotName(groupName, "vol-a"),
			}},
		})
		group.Annotations = map[string]string{
			types.SnapshotGroupAnnotationTerminalPhase: string(longhorn.SnapshotGroupPhaseInProgress),
		}

		_, err := m.Create(nil, group)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "terminal-phase annotation")
	})

	t.Run("unknown volume is rejected", func(t *testing.T) {
		ds := newTestDataStore(t)
		m := &snapshotGroupMutator{ds: ds}

		_, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"missing"},
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing")
	})

	t.Run("ineligible member volume is rejected with its reason", func(t *testing.T) {
		standby := newTestVolume("vol-standby", nil)
		standby.Status.IsStandby = true
		ds := newTestDataStore(t, newTestVolume("vol-a", nil), standby)
		m := &snapshotGroupMutator{ds: ds}

		_, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a", "vol-standby"},
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "vol-standby")
		assert.Contains(t, err.Error(), "standby")
	})

	t.Run("hooks command stamps the policy, its generation, the 60s deadline, and the snapshot operation", func(t *testing.T) {
		ds := newHookTestDataStore(t, testCache{
			volumes:  []*longhorn.Volume{newTestWorkloadVolume("vol-a", "mongo-data")},
			pvcs:     []*corev1.PersistentVolumeClaim{newTestPVC("mongo-data", map[string]string{"app": "mongo"})},
			policies: []*longhorn.VolumeHookPolicy{newTestPolicy("mongo", 4, map[string]string{"app": "mongo"})},
		})
		m := &snapshotGroupMutator{ds: ds}

		patchOps, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
			Hooks:   longhorn.SnapshotGroupHooksCommand,
		}))
		require.NoError(t, err)

		joined := strings.Join(patchOps, "\n")
		members := membersFromPatchOps(t, patchOps)
		require.Len(t, members, 1)
		assert.Equal(t, &longhorn.VolumeHookPolicyRef{Namespace: "app", Name: "mongo", UID: "uid-mongo", Generation: 4}, members[0].HookPolicyRef)
		assert.Contains(t, joined, `"path": "/spec/deadlineSeconds", "value": 60}`)
		assert.Contains(t, joined, `"path": "/spec/hookOperation", "value": "snapshot"}`)
	})

	t.Run("hooks command keeps an explicit hookOperation", func(t *testing.T) {
		ds := newHookTestDataStore(t, testCache{
			volumes:  []*longhorn.Volume{newTestWorkloadVolume("vol-a", "mongo-data")},
			pvcs:     []*corev1.PersistentVolumeClaim{newTestPVC("mongo-data", map[string]string{"app": "mongo"})},
			policies: []*longhorn.VolumeHookPolicy{newTestPolicy("mongo", 4, map[string]string{"app": "mongo"})},
		})
		m := &snapshotGroupMutator{ds: ds}

		patchOps, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes:       []string{"vol-a"},
			Hooks:         longhorn.SnapshotGroupHooksCommand,
			HookOperation: longhorn.HookOperationBackup,
		}))
		require.NoError(t, err)
		assert.NotContains(t, strings.Join(patchOps, "\n"), "/spec/hookOperation")
	})

	t.Run("hooks none stamps no policy and the 300s deadline", func(t *testing.T) {
		ds := newHookTestDataStore(t, testCache{
			volumes:  []*longhorn.Volume{newTestWorkloadVolume("vol-a", "mongo-data")},
			pvcs:     []*corev1.PersistentVolumeClaim{newTestPVC("mongo-data", map[string]string{"app": "mongo"})},
			policies: []*longhorn.VolumeHookPolicy{newTestPolicy("mongo", 4, map[string]string{"app": "mongo"})},
		})
		m := &snapshotGroupMutator{ds: ds}

		patchOps, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
		}))
		require.NoError(t, err)

		joined := strings.Join(patchOps, "\n")
		assert.NotContains(t, joined, "hookPolicy")
		assert.NotContains(t, joined, "/spec/hookOperation")
		assert.Contains(t, joined, `"path": "/spec/deadlineSeconds", "value": 300}`)
	})

	t.Run("hooks command rejects a member with no matching policy, naming the PVC", func(t *testing.T) {
		ds := newHookTestDataStore(t, testCache{
			volumes: []*longhorn.Volume{newTestWorkloadVolume("vol-a", "plain-data")},
			pvcs:    []*corev1.PersistentVolumeClaim{newTestPVC("plain-data", nil)},
		})
		m := &snapshotGroupMutator{ds: ds}

		_, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
			Hooks:   longhorn.SnapshotGroupHooksCommand,
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "app/plain-data")
		assert.Contains(t, err.Error(), "vol-a")
	})

	t.Run("hooks command rejects a member matched by two policies, naming both", func(t *testing.T) {
		ds := newHookTestDataStore(t, testCache{
			volumes: []*longhorn.Volume{newTestWorkloadVolume("vol-a", "mongo-data")},
			pvcs:    []*corev1.PersistentVolumeClaim{newTestPVC("mongo-data", map[string]string{"app": "mongo"})},
			policies: []*longhorn.VolumeHookPolicy{
				newTestPolicy("mongo", 1, map[string]string{"app": "mongo"}),
				newTestPolicy("mongo-too", 1, map[string]string{"app": "mongo"}),
			},
		})
		m := &snapshotGroupMutator{ds: ds}

		_, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
			Hooks:   longhorn.SnapshotGroupHooksCommand,
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "app/mongo")
		assert.Contains(t, err.Error(), "app/mongo-too")
	})

	t.Run("hooks command rejects a member whose policy has no hook for the operation", func(t *testing.T) {
		backupOnly := newTestPolicy("mongo", 1, map[string]string{"app": "mongo"})
		backupOnly.Spec.Hooks[0].Operations = []longhorn.HookOperation{longhorn.HookOperationBackup}
		ds := newHookTestDataStore(t, testCache{
			volumes:  []*longhorn.Volume{newTestWorkloadVolume("vol-a", "mongo-data")},
			pvcs:     []*corev1.PersistentVolumeClaim{newTestPVC("mongo-data", map[string]string{"app": "mongo"})},
			policies: []*longhorn.VolumeHookPolicy{backupOnly},
		})
		m := &snapshotGroupMutator{ds: ds}

		// hookOperation is left empty, so the check judges the defaulted
		// snapshot operation, which the backup-only hook does not cover.
		_, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
			Hooks:   longhorn.SnapshotGroupHooksCommand,
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "app/mongo has no hook that runs for operation snapshot")
		assert.Contains(t, err.Error(), "vol-a")

		// The same policy covers a backup group.
		patchOps, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes:       []string{"vol-a"},
			Hooks:         longhorn.SnapshotGroupHooksCommand,
			HookOperation: longhorn.HookOperationBackup,
		}))
		require.NoError(t, err)
		members := membersFromPatchOps(t, patchOps)
		require.Len(t, members, 1)
		assert.Equal(t, &longhorn.VolumeHookPolicyRef{Namespace: "app", Name: "mongo", UID: "uid-mongo", Generation: 1}, members[0].HookPolicyRef)
	})

	t.Run("hooks command rejects a member volume without a PVC", func(t *testing.T) {
		ds := newHookTestDataStore(t, testCache{
			volumes: []*longhorn.Volume{newTestVolume("vol-a", nil)},
		})
		m := &snapshotGroupMutator{ds: ds}

		_, err := m.Create(nil, newGroup(longhorn.SnapshotGroupSpec{
			Volumes: []string{"vol-a"},
			Hooks:   longhorn.SnapshotGroupHooksCommand,
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no bound PVC")
	})
}

func TestSnapshotGroupMutatorUpdate(t *testing.T) {
	ds := newTestDataStore(t)
	m := &snapshotGroupMutator{ds: ds}

	group := &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-db-snap", Namespace: testNamespace},
		Spec: longhorn.SnapshotGroupSpec{
			Volumes:         []string{"missing"}, // never re-resolved on update
			DeadlineSeconds: types.SnapshotGroupMinDeadlineSeconds,
		},
	}

	patchOps, err := m.Update(nil, group, group)
	require.NoError(t, err)

	// Update only maintains the finalizer; no re-resolution, no defaulting.
	joined := strings.Join(patchOps, "\n")
	assert.NotContains(t, joined, "/spec/members")
	assert.NotContains(t, joined, "/spec/deadlineSeconds")
	assert.Contains(t, joined, "finalizers")
}

// TestResolveSnapshotGroupMemberCandidates covers the resolver directly, as
// the REST preview action calls it: candidates are sorted by volume name and
// carry per-volume validation failures.
func TestResolveSnapshotGroupMemberCandidates(t *testing.T) {
	standby := newTestVolume("vol-standby", nil)
	standby.Status.IsStandby = true
	ds := newTestDataStore(t, newTestVolume("vol-b", nil), newTestVolume("vol-a", nil), standby)

	candidates, err := ds.ResolveSnapshotGroupMemberCandidates(&longhorn.SnapshotGroupSpec{
		Volumes: []string{"vol-b", "vol-a", "vol-standby"},
	})
	require.NoError(t, err)
	require.Len(t, candidates, 3)
	assert.Equal(t, "vol-a", candidates[0].VolumeName)
	assert.Empty(t, candidates[0].ValidationFailure)
	assert.Equal(t, "vol-b", candidates[1].VolumeName)
	assert.Empty(t, candidates[1].ValidationFailure)
	assert.Equal(t, "vol-standby", candidates[2].VolumeName)
	assert.Contains(t, candidates[2].ValidationFailure, "standby")
}
