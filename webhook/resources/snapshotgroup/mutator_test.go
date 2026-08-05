package snapshotgroup

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	for _, volume := range volumes {
		require.NoError(t, volumeIndexer.Add(volume))
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
