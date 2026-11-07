package volume

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

const (
	mutatorTestNamespace  = "longhorn-system"
	mutatorTestBackupName = "backup-1"
	mutatorTestBackupURL  = "s3://backupbucket@us-east-1/backupstore?backup=" + mutatorTestBackupName + "&volume=vol"
)

func newMutatorTestDataStore(t *testing.T, backups ...*longhorn.Backup) *datastore.DataStore {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(mutatorTestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(mutatorTestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	backupIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer()
	for _, backup := range backups {
		require.NoError(t, backupIndexer.Add(backup))
	}
	return ds
}

func newMutatorTestDRVolume(backupBlockSize int64) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "dr-vol",
			Namespace:  mutatorTestNamespace,
			Finalizers: []string{longhornFinalizerKeyForTest},
		},
		Spec: longhorn.VolumeSpec{
			Standby:         true,
			FromBackup:      mutatorTestBackupURL,
			BackupBlockSize: backupBlockSize,
		},
	}
}

// longhornFinalizerKeyForTest keeps the finalizer already present, so that the test
// is not about the finalizer patch.
const longhornFinalizerKeyForTest = "longhorn.io"

func hasBackupBlockSizePatch(patchOps []string, size string) bool {
	want := `"path": "/spec/backupBlockSize", "value": "` + size + `"`
	for _, op := range patchOps {
		if strings.Contains(op, want) {
			return true
		}
	}
	return false
}

func hasAnyBackupBlockSizePatch(patchOps []string) bool {
	for _, op := range patchOps {
		if strings.Contains(op, `"/spec/backupBlockSize"`) {
			return true
		}
	}
	return false
}

// TestMutateStandbyVolumeWithDeletedSourceBackup checks that a DR volume whose
// source backup was deleted before the volume was activated can still be updated.
// Otherwise every update is rejected, including the finalizer removal that
// finishes the deletion of the volume, and the volume can never be removed.
func TestMutateStandbyVolumeWithDeletedSourceBackup(t *testing.T) {
	existingBackup := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: mutatorTestBackupName, Namespace: mutatorTestNamespace},
		Spec:       longhorn.BackupSpec{BackupBlockSize: types.BackupBlockSize2Mi * 2},
	}

	tests := []struct {
		name        string
		backups     []*longhorn.Backup
		oldVolume   *longhorn.Volume // nil means Create
		newVolume   *longhorn.Volume
		expectErr   bool
		expectPatch string // expected /spec/backupBlockSize value, "" means no patch
	}{
		{
			name:      "update keeps the block size when the source backup is gone",
			oldVolume: newMutatorTestDRVolume(types.BackupBlockSize2Mi * 2),
			newVolume: newMutatorTestDRVolume(types.BackupBlockSize2Mi * 2),
		},
		{
			name:        "update falls back to the legacy default when the block size was never set",
			oldVolume:   newMutatorTestDRVolume(0),
			newVolume:   newMutatorTestDRVolume(0),
			expectPatch: "2097152",
		},
		{
			name:      "create still fails when the source backup is missing",
			newVolume: newMutatorTestDRVolume(0),
			expectErr: true,
		},
		{
			name:        "update still follows the source backup when it exists",
			backups:     []*longhorn.Backup{existingBackup},
			oldVolume:   newMutatorTestDRVolume(types.BackupBlockSize2Mi),
			newVolume:   newMutatorTestDRVolume(types.BackupBlockSize2Mi),
			expectPatch: "4194304",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := &volumeMutator{ds: newMutatorTestDataStore(t, tc.backups...)}

			patchOps, err := v.mutate(tc.newVolume, tc.oldVolume, nil)
			if tc.expectErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tc.expectPatch == "" {
				assert.False(t, hasAnyBackupBlockSizePatch(patchOps), "unexpected backupBlockSize patch: %v", patchOps)
				return
			}
			assert.True(t, hasBackupBlockSizePatch(patchOps, tc.expectPatch), "missing backupBlockSize %s patch: %v", tc.expectPatch, patchOps)
		})
	}
}
