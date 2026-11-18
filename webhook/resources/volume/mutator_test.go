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

func withMutatorTestSize(volume *longhorn.Volume, size int64) *longhorn.Volume {
	volume.Spec.Size = size
	return volume
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

// TestMutateStandbyVolumeBackupBlockSize checks when the mutator reads the backup block size of a DR
// volume from its source backup. The validator does not allow changing a valid backup block size, so an
// existing volume that already has one must not depend on the source backup. Otherwise, once the source
// backup is deleted before the volume is activated, every update of the volume is rejected, including the
// finalizer removal that finishes its deletion, and the volume can never be removed.
func TestMutateStandbyVolumeBackupBlockSize(t *testing.T) {
	backup16Mi := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: mutatorTestBackupName, Namespace: mutatorTestNamespace},
		Spec:       longhorn.BackupSpec{BackupBlockSize: types.BackupBlockSize16Mi},
	}

	// A backup created before Longhorn introduced the backup block size has none.
	backupLegacy := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: mutatorTestBackupName, Namespace: mutatorTestNamespace},
	}
	backupInvalid := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: mutatorTestBackupName, Namespace: mutatorTestNamespace},
		Spec:       longhorn.BackupSpec{BackupBlockSize: types.BackupBlockSizeInvalid},
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
			name:      "update does not need the source backup when the block size is valid",
			oldVolume: newMutatorTestDRVolume(types.BackupBlockSize16Mi),
			newVolume: newMutatorTestDRVolume(types.BackupBlockSize16Mi),
		},
		{
			name:      "update does not follow the source backup when the block size is valid",
			backups:   []*longhorn.Backup{backup16Mi},
			oldVolume: newMutatorTestDRVolume(types.BackupBlockSize2Mi),
			newVolume: newMutatorTestDRVolume(types.BackupBlockSize2Mi),
		},
		{
			name:      "update leaves a requested change of a valid block size to the validator",
			backups:   []*longhorn.Backup{backup16Mi},
			oldVolume: newMutatorTestDRVolume(types.BackupBlockSize2Mi),
			newVolume: newMutatorTestDRVolume(types.BackupBlockSize16Mi),
		},
		{
			name:        "update does not read the source backup when the block size was never set",
			backups:     []*longhorn.Backup{backup16Mi},
			oldVolume:   newMutatorTestDRVolume(0),
			newVolume:   newMutatorTestDRVolume(0),
			expectPatch: "2097152",
		},
		{
			name:        "update falls back to the default when it was never set and the source backup is gone",
			oldVolume:   newMutatorTestDRVolume(0),
			newVolume:   newMutatorTestDRVolume(0),
			expectPatch: "2097152",
		},
		{
			name:        "create sets the block size from the source backup",
			backups:     []*longhorn.Backup{backup16Mi},
			newVolume:   newMutatorTestDRVolume(0),
			expectPatch: "16777216",
		},
		{
			name:        "create does not check the requested size, which is replaced by the size of the backup volume",
			backups:     []*longhorn.Backup{backup16Mi},
			newVolume:   withMutatorTestSize(newMutatorTestDRVolume(0), 100*types.BackupBlockSizeMi),
			expectPatch: "16777216",
		},
		{
			name:        "create falls back to the legacy default when the source backup has no block size",
			backups:     []*longhorn.Backup{backupLegacy},
			newVolume:   newMutatorTestDRVolume(0),
			expectPatch: "2097152",
		},
		{
			name:        "create leaves rejecting an invalid block size of the source backup to the validator",
			backups:     []*longhorn.Backup{backupInvalid},
			newVolume:   newMutatorTestDRVolume(0),
			expectPatch: "-1",
		},
		{
			name:      "create fails when the source backup is missing",
			newVolume: newMutatorTestDRVolume(0),
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := &volumeMutator{ds: newMutatorTestDataStore(t, tc.backups...)}

			patchOps, err := v.mutate(tc.oldVolume, tc.newVolume, nil)
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
