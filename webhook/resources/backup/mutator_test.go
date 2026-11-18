package backup

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestBackupMutatorCreateWithoutVolumeOrSnapshot(t *testing.T) {
	mutator := &backupMutator{}
	backup := &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backup"},
	}

	patchOps, err := mutator.Create(nil, backup)
	if err == nil {
		t.Fatal("Create() error = nil, want missing backup volume label error")
	}
	if patchOps != nil {
		t.Errorf("Create() patchOps = %v, want nil", patchOps)
	}

	const want = "cannot find the backup volume label for backup test-backup"
	if err.Error() != want {
		t.Errorf("Create() error = %q, want %q", err, want)
	}
}
