package backup

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func backupWithTargetLabel(target string) *longhorn.Backup {
	return &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "longhorn-system",
			Labels: map[string]string{
				types.LonghornLabelBackupTarget: target,
			},
		},
	}
}

func backupWithoutTargetLabel() *longhorn.Backup {
	return &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "longhorn-system",
		},
	}
}

func TestBackupValidatorUpdateBackupTargetLabel(t *testing.T) {
	newBackupFor := func(old *longhorn.Backup) *longhorn.Backup {
		backup := old.DeepCopy()
		if backup.Labels == nil {
			backup.Labels = map[string]string{}
		}
		return backup
	}

	removingLabel := func(backup *longhorn.Backup) *longhorn.Backup {
		backup = newBackupFor(backup)
		delete(backup.Labels, types.LonghornLabelBackupTarget)
		return backup
	}

	emptyingLabel := func(backup *longhorn.Backup) *longhorn.Backup {
		backup = newBackupFor(backup)
		backup.Labels[types.LonghornLabelBackupTarget] = ""
		return backup
	}

	changingLabel := func(backup *longhorn.Backup) *longhorn.Backup {
		backup = newBackupFor(backup)
		backup.Labels[types.LonghornLabelBackupTarget] = "other-target"
		return backup
	}

	addingLabel := func(backup *longhorn.Backup) *longhorn.Backup {
		backup = newBackupFor(backup)
		backup.Labels[types.LonghornLabelBackupTarget] = "default"
		return backup
	}

	keepingBackup := func(backup *longhorn.Backup) *longhorn.Backup {
		return newBackupFor(backup)
	}

	testCases := []struct {
		name    string
		old     *longhorn.Backup
		new     *longhorn.Backup
		wantErr bool
	}{
		{
			name:    "removing the label key is rejected",
			old:     backupWithTargetLabel("default"),
			new:     removingLabel(backupWithTargetLabel("default")),
			wantErr: true,
		},
		{
			name:    "emptying the label value is rejected",
			old:     backupWithTargetLabel("default"),
			new:     emptyingLabel(backupWithTargetLabel("default")),
			wantErr: true,
		},
		{
			name:    "changing the label value is rejected",
			old:     backupWithTargetLabel("default"),
			new:     changingLabel(backupWithTargetLabel("default")),
			wantErr: true,
		},
		{
			name:    "keeping the label value is allowed",
			old:     backupWithTargetLabel("default"),
			new:     keepingBackup(backupWithTargetLabel("default")),
			wantErr: false,
		},
		{
			name:    "adding the label key to a backup without one is allowed",
			old:     backupWithoutTargetLabel(),
			new:     addingLabel(backupWithoutTargetLabel()),
			wantErr: false,
		},
		{
			name:    "leaving a backup without the label key untouched is allowed",
			old:     backupWithoutTargetLabel(),
			new:     keepingBackup(backupWithoutTargetLabel()),
			wantErr: false,
		},
	}

	v := &backupValidator{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := v.Update(nil, tc.old, tc.new)
			if (err != nil) != tc.wantErr {
				t.Errorf("Update() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateLocalVolumeBackup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dataEngine longhorn.DataEngineType
		wantErr    bool
	}{
		{name: "v1 volume", dataEngine: longhorn.DataEngineTypeV1},
		{name: "v2 volume", dataEngine: longhorn.DataEngineTypeV2},
		{name: "local volume", dataEngine: longhorn.DataEngineTypeLocal, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &longhorn.Volume{Spec: longhorn.VolumeSpec{DataEngine: tc.dataEngine}}
			err := validateLocalVolumeBackup(v)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateLocalVolumeBackup() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
