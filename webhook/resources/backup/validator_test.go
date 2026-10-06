package backup

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

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
