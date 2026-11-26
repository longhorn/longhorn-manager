package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestVolumeAccessTypes(t *testing.T) {
	testCases := []struct {
		name                string
		spec                longhorn.VolumeSpec
		expectedMigratable  bool
		expectedSharedBlock bool
		expectedRegularRWX  bool
	}{
		{
			name: "RWO volume",
			spec: longhorn.VolumeSpec{AccessMode: longhorn.AccessModeReadWriteOnce},
		},
		{
			name:               "regular RWX volume",
			spec:               longhorn.VolumeSpec{AccessMode: longhorn.AccessModeReadWriteMany},
			expectedRegularRWX: true,
		},
		{
			name:               "migratable volume",
			spec:               longhorn.VolumeSpec{AccessMode: longhorn.AccessModeReadWriteMany, Migratable: true},
			expectedMigratable: true,
		},
		{
			name:                "shared block volume",
			spec:                longhorn.VolumeSpec{AccessMode: longhorn.AccessModeReadWriteMany, SharedBlock: true},
			expectedSharedBlock: true,
		},
		{
			name:                "shared block volume marked migratable is not migrated",
			spec:                longhorn.VolumeSpec{AccessMode: longhorn.AccessModeReadWriteMany, SharedBlock: true, Migratable: true},
			expectedSharedBlock: true,
		},
		{
			name: "shared block flag on RWO volume is ignored",
			spec: longhorn.VolumeSpec{AccessMode: longhorn.AccessModeReadWriteOnce, SharedBlock: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := &longhorn.Volume{Spec: tc.spec}
			assert.Equal(t, tc.expectedMigratable, IsMigratableVolume(v))
			assert.Equal(t, tc.expectedSharedBlock, IsSharedBlockVolume(v))
			assert.Equal(t, tc.expectedRegularRWX, IsRegularRWXVolume(v))
		})
	}
}
