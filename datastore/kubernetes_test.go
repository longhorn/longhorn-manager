package datastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestNewPVCManifestForVolume(t *testing.T) {
	tests := map[string]struct {
		volume             *longhorn.Volume
		expectedAccessMode corev1.PersistentVolumeAccessMode
	}{
		"read write once": {
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:       1024 * 1024 * 1024, // 1Gi
					AccessMode: longhorn.AccessModeReadWriteOnce,
				},
			},
			expectedAccessMode: corev1.ReadWriteOnce,
		},
		"read write many": {
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:       1024 * 1024 * 1024, // 1Gi
					AccessMode: longhorn.AccessModeReadWriteMany,
				},
			},
			expectedAccessMode: corev1.ReadWriteMany,
		},
		"read write once pod": {
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:       1024 * 1024 * 1024, // 1Gi
					AccessMode: longhorn.AccessModeReadWriteOncePod,
				},
			},
			expectedAccessMode: corev1.ReadWriteOncePod,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pvc := NewPVCManifestForVolume(tc.volume, "pv-name", "default", "pvc-name", "longhorn")
			require.NotNil(t, pvc)
			assert.Equal(t, []corev1.PersistentVolumeAccessMode{tc.expectedAccessMode}, pvc.Spec.AccessModes)
		})
	}
}

func TestNewPVManifestForVolumeAttributesAndAccessModes(t *testing.T) {
	newVolume := func(mode longhorn.AccessMode, migratable, encrypted bool, replicas, srt int, diskSel, nodeSel []string) *longhorn.Volume {
		return &longhorn.Volume{
			Spec: longhorn.VolumeSpec{
				Size:                2 * 1024 * 1024 * 1024, // 2Gi
				AccessMode:          mode,
				Migratable:          migratable,
				Encrypted:           encrypted,
				NumberOfReplicas:    replicas,
				StaleReplicaTimeout: srt,
				DiskSelector:        diskSel,
				NodeSelector:        nodeSel,
			},
		}
	}

	t.Run("rwop volume manifest attributes", func(t *testing.T) {
		v := newVolume(longhorn.AccessModeReadWriteOncePod, false, true, 3, 2880, []string{"ssd"}, []string{"fast"})
		pv := NewPVManifestForVolume(v, "pv-rwop", "longhorn", "ext4")
		require.NotNil(t, pv)
		assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}, pv.Spec.AccessModes)
		attrs := pv.Spec.CSI.VolumeAttributes
		require.NotNil(t, attrs)
		assert.Equal(t, "ssd", attrs["diskSelector"])
		assert.Equal(t, "fast", attrs["nodeSelector"])
		assert.Equal(t, "3", attrs["numberOfReplicas"])
		assert.Equal(t, "2880", attrs["staleReplicaTimeout"])
		assert.Equal(t, "true", attrs["encrypted"])
		_, hasMigratable := attrs["migratable"]
		assert.False(t, hasMigratable)
	})

	t.Run("rwx volume manifest attributes", func(t *testing.T) {
		v := newVolume(longhorn.AccessModeReadWriteMany, true, false, 2, 1440, []string{"nvme", "hot"}, []string{"zone-a"})
		pv := NewPVManifestForVolume(v, "pv-rwx", "longhorn", "ext4")
		require.NotNil(t, pv)
		assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, pv.Spec.AccessModes)
		attrs := pv.Spec.CSI.VolumeAttributes
		require.NotNil(t, attrs)
		assert.Equal(t, "nvme,hot", attrs["diskSelector"])
		assert.Equal(t, "zone-a", attrs["nodeSelector"])
		assert.Equal(t, "2", attrs["numberOfReplicas"])
		assert.Equal(t, "1440", attrs["staleReplicaTimeout"])
		assert.Equal(t, "true", attrs["migratable"])
		_, hasEncrypted := attrs["encrypted"]
		assert.False(t, hasEncrypted)
	})
}
