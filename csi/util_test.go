package csi

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestGetVolumeOptions(t *testing.T) {
	tests := map[string]struct {
		volumeID       string
		volumeOptions  map[string]string
		expectedVolume *longhornclient.Volume
		expectedError  bool
	}{
		"defaults": {
			volumeID: "test-vol",
			volumeOptions: map[string]string{
				"numberOfReplicas": "3",
			},
			expectedVolume: &longhornclient.Volume{
				NumberOfReplicas:        3,
				StaleReplicaTimeout:     defaultStaleReplicaTimeout,
				AccessMode:              string(longhorn.AccessModeReadWriteOnce),
				DataEngine:              string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
			},
		},
		"exclusive access": {
			volumeID: "test-vol-exclusive",
			volumeOptions: map[string]string{
				"exclusive": "true",
			},
			expectedVolume: &longhornclient.Volume{
				StaleReplicaTimeout:     defaultStaleReplicaTimeout,
				AccessMode:              string(longhorn.AccessModeReadWriteOncePod),
				DataEngine:              string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
			},
		},
		"shared access": {
			volumeID: "test-vol-shared",
			volumeOptions: map[string]string{
				"share": "true",
			},
			expectedVolume: &longhornclient.Volume{
				StaleReplicaTimeout:     defaultStaleReplicaTimeout,
				AccessMode:              string(longhorn.AccessModeReadWriteMany),
				DataEngine:              string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
			},
		},
		"exclusive and shared conflict": {
			volumeID: "test-vol-conflict",
			volumeOptions: map[string]string{
				"exclusive": "true",
				"share":     "true",
			},
			expectedError: true,
		},
		"migratable requires RWX": {
			volumeID: "test-vol-migratable-no-rwx",
			volumeOptions: map[string]string{
				"migratable": "true",
			},
			expectedVolume: &longhornclient.Volume{
				StaleReplicaTimeout:     defaultStaleReplicaTimeout,
				AccessMode:              string(longhorn.AccessModeReadWriteOnce),
				DataEngine:              string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
				Migratable:              false,
			},
		},
		"migratable with RWX": {
			volumeID: "test-vol-migratable-rwx",
			volumeOptions: map[string]string{
				"share":      "true",
				"migratable": "true",
			},
			expectedVolume: &longhornclient.Volume{
				StaleReplicaTimeout:     defaultStaleReplicaTimeout,
				AccessMode:              string(longhorn.AccessModeReadWriteMany),
				DataEngine:              string(longhorn.DataEngineTypeV1),
				RevisionCounterDisabled: true,
				Migratable:              true,
			},
		},
		"dataEngine override to v2": {
			volumeID: "test-vol-dataengine-v2",
			volumeOptions: map[string]string{
				"dataEngine": "v2",
			},
			expectedVolume: &longhornclient.Volume{
				StaleReplicaTimeout:     defaultStaleReplicaTimeout,
				AccessMode:              string(longhorn.AccessModeReadWriteOnce),
				DataEngine:              string(longhorn.DataEngineTypeV2),
				RevisionCounterDisabled: true,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			vol, err := getVolumeOptions(tc.volumeID, tc.volumeOptions)
			if tc.expectedError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedVolume, vol)
		})
	}
}

func TestRequireExclusiveAccess(t *testing.T) {
	testCases := []struct {
		name       string
		volume     *longhornclient.Volume
		capability *csi.VolumeCapability
		expected   bool
	}{
		{
			name: "rwop volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOncePod),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
				},
			},
			expected: true,
		},
		{
			name: "rwo volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOnce),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
			expected: false,
		},
		{
			name: "rwx volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteMany),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := requireExclusiveAccess(tc.volume, tc.capability)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestRequiresSharedAccess(t *testing.T) {
	testCases := []struct {
		name       string
		volume     *longhornclient.Volume
		capability *csi.VolumeCapability
		expected   bool
	}{
		{
			name: "rwx volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteMany),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
			expected: true,
		},
		{
			name: "migratable rwx volume",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteMany),
				Migratable: true,
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
			expected: true,
		},
		{
			name: "multi-node single writer capability",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOnce),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
				},
			},
			expected: true,
		},
		{
			name: "multi-node reader capability",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOnce),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
				},
			},
			expected: true,
		},
		{
			name: "single-node writer should not require shared access",
			volume: &longhornclient.Volume{
				AccessMode: string(longhorn.AccessModeReadWriteOnce),
			},
			capability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := requiresSharedAccess(tc.volume, tc.capability)
			assert.Equal(t, tc.expected, result)
		})
	}
}
