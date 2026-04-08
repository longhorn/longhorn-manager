package engine

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestGetMaxAllowedEnginesForVolume(t *testing.T) {
	testCases := []struct {
		name      string
		volume    *longhorn.Volume
		newEngine *longhorn.Engine
		expected  int
	}{
		{
			name:      "nil volume",
			volume:    nil,
			newEngine: nil,
			expected:  1,
		},
		{
			name: "migratable volume allows two",
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Migratable: true,
					DataEngine: longhorn.DataEngineTypeV2,
				},
			},
			newEngine: &longhorn.Engine{},
			expected:  2,
		},
		{
			name: "v2 non-migratable switchover allows two on target node",
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Migratable:   false,
					DataEngine:   longhorn.DataEngineTypeV2,
					EngineNodeID: "node-b",
				},
				Status: longhorn.VolumeStatus{
					CurrentNodeID:       "node-a",
					CurrentEngineNodeID: "node-a",
				},
			},
			newEngine: &longhorn.Engine{
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{NodeID: "node-b"}},
			},
			expected: 2,
		},
		{
			name: "v2 non-migratable switchover allows two with empty node on create",
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Migratable:   false,
					DataEngine:   longhorn.DataEngineTypeV2,
					EngineNodeID: "node-b",
				},
				Status: longhorn.VolumeStatus{
					CurrentNodeID:       "node-a",
					CurrentEngineNodeID: "node-a",
				},
			},
			newEngine: &longhorn.Engine{
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{NodeID: ""}},
			},
			expected: 2,
		},
		{
			name: "v2 non-migratable switchover keeps one on non-target node",
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Migratable:   false,
					DataEngine:   longhorn.DataEngineTypeV2,
					EngineNodeID: "node-b",
				},
				Status: longhorn.VolumeStatus{
					CurrentNodeID:       "node-a",
					CurrentEngineNodeID: "node-a",
				},
			},
			newEngine: &longhorn.Engine{
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{NodeID: "node-c"}},
			},
			expected: 1,
		},
		{
			name: "v2 non-migratable no switchover keeps one",
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Migratable:   false,
					DataEngine:   longhorn.DataEngineTypeV2,
					EngineNodeID: "node-a",
				},
				Status: longhorn.VolumeStatus{
					CurrentNodeID:       "node-a",
					CurrentEngineNodeID: "node-a",
				},
			},
			newEngine: &longhorn.Engine{
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{NodeID: "node-a"}},
			},
			expected: 1,
		},
		{
			name: "v2 non-migratable detached keeps one",
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Migratable:   false,
					DataEngine:   longhorn.DataEngineTypeV2,
					EngineNodeID: "node-b",
				},
				Status: longhorn.VolumeStatus{
					CurrentNodeID:       "",
					CurrentEngineNodeID: "node-a",
				},
			},
			newEngine: &longhorn.Engine{
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{NodeID: "node-b"}},
			},
			expected: 1,
		},
		{
			name: "v1 non-migratable keeps one",
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Migratable:   false,
					DataEngine:   longhorn.DataEngineTypeV1,
					EngineNodeID: "node-b",
				},
				Status: longhorn.VolumeStatus{
					CurrentNodeID:       "node-a",
					CurrentEngineNodeID: "node-a",
				},
			},
			newEngine: &longhorn.Engine{
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{NodeID: "node-b"}},
			},
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := getMaxAllowedEnginesForVolume(tc.volume, tc.newEngine)
			if actual != tc.expected {
				t.Fatalf("expected %d, got %d", tc.expected, actual)
			}
		})
	}
}
