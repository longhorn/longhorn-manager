package csi

import (
	"testing"

	longhornclient "github.com/longhorn/longhorn-manager/client"
)

func TestGetV2VolumeEndpointForNode(t *testing.T) {
	for _, tc := range []struct {
		name        string
		volume      *longhornclient.Volume
		nodeID      string
		expected    string
		expectError bool
	}{
		{
			name:   "non migratable falls back to any ready endpoint",
			nodeID: "node-b",
			volume: &longhornclient.Volume{
				Name:       "vol-a",
				Migratable: false,
				Controllers: []longhornclient.Controller{
					{HostId: "node-a", Endpoint: "/dev/longhorn/vol-a"},
				},
			},
			expected: "/dev/longhorn/vol-a",
		},
		{
			name:   "migratable selects destination node endpoint",
			nodeID: "node-b",
			volume: &longhornclient.Volume{
				Name:       "vol-b",
				Migratable: true,
				Controllers: []longhornclient.Controller{
					{HostId: "node-a", Endpoint: "/dev/longhorn/vol-b"},
					{HostId: "node-b", Endpoint: "/dev/longhorn/vol-b"},
				},
			},
			expected: "/dev/longhorn/vol-b",
		},
		{
			name:   "migratable does not fall back to another node endpoint",
			nodeID: "node-b",
			volume: &longhornclient.Volume{
				Name:       "vol-c",
				Migratable: true,
				Controllers: []longhornclient.Controller{
					{HostId: "node-a", Endpoint: "/dev/longhorn/vol-c"},
				},
			},
			expectError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, err := getV2VolumeEndpointForNode(tc.volume, tc.nodeID)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got endpoint %q", endpoint)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if endpoint != tc.expected {
				t.Fatalf("expected endpoint %q, got %q", tc.expected, endpoint)
			}
		})
	}
}
