package csi

import (
	"testing"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestCanFormatEncryptedVolume(t *testing.T) {
	for _, tc := range []struct {
		name        string
		volume      *longhornclient.Volume
		expectError bool
	}{
		{
			name: "healthy blank volume can be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-a",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				Controllers: []longhornclient.Controller{
					{Name: "vol-a-e-0", Running: true, ActualSize: "0"},
				},
			},
		},
		{
			name: "healthy volume with stopped engine cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-a1",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				Controllers: []longhornclient.Controller{
					{Name: "vol-a1-e-0", ActualSize: "0"},
				},
			},
			expectError: true,
		},
		{
			name: "volume without engine reported actual size cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-b",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				Controllers: []longhornclient.Controller{
					{Name: "vol-b-e-0", Running: true},
				},
			},
			expectError: true,
		},
		{
			name: "volume without engine cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-b1",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
			},
			expectError: true,
		},
		{
			name: "volume with actual size below the LUKS2 header size can be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-b2",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				Controllers: []longhornclient.Controller{
					{Name: "vol-b2-e-0", Running: true, ActualSize: "4194304"},
				},
			},
		},
		{
			name: "degraded volume cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-c",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessDegraded),
				Controllers: []longhornclient.Controller{
					{Name: "vol-c-e-0", Running: true, ActualSize: "0"},
				},
			},
			expectError: true,
		},
		{
			name: "volume with unknown robustness cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-d",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessUnknown),
				Controllers: []longhornclient.Controller{
					{Name: "vol-d-e-0", Running: true, ActualSize: "0"},
				},
			},
			expectError: true,
		},
		{
			name: "volume with data cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-e",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				Controllers: []longhornclient.Controller{
					{Name: "vol-e-e-0", Running: true, ActualSize: "20971520"},
				},
			},
			expectError: true,
		},
		{
			name: "volume restored from backup cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-f",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				FromBackup: "s3://backupbucket@us-east-1/?backup=backup-1&volume=vol-x",
				Controllers: []longhornclient.Controller{
					{Name: "vol-f-e-0", Running: true, ActualSize: "0"},
				},
			},
			expectError: true,
		},
		{
			name: "cloned volume cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-g",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				DataSource: "vol://vol-x",
				Controllers: []longhornclient.Controller{
					{Name: "vol-g-e-0", Running: true, ActualSize: "0"},
				},
			},
			expectError: true,
		},
		{
			name: "volume with backup cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:       "vol-h",
				Encrypted:  true,
				Robustness: string(longhorn.VolumeRobustnessHealthy),
				LastBackup: "backup-1",
				Controllers: []longhornclient.Controller{
					{Name: "vol-h-e-0", Running: true, ActualSize: "0"},
				},
			},
			expectError: true,
		},
		{
			name: "volume with backing image cannot be encrypted",
			volume: &longhornclient.Volume{
				Name:         "vol-i",
				Encrypted:    true,
				Robustness:   string(longhorn.VolumeRobustnessHealthy),
				BackingImage: "bi-1",
				Controllers: []longhornclient.Controller{
					{Name: "vol-i-e-0", Running: true, ActualSize: "0"},
				},
			},
			expectError: true,
		},
		{
			name:        "missing volume cannot be encrypted",
			volume:      nil,
			expectError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := canFormatEncryptedVolume(tc.volume)
			if tc.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

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
