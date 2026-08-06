package csi

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"

	longhornclient "github.com/longhorn/longhorn-manager/client"
)

func TestNodeRequestLogsStripSecrets(t *testing.T) {
	const secret = "sentinel-csi-secret-value"

	for _, tc := range []struct {
		name string
		call func(*NodeServer)
	}{
		{
			name: "NodePublishVolume",
			call: func(ns *NodeServer) {
				_, _ = ns.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
					Secrets: map[string]string{"secret": secret},
				})
			},
		},
		{
			name: "NodeStageVolume",
			call: func(ns *NodeServer) {
				_, _ = ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
					Secrets: map[string]string{"secret": secret},
				})
			},
		},
		{
			name: "NodeExpandVolume",
			call: func(ns *NodeServer) {
				_, _ = ns.NodeExpandVolume(context.Background(), &csipb.NodeExpandVolumeRequest{
					Secrets: map[string]string{"secret": secret},
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := logrus.New()
			logger.SetOutput(&output)
			ns := &NodeServer{
				log: logger.WithField("component", "test"),
			}

			tc.call(ns)

			logOutput := output.String()
			if strings.Contains(logOutput, secret) {
				t.Fatalf("request log contains secret value: %s", logOutput)
			}
			if !strings.Contains(logOutput, tc.name+" is called with req") {
				t.Fatalf("request log is missing method context: %s", logOutput)
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
