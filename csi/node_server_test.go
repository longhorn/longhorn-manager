package csi

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"

	corev1 "k8s.io/api/core/v1"

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

func newWorkloadStatus(podName, podStatus string) longhornclient.WorkloadStatus {
	return longhornclient.WorkloadStatus{
		PodName:   podName,
		PodStatus: podStatus,
	}
}

func TestIsWorkloadMountReady(t *testing.T) {
	for _, tc := range []struct {
		name      string
		workloads []longhornclient.WorkloadStatus
		ready     bool
	}{
		{
			name:      "empty workloads (first mount)",
			workloads: nil,
			ready:     true,
		},
		{
			name: "all running",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("pod-a", string(corev1.PodRunning)),
				newWorkloadStatus("pod-b", string(corev1.PodRunning)),
			},
			ready: true,
		},
		{
			name: "running with retained failed workloads (regression for #13723)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("failed-1", string(corev1.PodFailed)),
				newWorkloadStatus("failed-2", string(corev1.PodFailed)),
				newWorkloadStatus("failed-3", string(corev1.PodFailed)),
				newWorkloadStatus("failed-4", string(corev1.PodFailed)),
				newWorkloadStatus("running-1", string(corev1.PodRunning)),
				newWorkloadStatus("running-2", string(corev1.PodRunning)),
			},
			ready: true,
		},
		{
			name: "running plus succeeded",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("succeeded-1", string(corev1.PodSucceeded)),
				newWorkloadStatus("running-1", string(corev1.PodRunning)),
			},
			ready: true,
		},
		{
			name: "all failed",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("failed-1", string(corev1.PodFailed)),
				newWorkloadStatus("failed-2", string(corev1.PodFailed)),
			},
			ready: false,
		},
		{
			name: "all succeeded",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("succ-1", string(corev1.PodSucceeded)),
			},
			ready: false,
		},
		{
			name: "all terminal (failed and succeeded mixed)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("failed-1", string(corev1.PodFailed)),
				newWorkloadStatus("succ-1", string(corev1.PodSucceeded)),
			},
			ready: false,
		},
		{
			name: "pending only (new pod starting)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("pending-1", string(corev1.PodPending)),
			},
			ready: true,
		},
		{
			name: "pending plus running plus failed (pod rolling update)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("failed-1", string(corev1.PodFailed)),
				newWorkloadStatus("running-1", string(corev1.PodRunning)),
				newWorkloadStatus("pending-1", string(corev1.PodPending)),
			},
			ready: true,
		},
		{
			name: "pending plus failed (new pod replacing failed)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("failed-1", string(corev1.PodFailed)),
				newWorkloadStatus("pending-1", string(corev1.PodPending)),
			},
			ready: true,
		},
		{
			name: "unknown only (node unreachable, no running pods)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("unknown-1", string(corev1.PodUnknown)),
			},
			ready: false,
		},
		{
			name: "running plus unknown (transitioning state)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("running-1", string(corev1.PodRunning)),
				newWorkloadStatus("unknown-1", string(corev1.PodUnknown)),
			},
			ready: false,
		},
		{
			name: "unknown plus failed (node failure scenario)",
			workloads: []longhornclient.WorkloadStatus{
				newWorkloadStatus("failed-1", string(corev1.PodFailed)),
				newWorkloadStatus("unknown-1", string(corev1.PodUnknown)),
			},
			ready: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			podsStatus := map[corev1.PodPhase][]string{}
			for _, w := range tc.workloads {
				phase := corev1.PodPhase(w.PodStatus)
				podsStatus[phase] = append(podsStatus[phase], w.PodName)
			}
			got := isWorkloadMountReady(podsStatus)
			if got != tc.ready {
				t.Errorf("isWorkloadMountReady() = %v, want %v; podsStatus=%+v", got, tc.ready, podsStatus)
			}
		})
	}
}
