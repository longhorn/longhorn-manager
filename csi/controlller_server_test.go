package csi

import (
	"context"
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestGetCapacity(t *testing.T) {
	cs := &ControllerServer{
		lhNamespace: "longhorn-system-test",
		log:         logrus.StandardLogger().WithField("component", "test-get-capacity"),
	}
	for _, test := range []struct {
		nodeID            string
		createNode        bool
		dataEngine        string
		availableCapacity int64
		err               error
	}{
		{
			nodeID:     "node-0",
			dataEngine: "v1",
			err:        status.Errorf(codes.NotFound, "node node-0 not found"),
		},
		{
			nodeID:     "node-0",
			createNode: true,
			dataEngine: "v5",
			err:        status.Errorf(codes.InvalidArgument, "unknown data engine type v5"),
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v1",
			availableCapacity: 1028,
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v2",
			availableCapacity: 1028,
		},
	} {
		cs.lhClient = lhfake.NewSimpleClientset()
		if test.createNode {
			node := newNode(test.nodeID, cs.lhNamespace, test.dataEngine, test.availableCapacity)
			_, err := cs.lhClient.LonghornV1beta2().Nodes(cs.lhNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
			if err != nil {
				t.Errorf("failed to create mock node: %v", err)
			}
		}
		req := &csi.GetCapacityRequest{
			AccessibleTopology: &csi.Topology{
				Segments: map[string]string{
					nodeTopologyKey: test.nodeID,
				},
			},
			Parameters: map[string]string{
				"dataEngine": test.dataEngine,
			},
		}
		res, err := cs.GetCapacity(context.TODO(), req)

		expectedStatus := status.Convert(test.err)
		actualStatus := status.Convert(err)
		if expectedStatus.Code() != actualStatus.Code() {
			t.Errorf("expected error code: %v, but got: %v", expectedStatus.Code(), actualStatus.Code())
		} else if expectedStatus.Message() != actualStatus.Message() {
			t.Errorf("expected error message: '%s', but got: '%s'", expectedStatus.Message(), actualStatus.Message())
		}
		if res != nil && res.AvailableCapacity != test.availableCapacity {
			t.Errorf("expected available capacity: %d, but got: %d", res.AvailableCapacity, test.availableCapacity)
		}
	}
}

func TestParseDataEngine(t *testing.T) {
	for _, test := range []struct {
		parameters map[string]string
		err        error
		dataEngine string
	}{
		{
			err: fmt.Errorf("missing storage class parameters"),
		},
		{
			parameters: map[string]string{},
			err:        fmt.Errorf("storage class parameters missing data engine key"),
		},
		{
			parameters: map[string]string{
				"dataEngine": "v1",
			},
			dataEngine: "v1",
		},
		{
			parameters: map[string]string{
				"dataEngine": "v5",
			},
			dataEngine: "v5",
		},
	} {
		dataEngine, err := parseDataEngine(test.parameters)
		checkError(t, test.err, err)
		if test.dataEngine != string(dataEngine) {
			t.Errorf("expected dataEngine: %s, but got: %s", test.dataEngine, dataEngine)
		}
	}
}

func TestParseNodeID(t *testing.T) {
	for _, test := range []struct {
		topology *csi.Topology
		err      error
		nodeID   string
	}{
		{
			err: fmt.Errorf("missing accessible topology request parameter"),
		},
		{
			topology: &csi.Topology{
				Segments: nil,
			},
			err: fmt.Errorf("missing accessible topology request parameter"),
		},
		{
			topology: &csi.Topology{
				Segments: map[string]string{
					"some-key": "some-value",
				},
			},
			err: fmt.Errorf("accessible topology request parameter is missing kubernetes.io/hostname key"),
		},
		{
			topology: &csi.Topology{
				Segments: map[string]string{
					nodeTopologyKey: "node-0",
				},
			},
			nodeID: "node-0",
		},
	} {
		nodeID, err := parseNodeID(test.topology)
		checkError(t, test.err, err)
		if test.nodeID != nodeID {
			t.Errorf("expected nodeID: %s, but got: %s", test.nodeID, nodeID)
		}
	}
}

func checkError(t *testing.T, expected, actual error) {
	if expected == nil {
		if actual != nil {
			t.Errorf("expected no error but got: %v", actual)
		}
	} else {
		if actual == nil {
			t.Errorf("expected error: %v, but got no error", expected)
		}
		if expected.Error() != actual.Error() {
			t.Errorf("expected error: %v, but got: %v", expected, actual)
		}
	}
}

func newNode(name, namespace, dataEngine string, storageSize int64) *longhorn.Node {
	diskType := longhorn.DiskTypeFilesystem
	if dataEngine == "v2" {
		diskType = longhorn.DiskTypeBlock
	}
	return &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{
				"disk": {
					StorageAvailable: storageSize,
					Type:             diskType,
				},
			},
		},
	}
}
