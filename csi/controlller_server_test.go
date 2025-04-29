package csi

import (
	"context"
	"fmt"
	"github.com/longhorn/longhorn-manager/types"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

type disk struct {
	spec   longhorn.DiskSpec
	status longhorn.DiskStatus
}

func TestGetCapacity(t *testing.T) {
	cs := &ControllerServer{
		lhNamespace: "longhorn-system-test",
		log:         logrus.StandardLogger().WithField("component", "test-get-capacity"),
	}
	for _, test := range []struct {
		nodeID            string
		createNode        bool
		dataEngine        string
		diskSelector      string
		availableCapacity int64
		disks             []*disk
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
			err:        status.Errorf(codes.InvalidArgument, "storage class parameters missing 'dataEngine' key"),
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
			availableCapacity: 0,
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v2",
			availableCapacity: 0,
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v1",
			availableCapacity: 1150,
			disks:             []*disk{newDisk(1450, 300, "ssd", false, true, true, false), newDisk(1000, 500, "", false, true, true, false), newDisk(2000, 100, "", true, true, true, false)},
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v2",
			availableCapacity: 1650,
			disks:             []*disk{newDisk(1950, 300, "", true, true, true, false), newDisk(1500, 500, "", true, true, true, false), newDisk(2000, 100, "", false, true, true, false)},
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v2",
			diskSelector:      "ssd,fast",
			availableCapacity: 1000,
			disks:             []*disk{newDisk(1100, 100, "fast,nvmf,ssd,hot", true, true, true, false), newDisk(2500, 500, "ssd,slow,green", true, true, true, false), newDisk(2000, 100, "hdd,fast", true, true, true, false)},
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v2",
			availableCapacity: 400,
			disks:             []*disk{newDisk(1100, 100, "ssd", true, false, true, false), newDisk(500, 100, "hdd", true, true, true, false)},
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v2",
			availableCapacity: 400,
			disks:             []*disk{newDisk(1100, 100, "ssd", true, true, false, false), newDisk(500, 100, "hdd", true, true, true, false)},
		},
		{
			nodeID:            "node-0",
			createNode:        true,
			dataEngine:        "v2",
			availableCapacity: 400,
			disks:             []*disk{newDisk(1100, 100, "ssd", true, true, true, true), newDisk(500, 100, "hdd", true, true, true, false)},
		},
	} {
		cs.lhClient = lhfake.NewSimpleClientset()
		cs.lhClient.LonghornV1beta2().Settings(cs.lhNamespace).Create(context.TODO(), newSetting(string(types.SettingNameAllowEmptyDiskSelectorVolume), "true"), metav1.CreateOptions{})
		if test.createNode {
			node := newNode(test.nodeID, cs.lhNamespace, test.disks)
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
			Parameters: map[string]string{},
		}
		if test.dataEngine != "" {
			req.Parameters["dataEngine"] = test.dataEngine
		}
		req.Parameters["diskSelector"] = test.diskSelector
		res, err := cs.GetCapacity(context.TODO(), req)

		expectedStatus := status.Convert(test.err)
		actualStatus := status.Convert(err)
		if expectedStatus.Code() != actualStatus.Code() {
			t.Errorf("expected error code: %v, but got: %v", expectedStatus.Code(), actualStatus.Code())
		} else if expectedStatus.Message() != actualStatus.Message() {
			t.Errorf("expected error message: '%s', but got: '%s'", expectedStatus.Message(), actualStatus.Message())
		}
		if res != nil && res.AvailableCapacity != test.availableCapacity {
			t.Errorf("expected available capacity: %d, but got: %d", test.availableCapacity, res.AvailableCapacity)
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

func newDisk(storageAvailable, storageReserved int64, tags string, isBlockType, isCondOk, allowScheduling, evictionRequested bool) *disk {
	disk := &disk{
		spec: longhorn.DiskSpec{
			StorageReserved:   storageReserved,
			Tags:              strings.Split(tags, ","),
			AllowScheduling:   allowScheduling,
			EvictionRequested: evictionRequested,
		},
		status: longhorn.DiskStatus{
			StorageAvailable: storageAvailable,
			Type:             longhorn.DiskTypeFilesystem,
		},
	}
	if isBlockType {
		disk.status.Type = longhorn.DiskTypeBlock
	}
	if isCondOk {
		disk.status.Conditions = []longhorn.Condition{{Type: longhorn.DiskConditionTypeSchedulable, Status: longhorn.ConditionStatusTrue}}
	}
	return disk
}

func newNode(name, namespace string, disks []*disk) *longhorn.Node {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: longhorn.NodeSpec{
			Disks: map[string]longhorn.DiskSpec{},
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{},
		},
	}
	for i, disk := range disks {
		name := fmt.Sprintf("disk-%d", i)
		node.Spec.Disks[name] = disk.spec
		node.Status.DiskStatus[name] = &disk.status
	}
	return node
}

func newSetting(name, value string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Value: value,
	}
}
