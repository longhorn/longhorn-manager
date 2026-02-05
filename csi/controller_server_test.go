package csi

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

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
		testName                            string
		node                                *longhorn.Node
		skipNodeCreation                    bool
		skipNodeSettingCreation             bool
		skipDiskSettingCreation             bool
		skipOverProvisioningSettingCreation bool
		overProvisioningPercentage          string
		dataEngine                          string
		diskSelector                        string
		nodeSelector                        string
		availableCapacity                   int64
		disks                               []*disk
		err                                 error
	}{
		{
			testName:         "Node not found",
			skipNodeCreation: true,
			node:             newNode("node-0", "storage", true, true, true, false),
			err:              status.Errorf(codes.NotFound, "node node-0 not found"),
		},
		{
			testName:                "Node setting not found",
			skipNodeSettingCreation: true,
			node:                    newNode("node-0", "storage", true, true, true, false),
			err:                     status.Errorf(codes.Internal, "failed to get setting allow-empty-node-selector-volume: settings.longhorn.io \"allow-empty-node-selector-volume\" not found"),
		},
		{
			testName:                "Disk setting not found",
			skipDiskSettingCreation: true,
			node:                    newNode("node-0", "storage", true, true, true, false),
			err:                     status.Errorf(codes.Internal, "failed to get setting allow-empty-disk-selector-volume: settings.longhorn.io \"allow-empty-disk-selector-volume\" not found"),
		},
		{
			testName:                            "Over-provisioning setting not found",
			skipOverProvisioningSettingCreation: true,
			node:                                newNode("node-0", "storage", true, true, true, false),
			err:                                 status.Errorf(codes.Internal, "failed to get setting storage-over-provisioning-percentage: settings.longhorn.io \"storage-over-provisioning-percentage\" not found"),
		},
		{
			testName:                   "Invalid over-provisioning setting value",
			overProvisioningPercentage: "xyz",
			node:                       newNode("node-0", "storage", true, true, true, false),
			err:                        status.Errorf(codes.Internal, "failed to get setting storage-over-provisioning-percentage: strconv.ParseInt: parsing \"xyz\": invalid syntax"),
		},
		{
			testName:                   "Unknown data engine type",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v5",
			err:                        status.Errorf(codes.InvalidArgument, "unknown data engine type v5"),
		},
		{
			testName:                   "v1 engine with no disks",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v1",
			availableCapacity:          0,
		},
		{
			testName:                   "v2 engine with no disks",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v2",
			availableCapacity:          0,
		},
		{
			testName:                   "Node condition is not ready",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", false, true, true, false),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			availableCapacity:          0,
		},
		{
			testName:                   "Node condition is not schedulable",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, false, true, false),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			availableCapacity:          0,
		},
		{
			testName:                   "Scheduling not allowed on a node",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, false, false),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			availableCapacity:          0,
		},
		{
			testName:                   "Node eviction is requested",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, true),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			availableCapacity:          0,
		},
		{
			testName:                   "Node tags don't match node selector",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "large,fast,linux", true, true, true, false),
			nodeSelector:               "fast,storage",
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			availableCapacity:          0,
		},
		{
			testName:                   "Must default to v1 engine when dataEngine key is missing",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false), newDisk(2000, 100, 0, "", true, true, true, false)},
			availableCapacity:          1150,
		},
		{
			testName:                   "v1 engine with two valid disks",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage,large,fast,linux", true, true, true, false),
			nodeSelector:               "fast,storage",
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			availableCapacity:          1150,
		},
		{
			testName:                   "v1 engine with two valid disks and one with mismatched engine type",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v1",
			availableCapacity:          1150,
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false), newDisk(2000, 100, 0, "", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with two valid disks and one with mismatched engine type",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v2",
			availableCapacity:          1650,
			disks:                      []*disk{newDisk(1950, 300, 0, "", true, true, true, false), newDisk(1500, 500, 0, "", true, true, true, false), newDisk(2000, 100, 0, "", false, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and two with unmatched tags",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v2",
			diskSelector:               "ssd,fast",
			availableCapacity:          1000,
			disks:                      []*disk{newDisk(1100, 100, 0, "fast,nvmf,ssd,hot", true, true, true, false), newDisk(2500, 500, 0, "ssd,slow,green", true, true, true, false), newDisk(2000, 100, 0, "hdd,fast", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and one with unhealthy condition",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v2",
			availableCapacity:          400,
			disks:                      []*disk{newDisk(1100, 100, 0, "ssd", true, false, true, false), newDisk(500, 100, 0, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and one with scheduling disabled",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v2",
			availableCapacity:          400,
			disks:                      []*disk{newDisk(1100, 100, 0, "ssd", true, true, false, false), newDisk(500, 100, 0, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and one marked for eviction",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v2",
			availableCapacity:          400,
			disks:                      []*disk{newDisk(1100, 100, 0, "ssd", true, true, true, true), newDisk(500, 100, 0, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with over-provisioning set to 200",
			overProvisioningPercentage: "200",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v2",
			availableCapacity:          1700,
			disks:                      []*disk{newDisk(1100, 100, 300, "ssd", true, true, true, false), newDisk(500, 100, 100, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v1 engine with over-provisioning set to 400",
			overProvisioningPercentage: "400",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v1",
			availableCapacity:          1500,
			disks:                      []*disk{newDisk(900, 400, 600, "ssd", false, true, true, false), newDisk(1500, 500, 2500, "hdd", false, true, true, false)},
		},
		{
			testName:                   "Capacity floors at 0 when scheduled exceeds over-provisioning limit",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", true, true, true, false),
			dataEngine:                 "v1",
			availableCapacity:          0,
			disks:                      []*disk{newDisk(1000, 100, 1000, "", false, true, true, false)},
		},
	} {
		t.Run(test.testName, func(t *testing.T) {
			cs.lhClient = lhfake.NewSimpleClientset() // nolint: staticcheck
			if !test.skipNodeCreation {
				addDisksToNode(test.node, test.disks)
				_, err := cs.lhClient.LonghornV1beta2().Nodes(cs.lhNamespace).Create(context.TODO(), test.node, metav1.CreateOptions{})
				if err != nil {
					t.Error("failed to create node")
				}
			}
			if !test.skipNodeSettingCreation {
				_, err := cs.lhClient.LonghornV1beta2().Settings(cs.lhNamespace).Create(context.TODO(), newSetting(string(types.SettingNameAllowEmptyNodeSelectorVolume), "true"), metav1.CreateOptions{})
				if err != nil {
					t.Errorf("failed to create setting %v", types.SettingNameAllowEmptyNodeSelectorVolume)
				}
			}
			if !test.skipDiskSettingCreation {
				_, err := cs.lhClient.LonghornV1beta2().Settings(cs.lhNamespace).Create(context.TODO(), newSetting(string(types.SettingNameAllowEmptyDiskSelectorVolume), "true"), metav1.CreateOptions{})
				if err != nil {
					t.Errorf("failed to create setting %v", types.SettingNameAllowEmptyDiskSelectorVolume)
				}
			}
			if !test.skipOverProvisioningSettingCreation {
				_, err := cs.lhClient.LonghornV1beta2().Settings(cs.lhNamespace).Create(context.TODO(), newSetting(string(types.SettingNameStorageOverProvisioningPercentage), test.overProvisioningPercentage), metav1.CreateOptions{})
				if err != nil {
					t.Errorf("failed to create setting %v", types.SettingNameStorageOverProvisioningPercentage)
				}
			}

			req := &csi.GetCapacityRequest{
				AccessibleTopology: &csi.Topology{
					Segments: map[string]string{
						nodeTopologyKey: test.node.Name,
					},
				},
				Parameters: map[string]string{},
			}
			if test.dataEngine != "" {
				req.Parameters["dataEngine"] = test.dataEngine
			}
			req.Parameters["diskSelector"] = test.diskSelector
			req.Parameters["nodeSelector"] = test.nodeSelector
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
		})
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

func newDisk(storageMaximum, storageReserved, storageScheduled int64, tags string, isBlockType, isCondOk, allowScheduling, evictionRequested bool) *disk {
	disk := &disk{
		spec: longhorn.DiskSpec{
			StorageReserved:   storageReserved,
			Tags:              strings.Split(tags, ","),
			AllowScheduling:   allowScheduling,
			EvictionRequested: evictionRequested,
		},
		status: longhorn.DiskStatus{
			StorageMaximum:   storageMaximum,
			StorageScheduled: storageScheduled,
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

func newNode(name, tags string, isCondReady, isCondSchedulable, allowScheduling, evictionRequested bool) *longhorn.Node {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: longhorn.NodeSpec{
			Disks:             map[string]longhorn.DiskSpec{},
			Tags:              strings.Split(tags, ","),
			AllowScheduling:   allowScheduling,
			EvictionRequested: evictionRequested,
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{},
		},
	}
	if isCondReady {
		node.Status.Conditions = append(node.Status.Conditions, longhorn.Condition{Type: longhorn.NodeConditionTypeReady, Status: longhorn.ConditionStatusTrue})
	}
	if isCondSchedulable {
		node.Status.Conditions = append(node.Status.Conditions, longhorn.Condition{Type: longhorn.NodeConditionTypeSchedulable, Status: longhorn.ConditionStatusTrue})
	}
	return node
}

func addDisksToNode(node *longhorn.Node, disks []*disk) {
	for i, disk := range disks {
		name := fmt.Sprintf("disk-%d", i)
		node.Spec.Disks[name] = disk.spec
		node.Status.DiskStatus[name] = &disk.status
	}
}

func newSetting(name, value string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Value: value,
	}
}
