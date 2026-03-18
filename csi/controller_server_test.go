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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake" // nolint: staticcheck
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

const testNamespace = "longhorn-system-test"

func newTestControllerServer(t *testing.T, objs ...runtime.Object) *ControllerServer {
	t.Helper()
	lhClient := lhfake.NewSimpleClientset(objs...) // nolint: staticcheck
	informerFactory := lhinformers.NewSharedInformerFactoryWithOptions(lhClient, 0, lhinformers.WithNamespace(testNamespace))
	settingInformer := informerFactory.Longhorn().V1beta2().Settings()
	_ = settingInformer.Informer()
	nodeInformer := informerFactory.Longhorn().V1beta2().Nodes()
	_ = nodeInformer.Informer()
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	informerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, settingInformer.Informer().HasSynced, nodeInformer.Informer().HasSynced) {
		t.Fatal("failed to sync informer caches")
	}
	return &ControllerServer{
		log:           logrus.StandardLogger().WithField("component", "test"),
		settingLister: settingInformer.Lister().Settings(testNamespace),
		nodeLister:    nodeInformer.Lister().Nodes(testNamespace),
	}
}

type disk struct {
	spec   longhorn.DiskSpec
	status longhorn.DiskStatus
}

func TestGetCapacity(t *testing.T) {
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
		maximumVolumeSize                   int64
		disks                               []*disk
		err                                 error
	}{
		{
			testName:                   "Node not found",
			skipNodeCreation:           true,
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			err:                        status.Errorf(codes.NotFound, "node node-0 not found"),
		},
		{
			testName:                "Node setting not found",
			skipNodeSettingCreation: true,
			node:                    newNode("node-0", "storage", "", true, true, true, false),
			err:                     status.Errorf(codes.Internal, "failed to get setting allow-empty-node-selector-volume: setting.longhorn.io \"allow-empty-node-selector-volume\" not found"),
		},
		{
			testName:                "Disk setting not found",
			skipDiskSettingCreation: true,
			node:                    newNode("node-0", "storage", "", true, true, true, false),
			err:                     status.Errorf(codes.Internal, "failed to get setting allow-empty-disk-selector-volume: setting.longhorn.io \"allow-empty-disk-selector-volume\" not found"),
		},
		{
			testName:                            "Over-provisioning setting not found",
			skipOverProvisioningSettingCreation: true,
			node:                                newNode("node-0", "storage", "", true, true, true, false),
			err:                                 status.Errorf(codes.Internal, "failed to get setting storage-over-provisioning-percentage: setting.longhorn.io \"storage-over-provisioning-percentage\" not found"),
		},
		{
			testName:                   "Invalid over-provisioning setting value",
			overProvisioningPercentage: "xyz",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			err:                        status.Errorf(codes.Internal, "failed to get setting storage-over-provisioning-percentage: strconv.ParseInt: parsing \"xyz\": invalid syntax"),
		},
		{
			testName:                   "Unknown data engine type",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v5",
			maximumVolumeSize:          0,
		},
		{
			testName:                   "v1 engine with no disks",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v1",
			maximumVolumeSize:          0,
		},
		{
			testName:                   "v2 engine with no disks",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v2",
			maximumVolumeSize:          0,
		},
		{
			testName:                   "Node condition is not ready",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", false, true, true, false),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			maximumVolumeSize:          0,
		},
		{
			testName:                   "Node condition is not schedulable",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, false, true, false),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			maximumVolumeSize:          0,
		},
		{
			testName:                   "Scheduling not allowed on a node",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, false, false),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			maximumVolumeSize:          0,
		},
		{
			testName:                   "Node eviction is requested",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, true),
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			maximumVolumeSize:          0,
		},
		{
			testName:                   "Node tags don't match node selector",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "large,fast,linux", "", true, true, true, false),
			nodeSelector:               "fast,storage",
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			maximumVolumeSize:          0,
		},
		{
			testName:                   "Must default to v1 engine when dataEngine key is missing",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false), newDisk(2000, 100, 0, "", true, true, true, false)},
			maximumVolumeSize:          1150,
		},
		{
			testName:                   "v1 engine with two valid disks",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage,large,fast,linux", "", true, true, true, false),
			nodeSelector:               "fast,storage",
			dataEngine:                 "v1",
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false)},
			maximumVolumeSize:          1150,
		},
		{
			testName:                   "v1 engine with two valid disks and one with mismatched engine type",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v1",
			maximumVolumeSize:          1150,
			disks:                      []*disk{newDisk(1450, 300, 0, "ssd", false, true, true, false), newDisk(1000, 500, 0, "", false, true, true, false), newDisk(2000, 100, 0, "", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with two valid disks and one with mismatched engine type",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v2",
			maximumVolumeSize:          1650,
			disks:                      []*disk{newDisk(1950, 300, 0, "", true, true, true, false), newDisk(1500, 500, 0, "", true, true, true, false), newDisk(2000, 100, 0, "", false, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and two with unmatched tags",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v2",
			diskSelector:               "ssd,fast",
			maximumVolumeSize:          1000,
			disks:                      []*disk{newDisk(1100, 100, 0, "fast,nvmf,ssd,hot", true, true, true, false), newDisk(2500, 500, 0, "ssd,slow,green", true, true, true, false), newDisk(2000, 100, 0, "hdd,fast", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and one with unhealthy condition",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v2",
			maximumVolumeSize:          400,
			disks:                      []*disk{newDisk(1100, 100, 0, "ssd", true, false, true, false), newDisk(500, 100, 0, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and one with scheduling disabled",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v2",
			maximumVolumeSize:          400,
			disks:                      []*disk{newDisk(1100, 100, 0, "ssd", true, true, false, false), newDisk(500, 100, 0, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with one valid disk and one marked for eviction",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v2",
			maximumVolumeSize:          400,
			disks:                      []*disk{newDisk(1100, 100, 0, "ssd", true, true, true, true), newDisk(500, 100, 0, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v2 engine with over-provisioning set to 200",
			overProvisioningPercentage: "200",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v2",
			maximumVolumeSize:          1700,
			disks:                      []*disk{newDisk(1100, 100, 300, "ssd", true, true, true, false), newDisk(500, 100, 100, "hdd", true, true, true, false)},
		},
		{
			testName:                   "v1 engine with over-provisioning set to 400",
			overProvisioningPercentage: "400",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v1",
			maximumVolumeSize:          1500,
			disks:                      []*disk{newDisk(900, 400, 600, "ssd", false, true, true, false), newDisk(1500, 500, 2500, "hdd", false, true, true, false)},
		},
		{
			testName:                   "Capacity floors at 0 when scheduled exceeds over-provisioning limit",
			overProvisioningPercentage: "100",
			node:                       newNode("node-0", "storage", "", true, true, true, false),
			dataEngine:                 "v1",
			maximumVolumeSize:          0,
			disks:                      []*disk{newDisk(1000, 100, 1000, "", false, true, true, false)},
		},
	} {
		t.Run(test.testName, func(t *testing.T) {
			var objs []runtime.Object
			objs = append(objs, newSetting(string(types.SettingNameCSIStorageCapacityTracking), "node"))
			if !test.skipNodeCreation {
				addDisksToNode(test.node, test.disks)
				objs = append(objs, test.node)
			}
			if !test.skipNodeSettingCreation {
				objs = append(objs, newSetting(string(types.SettingNameAllowEmptyNodeSelectorVolume), "true"))
			}
			if !test.skipDiskSettingCreation {
				objs = append(objs, newSetting(string(types.SettingNameAllowEmptyDiskSelectorVolume), "true"))
			}
			if !test.skipOverProvisioningSettingCreation {
				objs = append(objs, newSetting(string(types.SettingNameStorageOverProvisioningPercentage), test.overProvisioningPercentage))
			}
			cs := newTestControllerServer(t, objs...)

			req := &csi.GetCapacityRequest{
				AccessibleTopology: &csi.Topology{
					Segments: map[string]string{
						corev1.LabelHostname: test.node.Name,
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
			if res != nil && res.MaximumVolumeSize.Value != test.maximumVolumeSize {
				t.Errorf("expected maximum volume size: %d, but got: %d", test.maximumVolumeSize, res.MaximumVolumeSize.Value)
			}
		})
	}
}

func TestGetCapacityPerZone(t *testing.T) {
	// Zone A: 3 nodes, 2 disks each
	//   node-a0: disk0(max=2000, reserved=200, scheduled=0) + disk1(max=1000, reserved=100, scheduled=0)
	//   node-a1: disk0(max=3000, reserved=300, scheduled=500) + disk1(max=1500, reserved=150, scheduled=0)
	//   node-a2: disk0(max=1800, reserved=100, scheduled=200) + disk1(max=2200, reserved=200, scheduled=100)
	//
	// Zone B: 3 nodes, 2 disks each
	//   node-b0: disk0(max=5000, reserved=500, scheduled=1000) + disk1(max=3000, reserved=300, scheduled=0)
	//   node-b1: disk0(max=1000, reserved=100, scheduled=0) + disk1(max=800, reserved=100, scheduled=0)
	//   node-b2: disk0(max=4000, reserved=400, scheduled=2000) + disk1(max=2000, reserved=200, scheduled=500)

	zoneANodes := []*longhorn.Node{
		newNode("node-a0", "", "zone-a", true, true, true, false),
		newNode("node-a1", "", "zone-a", true, true, true, false),
		newNode("node-a2", "", "zone-a", true, true, true, false),
	}
	zoneADisks := [][]*disk{
		{newDisk(2000, 200, 0, "", false, true, true, false), newDisk(1000, 100, 0, "", false, true, true, false)},
		{newDisk(3000, 300, 500, "", false, true, true, false), newDisk(1500, 150, 0, "", false, true, true, false)},
		{newDisk(1800, 100, 200, "", false, true, true, false), newDisk(2200, 200, 100, "", false, true, true, false)},
	}

	zoneBNodes := []*longhorn.Node{
		newNode("node-b0", "", "zone-b", true, true, true, false),
		newNode("node-b1", "", "zone-b", true, true, true, false),
		newNode("node-b2", "", "zone-b", true, true, true, false),
	}
	zoneBDisks := [][]*disk{
		{newDisk(5000, 500, 1000, "", false, true, true, false), newDisk(3000, 300, 0, "", false, true, true, false)},
		{newDisk(1000, 100, 0, "", false, true, true, false), newDisk(800, 100, 0, "", false, true, true, false)},
		{newDisk(4000, 400, 2000, "", false, true, true, false), newDisk(2000, 200, 500, "", false, true, true, false)},
	}

	// Per-node available capacity (over-provisioning=100%):
	//   schedulable per disk = (max - reserved) * 100/100 - scheduled
	//
	// Zone A:
	//   node-a0: (2000-200)-0=1800, (1000-100)-0=900     → available=2700, maxVol=1800
	//   node-a1: (3000-300)-500=2200, (1500-150)-0=1350   → available=3550, maxVol=2200
	//   node-a2: (1800-100)-200=1500, (2200-200)-100=1900 → available=3400, maxVol=1900
	//   Zone total: available=9650, maxVol=max(1800,2200,1900)=2200
	//
	// Zone B:
	//   node-b0: (5000-500)-1000=3500, (3000-300)-0=2700   → available=6200, maxVol=3500
	//   node-b1: (1000-100)-0=900, (800-100)-0=700         → available=1600, maxVol=900
	//   node-b2: (4000-400)-2000=1600, (2000-200)-500=1300 → available=2900, maxVol=1600
	//   Zone total: available=10700, maxVol=max(3500,900,1600)=3500

	allNodes := append(zoneANodes, zoneBNodes...)
	allDisks := append(zoneADisks, zoneBDisks...)
	var objs []runtime.Object
	for i, node := range allNodes {
		addDisksToNode(node, allDisks[i])
		objs = append(objs, node)
	}
	objs = append(objs,
		newSetting(string(types.SettingNameCSIStorageCapacityTracking), "zone"),
		newSetting(string(types.SettingNameAllowEmptyNodeSelectorVolume), "true"),
		newSetting(string(types.SettingNameAllowEmptyDiskSelectorVolume), "true"),
		newSetting(string(types.SettingNameStorageOverProvisioningPercentage), "100"),
	)
	cs := newTestControllerServer(t, objs...)

	for _, tc := range []struct {
		zone              string
		expectedAvailable int64
		expectedMaxVol    int64
	}{
		{zone: "zone-a", expectedAvailable: 9650, expectedMaxVol: 2200},
		{zone: "zone-b", expectedAvailable: 10700, expectedMaxVol: 3500},
	} {
		t.Run(tc.zone, func(t *testing.T) {
			req := &csi.GetCapacityRequest{
				AccessibleTopology: &csi.Topology{
					Segments: map[string]string{
						corev1.LabelTopologyZone: tc.zone,
					},
				},
				Parameters: map[string]string{},
			}
			res, err := cs.GetCapacity(context.TODO(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.AvailableCapacity != tc.expectedAvailable {
				t.Errorf("expected available capacity %d, got %d", tc.expectedAvailable, res.AvailableCapacity)
			}
			if res.MaximumVolumeSize.Value != tc.expectedMaxVol {
				t.Errorf("expected maximum volume size %d, got %d", tc.expectedMaxVol, res.MaximumVolumeSize.Value)
			}
		})
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

func newNode(name, tags, zone string, isCondReady, isCondSchedulable, allowScheduling, evictionRequested bool) *longhorn.Node {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: longhorn.NodeSpec{
			Disks:             map[string]longhorn.DiskSpec{},
			Tags:              strings.Split(tags, ","),
			AllowScheduling:   allowScheduling,
			EvictionRequested: evictionRequested,
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{},
			Zone:       zone,
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
			Name:      name,
			Namespace: testNamespace,
		},
		Value: value,
	}
}

func TestGetAccessibleTopologyFromRequirements(t *testing.T) {

	for _, test := range []struct {
		testName            string
		allowedTopologyKeys string
		skipSettingCreation bool
		accessibilityReqs   *csi.TopologyRequirement
		expectedTopology    []*csi.Topology
	}{
		{
			testName:          "Nil accessibility requirements",
			accessibilityReqs: nil,
			expectedTopology:  nil,
		},
		{
			testName:            "Empty setting filters all keys",
			allowedTopologyKeys: "",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":        "node1",
						"topology.kubernetes.io/zone":   "us-east-1a",
						"topology.kubernetes.io/region": "us-east-1",
					}},
				},
			},
			expectedTopology: nil,
		},
		{
			testName:            "Filter to zone only",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":        "node1",
						"topology.kubernetes.io/zone":   "us-east-1a",
						"topology.kubernetes.io/region": "us-east-1",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
				}},
			},
		},
		{
			testName:            "Filter to zone and region",
			allowedTopologyKeys: "topology.kubernetes.io/zone,topology.kubernetes.io/region",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":        "node1",
						"topology.kubernetes.io/zone":   "us-east-1a",
						"topology.kubernetes.io/region": "us-east-1",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone":   "us-east-1a",
					"topology.kubernetes.io/region": "us-east-1",
				}},
			},
		},
		{
			testName:            "No matching keys results in nil",
			allowedTopologyKeys: "nonexistent.key/something",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname": "node1",
					}},
				},
			},
			expectedTopology: nil,
		},
		{
			testName:            "Setting CR not found - filters all keys",
			skipSettingCreation: true,
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node1",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
				},
			},
			expectedTopology: nil,
		},
		{
			testName:            "Requisite topology used when no Preferred",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			accessibilityReqs: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node1",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
				}},
			},
		},
		{
			testName:            "Whitespace in setting value is trimmed",
			allowedTopologyKeys: " topology.kubernetes.io/zone , topology.kubernetes.io/region ",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":        "node1",
						"topology.kubernetes.io/zone":   "us-east-1a",
						"topology.kubernetes.io/region": "us-east-1",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone":   "us-east-1a",
					"topology.kubernetes.io/region": "us-east-1",
				}},
			},
		},
		{
			testName:            "Multiple topology entries filtered independently",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node1",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node2",
						"topology.kubernetes.io/zone": "us-east-1b",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
				}},
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1b",
				}},
			},
		},
		{
			testName:            "Duplicate topology entries with multiple allowed keys are deduplicated",
			allowedTopologyKeys: "topology.kubernetes.io/zone,topology.kubernetes.io/region",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":        "node1",
						"topology.kubernetes.io/zone":   "us-east-1a",
						"topology.kubernetes.io/region": "us-east-1",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":        "node2",
						"topology.kubernetes.io/zone":   "us-east-1b",
						"topology.kubernetes.io/region": "us-east-1",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":        "node3",
						"topology.kubernetes.io/zone":   "us-east-1a",
						"topology.kubernetes.io/region": "us-east-1",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone":   "us-east-1a",
					"topology.kubernetes.io/region": "us-east-1",
				}},
				{Segments: map[string]string{
					"topology.kubernetes.io/zone":   "us-east-1b",
					"topology.kubernetes.io/region": "us-east-1",
				}},
			},
		},
		{
			testName:            "Duplicate topology entries after filtering are deduplicated",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node1",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node2",
						"topology.kubernetes.io/zone": "us-east-1b",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node3",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
				}},
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1b",
				}},
			},
		},
	} {
		t.Run(test.testName, func(t *testing.T) {
			var objs []runtime.Object
			if !test.skipSettingCreation {
				objs = append(objs, newSetting(string(types.SettingNameCSIAllowedTopologyKeys), test.allowedTopologyKeys))
			}
			cs := newTestControllerServer(t, objs...)

			result := cs.getAccessibleTopologyFromRequirements(context.TODO(), test.accessibilityReqs, false)

			if len(result) != len(test.expectedTopology) {
				t.Fatalf("expected %d topology entries, got %d", len(test.expectedTopology), len(result))
			}
			for i, expected := range test.expectedTopology {
				if len(expected.Segments) != len(result[i].Segments) {
					t.Errorf("topology[%d] segments count mismatch: expected %v, got %v", i, expected.Segments, result[i].Segments)
					continue
				}
				for key, expectedVal := range expected.Segments {
					if gotVal, ok := result[i].Segments[key]; !ok || gotVal != expectedVal {
						t.Errorf("topology[%d] segment %q: expected %q, got %q (exists=%v)", i, key, expectedVal, gotVal, ok)
					}
				}
			}
		})
	}
}

func TestGetAccessibleTopologyStrictTopology(t *testing.T) {

	for _, test := range []struct {
		testName            string
		allowedTopologyKeys string
		strictTopology      bool
		accessibilityReqs   *csi.TopologyRequirement
		expectedTopology    []*csi.Topology
	}{
		{
			testName:            "strictTopology pins to first Preferred element",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			strictTopology:      true,
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node1",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node2",
						"topology.kubernetes.io/zone": "us-east-1b",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node3",
						"topology.kubernetes.io/zone": "us-east-1c",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
				}},
			},
		},
		{
			testName:            "strictTopology disabled returns all elements",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			strictTopology:      false,
			accessibilityReqs: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node1",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node2",
						"topology.kubernetes.io/zone": "us-east-1b",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
				}},
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1b",
				}},
			},
		},
		{
			testName:            "strictTopology with Requisite only pins to first element",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			strictTopology:      true,
			accessibilityReqs: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node1",
						"topology.kubernetes.io/zone": "us-east-1a",
					}},
					{Segments: map[string]string{
						"kubernetes.io/hostname":      "node2",
						"topology.kubernetes.io/zone": "us-east-1b",
					}},
				},
			},
			expectedTopology: []*csi.Topology{
				{Segments: map[string]string{
					"topology.kubernetes.io/zone": "us-east-1a",
				}},
			},
		},
		{
			testName:            "strictTopology with nil requirements",
			allowedTopologyKeys: "topology.kubernetes.io/zone",
			strictTopology:      true,
			accessibilityReqs:   nil,
			expectedTopology:    nil,
		},
	} {
		t.Run(test.testName, func(t *testing.T) {
			cs := newTestControllerServer(t, newSetting(string(types.SettingNameCSIAllowedTopologyKeys), test.allowedTopologyKeys))

			result := cs.getAccessibleTopologyFromRequirements(context.TODO(), test.accessibilityReqs, test.strictTopology)

			if len(result) != len(test.expectedTopology) {
				t.Fatalf("expected %d topology entries, got %d", len(test.expectedTopology), len(result))
			}
			for i, expected := range test.expectedTopology {
				if len(expected.Segments) != len(result[i].Segments) {
					t.Errorf("topology[%d] segments count mismatch: expected %v, got %v", i, expected.Segments, result[i].Segments)
					continue
				}
				for key, expectedVal := range expected.Segments {
					if gotVal, ok := result[i].Segments[key]; !ok || gotVal != expectedVal {
						t.Errorf("topology[%d] segment %q: expected %q, got %q (exists=%v)", i, key, expectedVal, gotVal, ok)
					}
				}
			}
		})
	}
}
