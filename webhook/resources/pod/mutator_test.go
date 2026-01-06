package pod

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type PodMutatorTestSuite struct{}

var _ = Suite(&PodMutatorTestSuite{})

func (s *PodMutatorTestSuite) TestCanFitVolumes(c *C) {
	testCases := map[string]struct {
		disks                      map[string]diskConfig
		volumes                    []int64
		overProvisioningPercentage int64
		expectedResult             bool
	}{
		"empty volumes should always fit": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"single volume fits on single disk": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{500},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"single volume too large for single disk": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{1500},
			overProvisioningPercentage: 100,
			expectedResult:             false,
		},
		"multiple volumes fit on single disk": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{300, 200, 400},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"multiple volumes exceed single disk max": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{600, 600},
			overProvisioningPercentage: 100,
			expectedResult:             false,
		},
		"volumes distribute across multiple disks": {
			disks: map[string]diskConfig{
				"disk-1": {max: 500, reserved: 0, scheduled: 0},
				"disk-2": {max: 500, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{400, 400},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"total fits but no valid assignment": {
			disks: map[string]diskConfig{
				"disk-1": {max: 100, reserved: 0, scheduled: 0},
				"disk-2": {max: 50, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{80, 70},
			overProvisioningPercentage: 100,
			expectedResult:             false,
		},
		"three volumes on three disks": {
			disks: map[string]diskConfig{
				"disk-1": {max: 100, reserved: 0, scheduled: 0},
				"disk-2": {max: 100, reserved: 0, scheduled: 0},
				"disk-3": {max: 100, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{90, 80, 70},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"reserved space reduces available max": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 200, scheduled: 0},
			},
			volumes:                    []int64{900},
			overProvisioningPercentage: 100,
			expectedResult:             false,
		},
		"already scheduled space reduces available max": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 400},
			},
			volumes:                    []int64{700},
			overProvisioningPercentage: 100,
			expectedResult:             false,
		},
		"200% over-provisioning allows more than physical max": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{1500},
			overProvisioningPercentage: 200,
			expectedResult:             true,
		},
		"50% over-provisioning limits max": {
			disks: map[string]diskConfig{
				"disk-1": {max: 1000, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{600},
			overProvisioningPercentage: 50,
			expectedResult:             false,
		},
		"multiple volumes on multiple disks": {
			disks: map[string]diskConfig{
				"disk-1": {max: 200, reserved: 20, scheduled: 50},
				"disk-2": {max: 300, reserved: 10, scheduled: 100},
				"disk-3": {max: 500, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{120, 180, 200},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"5 disks 4 volumes: requires optimal distribution": {
			disks: map[string]diskConfig{
				"disk-1": {max: 150, reserved: 0, scheduled: 0},
				"disk-2": {max: 200, reserved: 0, scheduled: 0},
				"disk-3": {max: 100, reserved: 0, scheduled: 0},
				"disk-4": {max: 180, reserved: 0, scheduled: 0},
				"disk-5": {max: 120, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{140, 190, 95, 170},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"5 disks 3 volumes: with mixed reserved and scheduled": {
			disks: map[string]diskConfig{
				"disk-1": {max: 500, reserved: 50, scheduled: 100},
				"disk-2": {max: 400, reserved: 40, scheduled: 150},
				"disk-3": {max: 300, reserved: 30, scheduled: 50},
				"disk-4": {max: 600, reserved: 60, scheduled: 200},
				"disk-5": {max: 450, reserved: 45, scheduled: 0},
			},
			volumes:                    []int64{300, 250, 350},
			overProvisioningPercentage: 100,
			expectedResult:             true,
		},
		"5 disks 4 volumes: total sufficient but no valid assignment": {
			disks: map[string]diskConfig{
				"disk-1": {max: 150, reserved: 0, scheduled: 0},
				"disk-2": {max: 110, reserved: 0, scheduled: 0},
				"disk-3": {max: 150, reserved: 0, scheduled: 0},
				"disk-4": {max: 110, reserved: 0, scheduled: 0},
				"disk-5": {max: 120, reserved: 0, scheduled: 0},
			},
			volumes:                    []int64{120, 120, 120, 120},
			overProvisioningPercentage: 100,
			expectedResult:             false,
		},
	}

	for name, tc := range testCases {
		c.Logf("Test case: %s", name)

		p := &podMutator{}
		node := newTestNodeWithDisks(tc.disks)
		volumes := newTestVolumes(tc.volumes)

		result := p.canFitVolumes(node, volumes, true, true, tc.overProvisioningPercentage)
		c.Assert(result, Equals, tc.expectedResult, Commentf("Test case: %s", name))
	}
}

type diskConfig struct {
	max       int64
	reserved  int64
	scheduled int64
}

func newTestNodeWithDisks(disks map[string]diskConfig) *longhorn.Node {
	diskSpecs := make(map[string]longhorn.DiskSpec)
	diskStatuses := make(map[string]*longhorn.DiskStatus)

	for diskName, config := range disks {
		diskSpecs[diskName] = longhorn.DiskSpec{
			StorageReserved: config.reserved,
			Tags:            []string{},
			AllowScheduling: true,
		}

		diskStatuses[diskName] = &longhorn.DiskStatus{
			StorageMaximum:   config.max,
			StorageScheduled: config.scheduled,
			Conditions: []longhorn.Condition{
				{
					Type:   longhorn.DiskConditionTypeReady,
					Status: longhorn.ConditionStatusTrue,
				},
				{
					Type:   longhorn.DiskConditionTypeSchedulable,
					Status: longhorn.ConditionStatusTrue,
				},
			},
		}
	}

	return &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
		Spec: longhorn.NodeSpec{
			AllowScheduling:   true,
			EvictionRequested: false,
			Disks:             diskSpecs,
			Tags:              []string{},
		},
		Status: longhorn.NodeStatus{
			Conditions: []longhorn.Condition{
				{
					Type:   longhorn.NodeConditionTypeReady,
					Status: longhorn.ConditionStatusTrue,
				},
				{
					Type:   longhorn.NodeConditionTypeSchedulable,
					Status: longhorn.ConditionStatusTrue,
				},
			},
			DiskStatus: diskStatuses,
		},
	}
}

func newTestVolumes(sizes []int64) []*longhorn.Volume {
	volumes := make([]*longhorn.Volume, len(sizes))
	for i, size := range sizes {
		volumes[i] = &longhorn.Volume{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("vol-%d", i),
			},
			Spec: longhorn.VolumeSpec{
				Size:         size,
				NodeSelector: []string{},
				DiskSelector: []string{},
			},
		}
	}
	return volumes
}
