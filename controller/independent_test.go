package controller

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestBackingImageCleanup(c *C) {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestNode1,
			Namespace: TestNamespace,
		},
		Spec: types.NodeSpec{
			AllowScheduling: true,
			Disks: map[string]types.DiskSpec{
				TestDiskID1: {
					Path:            TestDefaultDataPath + "1",
					AllowScheduling: true,
				},
				TestDiskID2: {
					Path:            TestDefaultDataPath + "2",
					AllowScheduling: true,
				},
				TestDiskID3: {
					Path:            TestDefaultDataPath + "3",
					AllowScheduling: true,
				},
			},
		},
		Status: types.NodeStatus{
			Conditions: map[string]types.Condition{
				types.NodeConditionTypeSchedulable: newNodeCondition(types.NodeConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeReady:       newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]*types.DiskStatus{
				TestDiskID1: {
					Conditions: map[string]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
						types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
					},
					DiskUUID: TestDiskID1,
				},
				TestDiskID2: {
					Conditions: map[string]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
						types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
					},
					DiskUUID: TestDiskID2,
				},
				TestDiskID3: {
					Conditions: map[string]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
						types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
					},
					DiskUUID: TestDiskID3,
				},
			},
		},
	}
	biTemplate := &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: types.BackingImageSpec{
			Disks: map[string]struct{}{
				TestDiskID1: {},
				TestDiskID2: {},
				TestDiskID3: {},
			},
		},
		Status: types.BackingImageStatus{
			DiskFileStatusMap: map[string]*types.BackingImageDiskFileStatus{
				TestDiskID1: {State: types.BackingImageStateReady},
				TestDiskID2: {State: types.BackingImageStateReady},
				TestDiskID3: {State: types.BackingImageStateReady},
			},
			DiskLastRefAtMap: map[string]string{
				TestDiskID3: util.Now(),
			},
		},
	}
	bidsTemplate := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: types.BackingImageDataSourceSpec{
			NodeID:          TestNode1,
			DiskUUID:        TestDiskID1,
			DiskPath:        TestDefaultDataPath,
			FileTransferred: true,
		},
		Status: types.BackingImageDataSourceStatus{},
	}

	// Test case 1: clean up unused ready disk file when there are enough ready files
	bi := biTemplate.DeepCopy()
	expectedBI := bi.DeepCopy()
	expectedBI.Spec.Disks = map[string]struct{}{
		TestDiskID1: {},
		TestDiskID2: {},
	}
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 2)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 2: cannot delete the unused ready disk file if there are no enough ready files
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{
		TestDiskID1: {State: types.BackingImageStatePending},
		TestDiskID2: {State: types.BackingImageStateInProgress},
		TestDiskID3: {State: types.BackingImageStateReady},
	}
	expectedBI = bi.DeepCopy()
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 1)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 2: clean up all unused files when there are enough ready files
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{
		TestDiskID1: {State: types.BackingImageStatePending},
		TestDiskID2: {State: types.BackingImageStateInProgress},
		TestDiskID3: {State: types.BackingImageStateReady},
	}
	bi.Status.DiskLastRefAtMap = map[string]string{
		TestDiskID1: util.Now(),
		TestDiskID2: util.Now(),
	}
	expectedBI = bi.DeepCopy()
	expectedBI.Spec.Disks = map[string]struct{}{
		TestDiskID3: {},
	}
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 1)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 3: retain (some) unused handling files if there are no enough ready files.
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{
		TestDiskID1: {State: types.BackingImageStateFailed},
		TestDiskID2: {},
		TestDiskID3: {State: types.BackingImageStateReady},
	}
	bi.Status.DiskLastRefAtMap = map[string]string{
		TestDiskID1: util.Now(),
		TestDiskID2: util.Now(),
	}
	expectedBI = bi.DeepCopy()
	expectedBI.Spec.Disks = map[string]struct{}{
		TestDiskID2: {},
		TestDiskID3: {},
	}
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 2)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 3: retain all files if there are no enough files.
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{
		TestDiskID1: {State: types.BackingImageStateFailed},
		TestDiskID2: {},
		TestDiskID3: {State: types.BackingImageStateReady},
	}
	bi.Status.DiskLastRefAtMap = map[string]string{
		TestDiskID1: util.Now(),
		TestDiskID2: util.Now(),
		TestDiskID3: util.Now(),
	}
	expectedBI = bi.DeepCopy()
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 3)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 4: retain the 1st file anyway
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*types.BackingImageDiskFileStatus{
		TestDiskID1: {},
	}
	bi.Status.DiskLastRefAtMap = map[string]string{
		TestDiskID1: util.Now(),
	}
	expectedBI = bi.DeepCopy()
	bids := bidsTemplate.DeepCopy()
	bids.Spec.FileTransferred = false
	BackingImageDiskFileCleanup(node, bi, bids, time.Duration(0), 0)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)
}
