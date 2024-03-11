package controller

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestBackingImageCleanup(c *C) {
	node := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestNode1,
			Namespace: TestNamespace,
		},
		Spec: longhorn.NodeSpec{
			AllowScheduling: true,
			Disks: map[string]longhorn.DiskSpec{
				TestDiskID1: {
					Type:            longhorn.DiskTypeFilesystem,
					Path:            TestDefaultDataPath + "1",
					DiskDriver:      longhorn.DiskDriverNone,
					AllowScheduling: true,
				},
				TestDiskID2: {
					Type:            longhorn.DiskTypeFilesystem,
					Path:            TestDefaultDataPath + "2",
					DiskDriver:      longhorn.DiskDriverNone,
					AllowScheduling: true,
				},
				TestDiskID3: {
					Type:            longhorn.DiskTypeFilesystem,
					Path:            TestDefaultDataPath + "3",
					DiskDriver:      longhorn.DiskDriverNone,
					AllowScheduling: true,
				},
			},
		},
		Status: longhorn.NodeStatus{
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.NodeConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
				newNodeCondition(longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]*longhorn.DiskStatus{
				TestDiskID1: {
					Conditions: []longhorn.Condition{
						newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
						newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					},
					DiskUUID: TestDiskID1,
					Type:     longhorn.DiskTypeFilesystem,
				},
				TestDiskID2: {
					Conditions: []longhorn.Condition{
						newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
						newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					},
					DiskUUID: TestDiskID2,
					Type:     longhorn.DiskTypeFilesystem,
				},
				TestDiskID3: {
					Conditions: []longhorn.Condition{
						newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
						newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					},
					DiskUUID: TestDiskID3,
					Type:     longhorn.DiskTypeFilesystem,
				},
			},
		},
	}
	biTemplate := &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageSpec{
			Disks: map[string]string{
				TestDiskID1: "",
				TestDiskID2: "",
				TestDiskID3: "",
			},
		},
		Status: longhorn.BackingImageStatus{
			DiskFileStatusMap: map[string]*longhorn.BackingImageDiskFileStatus{
				TestDiskID1: {State: longhorn.BackingImageStateReady},
				TestDiskID2: {State: longhorn.BackingImageStateReady},
				TestDiskID3: {State: longhorn.BackingImageStateReady},
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
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:          TestNode1,
			DiskUUID:        TestDiskID1,
			DiskPath:        TestDefaultDataPath,
			FileTransferred: true,
		},
		Status: longhorn.BackingImageDataSourceStatus{},
	}

	// Test case 1: clean up unused ready disk file when there are enough ready files
	bi := biTemplate.DeepCopy()
	expectedBI := bi.DeepCopy()
	expectedBI.Spec.Disks = map[string]string{
		TestDiskID1: "",
		TestDiskID2: "",
	}
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 2)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 2: cannot delete the unused ready disk file if there are no enough ready files
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStatePending},
		TestDiskID2: {State: longhorn.BackingImageStateInProgress},
		TestDiskID3: {State: longhorn.BackingImageStateReady},
	}
	expectedBI = bi.DeepCopy()
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 1)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 2: clean up all unused files when there are enough ready files
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStatePending},
		TestDiskID2: {State: longhorn.BackingImageStateInProgress},
		TestDiskID3: {State: longhorn.BackingImageStateReady},
	}
	bi.Status.DiskLastRefAtMap = map[string]string{
		TestDiskID1: util.Now(),
		TestDiskID2: util.Now(),
	}
	expectedBI = bi.DeepCopy()
	expectedBI.Spec.Disks = map[string]string{
		TestDiskID3: "",
	}
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 1)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 3: retain (some) unused handling files if there are no enough ready files.
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStateFailed},
		TestDiskID2: {},
		TestDiskID3: {State: longhorn.BackingImageStateReady},
	}
	bi.Status.DiskLastRefAtMap = map[string]string{
		TestDiskID1: util.Now(),
		TestDiskID2: util.Now(),
	}
	expectedBI = bi.DeepCopy()
	expectedBI.Spec.Disks = map[string]string{
		TestDiskID2: "",
		TestDiskID3: "",
	}
	BackingImageDiskFileCleanup(node, bi, bidsTemplate, time.Duration(0), 2)
	c.Assert(bi.Spec.Disks, DeepEquals, expectedBI.Spec.Disks)

	// Test case 3: retain all files if there are no enough files.
	bi = biTemplate.DeepCopy()
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStateFailed},
		TestDiskID2: {},
		TestDiskID3: {State: longhorn.BackingImageStateReady},
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
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
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
