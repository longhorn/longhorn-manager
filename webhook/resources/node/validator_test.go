package node

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestValidateNodeDiskPathsDuplicate(t *testing.T) {
	disks := map[string]longhorn.DiskSpec{
		"disk-1": {Path: "/fake/path/disk1"},
		"disk-2": {Path: "/fake/path/disk1"},
	}

	err := validateNodeDiskPaths("node1", disks)
	assert.Error(t, err)

	assert.Contains(t, err.Error(), "duplicate disk paths")
	assert.Contains(t, err.Error(), "node1")
	assert.Contains(t, err.Error(), "disk-1")
	assert.Contains(t, err.Error(), "disk-2")
	assert.Contains(t, err.Error(), "/fake/path/disk1")
}

func TestValidateNodeDiskPathsUnique(t *testing.T) {
	disks := map[string]longhorn.DiskSpec{
		"disk-1": {Path: "/fake/path/disk1"},
		"disk-2": {Path: "/fake/path/disk2"},
	}

	err := validateNodeDiskPaths("node1", disks)
	assert.NoError(t, err)
}

func TestValidateNodeDiskPathsNormalizedDuplicate(t *testing.T) {
	disks := map[string]longhorn.DiskSpec{
		"disk-1": {Path: "/fake/path/disk1"},
		"disk-2": {Path: "/fake/path/disk1"},
	}

	err := validateNodeDiskPaths("node1", disks)
	assert.Error(t, err)

	assert.Contains(t, err.Error(), "duplicate disk paths")
	assert.Contains(t, err.Error(), "node1")
	assert.Contains(t, err.Error(), "/fake/path/disk1")
}

func TestFilepathCleanWithBDF(t *testing.T) {
	input := "00:1f.3"
	cleaned := filepath.Clean(input)
	assert.Equal(t, "00:1f.3", cleaned, "filepath.Clean should not alter BDF paths")
}

func TestValidateDiskBlockSize(t *testing.T) {
	testCases := map[string]struct {
		disk        longhorn.DiskSpec
		expectError bool
	}{
		"unset filesystem block size": {
			disk: longhorn.DiskSpec{Type: longhorn.DiskTypeFilesystem},
		},
		"unset block disk block size": {
			disk: longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverAuto},
		},
		"512 byte AIO block size": {
			disk: longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverAio, BlockSize: 512},
		},
		"4096 byte block size with auto driver": {
			disk: longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverAuto, BlockSize: 4096},
		},
		"4096 byte block size before driver defaulting": {
			disk: longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverNone, BlockSize: 4096},
		},
		"filesystem with explicit block size": {
			disk:        longhorn.DiskSpec{Type: longhorn.DiskTypeFilesystem, BlockSize: 4096},
			expectError: true,
		},
		"NVMe driver with explicit block size": {
			disk:        longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverNvme, BlockSize: 4096},
			expectError: true,
		},
		"1024 byte AIO block size": {
			disk:        longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverAio, BlockSize: 1024},
			expectError: true,
		},
		"auto driver with PCI address": {
			disk:        longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, Path: "0000:00:1f.3", DiskDriver: longhorn.DiskDriverAuto, BlockSize: 4096},
			expectError: true,
		},
		"auto driver with stable device path": {
			disk: longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, Path: "/dev/disk/by-path/pci-0000:00:1f.3-nvme-1", DiskDriver: longhorn.DiskDriverAuto, BlockSize: 4096},
		},
		"non-power-of-two block size": {
			disk:        longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverAio, BlockSize: 513},
			expectError: true,
		},
		"negative block size": {
			disk:        longhorn.DiskSpec{Type: longhorn.DiskTypeBlock, DiskDriver: longhorn.DiskDriverAio, BlockSize: -1},
			expectError: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateDiskBlockSize("disk-1", testCase.disk)
			if testCase.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestValidateDiskBlockSizeUpdate(t *testing.T) {
	testCases := map[string]struct {
		oldBlockSize    int64
		newBlockSize    int64
		diskUUID        string
		actualBlockSize int64
		readyStatus     longhorn.ConditionStatus
		expectError     bool
	}{
		"unset and 512 are equivalent": {
			oldBlockSize:    0,
			newBlockSize:    512,
			diskUUID:        "disk-uuid",
			actualBlockSize: 512,
		},
		"512 and unset are equivalent": {
			oldBlockSize:    512,
			newBlockSize:    0,
			diskUUID:        "disk-uuid",
			actualBlockSize: 512,
		},
		"legacy 4096 byte disk can record its actual block size": {
			oldBlockSize:    0,
			newBlockSize:    4096,
			diskUUID:        "disk-uuid",
			actualBlockSize: 4096,
		},
		"initialized disk block size is immutable": {
			oldBlockSize:    512,
			newBlockSize:    4096,
			diskUUID:        "disk-uuid",
			actualBlockSize: 512,
			expectError:     true,
		},
		"legacy initialized not-ready disk with unknown actual block size can be asserted": {
			oldBlockSize: 0,
			newBlockSize: 4096,
			diskUUID:     "disk-uuid",
			readyStatus:  longhorn.ConditionStatusFalse,
		},
		"legacy initialized ready disk with unknown actual block size cannot be asserted": {
			oldBlockSize: 0,
			newBlockSize: 4096,
			diskUUID:     "disk-uuid",
			readyStatus:  longhorn.ConditionStatusTrue,
			expectError:  true,
		},
		"explicit initialized disk with unknown actual block size cannot change": {
			oldBlockSize: 512,
			newBlockSize: 4096,
			diskUUID:     "disk-uuid",
			expectError:  true,
		},
		"uninitialized disk block size may change": {
			oldBlockSize: 512,
			newBlockSize: 4096,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			oldNode := &longhorn.Node{
				Spec: longhorn.NodeSpec{
					Disks: map[string]longhorn.DiskSpec{
						"disk-1": {
							Type:      longhorn.DiskTypeBlock,
							Path:      "/dev/nvme0n1",
							BlockSize: testCase.oldBlockSize,
						},
					},
				},
				Status: longhorn.NodeStatus{
					DiskStatus: map[string]*longhorn.DiskStatus{
						"disk-1": {
							DiskUUID:        testCase.diskUUID,
							ActualBlockSize: testCase.actualBlockSize,
							Conditions: []longhorn.Condition{
								{
									Type:   longhorn.DiskConditionTypeReady,
									Status: testCase.readyStatus,
								},
							},
						},
					},
				},
			}
			oldNode.Name = "node-1"

			newNode := oldNode.DeepCopy()
			newDisk := newNode.Spec.Disks["disk-1"]
			newDisk.BlockSize = testCase.newBlockSize
			newNode.Spec.Disks["disk-1"] = newDisk

			err := validateDiskBlockSizeUpdate(oldNode, newNode)
			if testCase.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
