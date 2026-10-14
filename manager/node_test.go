package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestMergeOmittedDiskBlockSizes(t *testing.T) {
	testCases := map[string]struct {
		currentBlockSize int64
		updatedBlockSize int64
		blockSizePresent bool
		diskUUID         string
		expected         int64
	}{
		"older client omission preserves initialized disk": {
			currentBlockSize: 4096,
			diskUUID:         "disk-uuid",
			expected:         4096,
		},
		"older client omission preserves uninitialized disk": {
			currentBlockSize: 4096,
			expected:         4096,
		},
		"explicit zero may clear uninitialized requested size": {
			currentBlockSize: 4096,
			blockSizePresent: true,
			expected:         0,
		},
		"explicit update is preserved": {
			currentBlockSize: 512,
			updatedBlockSize: 4096,
			blockSizePresent: true,
			expected:         4096,
		},
		"omission keeps zero default": {
			expected: 0,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			node := &longhorn.Node{
				Spec: longhorn.NodeSpec{
					Disks: map[string]longhorn.DiskSpec{
						"disk-1": {Type: longhorn.DiskTypeBlock, BlockSize: testCase.currentBlockSize},
					},
				},
				Status: longhorn.NodeStatus{
					DiskStatus: map[string]*longhorn.DiskStatus{
						"disk-1": {DiskUUID: testCase.diskUUID},
					},
				},
			}
			updates := map[string]longhorn.DiskSpec{
				"disk-1": {BlockSize: testCase.updatedBlockSize},
			}
			blockSizePresent := map[string]bool{
				"disk-1": testCase.blockSizePresent,
			}

			merged := mergeOmittedDiskBlockSizes(node, updates, blockSizePresent)

			assert.Equal(t, testCase.expected, merged["disk-1"].BlockSize)
			assert.Equal(t, testCase.updatedBlockSize, updates["disk-1"].BlockSize, "merge must not mutate retry input")
		})
	}
}
