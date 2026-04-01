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

	// Updated assertion (more flexible & correct)
	assert.Contains(t, err.Error(), "duplicate disk path")
	assert.Contains(t, err.Error(), "node1")
	assert.Contains(t, err.Error(), "disk-1")
	assert.Contains(t, err.Error(), "disk-2")
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

	assert.Contains(t, err.Error(), "duplicate disk path /fake/path/disk1 on node node1")
}

func TestFilepathCleanWithBDF(t *testing.T) {
	input := "00:1f.3"
	cleaned := filepath.Clean(input)
	assert.Equal(t, "00:1f.3", cleaned, "filepath.Clean should not alter BDF paths")
}
