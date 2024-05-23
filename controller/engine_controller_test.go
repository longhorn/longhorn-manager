package controller

import (
	"fmt"
	"testing"

	"github.com/longhorn/longhorn-manager/util"
	"github.com/stretchr/testify/require"
)

func TestTruncateSnapshotSize(t *testing.T) {
	assert := require.New(t)

	type testCase struct {
		nominalSize               int64
		sizeBase                  int64
		sizeSmallerThanTruncation int64
		sizeLargerThanTruncation  int64
	}
	tests := map[string]testCase{}

	// We only detect changes of 1 MiB for 1 GiB volumes.
	tc := testCase{}
	tc.nominalSize = 1 * util.GiB
	tc.sizeBase = 500 * util.MiB
	tc.sizeSmallerThanTruncation = tc.sizeBase + 500*util.KiB
	tc.sizeLargerThanTruncation = tc.sizeBase + 1*util.MiB
	tests["1 GB nominal size"] = tc

	// We only detect changes of 10 MiB for 10 GiB volumes.
	tc = testCase{}
	tc.nominalSize = 10 * util.GiB
	tc.sizeBase = 5 * util.GiB
	tc.sizeSmallerThanTruncation = tc.sizeBase + 5*util.MiB
	tc.sizeLargerThanTruncation = tc.sizeBase + 10*util.MiB
	tests["10 GB nominal size"] = tc

	// We only detect changes of 100 MiB for 100 GiB volumes.
	tc = testCase{}
	tc.nominalSize = 100 * util.GiB
	tc.sizeBase = 50 * util.GiB
	tc.sizeSmallerThanTruncation = tc.sizeBase + 50*util.MiB
	tc.sizeLargerThanTruncation = tc.sizeBase + 100*util.MiB
	tests["100 GB nominal size"] = tc

	// We only detect changes of 100 MiB for > 100 GiB volumes.
	tc = testCase{}
	tc.nominalSize = 1 * util.TiB
	tc.sizeBase = 500 * util.GiB
	tc.sizeSmallerThanTruncation = tc.sizeBase + 50*util.MiB
	tc.sizeLargerThanTruncation = tc.sizeBase + 100*util.MiB
	tests["> 100 GB nominal size"] = tc

	for name, tc := range tests {
		fmt.Printf("testing %v\n", name)
		assert.Equal(tc.sizeBase, truncateSnapshotSize(tc.sizeSmallerThanTruncation, tc.nominalSize))
		assert.Less(tc.sizeBase, truncateSnapshotSize(tc.sizeLargerThanTruncation, tc.nominalSize))
	}

	// Really just ensure there is no panic. We don't expect any truncation.
	fmt.Printf("testing 0 GB nominal size\n")
	assert.Equal(truncateSnapshotSize(5*util.GiB, 0), int64(5*util.GiB))
	assert.Equal(truncateSnapshotSize(5*util.TiB, 0), int64(5*util.TiB))
}
