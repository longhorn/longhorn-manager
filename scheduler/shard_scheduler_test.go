package scheduler

import (
	. "gopkg.in/check.v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-manager/util"
)

const (
	miB = int64(1) << 20
	giB = int64(1) << 30
)

func (s *TestSuite) TestComputeShardSize(c *C) {
	type testCase struct {
		volumeSize  int64
		k           int
		stripSizeKB int
	}
	testCases := map[string]testCase{
		"single shard holds the whole volume": {
			volumeSize: giB, k: 1, stripSizeKB: 64,
		},
		"even division across four shards": {
			volumeSize: 4 * giB, k: 4, stripSizeKB: 64,
		},
		"uneven division rounds each shard up": {
			volumeSize: giB + 1, k: 4, stripSizeKB: 64,
		},
		"small volume where the reservation dominates": {
			volumeSize: 10 * miB, k: 2, stripSizeKB: 64,
		},
		"minimum strip size": {
			volumeSize: 2 * giB, k: 3, stripSizeKB: 4,
		},
		"maximum strip size": {
			volumeSize: 8 * giB, k: 8, stripSizeKB: 1024,
		},
	}

	for name, tc := range testCases {
		got := ComputeShardSize(tc.volumeSize, tc.k, tc.stripSizeKB)

		reservation := int64(spdktypes.EcFrontReservationBytes(uint32(tc.stripSizeKB)))
		perDiskUser := (tc.volumeSize + int64(tc.k) - 1) / int64(tc.k) // ceil(volumeSize/k)
		want := util.RoundUpSize(perDiskUser + reservation)

		c.Assert(got, Equals, want, Commentf("case %q: unexpected shard size", name))
		// Every shard lvol must be 2 MiB aligned so SPDK accepts it on create and expand.
		c.Assert(got%util.SizeAlignment, Equals, int64(0), Commentf("case %q: shard size not 2 MiB aligned", name))
		// The k shards' user regions together must cover the whole volume. A floor
		// instead of a ceil division would leave the tail of the volume uncovered.
		c.Assert((got-reservation)*int64(tc.k) >= tc.volumeSize, Equals, true, Commentf("case %q: shards do not cover the volume", name))
	}

	// k <= 0 cannot divide the volume; the guard returns the size unchanged.
	c.Assert(ComputeShardSize(giB, 0, 64), Equals, giB)
	c.Assert(ComputeShardSize(giB, -1, 64), Equals, giB)
}
