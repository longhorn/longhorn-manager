package types

import (
	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestValidateECParameters(c *C) {
	type testCase struct {
		dataChunks   int
		parityChunks int
		stripSizeKB  int
		expectError  bool
	}
	testCases := map[string]testCase{
		"minimum data, parity and strip size": {
			dataChunks: 1, parityChunks: 1, stripSizeKB: 4, expectError: false,
		},
		"typical 4+2 with 64 KiB strip": {
			dataChunks: 4, parityChunks: 2, stripSizeKB: 64, expectError: false,
		},
		"sum equals the base bdev cap": {
			dataChunks: 30, parityChunks: 2, stripSizeKB: 128, expectError: false,
		},
		"maximum strip size": {
			dataChunks: 2, parityChunks: 1, stripSizeKB: 1024, expectError: false,
		},

		"dataChunks is zero": {
			dataChunks: 0, parityChunks: 1, stripSizeKB: 64, expectError: true,
		},
		"dataChunks is negative": {
			dataChunks: -1, parityChunks: 1, stripSizeKB: 64, expectError: true,
		},
		"parityChunks is zero": {
			dataChunks: 2, parityChunks: 0, stripSizeKB: 64, expectError: true,
		},
		"parityChunks is negative": {
			dataChunks: 2, parityChunks: -1, stripSizeKB: 64, expectError: true,
		},
		"sum exceeds the base bdev cap": {
			dataChunks: 31, parityChunks: 2, stripSizeKB: 64, expectError: true,
		},
		"strip size below the minimum": {
			dataChunks: 2, parityChunks: 1, stripSizeKB: 2, expectError: true,
		},
		"strip size is a power of two but above the maximum": {
			dataChunks: 2, parityChunks: 1, stripSizeKB: 2048, expectError: true,
		},
		"strip size in range but not a power of two": {
			dataChunks: 2, parityChunks: 1, stripSizeKB: 48, expectError: true,
		},
	}

	for name, tc := range testCases {
		err := ValidateECParameters(tc.dataChunks, tc.parityChunks, tc.stripSizeKB)
		if tc.expectError {
			c.Assert(err, NotNil, Commentf(TestErrResultFmt, name))
		} else {
			c.Assert(err, IsNil, Commentf(TestErrErrorFmt, name, err))
		}
	}
}
