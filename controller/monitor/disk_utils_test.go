package monitor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveDiskBlockSize(t *testing.T) {
	testCases := map[string]struct {
		blockSize       int64
		actualBlockSize int64
		initialized     bool
		expected        int64
		expectError     bool
	}{
		"unset uses default": {
			blockSize: 0,
			expected:  defaultBlockSize,
		},
		"explicit 512 is preserved": {
			blockSize: 512,
			expected:  512,
		},
		"explicit 4096 is preserved": {
			blockSize: 4096,
			expected:  4096,
		},
		"initialized legacy disk uses actual size": {
			actualBlockSize: 4096,
			initialized:     true,
			expected:        4096,
		},
		"matching explicit and actual size is preserved": {
			blockSize:       4096,
			actualBlockSize: 4096,
			initialized:     true,
			expected:        4096,
		},
		"explicit and actual size mismatch fails closed": {
			blockSize:       512,
			actualBlockSize: 4096,
			initialized:     true,
			expectError:     true,
		},
		"mismatch fails closed before UUID status is recorded": {
			blockSize:       4096,
			actualBlockSize: 512,
			expectError:     true,
		},
		"initialized disk without an observed size is not guessed": {
			initialized: true,
			expectError: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := resolveDiskBlockSize(testCase.blockSize, testCase.actualBlockSize, testCase.initialized)
			if testCase.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}
