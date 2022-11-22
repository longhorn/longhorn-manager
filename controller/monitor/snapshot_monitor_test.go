package monitor

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type testset struct {
	hashStatus       map[string]*longhorn.HashStatus
	existingChecksum string

	expectedChecksum string
	expectedErr      error
}

func TestDetermineChecksumFromHashStatus(t *testing.T) {
	assert := require.New(t)

	snapshotName := "snap-01"

	testsets := []testset{
		// 1 replica
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		// The snapshot is somehow changed
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "cde",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "cde",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since snapshot disk files are silently corrupted", snapshotName),
		},

		// 2 replicas
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since the existing checksum is not found in the checksums from replicas", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since there is no existing checksum", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "abc",
					Error:             "",
					SilentlyCorrupted: false,
				},
				"addr-02": {
					State:             "Completed",
					Checksum:          "def",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "def",
					Error:             "",
					SilentlyCorrupted: false,
				},
				"addr-02": {
					State:             "Completed",
					Checksum:          "ghi",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "def",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "def",
					Error:             "",
					SilentlyCorrupted: true,
				},
				"addr-02": {
					State:             "Completed",
					Checksum:          "ghi",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since snapshot disk files are silently corrupted", snapshotName),
		},

		// 3 replicas
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "fgh",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "fgh",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since there is no existing checksum", snapshotName),
		},

		// 5 replicas
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-04": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
				"addr-05": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since there is no existing checksum", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-04": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
				"addr-05": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"from the existing checksum and the checksums from replicas", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-04": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
				"addr-05": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "cde",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},
	}

	log := logrus.StandardLogger().WithField("node", "unknown")

	for _, t := range testsets {
		finalChecksum, err := determineChecksumFromHashStatus(log, snapshotName, t.existingChecksum, t.hashStatus)
		assert.Equal(finalChecksum, t.expectedChecksum)
		if t.expectedErr == nil {
			assert.Nil(err)
		} else {
			assert.Equal(err.Error(), t.expectedErr.Error())
		}
	}
}
