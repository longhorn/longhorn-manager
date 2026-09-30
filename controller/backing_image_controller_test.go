package controller

import (
	. "gopkg.in/check.v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *TestSuite) TestShouldAdoptBackingImageDataSourceRecovery(c *C) {
	type testCase struct {
		name     string
		bids     *longhorn.BackingImageDataSource
		expected bool
	}

	cases := []testCase{
		{
			name: "adopt download recovery after a completed transfer",
			bids: &longhorn.BackingImageDataSource{
				Spec: longhorn.BackingImageDataSourceSpec{
					SourceType:        longhorn.BackingImageDataSourceTypeDownload,
					FileTransferred:   false,
					RecoveryRequested: false,
				},
				Status: longhorn.BackingImageDataSourceStatus{
					FileEverTransferred: true,
				},
			},
			expected: true,
		},
		{
			name: "do not adopt a first prepare",
			bids: &longhorn.BackingImageDataSource{
				Spec: longhorn.BackingImageDataSourceSpec{
					SourceType:      longhorn.BackingImageDataSourceTypeDownload,
					FileTransferred: false,
				},
			},
			expected: false,
		},
		{
			name: "do not adopt when already transferred",
			bids: &longhorn.BackingImageDataSource{
				Spec: longhorn.BackingImageDataSourceSpec{
					SourceType:      longhorn.BackingImageDataSourceTypeDownload,
					FileTransferred: true,
				},
				Status: longhorn.BackingImageDataSourceStatus{
					FileEverTransferred: true,
				},
			},
			expected: false,
		},
		{
			name: "do not adopt when recovery is already requested",
			bids: &longhorn.BackingImageDataSource{
				Spec: longhorn.BackingImageDataSourceSpec{
					SourceType:        longhorn.BackingImageDataSourceTypeDownload,
					RecoveryRequested: true,
				},
				Status: longhorn.BackingImageDataSourceStatus{
					FileEverTransferred: true,
				},
			},
			expected: false,
		},
		{
			name: "do not adopt non-download sources",
			bids: &longhorn.BackingImageDataSource{
				Spec: longhorn.BackingImageDataSourceSpec{
					SourceType: longhorn.BackingImageDataSourceTypeUpload,
				},
				Status: longhorn.BackingImageDataSourceStatus{
					FileEverTransferred: true,
				},
			},
			expected: false,
		},
	}

	for _, tc := range cases {
		c.Assert(shouldAdoptBackingImageDataSourceRecovery(tc.bids), Equals, tc.expected, Commentf(tc.name))
	}
}
