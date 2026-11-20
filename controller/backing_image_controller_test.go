package controller

import (
	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *TestSuite) TestSyncBackingImageDataSourceCondition(c *C) {
	type testCase struct {
		name            string
		fileTransferred bool
		state           longhorn.BackingImageState
		message         string
		expectedStatus  longhorn.ConditionStatus
		expectedReason  string
		expectedMessage string
	}

	cases := []testCase{
		{
			name:            "failed download reports the data source message",
			state:           longhorn.BackingImageStateFailed,
			message:         "download failed",
			expectedStatus:  longhorn.ConditionStatusTrue,
			expectedReason:  longhorn.BackingImageConditionReasonDataSource,
			expectedMessage: "download failed",
		},
		{
			name:            "failed and cleaned up without a message reports the state",
			state:           longhorn.BackingImageStateFailedAndCleanUp,
			expectedStatus:  longhorn.ConditionStatusTrue,
			expectedReason:  longhorn.BackingImageConditionReasonDataSource,
			expectedMessage: "backing image data source is failed-and-cleanup",
		},
		{
			name:           "retry in progress clears the condition",
			state:          longhorn.BackingImageStateInProgress,
			expectedStatus: longhorn.ConditionStatusFalse,
		},
		{
			name:           "recreated pod with empty state clears the condition",
			state:          "",
			expectedStatus: longhorn.ConditionStatusFalse,
		},
		{
			name:            "transferred file clears the condition",
			fileTransferred: true,
			state:           longhorn.BackingImageStateFailed,
			expectedStatus:  longhorn.ConditionStatusFalse,
		},
	}

	for _, tc := range cases {
		bi := &longhorn.BackingImage{}
		bids := &longhorn.BackingImageDataSource{
			Spec: longhorn.BackingImageDataSourceSpec{FileTransferred: tc.fileTransferred},
			Status: longhorn.BackingImageDataSourceStatus{
				CurrentState: tc.state,
				Message:      tc.message,
			},
		}
		syncBackingImageDataSourceCondition(bi, bids)
		cond := types.GetCondition(bi.Status.Conditions, longhorn.BackingImageConditionTypeFailed)
		c.Assert(cond.Status, Equals, tc.expectedStatus, Commentf(tc.name))
		c.Assert(cond.Reason, Equals, tc.expectedReason, Commentf(tc.name))
		c.Assert(cond.Message, Equals, tc.expectedMessage, Commentf(tc.name))
	}
}

func (s *TestSuite) TestSyncBackingImageDataSourceConditionRecovers(c *C) {
	bi := &longhorn.BackingImage{}
	bids := &longhorn.BackingImageDataSource{
		Status: longhorn.BackingImageDataSourceStatus{
			CurrentState: longhorn.BackingImageStateFailed,
			Message:      "download failed",
		},
	}
	syncBackingImageDataSourceCondition(bi, bids)
	c.Assert(types.GetCondition(bi.Status.Conditions, longhorn.BackingImageConditionTypeFailed).Status, Equals, longhorn.ConditionStatusTrue)

	bids.Spec.FileTransferred = true
	syncBackingImageDataSourceCondition(bi, bids)
	c.Assert(types.GetCondition(bi.Status.Conditions, longhorn.BackingImageConditionTypeFailed).Status, Equals, longhorn.ConditionStatusFalse)
}
