package controller

import (
	. "gopkg.in/check.v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *TestSuite) TestSyncBackingImageDataSourceFileEverTransferred(c *C) {
	bids := &longhorn.BackingImageDataSource{}
	syncBackingImageDataSourceFileEverTransferred(bids)
	c.Assert(bids.Status.FileEverTransferred, Equals, false)

	bids.Spec.FileTransferred = true
	syncBackingImageDataSourceFileEverTransferred(bids)
	c.Assert(bids.Status.FileEverTransferred, Equals, true)

	bids.Spec.FileTransferred = false
	syncBackingImageDataSourceFileEverTransferred(bids)
	c.Assert(bids.Status.FileEverTransferred, Equals, true)
}
