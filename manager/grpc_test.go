package manager

import (
	. "gopkg.in/check.v1"
)

const (
	GRPCAddress1 = ":50001"
	GRPCAddress2 = ":50002"
	GRPCAddress3 = ":50003"
)

var (
	event = Event{
		Type:       EventTypeNotify,
		VolumeName: "test",
	}
)

func (s *TestSuite) callback(c *C, ch chan Event) {
	for e := range ch {
		c.Assert(e.Type, Equals, event.Type)
		c.Assert(e.VolumeName, Equals, event.VolumeName)
	}
}

func (s *TestSuite) TestGRPC(c *C) {
	var err error
	grpcMgr1 := NewGRPCManager()
	callbackChan1 := make(chan Event)
	grpcMgr1.SetCallbackChan(callbackChan1)
	err = grpcMgr1.StartServer(GRPCAddress1)
	c.Assert(err, IsNil)
	go s.callback(c, callbackChan1)

	grpcMgr2 := NewGRPCManager()
	callbackChan2 := make(chan Event)
	grpcMgr2.SetCallbackChan(callbackChan2)
	err = grpcMgr2.StartServer(GRPCAddress2)
	c.Assert(err, IsNil)
	go s.callback(c, callbackChan2)

	grpcMgr3 := NewGRPCManager()
	callbackChan3 := make(chan Event)
	grpcMgr3.SetCallbackChan(callbackChan3)
	err = grpcMgr3.StartServer(GRPCAddress3)
	c.Assert(err, IsNil)
	go s.callback(c, callbackChan3)

	err = grpcMgr1.NodeNotify(GRPCAddress1, &event)
	c.Assert(err, IsNil)

	err = grpcMgr2.NodeNotify(GRPCAddress2, &event)
	c.Assert(err, IsNil)

	err = grpcMgr3.NodeNotify(GRPCAddress3, &event)
	c.Assert(err, IsNil)

	err = grpcMgr1.NodeNotify(GRPCAddress2, &event)
	c.Assert(err, IsNil)

	err = grpcMgr2.NodeNotify(GRPCAddress1, &event)
	c.Assert(err, IsNil)

	err = grpcMgr1.NodeNotify(GRPCAddress3, &event)
	c.Assert(err, IsNil)

	err = grpcMgr3.NodeNotify(GRPCAddress1, &event)
	c.Assert(err, IsNil)

	err = grpcMgr2.NodeNotify(GRPCAddress3, &event)
	c.Assert(err, IsNil)

	err = grpcMgr3.NodeNotify(GRPCAddress2, &event)
	c.Assert(err, IsNil)
}
