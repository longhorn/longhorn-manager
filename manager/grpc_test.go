package manager

import (
	. "gopkg.in/check.v1"
)

const (
	GRPCPort1 = 50001
	GRPCPort2 = 50002
	GRPCPort3 = 50003
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
	grpcMgr1 := NewGRPCManager("", GRPCPort1)
	callbackChan1 := make(chan Event)
	err = grpcMgr1.Start(callbackChan1)
	c.Assert(err, IsNil)
	go s.callback(c, callbackChan1)

	grpcMgr2 := NewGRPCManager("", GRPCPort2)
	callbackChan2 := make(chan Event)
	err = grpcMgr2.Start(callbackChan2)
	c.Assert(err, IsNil)
	go s.callback(c, callbackChan2)

	grpcMgr3 := NewGRPCManager("", GRPCPort3)
	callbackChan3 := make(chan Event)
	err = grpcMgr3.Start(callbackChan3)
	c.Assert(err, IsNil)
	go s.callback(c, callbackChan3)

	err = grpcMgr1.NodeNotify(grpcMgr1.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr2.NodeNotify(grpcMgr2.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr3.NodeNotify(grpcMgr3.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr1.NodeNotify(grpcMgr2.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr2.NodeNotify(grpcMgr1.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr1.NodeNotify(grpcMgr3.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr3.NodeNotify(grpcMgr1.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr2.NodeNotify(grpcMgr3.GetAddress(), &event)
	c.Assert(err, IsNil)

	err = grpcMgr3.NodeNotify(grpcMgr2.GetAddress(), &event)
	c.Assert(err, IsNil)
}
