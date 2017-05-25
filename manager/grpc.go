package manager

import (
	"fmt"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/yasker/lm-rewrite/manager/pb"
)

type GRPCManager struct {
	callbackChan chan Event
}

func NewGRPCManager() RPCManager {
	return &GRPCManager{}
}

func (r *GRPCManager) NodeNotifyRPC(ctx context.Context, req *pb.NodeNotifyRequest) (*pb.NodeNotifyResponse, error) {
	r.callbackChan <- Event{
		Type:       EventType(req.Event),
		VolumeName: req.VolumeName,
	}
	return &pb.NodeNotifyResponse{
		Result: "success",
	}, nil
}

func (r *GRPCManager) StartServer(address string, callbackChan chan Event) (err error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrap(err, "fail to start GRPC server")
	}
	r.callbackChan = callbackChan

	s := grpc.NewServer()
	pb.RegisterManagerServer(s, r)
	reflection.Register(s)
	go func() {
		if err := s.Serve(l); err != nil {
			logrus.Errorf("fail to serve GRPC server: %v", err)
		}
	}()
	return nil
}

func (r *GRPCManager) NodeNotify(address string, event *Event) error {
	//FIXME insecure
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "fail to connect to %v", address)
	}
	defer conn.Close()
	c := pb.NewManagerClient(conn)

	resp, err := c.NodeNotifyRPC(context.Background(), &pb.NodeNotifyRequest{
		Event:      string(event.Type),
		VolumeName: event.VolumeName,
	})
	if err != nil {
		return errors.Wrapf(err, "fail to notify %v", address)
	}
	if resp.Result != "success" {
		return fmt.Errorf("fail to notify %v: %+v", address, resp)
	}
	return nil
}
