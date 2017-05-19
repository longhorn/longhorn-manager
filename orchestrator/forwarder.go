package orchestrator

import (
	"fmt"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/yasker/lm-rewrite/orchestrator/pb"
	"github.com/yasker/lm-rewrite/types"
)

type OrchestratorForwarder struct {
	orch    Orchestrator
	locator NodeLocator
}

func NewOrchestratorForwarder(orch Orchestrator, locator NodeLocator) (Orchestrator, error) {
	return &OrchestratorForwarder{
		orch:    orch,
		locator: locator,
	}, nil
}

func (f *OrchestratorForwarder) StartServer(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrap(err, "fail to start orchestrator forwarder GRPC server")
	}
	s := grpc.NewServer()
	pb.RegisterForwarderServer(s, f)
	reflection.Register(s)
	go func() {
		if err := s.Serve(l); err != nil {
			logrus.Errorf("fail to serve orchestrator forwarder GRPC server: %v", err)
		}
	}()
	return nil
}

func (f *OrchestratorForwarder) InstanceOperationRPC(ctx context.Context, req *pb.InstanceOperationRequest) (*pb.InstanceOperationResponse, error) {
	var (
		instance *Instance
		err      error
		resp     *pb.InstanceOperationResponse
	)

	request := &Request{
		NodeID:       req.NodeID,
		InstanceID:   req.InstanceID,
		InstanceName: req.InstanceName,
		VolumeName:   req.VolumeName,
		VolumeSize:   req.VolumeSize,
		ReplicaURLs:  req.ReplicaURLs,
	}
	switch InstanceOperationType(req.Type) {
	case InstanceOperationTypeCreateController:
		instance, err = f.orch.CreateController(request)
	case InstanceOperationTypeCreateReplica:
		instance, err = f.orch.CreateReplica(request)
	case InstanceOperationTypeStartInstance:
		instance, err = f.orch.StartInstance(request)
	case InstanceOperationTypeStopInstance:
		instance, err = f.orch.StopInstance(request)
	case InstanceOperationTypeDeleteInstance:
		err = f.orch.DeleteInstance(request)
	case InstanceOperationTypeInspectInstance:
		instance, err = f.orch.InspectInstance(request)
	default:
		err = fmt.Errorf("invalid instance operation type", req.Type)
	}
	if err != nil {
		return nil, err
	}
	if instance != nil {
		resp = &pb.InstanceOperationResponse{
			InstanceID:   instance.ID,
			InstanceName: instance.Name,
			Running:      instance.Running,
			Address:      instance.Address,
		}
	}
	return resp, nil
}

func (f *OrchestratorForwarder) CreateController(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.CreateController(request)
	}
	address, err := f.locator.Node2Address(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create controller")
	}
	return f.InstanceOperation(address, InstanceOperationTypeCreateController, request)
}

func (f *OrchestratorForwarder) CreateReplica(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.CreateReplica(request)
	}
	address, err := f.locator.Node2Address(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create replica")
	}
	return f.InstanceOperation(address, InstanceOperationTypeCreateReplica, request)
}

func (f *OrchestratorForwarder) StartInstance(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.StartInstance(request)
	}
	address, err := f.locator.Node2Address(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start instance")
	}
	return f.InstanceOperation(address, InstanceOperationTypeStartInstance, request)
}

func (f *OrchestratorForwarder) StopInstance(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.StopInstance(request)
	}
	address, err := f.locator.Node2Address(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to stop instance")
	}
	return f.InstanceOperation(address, InstanceOperationTypeStopInstance, request)
}

func (f *OrchestratorForwarder) DeleteInstance(request *Request) error {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.DeleteInstance(request)
	}
	address, err := f.locator.Node2Address(request.NodeID)
	if err != nil {
		return errors.Wrap(err, "unable to delete instance")
	}
	if _, err := f.InstanceOperation(address, InstanceOperationTypeDeleteInstance, request); err != nil {
		return err
	}
	return nil
}

func (f *OrchestratorForwarder) InspectInstance(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.InspectInstance(request)
	}
	address, err := f.locator.Node2Address(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to inspect instance")
	}
	return f.InstanceOperation(address, InstanceOperationTypeInspectInstance, request)
}

func (f *OrchestratorForwarder) GetCurrentNode() *types.NodeInfo {
	return f.orch.GetCurrentNode()
}

func (f *OrchestratorForwarder) InstanceOperation(address string, opType InstanceOperationType, request *Request) (*Instance, error) {
	var instance *Instance

	//FIXME insecure
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrapf(err, "fail to connect to %v", address)
	}
	defer conn.Close()
	c := pb.NewForwarderClient(conn)

	resp, err := c.InstanceOperationRPC(context.Background(), &pb.InstanceOperationRequest{
		Type:         string(opType),
		NodeID:       request.NodeID,
		InstanceID:   request.InstanceID,
		InstanceName: request.InstanceName,
		VolumeName:   request.VolumeName,
		VolumeSize:   request.VolumeSize,
		ReplicaURLs:  request.ReplicaURLs,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "fail to execute instance operation on %v", address)
	}
	if resp != nil {
		instance = &Instance{
			ID:      resp.InstanceID,
			Name:    resp.InstanceName,
			Running: resp.Running,
			Address: resp.Address,
		}
	}
	return instance, nil
}
