package orchsim

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

type OrchSim struct {
	currentNode *types.NodeInfo
	records     map[string]*InstanceRecord
	mutex       *sync.RWMutex
	engines     *engineapi.EngineSimulatorCollection
}

type StateType string

const (
	StateRunning = StateType("running")
	StateStopped = StateType("stopped")
)

type InstanceRecord struct {
	ID    string
	Name  string
	State StateType
	IP    string
}

func NewOrchestratorSimulator(port int, engines *engineapi.EngineSimulatorCollection) *OrchSim {
	nodeID := util.UUID()
	return &OrchSim{
		currentNode: &types.NodeInfo{
			ID:               nodeID,
			IP:               "127.0.0.1",
			OrchestratorPort: port,
			Metadata: types.Metadata{
				Name: "sim-" + nodeID,
			},
		},
		records: map[string]*InstanceRecord{},
		mutex:   &sync.RWMutex{},
		engines: engines,
	}
}

func (s *OrchSim) CreateController(request *orchestrator.Request) (*orchestrator.Instance, error) {
	if err := orchestrator.ValidateRequestCreateController(request); err != nil {
		return nil, err
	}
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}

	instance := &InstanceRecord{
		ID:    util.UUID(),
		Name:  request.InstanceName,
		State: StateRunning,
		IP:    "ip-" + request.InstanceName + "-" + util.UUID()[:8],
	}

	if err := s.engines.CreateEngineSimulator(&engineapi.EngineSimulatorRequest{
		VolumeName:     request.VolumeName,
		VolumeSize:     request.VolumeSize,
		ControllerAddr: instance.IP,
		// ReplicaURLs should contains port
		ReplicaAddrs: request.ReplicaURLs,
	}); err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.createRecord(instance); err != nil {
		return nil, err
	}
	return &orchestrator.Instance{
		ID:      instance.ID,
		Name:    instance.Name,
		NodeID:  s.currentNode.ID,
		Running: instance.State == StateRunning,
		IP:      instance.IP,
	}, nil
}

func (s *OrchSim) CreateReplica(request *orchestrator.Request) (*orchestrator.Instance, error) {
	if err := orchestrator.ValidateRequestCreateReplica(request); err != nil {
		return nil, err
	}
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}

	instance := &InstanceRecord{
		ID:    util.UUID(),
		Name:  request.InstanceName,
		State: StateStopped,
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.createRecord(instance); err != nil {
		return nil, err
	}
	return &orchestrator.Instance{
		ID:      instance.ID,
		Name:    instance.Name,
		NodeID:  s.currentNode.ID,
		Running: instance.State == StateRunning,
		IP:      instance.IP,
	}, nil
}

func (s *OrchSim) StartInstance(request *orchestrator.Request) (*orchestrator.Instance, error) {
	if err := orchestrator.ValidateRequestInstanceOps(request); err != nil {
		return nil, err
	}
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	instance, err := s.getRecord(request.InstanceName)
	if err != nil {
		return nil, err
	}
	if instance.State != StateRunning {
		instance.State = StateRunning
		instance.IP = "ip-" + instance.Name + "-" + util.UUID()[:8]
		if err := s.updateRecord(instance); err != nil {
			return nil, err
		}
	}
	return &orchestrator.Instance{
		ID:      instance.ID,
		Name:    instance.Name,
		NodeID:  s.currentNode.ID,
		Running: instance.State == StateRunning,
		IP:      instance.IP,
	}, nil
}

func (s *OrchSim) StopInstance(request *orchestrator.Request) (*orchestrator.Instance, error) {
	if err := orchestrator.ValidateRequestInstanceOps(request); err != nil {
		return nil, err
	}
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	instance, err := s.getRecord(request.InstanceName)
	if err != nil {
		return nil, err
	}

	// Hack to check if it's a replica
	if instance.State != StateStopped && strings.Contains(instance.Name, "replica") {
		if engine, err := s.engines.GetEngineSimulator(request.VolumeName); err == nil {
			if err := engine.SimulateStopReplica(engineapi.GetReplicaDefaultURL(instance.IP)); err != nil {
				return nil, err
			}
		}
		instance.State = StateStopped
		instance.IP = ""
		if err := s.updateRecord(instance); err != nil {
			return nil, err
		}
	}
	return &orchestrator.Instance{
		ID:      instance.ID,
		Name:    instance.Name,
		NodeID:  s.currentNode.ID,
		Running: instance.State == StateRunning,
		IP:      instance.IP,
	}, nil
}

func (s *OrchSim) DeleteInstance(request *orchestrator.Request) error {
	if err := orchestrator.ValidateRequestInstanceOps(request); err != nil {
		return err
	}
	if request.NodeID != s.currentNode.ID {
		return fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	instance, err := s.getRecord(request.InstanceName)
	if err != nil {
		return err
	}

	if strings.Contains(instance.Name, "controller") {
		if err := s.engines.DeleteEngineSimulator(request.VolumeName); err != nil {
			logrus.Warnf("Fail to delete engine simulator for %v", request.VolumeName)
		}
	} else {
		if engine, err := s.engines.GetEngineSimulator(request.VolumeName); err == nil {
			if err := engine.SimulateStopReplica(engineapi.GetReplicaDefaultURL(instance.IP)); err != nil {
				return nil
			}
		}
	}

	return s.deleteRecord(request.InstanceName)
}

func (s *OrchSim) InspectInstance(request *orchestrator.Request) (*orchestrator.Instance, error) {
	if err := orchestrator.ValidateRequestInstanceOps(request); err != nil {
		return nil, err
	}
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	instance, err := s.getRecord(request.InstanceName)
	if err != nil {
		return nil, err
	}
	return &orchestrator.Instance{
		ID:      instance.ID,
		Name:    instance.Name,
		NodeID:  s.currentNode.ID,
		Running: instance.State == StateRunning,
		IP:      instance.IP,
	}, nil
}

func (s *OrchSim) GetCurrentNode() *types.NodeInfo {
	return s.currentNode
}

// Must be locked
func (s *OrchSim) createRecord(instance *InstanceRecord) error {
	if s.records[instance.Name] != nil {
		return fmt.Errorf("duplicate instance with name %v", instance.Name)
	}
	s.records[instance.Name] = instance
	return nil
}

// Must be locked
func (s *OrchSim) updateRecord(instance *InstanceRecord) error {
	if s.records[instance.Name] == nil {
		return fmt.Errorf("unable to find instance with name %v", instance.Name)
	}
	s.records[instance.Name] = instance
	return nil
}

// Must be locked
func (s *OrchSim) getRecord(instanceName string) (*InstanceRecord, error) {
	if s.records[instanceName] == nil {
		return nil, fmt.Errorf("unable to find instance %v", instanceName)
	}
	return s.records[instanceName], nil
}

// Must be locked
func (s *OrchSim) deleteRecord(instanceName string) error {
	if s.records[instanceName] == nil {
		return fmt.Errorf("unable to find instance %v", instanceName)
	}
	delete(s.records, instanceName)
	return nil
}
