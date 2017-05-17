package orchsim

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"

	"github.com/yasker/lm-rewrite/engineapi"
	"github.com/yasker/lm-rewrite/orchestrator"
	"github.com/yasker/lm-rewrite/types"
	"github.com/yasker/lm-rewrite/util"
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

func NewOrchestratorSimulator(engines *engineapi.EngineSimulatorCollection) (orchestrator.Orchestrator, error) {
	nodeID := util.UUID()
	return &OrchSim{
		currentNode: &types.NodeInfo{
			ID:      nodeID,
			Name:    "sim-" + nodeID,
			Address: "sim-address-" + nodeID,
		},
		records: map[string]*InstanceRecord{},
		mutex:   &sync.RWMutex{},
		engines: engines,
	}, nil
}

func (s *OrchSim) CreateController(request *orchestrator.Request) (*types.ControllerInfo, error) {
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}
	if request.InstanceName == "" {
		return nil, fmt.Errorf("missing required field %+v", request)
	}

	if request.VolumeName == "" ||
		request.VolumeSize == "" ||
		request.ReplicaURLs == nil {
		return nil, fmt.Errorf("missing required field %+v", request)
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
		ReplicaAddrs:   request.ReplicaURLs,
	}); err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.createRecord(instance); err != nil {
		return nil, err
	}
	return &types.ControllerInfo{
		InstanceInfo: types.InstanceInfo{
			ID:         instance.ID,
			Name:       instance.Name,
			VolumeName: request.VolumeName,
			NodeID:     s.currentNode.ID,
			Address:    instance.IP,
			Running:    instance.State == StateRunning,
		},
	}, nil
}

func (s *OrchSim) CreateReplica(request *orchestrator.Request) (*types.ReplicaInfo, error) {
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}
	if request.InstanceName == "" || request.VolumeName == "" {
		return nil, fmt.Errorf("missing required field %+v", request)
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
	return &types.ReplicaInfo{
		InstanceInfo: types.InstanceInfo{
			ID:         instance.ID,
			Name:       instance.Name,
			VolumeName: request.VolumeName,
			NodeID:     s.currentNode.ID,
			Address:    instance.IP,
			Running:    instance.State == StateRunning,
		},

		Mode:         "",
		BadTimestamp: "",
	}, nil
}

func (s *OrchSim) StartInstance(request *orchestrator.Request) (*types.InstanceInfo, error) {
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}

	if request.InstanceName == "" || request.VolumeName == "" {
		return nil, fmt.Errorf("missing required field %+v", request)
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
	return &types.InstanceInfo{
		ID:         instance.ID,
		Name:       instance.Name,
		VolumeName: request.VolumeName,
		NodeID:     s.currentNode.ID,
		Address:    instance.IP,
		Running:    instance.State == StateRunning,
	}, nil
}

func (s *OrchSim) StopInstance(request *orchestrator.Request) (*types.InstanceInfo, error) {
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}
	if request.InstanceName == "" || request.VolumeName == "" {
		return nil, fmt.Errorf("missing required field %+v", request)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	instance, err := s.getRecord(request.InstanceName)
	if err != nil {
		return nil, err
	}

	if engine, err := s.engines.GetEngineSimulator(request.VolumeName); err == nil {
		engine.SimulateStopReplica(instance.IP)
	}

	if instance.State != StateStopped {
		instance.State = StateStopped
		instance.IP = ""
		if err := s.updateRecord(instance); err != nil {
			return nil, err
		}
	}
	return &types.InstanceInfo{
		ID:         instance.ID,
		Name:       instance.Name,
		VolumeName: request.VolumeName,
		NodeID:     s.currentNode.ID,
		Address:    instance.IP,
		Running:    instance.State == StateRunning,
	}, nil
}

func (s *OrchSim) DeleteInstance(request *orchestrator.Request) error {
	if request.NodeID != s.currentNode.ID {
		return fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}
	if request.InstanceName == "" || request.VolumeName == "" {
		return fmt.Errorf("missing required field %+v", request)
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
			engine.SimulateStopReplica(instance.IP)
		}
	}

	return s.deleteRecord(request.InstanceName)
}

func (s *OrchSim) InspectInstance(request *orchestrator.Request) (*types.InstanceInfo, error) {
	if request.NodeID != s.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", request.NodeID,
			s.currentNode.ID)
	}
	if request.InstanceName == "" || request.VolumeName == "" {
		return nil, fmt.Errorf("missing required field %+v", request)
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	instance, err := s.getRecord(request.InstanceName)
	if err != nil {
		return nil, err
	}
	return &types.InstanceInfo{
		ID:         instance.ID,
		Name:       instance.Name,
		VolumeName: request.VolumeName,
		NodeID:     s.currentNode.ID,
		Address:    instance.IP,
		Running:    instance.State == StateRunning,
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
