package scheduler

import (
	"fmt"

	"github.com/pkg/errors"
)

type Scheduler struct {
	m ResourceManager
}

func NewScheduler(resourceManager ResourceManager) *Scheduler {
	return &Scheduler{
		m: resourceManager,
	}
}

func (s *Scheduler) Schedule(spec *Spec, policy *Policy) (node *Node, err error) {
	defer func() {
		errors.Wrap(err, "fail to schedule")
	}()
	nodes, err := s.m.ListSchedulingNodes()
	if err != nil {
		return nil, err
	}

	normalPriorityList := []*Node{}
	lowPriorityList := []*Node{}

	for id, node := range nodes {
		if policy != nil {
			if policy.Binding == PolicyBindingTypeSoftAntiAffinity {
				if _, ok := policy.NodeIDs[id]; ok {
					lowPriorityList = append(lowPriorityList, node)
				} else {
					normalPriorityList = append(normalPriorityList, node)
				}
			} else {
				return nil, errors.Errorf("Unsupported schedule policy binding %v", policy.Binding)
			}
		} else {
			normalPriorityList = append(normalPriorityList, node)
		}
	}

	priorityList := append(normalPriorityList, lowPriorityList...)
	if len(priorityList) == 0 {
		return nil, fmt.Errorf("unable to find suitable host")
	}
	return priorityList[0], nil
}
