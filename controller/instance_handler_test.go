package controller

import (
	"fmt"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestSyncInstanceState(c *C) {
	var err error
	testCases := map[string]struct {
		//pod setup
		podPhase    v1.PodPhase
		podNodeName string
		podIP       string

		//state setup
		currentState  types.InstanceState
		currentNodeID string

		//replica expectation
		expectedState  types.InstanceState
		expectedNodeID string
		expectedIP     string
		expectedError  bool
	}{
		"all stopped": {
			"", "", "",
			types.InstanceStateStopped, "",
			types.InstanceStateError, "", "", false,
		},
		"pod starting for the first time": {
			v1.PodPending, TestNode1, "",
			types.InstanceStateError, "",
			types.InstanceStateStopped, "", "", false,
		},
		"pod running for first time": {
			v1.PodRunning, TestNode1, TestIP1,
			types.InstanceStateError, "",
			types.InstanceStateRunning, TestNode1, TestIP1, false,
		},
		"pod stopped after first run": {
			v1.PodPending, "", TestIP1,
			types.InstanceStateRunning, TestNode1,
			types.InstanceStateStopped, TestNode1, "", false,
		},
		"pod run after first run": {
			v1.PodRunning, TestNode1, TestIP2,
			types.InstanceStateStopped, TestNode1,
			types.InstanceStateRunning, TestNode1, TestIP2, false,
		},
		"pod run at another node after first run": {
			v1.PodRunning, TestNode2, TestIP2,
			types.InstanceStateStopped, TestNode1,
			types.InstanceStateError, TestNode1, "", true,
		},
	}
	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		defer pIndexer.Replace(make([]interface{}, 0), "0")

		h := newTestInstanceHandler(kubeInformerFactory, kubeClient)

		spec := &types.InstanceSpec{
			NodeID: tc.currentNodeID,
		}
		status := &types.InstanceStatus{
			State: tc.currentState,
		}
		pod := newPod(tc.podPhase, TestPodName, TestNamespace)
		pod.Spec.NodeName = tc.podNodeName
		pod.Status.PodIP = tc.podIP
		pIndexer.Add(pod)

		err = h.SyncInstanceState(pod.Name, spec, status)
		if tc.expectedError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(status.State, Equals, tc.expectedState)
		c.Assert(status.IP, Equals, tc.expectedIP)
		c.Assert(spec.NodeID, Equals, tc.expectedNodeID)
	}
}

func newTestInstanceHandler(kubeInformerFactory informers.SharedInformerFactory, kubeClient *fake.Clientset) *InstanceHandler {
	podInformer := kubeInformerFactory.Core().V1().Pods()
	return NewInstanceHandler(podInformer, kubeClient, TestNamespace)
}
