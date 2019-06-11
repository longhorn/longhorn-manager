package controller

import (
	"fmt"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/types"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestSyncStatusWithPod(c *C) {
	testCases := map[string]struct {
		//pod setup
		podPhase v1.PodPhase
		podIP    string
		deleted  bool

		//status expectation
		expectedState types.InstanceState
		expectedIP    string
	}{
		"all stopped": {
			"", "", false,
			types.InstanceStateStopped, "",
		},
		"all stopped with deleted": {
			v1.PodRunning, TestIP1, true,
			types.InstanceStateStopping, "",
		},
		"pod starting for the first time": {
			v1.PodPending, "", false,
			types.InstanceStateStarting, "",
		},
		"pod running for first time": {
			v1.PodRunning, TestIP1, false,
			types.InstanceStateRunning, TestIP1,
		},
		"pod failed": {
			v1.PodFailed, TestIP1, false,
			types.InstanceStateError, "",
		},
	}
	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		h := newTestInstanceHandler(kubeInformerFactory, kubeClient)

		status := &types.InstanceStatus{}
		spec := &types.InstanceSpec{}
		pod := newPod(tc.podPhase, TestPod1, TestNamespace, TestNode1)
		if pod != nil {
			pod.Status.PodIP = tc.podIP
			if tc.deleted {
				ts := metav1.Now()
				gracePeriod := int64(30)
				pod.DeletionTimestamp = &ts
				pod.DeletionGracePeriodSeconds = &gracePeriod
			}
		}

		h.syncStatusWithPod(pod, spec, status)
		c.Assert(status.CurrentState, Equals, tc.expectedState)
		c.Assert(status.IP, Equals, tc.expectedIP)
	}
}

func newTestInstanceHandler(kubeInformerFactory informers.SharedInformerFactory, kubeClient *fake.Clientset) *InstanceHandler {
	podInformer := kubeInformerFactory.Core().V1().Pods()
	fakeRecorder := record.NewFakeRecorder(100)
	return NewInstanceHandler(podInformer, kubeClient, TestNamespace, nil, fakeRecorder)
}
