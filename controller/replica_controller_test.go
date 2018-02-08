package controller

import (
	"fmt"
	"testing"

	"github.com/rancher/longhorn-manager/types"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"
	lhlisters "github.com/rancher/longhorn-manager/k8s/pkg/client/listers/longhorn/v1alpha1"

	. "gopkg.in/check.v1"
)

const (
	TestNamespace   = "default"
	TestThreadiness = 10
	TestVolumeSize  = "1g"
	TestRestoreFrom = "vfs://empty"
	TestRestoreName = "empty"
	TestIP1         = "1.2.3.4"
	TestIP2         = "5.6.7.8"
	TestNode1       = "test-node-name-1"
	TestNode2       = "test-node-name-2"
	TestOwnerID1    = TestNode1
	TestOwnerID2    = TestNode2

	TestReplica1Name = "replica-volumename-1"
)

var (
	alwaysReady = func() bool { return true }
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
}

func newReplica(desireState, currentState types.InstanceState, desireOwnerID, currentOwnerID, failedAt string) *longhorn.Replica {
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestReplica1Name,
			UID:       uuid.NewUUID(),
			Namespace: TestNamespace,
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				DesireState:   desireState,
				DesireOwnerID: desireOwnerID,
			},
			VolumeSize:  TestVolumeSize,
			RestoreFrom: TestRestoreFrom,
			RestoreName: TestRestoreName,
		},
		Status: types.ReplicaStatus{
			InstanceStatus: types.InstanceStatus{
				State:          currentState,
				CurrentOwnerID: currentOwnerID,
			},
		},
	}
}

func newPod(phase v1.PodPhase, replica *longhorn.Replica) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replica.Name,
			Namespace: replica.Namespace,
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
}

func getKey(obj interface{}, c *C) string {
	key, err := controller.KeyFunc(obj)
	c.Assert(err, IsNil)
	return key
}

func getReplica(name string, lister lhlisters.ReplicaLister) (*longhorn.Replica, error) {
	return lister.Replicas(TestNamespace).Get(name)
}

func newTestReplicaController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset,
	controllerID string) (*ReplicaController, *controller.FakePodControl) {
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	podInformer := kubeInformerFactory.Core().V1().Pods()
	jobInformer := kubeInformerFactory.Batch().V1().Jobs()

	rc := NewReplicaController(replicaInformer, podInformer, jobInformer, lhClient, kubeClient, TestNamespace, controllerID)

	fakeRecorder := record.NewFakeRecorder(100)
	rc.eventRecorder = fakeRecorder

	rc.rStoreSynced = alwaysReady
	rc.pStoreSynced = alwaysReady
	rc.jStoreSynced = alwaysReady

	fakePodControl := &controller.FakePodControl{}
	rc.podControl = fakePodControl

	return rc, fakePodControl
}

func (s *TestSuite) TestSyncReplicaWithPod(c *C) {
	var err error
	testCases := map[string]struct {
		//pod setup
		podPhase    v1.PodPhase
		podNodeName string
		podIP       string

		//replica setup
		replicaCurrentState types.InstanceState
		replicaNodeID       string

		//replica expectation
		expectedState  types.InstanceState
		expectedNodeID string
		expectedIP     string
		expectedError  bool
	}{
		"all stopped": {
			"", "", "",
			types.InstanceStateError, "",
			types.InstanceStateStopped, "", "", false,
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

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		rc, _ := newTestReplicaController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestOwnerID1)

		// the existing states doesn't matter here
		replica := newReplica(types.InstanceStateError, tc.replicaCurrentState, TestNode1, TestNode1, "")
		replica.Spec.NodeID = tc.replicaNodeID
		if tc.podPhase != "" {
			pod := newPod(tc.podPhase, replica)
			pod.Spec.NodeName = tc.podNodeName
			pod.Status.PodIP = tc.podIP
			pIndexer.Add(pod)
		}

		err = rc.syncReplicaWithPod(replica)
		if tc.expectedError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(replica.Status.State, Equals, tc.expectedState)
		c.Assert(replica.Status.IP, Equals, tc.expectedIP)
		c.Assert(replica.Spec.NodeID, Equals, tc.expectedNodeID)
	}
}

func (s *TestSuite) TestSyncReplica(c *C) {
	var (
		err error
	)

	testCases := map[string]struct {
		//replica setup
		desireState    types.InstanceState
		currentState   types.InstanceState
		desireOwnerID  string
		currentOwnerID string

		//replica exception
		expectedState   types.InstanceState
		expectedOwnerID string
		err             bool

		//pod expection
		expectedCreations int
		expectedDeletions int
	}{
		"replica keep stopped": {
			types.InstanceStateStopped, types.InstanceStateStopped, TestOwnerID1, TestOwnerID1,
			types.InstanceStateStopped, TestOwnerID1, false,
			0, 0,
		},
		"replica start": {
			types.InstanceStateRunning, types.InstanceStateStopped, TestOwnerID1, TestOwnerID1,
			types.InstanceStateRunning, TestOwnerID1, false,
			1, 0,
		},
		"replica keep running": {
			types.InstanceStateRunning, types.InstanceStateRunning, TestOwnerID1, TestOwnerID1,
			types.InstanceStateRunning, TestOwnerID1, false,
			0, 0,
		},
		"replica stop": {
			types.InstanceStateStopped, types.InstanceStateRunning, TestOwnerID1, TestOwnerID1,
			types.InstanceStateStopped, TestOwnerID1, false,
			0, 1,
		},
		"replica deleted when running": {
			types.InstanceStateDeleted, types.InstanceStateRunning, TestOwnerID1, TestOwnerID1,
			types.InstanceStateDeleted, TestOwnerID1, false,
			0, 1,
		},
		"replica stop and transfer ownership": {
			types.InstanceStateStopped, types.InstanceStateStopped, TestOwnerID1, "",
			types.InstanceStateStopped, TestOwnerID1, false,
			0, 0,
		},
		"replica stop and transfer ownership but other hasn't yield": {
			types.InstanceStateStopped, types.InstanceStateStopped, TestOwnerID1, TestOwnerID2,
			types.InstanceStateStopped, TestOwnerID2, true,
			0, 0,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		defer pIndexer.Replace(make([]interface{}, 0), "0")

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())
		rIndexer := lhInformerFactory.Longhorn().V1alpha1().Replicas().Informer().GetIndexer()
		defer rIndexer.Replace(make([]interface{}, 0), "0")

		rc, fakePodControl := newTestReplicaController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestOwnerID1)

		// Use indexer since fakeClientset won't update indexer store now
		rc.updateReplicaHandler = func(r *longhorn.Replica) (*longhorn.Replica, error) {
			err := rIndexer.Update(r)
			if err != nil {
				return nil, err
			}
			return r, nil
		}

		replica := newReplica(tc.desireState, tc.currentState, tc.desireOwnerID, tc.currentOwnerID, "")
		err = rIndexer.Add(replica)
		c.Assert(err, IsNil)

		if tc.currentState == types.InstanceStateRunning {
			pIndexer.Add(newPod(v1.PodRunning, replica))
		}

		err = rc.syncReplica(getKey(replica, c))
		if tc.err {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}

		// get replica pod, it should be added
		c.Assert(len(fakePodControl.Templates), Equals, tc.expectedCreations)
		c.Assert(len(fakePodControl.DeletePodName), Equals, tc.expectedDeletions)
		obj, exists, err := rIndexer.Get(replica)
		c.Assert(err, IsNil)
		c.Assert(exists, Equals, true)
		updatedReplica, ok := obj.(*longhorn.Replica)
		c.Assert(ok, Equals, true)
		c.Assert(updatedReplica.Status.CurrentOwnerID, Equals, tc.expectedOwnerID)
	}

}
