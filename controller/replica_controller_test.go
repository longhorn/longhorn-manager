package controllers

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

func newReplica(desireState, currentState types.InstanceState, failedAt string) *longhorn.Replica {
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestReplica1Name,
			UID:       uuid.NewUUID(),
			Namespace: TestNamespace,
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				DesireState: desireState,
			},
			VolumeSize:  TestVolumeSize,
			RestoreFrom: TestRestoreFrom,
			RestoreName: TestRestoreName,
		},
		Status: types.ReplicaStatus{
			InstanceStatus: types.InstanceStatus{
				State: currentState,
			},
		},
	}
}

func newPod(phase v1.PodPhase, replica *longhorn.Replica) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            replica.Name,
			Namespace:       replica.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(replica, controllerKind)},
		},
		Status: v1.PodStatus{Phase: phase},
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

func newTestReplicaController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory, lhClient *lhfake.Clientset, kubeClient *fake.Clientset) (*ReplicaController, *controller.FakePodControl) {
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	podInformer := kubeInformerFactory.Core().V1().Pods()
	jobInformer := kubeInformerFactory.Batch().V1().Jobs()

	rc := NewReplicaController(replicaInformer, podInformer, jobInformer, lhClient, kubeClient, TestNamespace)

	fakeRecorder := record.NewFakeRecorder(100)
	rc.eventRecorder = fakeRecorder

	rc.rStoreSynced = alwaysReady
	rc.pStoreSynced = alwaysReady
	rc.jStoreSynced = alwaysReady

	fakePodControl := &controller.FakePodControl{}
	rc.podControl = fakePodControl

	return rc, fakePodControl
}

func (s *TestSuite) TestReplicaCRUD(c *C) {
	var (
		err error
	)

	testCases := map[string]struct {
		//replica setup
		desireState  types.InstanceState
		currentState types.InstanceState

		//replica exception
		expectedState types.InstanceState

		//pod expection
		expectedCreations int
		expectedDeletions int
	}{
		"replica keep stopped": {
			types.InstanceStateStopped, types.InstanceStateStopped,
			types.InstanceStateStopped,
			0, 0,
		},
		"replica start": {
			types.InstanceStateRunning, types.InstanceStateStopped,
			types.InstanceStateRunning,
			1, 0,
		},
		"replica stop": {
			types.InstanceStateStopped, types.InstanceStateRunning,
			types.InstanceStateStopped,
			0, 1,
		},
		"replica deleted when running": {
			types.InstanceStateDeleted, types.InstanceStateRunning,
			types.InstanceStateDeleted,
			0, 1,
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

		rc, fakePodControl := newTestReplicaController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		// Use indexer since fakeClientset won't update indexer store now
		rc.updateReplicaHandler = func(r *longhorn.Replica) (*longhorn.Replica, error) {
			err := rIndexer.Update(r)
			if err != nil {
				return nil, err
			}
			return r, nil
		}

		replica := newReplica(tc.desireState, tc.currentState, "")
		err = rIndexer.Add(replica)
		c.Assert(err, IsNil)

		if tc.currentState == types.InstanceStateRunning {
			pIndexer.Add(newPod(v1.PodRunning, replica))
		}

		err = rc.syncReplica(getKey(replica, c))
		c.Assert(err, IsNil)

		// get replica pod, it should be added
		c.Assert(len(fakePodControl.Templates), Equals, tc.expectedCreations)
		c.Assert(len(fakePodControl.DeletePodName), Equals, tc.expectedDeletions)

		// Make sure the ControllerRefs are correct.
		for _, controllerRef := range fakePodControl.ControllerRefs {
			c.Assert(controllerRef.APIVersion, Equals, "longhorn.rancher.io/v1alpha1")
			c.Assert(controllerRef.Kind, Equals, "Replica")
			c.Assert(controllerRef.Name, Equals, replica.Name)
			c.Assert(controllerRef.UID, Equals, replica.UID)
			c.Assert(controllerRef.Controller, NotNil)
			c.Assert(*controllerRef.Controller, Equals, true)
		}
	}

}
