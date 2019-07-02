package controller

import (
	"fmt"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	lhlisters "github.com/longhorn/longhorn-manager/k8s/pkg/client/listers/longhorn/v1alpha1"

	. "gopkg.in/check.v1"
)

func newReplica(desireState, currentState types.InstanceState, failedAt string) *longhorn.Replica {
	ip := ""
	if currentState == types.InstanceStateRunning {
		ip = TestIP1
	}
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestReplica1Name,
			UID:       uuid.NewUUID(),
			Namespace: TestNamespace,
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  TestVolumeName,
				VolumeSize:  TestVolumeSize,
				DesireState: desireState,
				OwnerID:     TestOwnerID1,
			},
			RestoreFrom: TestRestoreFrom,
			RestoreName: TestRestoreName,
			DiskID:      "diskid",
		},
		Status: types.ReplicaStatus{
			InstanceStatus: types.InstanceStatus{
				CurrentState: currentState,
				IP:           ip,
			},
		},
	}
}

func newPod(phase v1.PodPhase, name, namespace, nodeID string) *v1.Pod {
	if phase == "" {
		return nil
	}
	ip := ""
	if phase == v1.PodRunning {
		ip = TestIP1
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			NodeName: nodeID,
		},
		Status: v1.PodStatus{
			Phase: phase,
			PodIP: ip,
		},
	}
}

func getReplica(name string, lister lhlisters.ReplicaLister) (*longhorn.Replica, error) {
	return lister.Replicas(TestNamespace).Get(name)
}

func newTestReplicaController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset,
	controllerID string) *ReplicaController {

	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1alpha1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1alpha1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1alpha1().Settings()
	imInformer := lhInformerFactory.Longhorn().V1alpha1().InstanceManagers()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer, imInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer,
		kubeClient, TestNamespace)

	rc := NewReplicaController(ds, scheme.Scheme, replicaInformer, podInformer, kubeClient, TestNamespace, controllerID)

	fakeRecorder := record.NewFakeRecorder(100)
	rc.eventRecorder = fakeRecorder

	rc.rStoreSynced = alwaysReady
	rc.pStoreSynced = alwaysReady

	return rc
}

func (s *TestSuite) TestSyncReplica(c *C) {
	var (
		err error
	)

	testCases := map[string]struct {
		//replica setup
		desireState  types.InstanceState
		currentState types.InstanceState

		//replica exception
		expectedState types.InstanceState
		err           bool

		//pod expection
		expectedPods int

		// replica exception
		NodeID   string
		DataPath string
	}{
		"replica keep stopped": {
			types.InstanceStateStopped, types.InstanceStateStopped,
			types.InstanceStateStopped, false,
			0, TestNode1, TestDefaultDataPath,
		},
		"replica start": {
			types.InstanceStateRunning, types.InstanceStateStopped,
			types.InstanceStateRunning, false,
			1, TestNode1, TestDefaultDataPath,
		},
		"replica keep running": {
			types.InstanceStateRunning, types.InstanceStateRunning,
			types.InstanceStateRunning, false,
			1, TestNode1, TestDefaultDataPath,
		},
		"replica stop": {
			types.InstanceStateStopped, types.InstanceStateRunning,
			types.InstanceStateStopped, false,
			0, TestNode1, TestDefaultDataPath,
		},
		"replica error": {
			types.InstanceStateRunning, types.InstanceStateStopped,
			types.InstanceStateRunning, true,
			1, "", "",
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

		rc := newTestReplicaController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestOwnerID1)

		// Need add to both indexer store and fake clientset, since they
		// haven't connected yet
		replica := newReplica(tc.desireState, tc.currentState, "")
		replica.Spec.EngineName = "engine-e"
		replica.Spec.NodeID = tc.NodeID
		replica.Spec.DataPath = tc.DataPath
		err = rIndexer.Add(replica)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1alpha1().Replicas(replica.Namespace).Create(replica)
		c.Assert(err, IsNil)

		if tc.currentState == types.InstanceStateRunning {
			pod := newPod(v1.PodRunning, replica.Name, replica.Namespace, replica.Spec.NodeID)
			err = pIndexer.Add(pod)
			c.Assert(err, IsNil)
			_, err = kubeClient.CoreV1().Pods(replica.Namespace).Create(pod)
			c.Assert(err, IsNil)
		}

		err = rc.syncReplica(getKey(replica, c))
		if tc.err {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			// check fake clientset for resource update
			podList, err := kubeClient.CoreV1().Pods(replica.Namespace).List(metav1.ListOptions{})
			c.Assert(err, IsNil)
			c.Assert(podList.Items, HasLen, tc.expectedPods)
		}

		// TODO State change won't work for now since pod state wasn't changed
		//updatedReplica, err := lhClient.LonghornV1alpha1().Replicas(rc.namespace).Get(replica.Name, metav1.GetOptions{})
		//c.Assert(err, IsNil)
		//c.Assert(updatedReplica.Status.State, Equals, tc.expectedState)
	}

}
