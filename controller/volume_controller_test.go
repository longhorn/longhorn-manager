package controller

import (
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

func getVolumeLabelSelector(volumeName string) string {
	return "longhornvolume=" + volumeName
}

func newTestVolumeController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset,
	controllerID string) *VolumeController {

	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()

	ds := datastore.NewDataStore(volumeInformer, engineInformer, replicaInformer, lhClient,
		podInformer, cronJobInformer, kubeClient, TestNamespace)

	vc := NewVolumeController(ds, scheme.Scheme, volumeInformer, engineInformer, replicaInformer, kubeClient, TestNamespace, controllerID, TestServiceAccount, TestEngineImage, TestManagerImage)

	fakeRecorder := record.NewFakeRecorder(100)
	vc.eventRecorder = fakeRecorder

	vc.vStoreSynced = alwaysReady
	vc.rStoreSynced = alwaysReady
	vc.eStoreSynced = alwaysReady
	vc.nowHandler = getTestNow

	return vc
}

type VolumeTestCase struct {
	volume   *longhorn.Volume
	engine   *longhorn.Engine
	replicas map[string]*longhorn.Replica

	expectVolume   *longhorn.Volume
	expectEngine   *longhorn.Engine
	expectReplicas map[string]*longhorn.Replica
}

func (s *TestSuite) TestVolumeLifeCycle(c *C) {
	var tc *VolumeTestCase
	testCases := map[string]*VolumeTestCase{}

	// normal volume creation
	tc = generateVolumeTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDetaching
	tc.engine = nil
	tc.replicas = nil
	testCases["volume create"] = tc

	// after creation, volume in detached state
	tc = generateVolumeTestCaseTemplate()
	tc.engine.Status.CurrentState = types.InstanceStateStopped
	for _, r := range tc.replicas {
		r.Status.CurrentState = types.InstanceStateStopped
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDetached
	testCases["volume detached"] = tc

	// volume attaching, start replicas
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	// replicas will be started first
	// engine will be started only after all the replicas are running
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = types.InstanceStateRunning
	}
	testCases["volume attaching - start replicas"] = tc

	// volume attaching, start engine
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	for _, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Spec.NodeID = util.RandomID()
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttaching
	tc.expectEngine.Spec.NodeID = tc.volume.Spec.NodeID
	tc.expectEngine.Spec.DesireState = types.InstanceStateRunning
	for name, r := range tc.expectReplicas {
		tc.expectEngine.Spec.ReplicaAddressMap[name] = r.Status.IP
	}
	testCases["volume attaching - start controller"] = tc

	// volume attached
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.engine.Spec.NodeID = tc.volume.Spec.NodeID
	tc.engine.Spec.DesireState = types.InstanceStateRunning
	tc.engine.Status.CurrentState = types.InstanceStateRunning
	tc.engine.Status.IP = randomIP()
	tc.engine.Status.Endpoint = "/dev/" + tc.volume.Name
	tc.engine.Status.ReplicaModeMap = map[string]types.ReplicaMode{}
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Spec.NodeID = util.RandomID()
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		tc.engine.Spec.ReplicaAddressMap[name] = r.Status.IP
		tc.engine.Status.ReplicaModeMap[name] = types.ReplicaModeRW
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateAttached
	tc.expectVolume.Status.Endpoint = tc.engine.Status.Endpoint
	tc.expectVolume.Status.Robustness = types.VolumeRobustnessHealthy
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
	}
	testCases["volume attached"] = tc

	// volume detaching - stop engine
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.volume.Status.Endpoint = "/dev/" + tc.volume.Name
	tc.volume.Status.Robustness = types.VolumeRobustnessHealthy
	tc.engine.Spec.NodeID = TestNode1
	tc.engine.Spec.DesireState = types.InstanceStateRunning
	tc.engine.Status.CurrentState = types.InstanceStateRunning
	tc.engine.Status.IP = randomIP()
	tc.engine.Status.Endpoint = "/dev/" + tc.volume.Name
	tc.engine.Status.ReplicaModeMap = map[string]types.ReplicaMode{}
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Spec.NodeID = util.RandomID()
		r.Spec.HealthyAt = getTestNow()
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		tc.engine.Spec.ReplicaAddressMap[name] = r.Status.IP
		tc.engine.Status.ReplicaModeMap[name] = types.ReplicaModeRW
	}
	tc.copyCurrentToExpect()
	tc.expectEngine.Spec.NodeID = ""
	tc.expectVolume.Status.State = types.VolumeStateDetaching
	tc.expectVolume.Status.Endpoint = ""
	tc.expectEngine.Spec.DesireState = types.InstanceStateStopped
	testCases["volume detaching - stop engine"] = tc

	// volume detaching - stop replicas
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.engine.Spec.NodeID = ""
	tc.engine.Status.CurrentState = types.InstanceStateStopped
	for name, r := range tc.replicas {
		r.Spec.DesireState = types.InstanceStateRunning
		r.Spec.NodeID = util.RandomID()
		r.Spec.HealthyAt = getTestNow()
		r.Status.CurrentState = types.InstanceStateRunning
		r.Status.IP = randomIP()
		tc.engine.Spec.ReplicaAddressMap[name] = r.Status.IP
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDetaching
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = types.InstanceStateStopped
	}
	testCases["volume detaching - stop replicas"] = tc

	// volume deleting
	tc = generateVolumeTestCaseTemplate()
	now := metav1.NewTime(time.Now())
	tc.volume.SetDeletionTimestamp(&now)
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = types.VolumeStateDeleting
	tc.expectEngine = nil
	tc.expectReplicas = nil
	testCases["volume deleting"] = tc

	s.runTestCases(c, testCases)
}

func newVolume(name string, replicaCount int) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Finalizers: []string{
				longhorn.SchemeGroupVersion.Group,
			},
		},
		Spec: types.VolumeSpec{
			NumberOfReplicas:    replicaCount,
			Size:                TestVolumeSize,
			OwnerID:             TestOwnerID1,
			StaleReplicaTimeout: TestVolumeStaleTimeout,
		},
	}
}

func newEngineForVolume(v *longhorn.Volume) *longhorn.Engine {
	return &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name: v.Name + "-e",
			Labels: map[string]string{
				"longhornvolume": v.Name,
			},
		},
		Spec: types.EngineSpec{
			InstanceSpec: types.InstanceSpec{
				OwnerID:     v.Spec.OwnerID,
				VolumeName:  v.Name,
				EngineImage: TestEngineImage,
				DesireState: types.InstanceStateStopped,
			},
			ReplicaAddressMap: map[string]string{},
		},
	}
}

func newReplicaForVolume(v *longhorn.Volume) *longhorn.Replica {
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name: v.Name + "-r-" + util.RandomID(),
			Labels: map[string]string{
				"longhornvolume": v.Name,
			},
		},
		Spec: types.ReplicaSpec{
			InstanceSpec: types.InstanceSpec{
				OwnerID:     v.Spec.OwnerID,
				VolumeName:  v.Name,
				EngineImage: TestEngineImage,
				DesireState: types.InstanceStateStopped,
			},
			VolumeSize: v.Spec.Size,
		},
	}
}

func generateVolumeTestCaseTemplate() *VolumeTestCase {
	volume := newVolume(TestVolumeName, 2)
	engine := newEngineForVolume(volume)
	replica1 := newReplicaForVolume(volume)
	replica2 := newReplicaForVolume(volume)
	return &VolumeTestCase{
		volume, engine, map[string]*longhorn.Replica{
			replica1.Name: replica1,
			replica2.Name: replica2,
		},
		nil, nil, map[string]*longhorn.Replica{},
	}
}

func (tc *VolumeTestCase) copyCurrentToExpect() {
	tc.expectVolume = tc.volume.DeepCopy()
	tc.expectEngine = tc.engine.DeepCopy()
	for n, r := range tc.replicas {
		tc.expectReplicas[n] = r.DeepCopy()
	}
}

func (s *TestSuite) runTestCases(c *C, testCases map[string]*VolumeTestCase) {
	for name, tc := range testCases {
		var err error
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())
		vIndexer := lhInformerFactory.Longhorn().V1alpha1().Volumes().Informer().GetIndexer()
		eIndexer := lhInformerFactory.Longhorn().V1alpha1().Engines().Informer().GetIndexer()
		rIndexer := lhInformerFactory.Longhorn().V1alpha1().Replicas().Informer().GetIndexer()

		vc := newTestVolumeController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestOwnerID1)

		// Need to put it into both fakeclientset and Indexer
		v, err := lhClient.LonghornV1alpha1().Volumes(TestNamespace).Create(tc.volume)
		c.Assert(err, IsNil)
		err = vIndexer.Add(v)
		c.Assert(err, IsNil)

		if tc.engine != nil {
			e, err := lhClient.LonghornV1alpha1().Engines(TestNamespace).Create(tc.engine)
			c.Assert(err, IsNil)
			err = eIndexer.Add(e)
			c.Assert(err, IsNil)
		}

		if tc.replicas != nil {
			for _, r := range tc.replicas {
				r, err = lhClient.LonghornV1alpha1().Replicas(TestNamespace).Create(r)
				c.Assert(err, IsNil)
				err = rIndexer.Add(r)
				c.Assert(err, IsNil)
			}
		}

		err = vc.syncVolume(getKey(v, c))
		c.Assert(err, IsNil)

		retV, err := lhClient.LonghornV1alpha1().Volumes(TestNamespace).Get(v.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(retV.Spec, DeepEquals, tc.expectVolume.Spec)
		c.Assert(retV.Status, DeepEquals, tc.expectVolume.Status)

		retE, err := lhClient.LonghornV1alpha1().Engines(TestNamespace).Get(types.GetEngineNameForVolume(v.Name), metav1.GetOptions{})
		if tc.expectEngine != nil {
			c.Assert(err, IsNil)
			c.Assert(retE.Spec, DeepEquals, tc.expectEngine.Spec)
			c.Assert(retE.Status, DeepEquals, tc.expectEngine.Status)
		} else {
			c.Assert(apierrors.IsNotFound(err), Equals, true)
		}

		retRs, err := lhClient.LonghornV1alpha1().Replicas(TestNamespace).List(metav1.ListOptions{LabelSelector: getVolumeLabelSelector(v.Name)})
		c.Assert(err, IsNil)
		c.Assert(retRs.Items, HasLen, len(tc.expectReplicas))
		for _, retR := range retRs.Items {
			if tc.replicas == nil {
				// test creation
				var expectR *longhorn.Replica
				for _, expectR = range tc.expectReplicas {
					break
				}
				c.Assert(retR.Spec, DeepEquals, expectR.Spec)
				c.Assert(retR.Status, DeepEquals, expectR.Status)
			} else {
				c.Assert(retR.Spec, DeepEquals, tc.expectReplicas[retR.Name].Spec)
				c.Assert(retR.Status, DeepEquals, tc.expectReplicas[retR.Name].Status)
			}
		}
	}
}

func getTestNow() string {
	return TestTimeNow
}
