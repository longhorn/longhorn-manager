package controller

import (
	"context"
	"errors"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newInstanceManagerUpgrade(name, nodeID, targetImage string, state longhorn.InstanceManagerUpgradeState) *longhorn.InstanceManagerUpgrade {
	return &longhorn.InstanceManagerUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: longhorn.InstanceManagerUpgradeSpec{
			NodeID:      nodeID,
			TargetImage: targetImage,
		},
		Status: longhorn.InstanceManagerUpgradeStatus{
			State: state,
		},
	}
}

func newTestInstanceManagerUpgradeController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*InstanceManagerUpgradeController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	imuc, err := NewInstanceManagerUpgradeController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestNamespace, controllerID, util.NewAtomicCounter())
	if err != nil {
		return nil, err
	}
	imuc.eventRecorder = record.NewFakeRecorder(100)
	for i := range imuc.cacheSyncs {
		imuc.cacheSyncs[i] = alwaysReady
	}

	return imuc, nil
}

func newTestInstanceManagerUpgradeControlController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*InstanceManagerUpgradeControlController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	imuc, err := NewInstanceManagerUpgradeControlController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	if err != nil {
		return nil, err
	}
	imuc.eventRecorder = record.NewFakeRecorder(100)
	for i := range imuc.cacheSyncs {
		imuc.cacheSyncs[i] = alwaysReady
	}

	return imuc, nil
}

func (s *TestSuite) TestGetNodeV2InstanceManager(c *C) {
	var err error
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	nodeIM := newInstanceManager("im-node", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	otherNodeIM := newInstanceManager("im-other-node", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	nodePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, nodeIM.Name, TestNamespace, TestNode1)
	otherNodePod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, otherNodeIM.Name, TestNamespace, TestNode2)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactories.Start(stopCh)

	for _, im := range []*longhorn.InstanceManager{nodeIM, otherNodeIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{nodePod, otherNodePod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	c.Assert(cache.WaitForCacheSync(stopCh, ds.InstanceManagerInformer.HasSynced), Equals, true)

	selected, err := ds.GetNodeV2InstanceManagerRO(TestNode1)
	c.Assert(err, IsNil)
	c.Assert(selected, NotNil)
	c.Assert(selected.Name, Equals, nodeIM.Name)
	_ = imIndexer
}

func (s *TestSuite) TestGetNodeV2InstanceManagerPrefersRunningSourceIM(c *C) {
	var err error
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 30*time.Second)

	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	newDefaultIM := newInstanceManager("im-new-default", longhorn.InstanceManagerStateUpgrading, TestNode1, TestNode1, "", nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactories.Start(stopCh)

	for _, im := range []*longhorn.InstanceManager{sourceIM, newDefaultIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
	}
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), sourcePod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(sourcePod)
	c.Assert(err, IsNil)
	c.Assert(cache.WaitForCacheSync(stopCh, ds.InstanceManagerInformer.HasSynced), Equals, true)

	selected, err := ds.GetNodeV2InstanceManagerRO(TestNode1)
	c.Assert(err, IsNil)
	c.Assert(selected, NotNil)
	c.Assert(selected.Name, Equals, sourceIM.Name)
}

func (s *TestSuite) TestEnsureSourceIMUpgradeTriggered(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, TestNamespace, TestNode1)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imuc.ensureSourceIMUpgradeTriggered(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)

	updated, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Get(context.TODO(), im.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updated.Spec.Image, Equals, TestExtraInstanceManagerImage)
}

func (s *TestSuite) TestBuildPlannedDetachedReplicaPlan(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	engineIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
	replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	volume := newVolume(TestVolumeName, 3)
	volume.Namespace = TestNamespace
	volume.Spec.DataEngine = longhorn.DataEngineTypeV2
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentImage = TestEngineImage
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	engine := newEngineForVolume(volume)
	engine.Spec.DataEngine = longhorn.DataEngineTypeV2
	engine.Spec.NodeID = TestNode2
	engine.Status.CurrentState = longhorn.InstanceStateRunning

	sourceReplica := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	sourceReplica.Namespace = TestNamespace
	sourceReplica.Status.CurrentState = longhorn.InstanceStateRunning
	otherReplica := newReplicaForVolume(volume, engine, TestNode2, TestDiskID2)
	otherReplica.Namespace = TestNamespace
	otherReplica.Status.CurrentState = longhorn.InstanceStateRunning

	unrelatedVolume := newVolume("unrelated-volume", 3)
	unrelatedVolume.Namespace = TestNamespace
	unrelatedVolume.Spec.DataEngine = longhorn.DataEngineTypeV2
	unrelatedVolume.Status.State = longhorn.VolumeStateAttached
	unrelatedVolume.Status.CurrentImage = TestEngineImage
	unrelatedEngine := newEngineForVolume(unrelatedVolume)
	unrelatedEngine.Spec.DataEngine = longhorn.DataEngineTypeV2
	unrelatedEngine.Spec.NodeID = TestNode2
	unrelatedEngine.Status.CurrentState = longhorn.InstanceStateError
	unrelatedReplica := newReplicaForVolume(unrelatedVolume, unrelatedEngine, TestNode2, TestDiskID2)
	unrelatedReplica.Namespace = TestNamespace
	unrelatedEngine.Spec.ReplicaAddressMap = map[string]string{
		unrelatedReplica.Name: "tcp://10.0.0.3:10000",
	}
	unrelatedEngine.Status.CurrentReplicaAddressMap = unrelatedEngine.Spec.ReplicaAddressMap

	engine.Spec.ReplicaAddressMap = map[string]string{
		sourceReplica.Name: "tcp://10.0.0.1:10000",
		otherReplica.Name:  "tcp://10.0.0.2:10000",
	}
	engine.Status.CurrentReplicaAddressMap = engine.Spec.ReplicaAddressMap
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{
		sourceReplica.Name: longhorn.ReplicaModeRW,
		otherReplica.Name:  longhorn.ReplicaModeRW,
	}

	for _, obj := range []interface{}{volume, engine, sourceReplica, otherReplica, unrelatedVolume, unrelatedEngine, unrelatedReplica} {
		switch typed := obj.(type) {
		case *longhorn.Volume:
			err = volumeIndexer.Add(typed)
		case *longhorn.Engine:
			err = engineIndexer.Add(typed)
		case *longhorn.Replica:
			err = replicaIndexer.Add(typed)
		}
		c.Assert(err, IsNil)
	}

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStatePending)
	plan, err := imuc.buildPlannedDetachedReplicaPlan(imu)
	c.Assert(err, IsNil)
	c.Assert(plan, DeepEquals, map[string][]longhorn.PlannedDetachedReplica{
		TestVolumeName: {
			{
				Name:    sourceReplica.Name,
				Address: "10.0.0.1:10000",
			},
		},
	})

	volume.Status.Robustness = longhorn.VolumeRobustnessDegraded
	err = volumeIndexer.Update(volume)
	c.Assert(err, IsNil)
	_, err = imuc.buildPlannedDetachedReplicaPlan(imu)
	c.Assert(errors.Is(err, errUpgradePrecondition), Equals, true)
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	err = volumeIndexer.Update(volume)
	c.Assert(err, IsNil)

	imu.Status.PlannedDetachedReplicas = plan
	applied, err := imuc.arePlannedDetachedReplicasApplied(imu)
	c.Assert(err, IsNil)
	c.Assert(applied, Equals, false)

	delete(engine.Spec.ReplicaAddressMap, sourceReplica.Name)
	err = engineIndexer.Update(engine)
	c.Assert(err, IsNil)
	applied, err = imuc.arePlannedDetachedReplicasApplied(imu)
	c.Assert(err, IsNil)
	c.Assert(applied, Equals, false)

	delete(engine.Status.CurrentReplicaAddressMap, sourceReplica.Name)
	delete(engine.Status.ReplicaModeMap, sourceReplica.Name)
	engine.Status.ReplicaModeMap["10.0.0.1:10000"] = longhorn.ReplicaModeERR
	err = engineIndexer.Update(engine)
	c.Assert(err, IsNil)
	applied, err = imuc.arePlannedDetachedReplicasApplied(imu)
	c.Assert(err, IsNil)
	c.Assert(applied, Equals, false)

	delete(engine.Status.ReplicaModeMap, "10.0.0.1:10000")
	err = engineIndexer.Update(engine)
	c.Assert(err, IsNil)
	applied, err = imuc.arePlannedDetachedReplicasApplied(imu)
	c.Assert(err, IsNil)
	c.Assert(applied, Equals, true)
}

func (s *TestSuite) TestPendingPersistsPlannedDetachedReplicasBeforeDetach(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	engineIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
	replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.DataEngine = longhorn.DataEngineTypeV2
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentImage = TestEngineImage
	volume.Status.CurrentEngineNodeID = TestNode2
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	engine := newEngineForVolume(volume)
	engine.Spec.DataEngine = longhorn.DataEngineTypeV2
	engine.Spec.NodeID = TestNode2
	engine.Status.CurrentState = longhorn.InstanceStateRunning

	sourceReplica := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	sourceReplica.Namespace = TestNamespace
	otherReplica := newReplicaForVolume(volume, engine, TestNode2, TestDiskID2)
	otherReplica.Namespace = TestNamespace
	engine.Spec.ReplicaAddressMap = map[string]string{
		sourceReplica.Name: "tcp://10.0.0.1:10000",
		otherReplica.Name:  "tcp://10.0.0.2:10000",
	}
	engine.Status.CurrentReplicaAddressMap = engine.Spec.ReplicaAddressMap
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{
		sourceReplica.Name: longhorn.ReplicaModeRW,
		otherReplica.Name:  longhorn.ReplicaModeRW,
	}

	for _, obj := range []interface{}{sourceIM, volume, engine, sourceReplica, otherReplica} {
		switch typed := obj.(type) {
		case *longhorn.InstanceManager:
			err = imIndexer.Add(typed)
		case *longhorn.Volume:
			err = volumeIndexer.Add(typed)
		case *longhorn.Engine:
			err = engineIndexer.Add(typed)
		case *longhorn.Replica:
			err = replicaIndexer.Add(typed)
		}
		c.Assert(err, IsNil)
	}
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), sourcePod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(sourcePod)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStatePending)
	err = imuc.reconcilePending(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStatePending)
	c.Assert(imu.Status.PlannedDetachedReplicas[TestVolumeName], HasLen, 1)
	c.Assert(imu.Status.PlannedDetachedReplicas[TestVolumeName][0].Name, Equals, sourceReplica.Name)

	updatedEngine, err := imuc.ds.GetEngineRO(engine.Name)
	c.Assert(err, IsNil)
	c.Assert(updatedEngine.Spec.ReplicaAddressMap[sourceReplica.Name], Equals, "tcp://10.0.0.1:10000")
	updatedIM, err := imuc.ds.GetInstanceManagerRO(sourceIM.Name)
	c.Assert(err, IsNil)
	c.Assert(updatedIM.Spec.Image, Equals, TestInstanceManagerImage)
}

func (s *TestSuite) TestWaitingForSourceIMWaitsForTargetPodSpecImage(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	imu.Status.StartedAt = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	tempIM := newInstanceManager("im-temp", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)
	sourcePod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	sourcePod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	tempPod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, tempIM.Name, TestNamespace, TestNode2)
	tempPod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	tempPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2

	for _, im := range []*longhorn.InstanceManager{sourceIM, tempIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{sourcePod, tempPod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForSourceIM(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
}

func (s *TestSuite) TestWaitingForSourceIMAdvancesAfterSourceIMReadyForEngineRestore(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	imu.Status.StartedAt = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	tempIM := newInstanceManager("im-temp", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)
	sourcePod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestExtraInstanceManagerImage}}
	sourcePod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "instance-manager",
		Image: TestExtraInstanceManagerImage,
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now())}},
	}}
	tempPod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, tempIM.Name, TestNamespace, TestNode2)
	tempPod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	tempPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2
	node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node.Spec.Disks[TestDiskID2] = longhorn.DiskSpec{
		Type:            longhorn.DiskTypeBlock,
		Path:            "/dev/longhorn/test-block-disk",
		DiskDriver:      longhorn.DiskDriverAio,
		AllowScheduling: true,
	}
	node.Status.DiskStatus[TestDiskID2] = &longhorn.DiskStatus{
		Conditions: []longhorn.Condition{
			newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
			newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
		},
		Type:       longhorn.DiskTypeBlock,
		DiskDriver: longhorn.DiskDriverAio,
	}

	for _, im := range []*longhorn.InstanceManager{sourceIM, tempIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{sourcePod, tempPod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = nodeIndexer.Add(node)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForSourceIM(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateRestoringEngines)
}

func (s *TestSuite) TestWaitingForSourceIMWaitsWhenSourceIMIsNotReadyForEngineRestore(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	imu.Status.StartedAt = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	tempIM := newInstanceManager("im-temp", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)
	sourcePod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestExtraInstanceManagerImage}}
	sourcePod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "instance-manager",
		Image: TestExtraInstanceManagerImage,
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now())}},
	}}
	tempPod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, tempIM.Name, TestNamespace, TestNode2)
	tempPod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	tempPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2
	node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node.Spec.Disks[TestDiskID2] = longhorn.DiskSpec{
		Type:            longhorn.DiskTypeBlock,
		Path:            "/dev/longhorn/test-block-disk",
		DiskDriver:      longhorn.DiskDriverAio,
		AllowScheduling: true,
	}
	node.Status.DiskStatus[TestDiskID2] = &longhorn.DiskStatus{
		Conditions: []longhorn.Condition{
			newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse, ""),
			newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
		},
		Type:       longhorn.DiskTypeBlock,
		DiskDriver: longhorn.DiskDriverAio,
	}

	for _, im := range []*longhorn.InstanceManager{sourceIM, tempIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{sourcePod, tempPod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = nodeIndexer.Add(node)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForSourceIM(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
}

func (s *TestSuite) TestWaitingForHealthyVolumesWaitsForOriginalNodeHealthBeforeComplete(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Status.CurrentEngineNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessDegraded

	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForHealthyVolumes(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
}

func (s *TestSuite) TestWaitingForHealthyVolumesCompletesAfterOriginalNodeHealthy(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Status.CurrentEngineNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy

	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForHealthyVolumes(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateCompleted)
}

func (s *TestSuite) TestRestoringEnginesWaitsForOriginalNodeSwitchover(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2
	volume.Status.Robustness = longhorn.VolumeRobustnessDegraded

	_, err = lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileRestoringEngines(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateRestoringEngines)
}

func (s *TestSuite) TestRestoringEnginesWaitsForHealthyVolumesAfterOriginalNodeSwitchover(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Status.CurrentEngineNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessDegraded

	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileRestoringEngines(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
}

func (s *TestSuite) TestSyncInPlaceUpgradedInstanceManagerPod(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
	pod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}

	for _, obj := range []cache.Indexer{imIndexer, imuIndexer, pIndexer} {
		_ = obj
	}

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	handled, err := imc.syncInPlaceUpgradedInstanceManagerPod(im, imu)
	c.Assert(err, IsNil)
	c.Assert(handled, Equals, true)

	updatedPod, err := kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedPod.Spec.Containers[0].Image, Equals, TestExtraInstanceManagerImage)
}

func (s *TestSuite) TestSyncInPlaceUpgradedInstanceManagerPodDoesNotBlockRecreateWhenPodMissing(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	handled, err := imc.syncInPlaceUpgradedInstanceManagerPod(im, imu)
	c.Assert(err, IsNil)
	c.Assert(handled, Equals, false)
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
	pod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestExtraInstanceManagerImage}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Ready: false}}

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingWhenPodMissingDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestHandlePodRecreatesMissingPodDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	createDangerZoneSettingsForV2(c, lhClient, sIndexer)

	kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = kubeNodeIndexer.Add(kubeNode)
	c.Assert(err, IsNil)

	lhNode := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = lhNodeIndexer.Add(lhNode)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateUpgrading, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	err = imc.handlePod(im)
	c.Assert(err, IsNil)

	pod, err := kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), im.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(pod.Spec.Containers[0].Name, Equals, "instance-manager")
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingWhenPodDeletingDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
	now := metav1.Now()
	pod.DeletionTimestamp = &now

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingWhenPodFailedDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{Phase: corev1.PodFailed}, im.Name, im.Namespace, im.Spec.NodeID)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestAreDangerZoneSettingsSyncedToIMPodShortCircuitsForUpgrading(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateUpgrading, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	isSynced, unsynced, isPodDeletedOrNotRunning, areInstancesRunningInPod, err := imc.areDangerZoneSettingsSyncedToIMPod(im)
	c.Assert(err, IsNil)
	c.Assert(isSynced, Equals, true)
	c.Assert(len(unsynced), Equals, 0)
	c.Assert(isPodDeletedOrNotRunning, Equals, true)
	c.Assert(areInstancesRunningInPod, Equals, false)
}

func (s *TestSuite) TestSyncInstanceManagerUpgradeSkipsIMUCCreationWhenAutomaticUpgradeDisabled(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	for _, setting := range []*longhorn.Setting{
		newDefaultInstanceManagerImageSetting(),
		newSetting(string(types.SettingNameAllowV2InstanceManagerAutomaticUpgrade), "false"),
	} {
		created, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(sIndexer.Add(created), IsNil)
	}

	im := newInstanceManager("im-old-v2", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)

	err = imc.syncInstanceManagerUpgrade(im)
	c.Assert(err, IsNil)

	imucs, err := lhClient.LonghornV1beta2().InstanceManagerUpgradeControls(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(imucs.Items, HasLen, 0)
}

func (s *TestSuite) TestInstanceManagerUpgradeControlDoesNotStartNextNodeWhenAutomaticUpgradeDisabled(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

	imucController, err := newTestInstanceManagerUpgradeControlController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	setting := newSetting(string(types.SettingNameAllowV2InstanceManagerAutomaticUpgrade), "false")
	createdSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(sIndexer.Add(createdSetting), IsNil)

	imuc := &longhorn.InstanceManagerUpgradeControl{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.InstanceManagerUpgradeControlName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.InstanceManagerUpgradeControlSpec{
			TargetImage: TestInstanceManagerImage,
		},
		Status: longhorn.InstanceManagerUpgradeControlStatus{
			Nodes: map[string]longhorn.NodeUpgradeInfo{
				TestNode1: {
					State: longhorn.NodeUpgradeStatePending,
				},
			},
		},
	}

	active, err := imucController.reconcile(imuc, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(active, Equals, false)
	c.Assert(imuc.Status.CurrentNode, Equals, "")
	c.Assert(imuc.Status.Nodes[TestNode1].State, Equals, longhorn.NodeUpgradeStatePending)

	imus, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(imus.Items, HasLen, 0)
}
