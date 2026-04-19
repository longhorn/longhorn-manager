package controller

import (
	"context"
	"fmt"
	"time"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	TestNode3    = "test-node-name-3"
	TestOwnerID3 = TestNode3
	TestIP3      = "9.10.11.12"
	TestDaemon3  = "longhorn-manager-3"
)

// TestAutoBalanceNodeLeastEffort verifies that when replica auto-balance is set
// to least-effort and replicas are unevenly distributed across nodes, the
// getReplenishReplicasCount function returns a non-empty hardNodeAffinity
// pointing to an unused node. Before the fix, hardNodeAffinity was always ""
// for the least-effort path, causing the scheduler to potentially place the new
// replica on an already-overcrowded node, leading to an infinite
// rebuild-cleanup loop.
func (s *TestSuite) TestAutoBalanceNodeLeastEffort(c *C) {
	testAutoBalanceReplicasCount(c, longhorn.ReplicaAutoBalanceLeastEffort)
}

// TestAutoBalanceNodeBestEffort verifies the same behavior for best-effort.
// Before the fix, the best-effort path could return adjustCount > 0 with
// hardNodeAffinity "" when nCandidates was empty.
func (s *TestSuite) TestAutoBalanceNodeBestEffort(c *C) {
	testAutoBalanceReplicasCount(c, longhorn.ReplicaAutoBalanceBestEffort)
}

func testAutoBalanceReplicasCount(c *C, autoBalanceSetting longhorn.ReplicaAutoBalance) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	// Create daemon pods for each node
	for _, dp := range []struct {
		name, node, ip string
	}{
		{TestDaemon1, TestNode1, TestIP1},
		{TestDaemon2, TestNode2, TestIP2},
		{TestDaemon3, TestNode3, TestIP3},
	} {
		d := newDaemonPod(corev1.PodRunning, dp.name, TestNamespace, dp.node, dp.ip, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), d, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
	}

	// Create engine image deployed on all 3 nodes
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true
	engineImage.Status.NodeDeploymentMap[TestNode2] = true
	engineImage.Status.NodeDeploymentMap[TestNode3] = true
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	err = eiIndexer.Add(ei)
	c.Assert(err, IsNil)

	// Create 3 Longhorn nodes, all allow scheduling
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node3 := newNode(TestNode3, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	for _, node := range []*longhorn.Node{node1, node2, node3} {
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nIndexer.Add(n)
		c.Assert(err, IsNil)

		knode := newKubernetesNode(
			node.Name,
			corev1.ConditionTrue,
			corev1.ConditionFalse,
			corev1.ConditionFalse,
			corev1.ConditionFalse,
			corev1.ConditionFalse,
			corev1.ConditionTrue,
		)
		kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = knIndexer.Add(kn)
		c.Assert(err, IsNil)
	}

	// Create instance managers for all 3 nodes
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	for _, imInfo := range []struct {
		name, ownerID, nodeID, ip string
	}{
		{TestInstanceManagerName + "-" + TestNode1, TestOwnerID1, TestNode1, TestIP1},
		{TestInstanceManagerName + "-" + TestNode2, TestOwnerID2, TestNode2, TestIP2},
		{TestInstanceManagerName + "-" + TestNode3, TestOwnerID3, TestNode3, TestIP3},
	} {
		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(
				imInfo.name, longhorn.InstanceManagerStateRunning,
				imInfo.ownerID, imInfo.nodeID, imInfo.ip,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	// Create settings
	settingsToCreate := map[string]string{
		string(types.SettingNameDefaultEngineImage):               TestEngineImage,
		string(types.SettingNameDefaultInstanceManagerImage):      TestInstanceManagerImage,
		string(types.SettingNameReplicaAutoBalance):               string(autoBalanceSetting),
		string(types.SettingNameReplicaReplenishmentWaitInterval): "0",
	}
	for name, value := range settingsToCreate {
		s := initSettingsNameValue(name, value)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Create a volume with 3 replicas, attached and healthy
	volume := newVolume(TestVolumeName, 3)
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	volume.Status.CurrentImage = TestEngineImage

	engine := newEngineForVolume(volume)
	engine.Spec.NodeID = TestNode1
	engine.Spec.DesireState = longhorn.InstanceStateRunning
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.OwnerID = TestNode1
	engine.Status.IP = TestIP1
	engine.Status.StorageIP = TestIP1
	engine.Status.Port = 9501
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}

	// Create replicas: 2 on TestNode1 (overcrowded), 1 on TestNode2, 0 on TestNode3
	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1) // Second replica on same node!
	replica3 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)

	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
	}

	// Set all replicas as healthy and running
	for name, r := range replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		engine.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		engine.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
	}

	// Test: getReplenishReplicasCount should return a non-empty hardNodeAffinity
	// pointing to the unused node (TestNode3) when auto-balance detects imbalance.
	replenishCount, hardNodeAffinity := vc.getReplenishReplicasCount(volume, replicas, engine)

	fmt.Printf("  autoBalance=%v replenishCount=%d hardNodeAffinity=%q\n",
		autoBalanceSetting, replenishCount, hardNodeAffinity)

	// With 2 replicas on TestNode1 and 1 on TestNode2, and TestNode3 empty:
	// The auto-balance logic should detect this imbalance and request 1 new replica.
	c.Assert(replenishCount, Equals, 1,
		Commentf("Expected replenishCount=1 for imbalanced replica distribution with %v", autoBalanceSetting))

	// CRITICAL ASSERTION: hardNodeAffinity must NOT be empty.
	// Before the fix for issue #11730 and #12926, the least-effort path returned ("", adjustCount) which caused the
	// scheduler to place the new replica on any node (potentially the overcrowded one),
	// creating an infinite rebuild-cleanup loop.
	c.Assert(hardNodeAffinity, Not(Equals), "",
		Commentf("hardNodeAffinity must not be empty to prevent rebuild-cleanup loop with %v", autoBalanceSetting))

	// The target node should be TestNode3 (the only unused node)
	c.Assert(hardNodeAffinity, Equals, TestNode3,
		Commentf("hardNodeAffinity should point to the unused node %v with %v", TestNode3, autoBalanceSetting))
}

// TestAutoBalanceReplicasCountBalanced verifies that when replicas are already
// evenly distributed across nodes, getReplenishReplicasCount returns 0 and
// no rebalancing is triggered.
func (s *TestSuite) TestAutoBalanceReplicasCountBalanced(c *C) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	// Create daemon pods
	for _, dp := range []struct {
		name, node, ip string
	}{
		{TestDaemon1, TestNode1, TestIP1},
		{TestDaemon2, TestNode2, TestIP2},
		{TestDaemon3, TestNode3, TestIP3},
	} {
		d := newDaemonPod(corev1.PodRunning, dp.name, TestNamespace, dp.node, dp.ip, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), d, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
	}

	// Create engine image
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true
	engineImage.Status.NodeDeploymentMap[TestNode2] = true
	engineImage.Status.NodeDeploymentMap[TestNode3] = true
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	err = eiIndexer.Add(ei)
	c.Assert(err, IsNil)

	// Create 3 Longhorn nodes
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node3 := newNode(TestNode3, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	for _, node := range []*longhorn.Node{node1, node2, node3} {
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nIndexer.Add(n)
		c.Assert(err, IsNil)
		knode := newKubernetesNode(node.Name, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = knIndexer.Add(kn)
		c.Assert(err, IsNil)
	}

	// Create instance managers
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	for _, imInfo := range []struct {
		name, ownerID, nodeID, ip string
	}{
		{TestInstanceManagerName + "-" + TestNode1, TestOwnerID1, TestNode1, TestIP1},
		{TestInstanceManagerName + "-" + TestNode2, TestOwnerID2, TestNode2, TestIP2},
		{TestInstanceManagerName + "-" + TestNode3, TestOwnerID3, TestNode3, TestIP3},
	} {
		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(imInfo.name, longhorn.InstanceManagerStateRunning, imInfo.ownerID, imInfo.nodeID, imInfo.ip,
				map[string]longhorn.InstanceProcess{}, map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1, TestInstanceManagerImage, false),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	// Create settings with least-effort
	for name, value := range map[string]string{
		string(types.SettingNameDefaultEngineImage):               TestEngineImage,
		string(types.SettingNameDefaultInstanceManagerImage):      TestInstanceManagerImage,
		string(types.SettingNameReplicaAutoBalance):               string(longhorn.ReplicaAutoBalanceLeastEffort),
		string(types.SettingNameReplicaReplenishmentWaitInterval): "0",
	} {
		s := initSettingsNameValue(name, value)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Create volume with 3 replicas evenly distributed: 1 per node
	volume := newVolume(TestVolumeName, 3)
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	volume.Status.CurrentImage = TestEngineImage

	engine := newEngineForVolume(volume)
	engine.Spec.NodeID = TestNode1
	engine.Spec.DesireState = longhorn.InstanceStateRunning
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.OwnerID = TestNode1
	engine.Status.IP = TestIP1
	engine.Status.StorageIP = TestIP1
	engine.Status.Port = 9501
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}

	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)
	replica3 := newReplicaForVolume(volume, engine, TestNode3, TestDiskID1)

	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
	}

	for name, r := range replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		engine.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		engine.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
	}

	// When balanced (1 replica per node), no replenishment should be needed
	replenishCount, hardNodeAffinity := vc.getReplenishReplicasCount(volume, replicas, engine)

	c.Assert(replenishCount, Equals, 0,
		Commentf("Expected replenishCount=0 when replicas are already balanced"))
	c.Assert(hardNodeAffinity, Equals, "",
		Commentf("Expected empty hardNodeAffinity when no rebalance is needed"))
}

// TestAutoBalanceNoTargetNode verifies that when auto-balance detects an
// imbalance but no suitable target node exists (e.g., all nodes already have
// replicas), getReplenishReplicasCount returns 0 instead of returning
// adjustCount > 0 with an empty hardNodeAffinity. Before the fix, it would
// return (adjustCount, ""), triggering a pointless rebuild-cleanup loop.
func (s *TestSuite) TestAutoBalanceNoTargetNode(c *C) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	// Create daemon pods for 2 nodes only
	for _, dp := range []struct {
		name, node, ip string
	}{
		{TestDaemon1, TestNode1, TestIP1},
		{TestDaemon2, TestNode2, TestIP2},
	} {
		d := newDaemonPod(corev1.PodRunning, dp.name, TestNamespace, dp.node, dp.ip, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), d, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
	}

	// Engine image deployed on 2 nodes
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true
	engineImage.Status.NodeDeploymentMap[TestNode2] = true
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	err = eiIndexer.Add(ei)
	c.Assert(err, IsNil)

	// Create 2 Longhorn nodes (only 2 nodes available)
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	for _, node := range []*longhorn.Node{node1, node2} {
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nIndexer.Add(n)
		c.Assert(err, IsNil)
		knode := newKubernetesNode(node.Name, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = knIndexer.Add(kn)
		c.Assert(err, IsNil)
	}

	// Create instance managers
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	for _, imInfo := range []struct {
		name, ownerID, nodeID, ip string
	}{
		{TestInstanceManagerName + "-" + TestNode1, TestOwnerID1, TestNode1, TestIP1},
		{TestInstanceManagerName + "-" + TestNode2, TestOwnerID2, TestNode2, TestIP2},
	} {
		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(imInfo.name, longhorn.InstanceManagerStateRunning, imInfo.ownerID, imInfo.nodeID, imInfo.ip,
				map[string]longhorn.InstanceProcess{}, map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1, TestInstanceManagerImage, false),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	// Settings with best-effort
	for name, value := range map[string]string{
		string(types.SettingNameDefaultEngineImage):               TestEngineImage,
		string(types.SettingNameDefaultInstanceManagerImage):      TestInstanceManagerImage,
		string(types.SettingNameReplicaAutoBalance):               string(longhorn.ReplicaAutoBalanceBestEffort),
		string(types.SettingNameReplicaReplenishmentWaitInterval): "0",
	} {
		s := initSettingsNameValue(name, value)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Volume with 3 replicas but only 2 nodes: 2 on node1, 1 on node2
	// All nodes already have replicas, so there's no unused node to target
	volume := newVolume(TestVolumeName, 3)
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	volume.Status.CurrentImage = TestEngineImage

	engine := newEngineForVolume(volume)
	engine.Spec.NodeID = TestNode1
	engine.Spec.DesireState = longhorn.InstanceStateRunning
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.OwnerID = TestNode1
	engine.Status.IP = TestIP1
	engine.Status.StorageIP = TestIP1
	engine.Status.Port = 9501
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}

	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1) // 2nd on node1
	replica3 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)

	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
	}

	for name, r := range replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		engine.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		engine.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
	}

	// After the fix, when there is no unused node to target, best-effort should not trigger rebalance.
	//
	// Before the fix, `getReplicaCountForAutoBalanceLeastEffort` or `getReplicaCountForAutoBalanceBestEffort`
	// in `getReplenishReplicasCount` could return (adjustCount > 0, hardNodeAffinity = "")
	// which may trigger a pointless rebuild.
	replenishCount, hardNodeAffinity := vc.getReplenishReplicasCount(volume, replicas, engine)
	c.Assert(replenishCount, Equals, 0,
		Commentf("Expected replenishCount=0 when all nodes already have replicas"))
	c.Assert(hardNodeAffinity, Equals, "",
		Commentf("Expected empty hardNodeAffinity when no target node is available"))
}

// TestAutoBalanceUnstableNodeRebuildLoop reproduces the exact rebuild-cleanup
// loop (issue #11730 and #12926) observed in production:
//
//  1. Zone/Node auto-balance creates a replica on a node whose kube Ready transition
//     time is 30+ minutes later than other nodes (the "unstable" node).
//  2. The replica rebuilds successfully → volume has N+1 healthy replicas.
//  3. cleanupExtraHealthyReplicas → cleanupAutoBalancedReplicas is called.
//     - Before the fix, the cleanup logic deleted the sole replica in zone-b
//     on the "unstable" node FIRST, causing auto-balance to immediately recreate it → infinite loop.
//     - After the fix, cleanupReplicaInUnstableEnv deleted the extra replicas from
//     overcrowded nodes or zones (the zone-a replicas, since zone-a has 2 while zone-b has 1) FIRST,
//     leaving the sole replica in zone-b untouched and the scheduling balanced.
func (s *TestSuite) TestAutoBalanceUnstableNodeRebuildLoop(c *C) {
	// ...existing code... (setup unchanged through replica creation)
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	// Create daemon pods
	for _, dp := range []struct {
		name, node, ip string
	}{
		{TestDaemon1, TestNode1, TestIP1},
		{TestDaemon2, TestNode2, TestIP2},
		{TestDaemon3, TestNode3, TestIP3},
	} {
		d := newDaemonPod(corev1.PodRunning, dp.name, TestNamespace, dp.node, dp.ip, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), d, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
	}

	// Create engine image
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true
	engineImage.Status.NodeDeploymentMap[TestNode2] = true
	engineImage.Status.NodeDeploymentMap[TestNode3] = true
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	err = eiIndexer.Add(ei)
	c.Assert(err, IsNil)

	// Create 3 Longhorn nodes in 2 zones:
	//   Node1 (zone "zone-a"), Node2 (zone "zone-a"), Node3 (zone "zone-b")
	// Node3 is the "unstable" node with a much later Ready transition time.
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node1.Status.Zone = "zone-a"
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2.Status.Zone = "zone-a"
	node3 := newNode(TestNode3, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node3.Status.Zone = "zone-b" // only node in zone-b

	for _, node := range []*longhorn.Node{node1, node2, node3} {
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nIndexer.Add(n)
		c.Assert(err, IsNil)
	}

	// Create kube nodes with different Ready transition times:
	//   Node1, Node2: became ready at t=0 (stable)
	//   Node3: became ready at t=0+2h (unstable — 2 hours later, well over 30 min threshold)
	stableReadyTime := metav1.NewTime(metav1.Now().Add(-24 * time.Hour))
	unstableReadyTime := metav1.NewTime(stableReadyTime.Add(2 * time.Hour)) // 2h later

	knode1 := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	setKubeNodeReadyTransitionTime(knode1, stableReadyTime)
	knode2 := newKubernetesNode(TestNode2, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	setKubeNodeReadyTransitionTime(knode2, stableReadyTime)
	knode3 := newKubernetesNode(TestNode3, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	setKubeNodeReadyTransitionTime(knode3, unstableReadyTime) // "unstable"

	for _, knode := range []*corev1.Node{knode1, knode2, knode3} {
		kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = knIndexer.Add(kn)
		c.Assert(err, IsNil)
	}

	// Create instance managers
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	for _, imInfo := range []struct {
		name, ownerID, nodeID, ip string
	}{
		{TestInstanceManagerName + "-" + TestNode1, TestOwnerID1, TestNode1, TestIP1},
		{TestInstanceManagerName + "-" + TestNode2, TestOwnerID2, TestNode2, TestIP2},
		{TestInstanceManagerName + "-" + TestNode3, TestOwnerID3, TestNode3, TestIP3},
	} {
		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(imInfo.name, longhorn.InstanceManagerStateRunning, imInfo.ownerID, imInfo.nodeID, imInfo.ip,
				map[string]longhorn.InstanceProcess{}, map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1, TestInstanceManagerImage, false),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	// Settings
	for name, value := range map[string]string{
		string(types.SettingNameDefaultEngineImage):               TestEngineImage,
		string(types.SettingNameDefaultInstanceManagerImage):      TestInstanceManagerImage,
		string(types.SettingNameReplicaAutoBalance):               string(longhorn.ReplicaAutoBalanceBestEffort),
		string(types.SettingNameReplicaReplenishmentWaitInterval): "0",
	} {
		s := initSettingsNameValue(name, value)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Create volume with 3 replicas.
	// Simulate the state AFTER auto-balance has added a new replica on the
	// unstable node (Node3 in zone-b) and it finished rebuilding:
	//   - replica1 on Node1 (zone-a) — original
	//   - replica2 on Node2 (zone-a) — original
	//   - replica3 on Node3 (zone-b) — just rebuilt by auto-balance (extra)
	// healthyCount=3 > NumberOfReplicas=2 → cleanupExtraHealthyReplicas fires
	volume := newVolume(TestVolumeName, 2)
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	volume.Status.CurrentImage = TestEngineImage

	engine := newEngineForVolume(volume)
	engine.Spec.NodeID = TestNode1
	engine.Spec.DesireState = longhorn.InstanceStateRunning
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.OwnerID = TestNode1
	engine.Status.IP = TestIP1
	engine.Status.StorageIP = TestIP1
	engine.Status.Port = 9501
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}

	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1) // zone-a
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1) // zone-a
	replica3 := newReplicaForVolume(volume, engine, TestNode3, TestDiskID1) // zone-b (unstable node)

	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
	}

	for name, r := range replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		engine.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		engine.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
	}

	replica1.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode1
	replica2.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode2
	replica3.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode3

	// Need to create replicas in the datastore for deleteReplica to work
	rIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	for _, r := range replicas {
		rObj, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), r, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = rIndexer.Add(rObj)
		c.Assert(err, IsNil)
	}

	// Call cleanupAutoBalancedReplicas (the full refactored function) with the 3 replicas.
	// The expected behavior with the fix is:
	//   Step 1: not-ready env — no nodes are down, skipped.
	//   Step 2: overcrowded node/zone — zone-a has 2 replicas (replica1 and replica2),
	//   so these 2 replicas are the preferred deletion candidates. Replica3 in zone-b
	//   or the only unstable node is NOT a candidate for deletion.
	//   Step 3 & 4: unstable env — never reached because step 2 handled it.
	//   → replica3 in zone-b is preserved, no loop.
	cleaned, err := vc.cleanupAutoBalancedReplicas(volume, replicas)
	c.Assert(err, IsNil)
	c.Assert(cleaned, Equals, true,
		Commentf("cleanupAutoBalancedReplicas should have deleted one replica"))

	fmt.Printf("  unstable-node-loop: cleaned=%v\n", cleaned)

	// Verify replica3 (zone-b, unstable node) was NOT deleted
	c.Assert(replicas[replica3.Name], NotNil,
		Commentf("replica3 on the unstable node (sole replica in zone-b) should not have been deleted"))

	// Verify one of the zone-a replicas WAS deleted (the overcrowded zone)
	zoneADeleted := replicas[replica1.Name] == nil || replicas[replica2.Name] == nil
	c.Assert(zoneADeleted, Equals, true,
		Commentf("one replica from zone-a (overcrowded zone with 2 replicas) should have been deleted"))
}

// TestAutoBalanceUnstableNodeMixedCase tests the mixed case where the unstable
// replica is in the overcrowded zone. Step 2 should prefer to delete the
// unstable replica from the overcrowded candidates, achieving both goals:
// removing excess from the overcrowded zone AND cleaning up the unstable data.
//
// Setup:
//
//	Zone-a: Node1 (r1, stable), Node2 (r2, UNSTABLE) — 2 replicas
//	Zone-b: Node3 (r3, stable) — 1 replica
//	NumberOfReplicas = 2 → 3 healthy, 1 extra
//
// Expected: r2 is deleted (unstable + overcrowded), r1 and r3 preserved.
func (s *TestSuite) TestAutoBalanceUnstableNodeMixedCase(c *C) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	// Create daemon pods
	for _, dp := range []struct {
		name, node, ip string
	}{
		{TestDaemon1, TestNode1, TestIP1},
		{TestDaemon2, TestNode2, TestIP2},
		{TestDaemon3, TestNode3, TestIP3},
	} {
		d := newDaemonPod(corev1.PodRunning, dp.name, TestNamespace, dp.node, dp.ip, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), d, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
	}

	// Create engine image
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true
	engineImage.Status.NodeDeploymentMap[TestNode2] = true
	engineImage.Status.NodeDeploymentMap[TestNode3] = true
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	err = eiIndexer.Add(ei)
	c.Assert(err, IsNil)

	// Zone-a: Node1 (stable), Node2 (UNSTABLE)
	// Zone-b: Node3 (stable)
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node1.Status.Zone = "zone-a"
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2.Status.Zone = "zone-a" // unstable node, but in the overcrowded zone
	node3 := newNode(TestNode3, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node3.Status.Zone = "zone-b"

	for _, node := range []*longhorn.Node{node1, node2, node3} {
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nIndexer.Add(n)
		c.Assert(err, IsNil)
	}

	// Kube nodes: Node2 is unstable (Ready transition 2h later)
	stableReadyTime := metav1.NewTime(metav1.Now().Add(-24 * time.Hour))
	unstableReadyTime := metav1.NewTime(stableReadyTime.Add(2 * time.Hour))

	knode1 := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	setKubeNodeReadyTransitionTime(knode1, stableReadyTime)
	knode2 := newKubernetesNode(TestNode2, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	setKubeNodeReadyTransitionTime(knode2, unstableReadyTime) // UNSTABLE
	knode3 := newKubernetesNode(TestNode3, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	setKubeNodeReadyTransitionTime(knode3, stableReadyTime)

	for _, knode := range []*corev1.Node{knode1, knode2, knode3} {
		kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = knIndexer.Add(kn)
		c.Assert(err, IsNil)
	}

	// Create instance managers
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	for _, imInfo := range []struct {
		name, ownerID, nodeID, ip string
	}{
		{TestInstanceManagerName + "-" + TestNode1, TestOwnerID1, TestNode1, TestIP1},
		{TestInstanceManagerName + "-" + TestNode2, TestOwnerID2, TestNode2, TestIP2},
		{TestInstanceManagerName + "-" + TestNode3, TestOwnerID3, TestNode3, TestIP3},
	} {
		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(imInfo.name, longhorn.InstanceManagerStateRunning, imInfo.ownerID, imInfo.nodeID, imInfo.ip,
				map[string]longhorn.InstanceProcess{}, map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1, TestInstanceManagerImage, false),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	// Settings
	for name, value := range map[string]string{
		string(types.SettingNameDefaultEngineImage):               TestEngineImage,
		string(types.SettingNameDefaultInstanceManagerImage):      TestInstanceManagerImage,
		string(types.SettingNameReplicaAutoBalance):               string(longhorn.ReplicaAutoBalanceBestEffort),
		string(types.SettingNameReplicaReplenishmentWaitInterval): "0",
	} {
		s := initSettingsNameValue(name, value)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Volume with 2 replicas, 3 healthy (one extra):
	//   r1 on Node1 (zone-a, stable)
	//   r2 on Node2 (zone-a, UNSTABLE)
	//   r3 on Node3 (zone-b, stable)
	volume := newVolume(TestVolumeName, 2)
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	volume.Status.CurrentImage = TestEngineImage

	engine := newEngineForVolume(volume)
	engine.Spec.NodeID = TestNode1
	engine.Spec.DesireState = longhorn.InstanceStateRunning
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.OwnerID = TestNode1
	engine.Status.IP = TestIP1
	engine.Status.StorageIP = TestIP1
	engine.Status.Port = 9501
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}

	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1) // zone-a, stable
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1) // zone-a, UNSTABLE
	replica3 := newReplicaForVolume(volume, engine, TestNode3, TestDiskID1) // zone-b, stable

	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
	}

	for name, r := range replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		engine.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		engine.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
	}

	replica1.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode1
	replica2.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode2
	replica3.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode3

	// Create replicas in the datastore for deleteReplica to work
	rIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	for _, r := range replicas {
		rObj, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), r, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = rIndexer.Add(rObj)
		c.Assert(err, IsNil)
	}

	cleaned, err := vc.cleanupAutoBalancedReplicas(volume, replicas)
	c.Assert(err, IsNil)
	c.Assert(cleaned, Equals, true)

	// replica2 (unstable + in overcrowded zone) should be the one deleted
	c.Assert(replicas[replica2.Name], IsNil,
		Commentf("replica2 (unstable node in overcrowded zone with 2 replicas) should have been deleted"))

	// replica1 (stable, zone-a) and replica3 (stable, zone-b) should survive
	c.Assert(replicas[replica1.Name], NotNil,
		Commentf("replica1 (stable node in zone-a) should have been preserved"))
	c.Assert(replicas[replica3.Name], NotNil,
		Commentf("replica3 (stable node in zone-b) should have been preserved"))
}

// setKubeNodeReadyTransitionTime sets the LastTransitionTime on the NodeReady
// condition of a kube node.
func setKubeNodeReadyTransitionTime(node *corev1.Node, t metav1.Time) {
	for i, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			node.Status.Conditions[i].LastTransitionTime = t
			return
		}
	}
}

// TestDataLocalityCleanupWhenEngineNodeOvercrowded verifies that
// cleanupDataLocalityReplicas correctly deletes a non-local replica even when
// the engine node is the most overcrowded node.
//
// Before the fix, cleanupDataLocalityReplicas passed all replicas (including
// local ones) to getPreferredOvercrowdedReplicaCandidatesForDeletion. When the
// engine node was overcrowded, the candidate list contained only local replicas
// which the deletion loop skipped — resulting in no deletion at all.
//
// Setup:
//
//	Engine on Node1.
//	Node1: r1, r2 (both local, overcrowded)
//	Node2: r3 (non-local)
//	NumberOfReplicas = 2, DataLocality = best-effort
//	3 healthy replicas → 1 extra
//
// Expected: r3 (non-local) is deleted.
func (s *TestSuite) TestDataLocalityCleanupWhenEngineNodeOvercrowded(c *C) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset()
	extensionsClient := apiextensionsfake.NewSimpleClientset()

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	// Create daemon pods
	for _, dp := range []struct {
		name, node, ip string
	}{
		{TestDaemon1, TestNode1, TestIP1},
		{TestDaemon2, TestNode2, TestIP2},
	} {
		d := newDaemonPod(corev1.PodRunning, dp.name, TestNamespace, dp.node, dp.ip, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), d, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
	}

	// Create engine image
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true
	engineImage.Status.NodeDeploymentMap[TestNode2] = true
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	err = eiIndexer.Add(ei)
	c.Assert(err, IsNil)

	// Create Longhorn nodes
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	for _, node := range []*longhorn.Node{node1, node2} {
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nIndexer.Add(n)
		c.Assert(err, IsNil)
		knode := newKubernetesNode(node.Name, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = knIndexer.Add(kn)
		c.Assert(err, IsNil)
	}

	// Create instance managers
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	for _, imInfo := range []struct {
		name, ownerID, nodeID, ip string
	}{
		{TestInstanceManagerName + "-" + TestNode1, TestOwnerID1, TestNode1, TestIP1},
		{TestInstanceManagerName + "-" + TestNode2, TestOwnerID2, TestNode2, TestIP2},
	} {
		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(imInfo.name, longhorn.InstanceManagerStateRunning, imInfo.ownerID, imInfo.nodeID, imInfo.ip,
				map[string]longhorn.InstanceProcess{}, map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1, TestInstanceManagerImage, false),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	// Settings
	for name, value := range map[string]string{
		string(types.SettingNameDefaultEngineImage):               TestEngineImage,
		string(types.SettingNameDefaultInstanceManagerImage):      TestInstanceManagerImage,
		string(types.SettingNameReplicaAutoBalance):               string(longhorn.ReplicaAutoBalanceDisabled),
		string(types.SettingNameReplicaReplenishmentWaitInterval): "0",
	} {
		s := initSettingsNameValue(name, value)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Volume with DataLocality = best-effort, 2 replicas desired.
	// Engine on Node1, which has 2 replicas (overcrowded). Node2 has 1.
	volume := newVolume(TestVolumeName, 2)
	volume.Spec.DataLocality = longhorn.DataLocalityBestEffort
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	volume.Status.CurrentImage = TestEngineImage

	engine := newEngineForVolume(volume)
	engine.Spec.NodeID = TestNode1
	engine.Spec.DesireState = longhorn.InstanceStateRunning
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.OwnerID = TestNode1
	engine.Status.IP = TestIP1
	engine.Status.StorageIP = TestIP1
	engine.Status.Port = 9501
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}

	// r1, r2 on Node1 (engine node, overcrowded), r3 on Node2 (non-local)
	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica3 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)

	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
	}

	for name, r := range replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		engine.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		engine.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
	}

	replica1.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode1
	replica2.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode1
	replica3.Status.InstanceManagerName = TestInstanceManagerName + "-" + TestNode2

	// Create replicas in the datastore for deleteReplica to work
	rIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	for _, r := range replicas {
		rObj, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), r, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = rIndexer.Add(rObj)
		c.Assert(err, IsNil)
	}

	// Call cleanupDataLocalityReplicas.
	// hasLocalReplicaOnSameNodeAsEngine is true (r1 and r2 are on engine node).
	// The function should delete r3 (the non-local replica on Node2).
	//
	// Bug scenario (if passing rs instead of nonLocalReplicaMap):
	//   getPreferredOvercrowdedReplicaCandidatesForDeletion(rs) sees Node1
	//   with 2 replicas → returns [r1, r2] → both on engine node → deletion
	//   loop skips both → cleaned=false. WRONG.
	cleaned, err := vc.cleanupDataLocalityReplicas(volume, engine, replicas)
	c.Assert(err, IsNil)
	c.Assert(cleaned, Equals, true,
		Commentf("cleanupDataLocalityReplicas should have deleted the non-local replica"))

	// r3 (non-local, on Node2) should be deleted
	c.Assert(replicas[replica3.Name], IsNil,
		Commentf("replica3 (non-local on Node2) should have been deleted"))

	// r1 and r2 (local, on engine Node1) should be preserved
	c.Assert(replicas[replica1.Name], NotNil,
		Commentf("replica1 (local on engine Node1) should have been preserved"))
	c.Assert(replicas[replica2.Name], NotNil,
		Commentf("replica2 (local on engine Node1) should have been preserved"))
}

// TestAutoBalanceCountingConsistency verifies that the add side
// (getReplenishReplicasCount / getReplicaCountForAutoBalanceNode) and cleanup
// side (getPreferredOvercrowdedReplicaCandidatesForDeletion) agree on replica
// counts when a healthy+active replica is in a non-Running state (e.g.,
// Starting).
//
// One Example Scenario:
//   - Node A: 2 replicas (r1 Running + r2 Starting but healthy+active)
//   - Node B: 1 replica (r3 Running)
//   - Node C: empty
//
// Before the fix:
//   - Add side used `CurrentState == Running` → undercounted Node A (saw 1)
//   - Cleanup side counted ALL replicas → overcounted Node A (saw 2+)
//   - This mismatch caused the add side to think Node A was balanced while the
//     cleanup side thought it was overcrowded, contributing to the loop.
//
// After the fix, both sides use `isHealthyAndActiveReplica`, so they agree:
//   - Add side: A=2, B=1, C=0 → imbalanced, adjustCount=1, target=C
//   - Cleanup side: A is overcrowded (2 replicas) → candidates include r1,r2
func (s *TestSuite) TestAutoBalanceCountingConsistency(c *C) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset()
	extensionsClient := apiextensionsfake.NewSimpleClientset()

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	// Create daemon pods for 3 nodes
	for _, dp := range []struct {
		name, node, ip string
	}{
		{TestDaemon1, TestNode1, TestIP1},
		{TestDaemon2, TestNode2, TestIP2},
		{TestDaemon3, TestNode3, TestIP3},
	} {
		d := newDaemonPod(corev1.PodRunning, dp.name, TestNamespace, dp.node, dp.ip, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), d, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
	}

	// Create engine image
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	engineImage.Status.NodeDeploymentMap[TestNode1] = true
	engineImage.Status.NodeDeploymentMap[TestNode2] = true
	engineImage.Status.NodeDeploymentMap[TestNode3] = true
	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
	err = eiIndexer.Add(ei)
	c.Assert(err, IsNil)

	// Create 3 Longhorn nodes
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node3 := newNode(TestNode3, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	for _, node := range []*longhorn.Node{node1, node2, node3} {
		n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nIndexer.Add(n)
		c.Assert(err, IsNil)
		knode := newKubernetesNode(node.Name, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = knIndexer.Add(kn)
		c.Assert(err, IsNil)
	}

	// Create instance managers
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	for _, imInfo := range []struct {
		name, ownerID, nodeID, ip string
	}{
		{TestInstanceManagerName + "-" + TestNode1, TestOwnerID1, TestNode1, TestIP1},
		{TestInstanceManagerName + "-" + TestNode2, TestOwnerID2, TestNode2, TestIP2},
		{TestInstanceManagerName + "-" + TestNode3, TestOwnerID3, TestNode3, TestIP3},
	} {
		im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(imInfo.name, longhorn.InstanceManagerStateRunning, imInfo.ownerID, imInfo.nodeID, imInfo.ip,
				map[string]longhorn.InstanceProcess{}, map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1, TestInstanceManagerImage, false),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}

	// Settings: least-effort auto-balance
	for name, value := range map[string]string{
		string(types.SettingNameDefaultEngineImage):               TestEngineImage,
		string(types.SettingNameDefaultInstanceManagerImage):      TestInstanceManagerImage,
		string(types.SettingNameReplicaAutoBalance):               string(longhorn.ReplicaAutoBalanceLeastEffort),
		string(types.SettingNameReplicaReplenishmentWaitInterval): "0",
	} {
		s := initSettingsNameValue(name, value)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Volume with 3 replicas:
	//   r1 on Node1 (healthy+active, Running)
	//   r2 on Node1 (healthy+active, Starting — NOT Running)
	//   r3 on Node2 (healthy+active, Running)
	//   Node3 is empty
	volume := newVolume(TestVolumeName, 3)
	volume.Status.State = longhorn.VolumeStateAttached
	volume.Status.CurrentNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	volume.Status.CurrentImage = TestEngineImage

	engine := newEngineForVolume(volume)
	engine.Spec.NodeID = TestNode1
	engine.Spec.DesireState = longhorn.InstanceStateRunning
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.OwnerID = TestNode1
	engine.Status.IP = TestIP1
	engine.Status.StorageIP = TestIP1
	engine.Status.Port = 9501
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}

	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1) // 2nd on same node
	replica3 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)

	replicas := map[string]*longhorn.Replica{
		replica1.Name: replica1,
		replica2.Name: replica2,
		replica3.Name: replica3,
	}

	for name, r := range replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		engine.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		engine.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
	}

	// Key: set r2 to Starting (healthy+active but NOT Running).
	// Before the fix, the add side (Running filter) would skip r2 and see
	// Node1 as having only 1 replica → balanced with Node2 (1 each).
	// After the fix, both sides count r2 → Node1 has 2, Node2 has 1.
	replica2.Status.CurrentState = longhorn.InstanceStateStarting

	// ---- Verify add side ----
	// getReplenishReplicasCount should see Node1=2 (r1+r2), Node2=1, Node3=0
	// and request 1 new replica targeting Node3.
	replenishCount, hardNodeAffinity := vc.getReplenishReplicasCount(volume, replicas, engine)

	c.Assert(replenishCount, Equals, 1,
		Commentf("Add side should detect imbalance: Node1=2, Node2=1, Node3=0"))
	c.Assert(hardNodeAffinity, Equals, TestNode3,
		Commentf("Add side should target the empty node (Node3)"))

	// ---- Verify cleanup side ----
	// getPreferredOvercrowdedReplicaCandidatesForDeletion should see Node1
	// as overcrowded (2 replicas including the Starting one).
	candidates, err := vc.getPreferredOvercrowdedReplicaCandidatesForDeletion(replicas)
	c.Assert(err, IsNil)

	// Node1 has 2 replicas → overcrowded → candidates should include r1 and r2
	candidateSet := make(map[string]bool, len(candidates))
	for _, name := range candidates {
		candidateSet[name] = true
	}
	c.Assert(candidateSet[replica1.Name], Equals, true,
		Commentf("Cleanup side should include r1 (on overcrowded Node1) in candidates"))
	c.Assert(candidateSet[replica2.Name], Equals, true,
		Commentf("Cleanup side should include r2 (Starting but healthy+active, on overcrowded Node1) in candidates"))
	c.Assert(candidateSet[replica3.Name], Equals, false,
		Commentf("Cleanup side should NOT include r3 (sole replica on Node2) in candidates"))

	// ---- Verify failed replicas are excluded from both sides ----
	// Add a failed replica on Node2 to verify it's not counted by either side.
	failedReplica := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)
	failedReplica.Spec.FailedAt = getTestNow()
	failedReplica.Status.CurrentState = longhorn.InstanceStateStopped
	replicas[failedReplica.Name] = failedReplica

	// Add side: failed replica should NOT be counted → Node2 still has 1
	replenishCount2, hardNodeAffinity2 := vc.getReplenishReplicasCount(volume, replicas, engine)
	c.Assert(replenishCount2, Equals, 1,
		Commentf("Add side should still detect same imbalance after adding failed replica"))
	c.Assert(hardNodeAffinity2, Equals, TestNode3,
		Commentf("Add side should still target Node3 after adding failed replica"))

	// Cleanup side: failed replica should NOT be counted → Node2 still has 1
	candidates2, err := vc.getPreferredOvercrowdedReplicaCandidatesForDeletion(replicas)
	c.Assert(err, IsNil)
	candidateSet2 := make(map[string]bool, len(candidates2))
	for _, name := range candidates2 {
		candidateSet2[name] = true
	}
	c.Assert(candidateSet2[failedReplica.Name], Equals, false,
		Commentf("Cleanup side should NOT include the failed replica in candidates"))
}
