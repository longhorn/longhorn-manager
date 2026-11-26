package controller

import (
	"context"
	"fmt"
	"time"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testBIPlacementNode3         = "test-node-name-3"
	testBIPlacementSelectorKey   = "longhorn-system-managed"
	testBIPlacementSelectorValue = "true"
	testBIPlacementSelector      = testBIPlacementSelectorKey + ":" + testBIPlacementSelectorValue
)

type backingImagePlacementTestEnv struct {
	c *C

	kubeClient *fake.Clientset
	lhClient   *lhfake.Clientset
	bic        *BackingImageController

	kubeNodeIndexer cache.Indexer
	sIndexer        cache.Indexer
	lhNodeIndexer   cache.Indexer
	biIndexer       cache.Indexer
	bidsIndexer     cache.Indexer
}

func newBackingImagePlacementTestEnv(c *C, nodeSelectorSettingValue string) *backingImagePlacementTestEnv {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	env := &backingImagePlacementTestEnv{
		c:               c,
		kubeClient:      kubeClient,
		lhClient:        lhClient,
		kubeNodeIndexer: informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer(),
		sIndexer:        informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer(),
		lhNodeIndexer:   informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer(),
		biIndexer:       informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer(),
		bidsIndexer:     informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer(),
	}

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)
	env.bic = bic

	for name, value := range map[types.SettingName]string{
		types.SettingNameSystemManagedComponentsNodeSelector:             nodeSelectorSettingValue,
		types.SettingNameTaintToleration:                                 "",
		types.SettingNameAllowEmptyNodeSelectorVolume:                    "true",
		types.SettingNameAllowEmptyDiskSelectorVolume:                    "true",
		types.SettingNameBackingImageRecoveryWaitInterval:                "0",
		types.SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit: "1",
	} {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(name), value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(env.sIndexer.Add(setting), IsNil)
	}

	return env
}

// addNode registers the Longhorn node and its Kubernetes node. A selected node gets the label
// matching testBIPlacementSelector.
func (env *backingImagePlacementTestEnv) addNode(lhNode *longhorn.Node, selected bool) {
	c := env.c
	lhNode, err := env.lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.lhNodeIndexer.Add(lhNode), IsNil)

	kubeNode := newKubernetesNode(lhNode.Name,
		corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
		corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	kubeNode.Labels = map[string]string{"kubernetes.io/hostname": lhNode.Name}
	if selected {
		kubeNode.Labels[testBIPlacementSelectorKey] = testBIPlacementSelectorValue
	}
	_, err = env.kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.kubeNodeIndexer.Add(kubeNode), IsNil)
}

func (env *backingImagePlacementTestEnv) addBackingImage(bi *longhorn.BackingImage) *longhorn.BackingImage {
	c := env.c
	bi, err := env.lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.biIndexer.Add(bi), IsNil)
	return bi
}

func (env *backingImagePlacementTestEnv) addBackingImageDataSource(bids *longhorn.BackingImageDataSource) {
	c := env.c
	bids, err := env.lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(env.bidsIndexer.Add(bids), IsNil)
}

func (env *backingImagePlacementTestEnv) getBackingImageDataSource(name string) *longhorn.BackingImageDataSource {
	bids, err := env.lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), name, metav1.GetOptions{})
	env.c.Assert(err, IsNil)
	return bids
}

func newBackingImageDataSourceForPlacementTest(nodeID, diskUUID string, fileTransferred bool) *longhorn.BackingImageDataSource {
	return &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:          nodeID,
			DiskUUID:        diskUUID,
			DiskPath:        TestDefaultDataPath,
			SourceType:      longhorn.BackingImageDataSourceTypeDownload,
			Parameters:      map[string]string{},
			FileTransferred: fileTransferred,
		},
	}
}

// addDiskForBackingImagePlacementTest adds a ready and schedulable disk to the node.
func addDiskForBackingImagePlacementTest(node *longhorn.Node, diskName, diskUUID string, diskType longhorn.DiskType) *longhorn.Node {
	diskSpec := node.Spec.Disks[TestDiskID1]
	diskSpec.Type = diskType
	diskSpec.Path = TestDefaultDataPath + "/" + diskName
	node.Spec.Disks[diskName] = diskSpec

	diskStatus := node.Status.DiskStatus[TestDiskID1].DeepCopy()
	diskStatus.DiskUUID = diskUUID
	diskStatus.Type = diskType
	diskStatus.DiskPath = diskSpec.Path
	node.Status.DiskStatus[diskName] = diskStatus
	return node
}

func (s *TestSuite) TestBackingImageDataSourceRecoveryRedownloadsOnSelectedNode(c *C) {
	for _, dataEngine := range []longhorn.DataEngineType{longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
		comment := Commentf("data engine %v", dataEngine)
		env := newBackingImagePlacementTestEnv(c, testBIPlacementSelector)
		env.addNode(newNodeForBackingImageNodeSelectorTest(TestNode1, TestDiskID1), false)
		env.addNode(newNodeForBackingImageNodeSelectorTest(TestNode2, TestDiskID2), true)

		bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
		bi.Spec.DataEngine = dataEngine
		bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
			TestDiskID1: {DataEngine: dataEngine},
		}
		bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
			TestDiskID1: {
				State:                   longhorn.BackingImageStateFailed,
				LastStateTransitionTime: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
			},
		}
		if types.IsDataEngineV2(dataEngine) {
			bi.Status.V2FirstCopyDisk = TestDiskID1
			bi.Status.V2FirstCopyStatus = longhorn.BackingImageStateReady
		}
		bi = env.addBackingImage(bi)
		env.addBackingImageDataSource(newBackingImageDataSourceForPlacementTest(TestNode1, TestDiskID1, true))

		err := env.bic.handleBackingImageDataSource(bi)
		c.Assert(err, IsNil, comment)

		// The data source restarts the file preparation on the node matching the selector.
		bids := env.getBackingImageDataSource(TestBackingImage)
		c.Assert(bids.Spec.FileTransferred, Equals, false, comment)
		c.Assert(bids.Spec.NodeID, Equals, TestNode2, comment)
		c.Assert(bids.Spec.DiskUUID, Equals, TestDiskID2, comment)
		if types.IsDataEngineV2(dataEngine) {
			c.Assert(bi.Status.V2FirstCopyDisk, Equals, "", comment)
			c.Assert(bi.Status.V2FirstCopyStatus, Equals, longhorn.BackingImageStatePending, comment)
		}
	}
}

func (s *TestSuite) TestBackingImageDataSourceReassignedToSelectedNodeWhenDiskNotReady(c *C) {
	env := newBackingImagePlacementTestEnv(c, testBIPlacementSelector)
	env.addNode(newNodeForBackingImageNodeSelectorTest(TestNode1, TestDiskID1), false)
	env.addNode(newNodeForBackingImageNodeSelectorTest(TestNode2, TestDiskID2), true)

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
	bi = env.addBackingImage(bi)
	// The assigned disk no longer belongs to any node before the first file is transferred.
	env.addBackingImageDataSource(newBackingImageDataSourceForPlacementTest(testBIPlacementNode3, TestDiskID3, false))

	err := env.bic.handleBackingImageDataSource(bi)
	c.Assert(err, IsNil)

	bids := env.getBackingImageDataSource(TestBackingImage)
	c.Assert(bids.Spec.FileTransferred, Equals, false)
	c.Assert(bids.Spec.NodeID, Equals, TestNode2)
	c.Assert(bids.Spec.DiskUUID, Equals, TestDiskID2)
}

func (s *TestSuite) TestBackingImageCopyReplenishmentPrefersNodeWithoutCopy(c *C) {
	testCases := map[string]struct {
		withSecondNode   bool
		expectedDiskUUID string
	}{
		"new copy goes to the node without a copy": {
			withSecondNode:   true,
			expectedDiskUUID: TestDiskID2,
		},
		"new copy falls back to the node holding a copy": {
			withSecondNode:   false,
			expectedDiskUUID: TestDiskID3,
		},
	}

	for name, tc := range testCases {
		comment := Commentf("test %q", name)
		env := newBackingImagePlacementTestEnv(c, "")
		node1 := newNodeForBackingImageNodeSelectorTest(TestNode1, TestDiskID1)
		node1 = addDiskForBackingImagePlacementTest(node1, "disk-3", TestDiskID3, longhorn.DiskTypeFilesystem)
		env.addNode(node1, true)
		if tc.withSecondNode {
			env.addNode(newNodeForBackingImageNodeSelectorTest(TestNode2, TestDiskID2), true)
		}
		env.addBackingImageDataSource(newBackingImageDataSourceForPlacementTest(TestNode1, TestDiskID1, true))

		bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
		bi.Spec.DataEngine = longhorn.DataEngineTypeV1
		bi.Spec.MinNumberOfCopies = 2
		bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
			TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
		}
		bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
			TestDiskID1: {State: longhorn.BackingImageStateReady},
		}
		bi = env.addBackingImage(bi)

		err := env.bic.replenishBackingImageCopies(bi)
		c.Assert(err, IsNil, comment)
		c.Assert(len(bi.Spec.DiskFileSpecMap), Equals, 2, comment)
		_, exists := bi.Spec.DiskFileSpecMap[tc.expectedDiskUUID]
		c.Assert(exists, Equals, true, comment)
	}
}

func (s *TestSuite) TestBackingImageFirstV2CopyRespectsNodeSelector(c *C) {
	testCases := map[string]struct {
		v1FileNodeSelected bool
	}{
		"v1 file node without block disk":         {v1FileNodeSelected: true},
		"v1 file node excluded with a block disk": {v1FileNodeSelected: false},
	}

	for name, tc := range testCases {
		comment := Commentf("test %q", name)
		env := newBackingImagePlacementTestEnv(c, testBIPlacementSelector)

		// The node holding the v1 file only has a block disk when it is excluded by the selector.
		node1 := newNodeForBackingImageNodeSelectorTest(TestNode1, TestDiskID1)
		if !tc.v1FileNodeSelected {
			node1 = addDiskForBackingImagePlacementTest(node1, "block-1", "block-uuid-1", longhorn.DiskTypeBlock)
		}
		env.addNode(node1, tc.v1FileNodeSelected)
		node2 := newNodeForBackingImageNodeSelectorTest(TestNode2, TestDiskID2)
		node2 = addDiskForBackingImagePlacementTest(node2, "block-2", "block-uuid-2", longhorn.DiskTypeBlock)
		env.addNode(node2, false)
		node3 := newNodeForBackingImageNodeSelectorTest(testBIPlacementNode3, TestDiskID3)
		node3 = addDiskForBackingImagePlacementTest(node3, "block-3", "block-uuid-3", longhorn.DiskTypeBlock)
		env.addNode(node3, true)

		bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
		bi.Spec.DataEngine = longhorn.DataEngineTypeV2
		bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
			TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
		}
		bi = env.addBackingImage(bi)

		readyNode, readyDiskName, err := env.bic.findReadyNodeAndDiskForFirstV2Copy(bi, TestDiskID1, env.bic.logger)
		c.Assert(err, IsNil, comment)
		c.Assert(readyNode.Name, Equals, testBIPlacementNode3, comment)
		c.Assert(readyDiskName, Equals, "block-3", comment)
	}
}

func (s *TestSuite) TestBackingImageCopyEvictionRequestedReconciledBothWays(c *C) {
	testCases := map[string]struct {
		nodeSelectorSettingValue string
		nodeSelected             bool
		nodeEvictionRequested    bool
		diskEvictionRequested    bool
		biNodeSelector           []string
		copyDiskUUID             string
		initialEviction          bool
		expectedEviction         bool
	}{
		"stale request is cleared when node matches selector again": {
			nodeSelectorSettingValue: testBIPlacementSelector,
			nodeSelected:             true,
			copyDiskUUID:             TestDiskID1,
			initialEviction:          true,
			expectedEviction:         false,
		},
		"stale request is cleared when selector is removed": {
			copyDiskUUID:     TestDiskID1,
			initialEviction:  true,
			expectedEviction: false,
		},
		"request is set when node is excluded by selector": {
			nodeSelectorSettingValue: testBIPlacementSelector,
			nodeSelected:             false,
			copyDiskUUID:             TestDiskID1,
			initialEviction:          false,
			expectedEviction:         true,
		},
		"request is kept for node eviction": {
			nodeEvictionRequested: true,
			copyDiskUUID:          TestDiskID1,
			initialEviction:       true,
			expectedEviction:      true,
		},
		"request is kept for disk eviction": {
			diskEvictionRequested: true,
			copyDiskUUID:          TestDiskID1,
			initialEviction:       true,
			expectedEviction:      true,
		},
		"request is set when node tags do not match backing image node selector": {
			biNodeSelector:   []string{"fast"},
			copyDiskUUID:     TestDiskID1,
			initialEviction:  false,
			expectedEviction: true,
		},
		"request on unknown disk is left untouched": {
			copyDiskUUID:     "unknown-disk-uuid",
			initialEviction:  true,
			expectedEviction: true,
		},
	}

	for name, tc := range testCases {
		comment := Commentf("test %q", name)
		env := newBackingImagePlacementTestEnv(c, tc.nodeSelectorSettingValue)

		node1 := newNodeForBackingImageNodeSelectorTest(TestNode1, TestDiskID1)
		node1.Spec.EvictionRequested = tc.nodeEvictionRequested
		diskSpec := node1.Spec.Disks[TestDiskID1]
		diskSpec.EvictionRequested = tc.diskEvictionRequested
		node1.Spec.Disks[TestDiskID1] = diskSpec
		env.addNode(node1, tc.nodeSelected)
		env.addBackingImageDataSource(newBackingImageDataSourceForPlacementTest(TestNode1, TestDiskID1, true))

		bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
		bi.Spec.NodeSelector = tc.biNodeSelector
		bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
			tc.copyDiskUUID: {DataEngine: longhorn.DataEngineTypeV1, EvictionRequested: tc.initialEviction},
		}
		bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
			tc.copyDiskUUID: {State: longhorn.BackingImageStateReady},
		}
		bi = env.addBackingImage(bi)

		err := env.bic.syncBackingImageCopyEvictionRequested(bi)
		c.Assert(err, IsNil, comment)
		c.Assert(bi.Spec.DiskFileSpecMap[tc.copyDiskUUID].EvictionRequested, Equals, tc.expectedEviction, comment)
	}
}

func (s *TestSuite) TestSyncBackingImageEvictionRequestedReconcilesStaleSelectorEviction(c *C) {
	testCases := map[string]struct {
		biNodeSelector   []string
		expectedEviction bool
	}{
		"stale request is cleared when node matches selector again": {
			expectedEviction: false,
		},
		"request is kept when node tags do not match backing image node selector": {
			biNodeSelector:   []string{"fast"},
			expectedEviction: true,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing node controller stale BI copy eviction: %v\n", name)
		comment := Commentf("test %q", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
		bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()

		nc, err := newTestNodeController(lhClient, kubeClient, extensionsClient, informerFactories, record.NewFakeRecorder(100), TestNode1)
		c.Assert(err, IsNil)

		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(types.SettingNameSystemManagedComponentsNodeSelector), testBIPlacementSelector), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(sIndexer.Add(setting), IsNil)

		// The node matches the selector again and no eviction is requested on it.
		kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue,
			corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{testBIPlacementSelectorKey: testBIPlacementSelectorValue}
		c.Assert(kubeNodeIndexer.Add(kubeNode), IsNil)

		bi := newBIForNodeSelectorTest(TestBackingImage)
		bi.Spec.NodeSelector = tc.biNodeSelector
		bi.Spec.DiskFileSpecMap[TestDiskID1].EvictionRequested = true
		bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(biIndexer.Add(bi), IsNil)

		bids := newBackingImageDataSourceForPlacementTest(TestNode1, TestDiskID1, true)
		bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(bidsIndexer.Add(bids), IsNil)

		node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
		err = nc.syncBackingImageEvictionRequested(node)
		c.Assert(err, IsNil, comment)

		updatedBI, err := lhClient.LonghornV1beta2().BackingImages(TestNamespace).Get(context.TODO(), bi.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(updatedBI.Spec.DiskFileSpecMap[TestDiskID1].EvictionRequested, Equals, tc.expectedEviction, comment)
	}
}
