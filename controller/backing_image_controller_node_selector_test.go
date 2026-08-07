package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
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

func newTestBackingImageController(
	lhClient *lhfake.Clientset,
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories,
	controllerID string,
) (*BackingImageController, error) {
	// Skip the Lister check that occurs on creation of resources.
	datastore.SkipListerCheck = true
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	logger := logrus.StandardLogger()
	proxyConnCounter := util.NewAtomicCounter()
	bic, err := NewBackingImageController(
		logger, ds, scheme.Scheme, kubeClient,
		TestNamespace, controllerID, TestServiceAccount, TestBIMImage,
		proxyConnCounter,
	)
	if err != nil {
		return nil, err
	}
	fakeRecorder := record.NewFakeRecorder(100)
	bic.eventRecorder = fakeRecorder
	for i := range bic.cacheSyncs {
		bic.cacheSyncs[i] = alwaysReady
	}
	return bic, nil
}

func newNodeForBackingImageNodeSelectorTest(name, diskUUID string) *longhorn.Node {
	node := newNode(name, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	node.Status.DiskStatus[TestDiskID1].DiskUUID = diskUUID
	return node
}

func (s *TestSuite) TestBackingImageControllerEnqueuesAllBackingImagesOnNodeSelectorSettingChange(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	for _, name := range []string{TestBackingImage, TestBackingImage + "-2"} {
		bi := newBackingIamge(name, longhorn.BackingImageDataSourceTypeDownload)
		bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = biIndexer.Add(bi)
		c.Assert(err, IsNil)
	}

	setting := newSetting(string(types.SettingNameSystemManagedComponentsNodeSelector), "kubernetes.io/hostname:"+TestNode2)
	bic.enqueueBackingImagesForSettingChange(setting)

	queuedKeys := map[string]bool{}
	for i := 0; i < 2; i++ {
		item, shutdown := bic.queue.Get()
		c.Assert(shutdown, Equals, false)
		key, ok := item.(string)
		c.Assert(ok, Equals, true)
		queuedKeys[key] = true
		bic.queue.Done(item)
	}

	c.Assert(queuedKeys[fmt.Sprintf("%s/%s", TestNamespace, TestBackingImage)], Equals, true)
	c.Assert(queuedKeys[fmt.Sprintf("%s/%s", TestNamespace, TestBackingImage+"-2")], Equals, true)
	c.Assert(bic.queue.Len(), Equals, 0)
}

// TestBackingImageBIDSNodeSelection verifies that when a node selector is set
// and the only available node is excluded, no BackingImageDataSource CR is
// created for the initial fetch.
func (s *TestSuite) TestBackingImageBIDSNodeSelection(c *C) {
	testCases := map[string]struct {
		nodeSelectorSettingValue string
		expectBIDSCreated        bool
	}{
		"no BIDS created when only node is excluded by selector": {
			nodeSelectorSettingValue: "kubernetes.io/hostname:does-not-exist",
			expectBIDSCreated:        false,
		},
		"BIDS created when node matches selector": {
			nodeSelectorSettingValue: "kubernetes.io/hostname:" + TestNode1,
			expectBIDSCreated:        true,
		},
		"BIDS created when no selector is set": {
			nodeSelectorSettingValue: "",
			expectBIDSCreated:        true,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing backing image BIDS node selection: %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

		bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)

		// Required settings.
		requiredSettings := []struct {
			name  types.SettingName
			value string
		}{
			{types.SettingNameSystemManagedComponentsNodeSelector, tc.nodeSelectorSettingValue},
			{types.SettingNameTaintToleration, ""},
			{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
			{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
			{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
		}
		var setting *longhorn.Setting
		for _, s := range requiredSettings {
			setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
				context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}

		// Longhorn node with a ready disk.
		lhNode := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		// Kube node with hostname label for TestNode1.
		kubeNode := newKubernetesNode(TestNode1,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": TestNode1}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)

		// BackingImage: no disk copies yet (DiskFileSpecMap empty) so BIDS creation is needed.
		bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
		bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
		bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = biIndexer.Add(bi)
		c.Assert(err, IsNil)

		err = bic.handleBackingImageDataSource(bi)
		if tc.expectBIDSCreated {
			c.Assert(err, IsNil, Commentf("test %q: expected no error", name))
			bids, getErr := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bi.Name, metav1.GetOptions{})
			c.Assert(getErr, IsNil, Commentf("test %q: BIDS should exist", name))
			c.Assert(bids.Spec.NodeID, Equals, TestNode1, Commentf("test %q: BIDS should be on TestNode1", name))
		} else {
			// Either an error or no BIDS created.
			if err == nil {
				_, getErr := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bi.Name, metav1.GetOptions{})
				c.Assert(datastore.ErrorIsNotFound(getErr), Equals, true, Commentf("test %q: no BIDS should be created when node is excluded", name))
			}
		}
	}
}

func (s *TestSuite) TestBackingImageBIDSNodeSelectionFromDiskFileSpecMap(c *C) {
	testCases := map[string]struct {
		nodeSelectorSettingValue string
		expectedNodeID           string
		expectedDiskUUID         string
		expectedFileTransferred  bool
	}{
		"existing ready file on selected node is reused": {
			nodeSelectorSettingValue: "kubernetes.io/hostname:" + TestNode1,
			expectedNodeID:           TestNode1,
			expectedDiskUUID:         TestDiskID1,
			expectedFileTransferred:  true,
		},
		"existing ready file on excluded node is skipped": {
			nodeSelectorSettingValue: "kubernetes.io/hostname:" + TestNode2,
			expectedNodeID:           TestNode2,
			expectedDiskUUID:         TestDiskID2,
			expectedFileTransferred:  false,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing backing image BIDS node selection from disk file spec map: %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

		bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)

		requiredSettings := []struct {
			name  types.SettingName
			value string
		}{
			{types.SettingNameSystemManagedComponentsNodeSelector, tc.nodeSelectorSettingValue},
			{types.SettingNameTaintToleration, ""},
			{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
			{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
			{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
		}
		for _, s := range requiredSettings {
			setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
				context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}

		for _, nodeName := range []string{TestNode1, TestNode2} {
			diskUUID := TestDiskID1
			if nodeName == TestNode2 {
				diskUUID = TestDiskID2
			}
			lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
			lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = lhNodeIndexer.Add(lhNode)
			c.Assert(err, IsNil)

			kubeNode := newKubernetesNode(nodeName,
				corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
				corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
			kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
			_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = kubeNodeIndexer.Add(kubeNode)
			c.Assert(err, IsNil)
		}

		bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
		bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
			TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
		}
		bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
			TestDiskID1: {State: longhorn.BackingImageStateReady},
		}
		bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = biIndexer.Add(bi)
		c.Assert(err, IsNil)

		err = bic.handleBackingImageDataSource(bi)
		c.Assert(err, IsNil, Commentf("test %q: expected no error", name))
		bids, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bi.Name, metav1.GetOptions{})
		c.Assert(err, IsNil, Commentf("test %q: BIDS should exist", name))
		c.Assert(bids.Spec.NodeID, Equals, tc.expectedNodeID, Commentf("test %q: unexpected BIDS node", name))
		c.Assert(bids.Spec.DiskUUID, Equals, tc.expectedDiskUUID, Commentf("test %q: unexpected BIDS disk", name))
		c.Assert(bids.Spec.FileTransferred, Equals, tc.expectedFileTransferred, Commentf("test %q: unexpected transferred flag", name))
	}
}

func (s *TestSuite) TestBackingImageDataSourceNotRescheduledBeforeFileTransferred(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
		{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
		{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	for _, nodeName := range []string{TestNode1, TestNode2} {
		diskUUID := TestDiskID1
		if nodeName == TestNode2 {
			diskUUID = TestDiskID2
		}
		lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		kubeNode := newKubernetesNode(nodeName,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
	}

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:          TestNode1,
			DiskUUID:        TestDiskID1,
			DiskPath:        TestDefaultDataPath,
			SourceType:      longhorn.BackingImageDataSourceTypeDownload,
			Parameters:      map[string]string{},
			FileTransferred: false,
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	err = bic.handleBackingImageDataSource(bi)
	c.Assert(err, IsNil)
	updatedBIDS, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), TestBackingImage, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedBIDS.Spec.NodeID, Equals, TestNode1)
	c.Assert(updatedBIDS.Spec.DiskUUID, Equals, TestDiskID1)
	c.Assert(updatedBIDS.Spec.DiskPath, Equals, TestDefaultDataPath)
	c.Assert(updatedBIDS.Spec.FileTransferred, Equals, false)
	for _, action := range lhClient.Actions() {
		c.Assert(action.Matches("delete", "backingimagedatasources"), Equals, false)
	}
}

func (s *TestSuite) TestBackingImageDataSourceAssignmentIsImmutable(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
		{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
		{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	for _, nodeName := range []string{TestNode1, TestNode2} {
		diskUUID := TestDiskID1
		if nodeName == TestNode2 {
			diskUUID = TestDiskID2
		}
		lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		kubeNode := newKubernetesNode(nodeName,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
	}

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:          TestNode1,
			DiskUUID:        TestDiskID1,
			DiskPath:        TestDefaultDataPath,
			SourceType:      longhorn.BackingImageDataSourceTypeDownload,
			Parameters:      map[string]string{},
			FileTransferred: false,
		},
		Status: longhorn.BackingImageDataSourceStatus{
			CurrentState: longhorn.BackingImageStateFailedAndCleanUp,
			Message:      backingImageDataSourcePodSpecMismatchMessage,
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	err = bic.handleBackingImageDataSource(bi)
	c.Assert(err, IsNil)
	updatedBIDS, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), TestBackingImage, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedBIDS.Spec.NodeID, Equals, TestNode1)
	c.Assert(updatedBIDS.Spec.DiskUUID, Equals, TestDiskID1)
	c.Assert(updatedBIDS.Spec.DiskPath, Equals, TestDefaultDataPath)
	c.Assert(updatedBIDS.Spec.FileTransferred, Equals, false)
}

func (s *TestSuite) TestBackingImageDataSourceRecoveryKeepsTransferredAssignment(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
		{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
		{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
		TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
	}
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {
			State:                   longhorn.BackingImageStateFailed,
			LastStateTransitionTime: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
		},
	}
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:          TestNode1,
			DiskUUID:        TestDiskID1,
			DiskPath:        TestDefaultDataPath,
			SourceType:      longhorn.BackingImageDataSourceTypeDownload,
			Parameters:      map[string]string{},
			FileTransferred: true,
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	err = bic.handleBackingImageDataSource(bi)
	c.Assert(err, IsNil)
	updatedBIDS, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), TestBackingImage, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedBIDS.Spec.NodeID, Equals, TestNode1)
	c.Assert(updatedBIDS.Spec.DiskUUID, Equals, TestDiskID1)
	c.Assert(updatedBIDS.Spec.DiskPath, Equals, TestDefaultDataPath)
	c.Assert(updatedBIDS.Spec.FileTransferred, Equals, true)
}
func (s *TestSuite) TestBackingImageCloneSourceSelectionRespectsNodeSelector(c *C) {
	testCases := map[string]struct {
		sourceDiskUUIDs []string
		expectError     bool
	}{
		"selected source copy is preferred": {
			sourceDiskUUIDs: []string{TestDiskID1, TestDiskID2},
			expectError:     false,
		},
		"only excluded source copy blocks clone": {
			sourceDiskUUIDs: []string{TestDiskID1},
			expectError:     true,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing backing image clone source node selection: %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
		bimIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageManagers().Informer().GetIndexer()

		bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)

		requiredSettings := []struct {
			name  types.SettingName
			value string
		}{
			{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
			{types.SettingNameTaintToleration, ""},
			{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
			{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
			{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
		}
		for _, s := range requiredSettings {
			setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
				context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}

		for _, nodeName := range []string{TestNode1, TestNode2} {
			diskUUID := TestDiskID1
			if nodeName == TestNode2 {
				diskUUID = TestDiskID2
			}
			lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
			lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = lhNodeIndexer.Add(lhNode)
			c.Assert(err, IsNil)

			kubeNode := newKubernetesNode(nodeName,
				corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
				corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
			kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
			_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = kubeNodeIndexer.Add(kubeNode)
			c.Assert(err, IsNil)
		}

		sourceBI := newBackingIamge("source-backing-image", longhorn.BackingImageDataSourceTypeDownload)
		sourceBI.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
		sourceBI.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{}
		for _, diskUUID := range tc.sourceDiskUUIDs {
			nodeName := TestNode1
			if diskUUID == TestDiskID2 {
				nodeName = TestNode2
			}
			sourceBI.Spec.DiskFileSpecMap[diskUUID] = &longhorn.BackingImageDiskFileSpec{DataEngine: longhorn.DataEngineTypeV1}
			sourceBI.Status.DiskFileStatusMap[diskUUID] = &longhorn.BackingImageDiskFileStatus{State: longhorn.BackingImageStateReady}

			bim := &longhorn.BackingImageManager{
				ObjectMeta: metav1.ObjectMeta{
					Name:      types.GetBackingImageManagerName(TestBIMImage, diskUUID),
					Namespace: TestNamespace,
					Labels:    types.GetBackingImageManagerLabels(nodeName, diskUUID),
				},
				Spec: longhorn.BackingImageManagerSpec{
					Image:         TestBIMImage,
					NodeID:        nodeName,
					DiskUUID:      diskUUID,
					DiskPath:      TestDefaultDataPath,
					BackingImages: map[string]string{sourceBI.Name: "source-backing-image-uuid"},
				},
				Status: longhorn.BackingImageManagerStatus{
					BackingImageFileMap: map[string]longhorn.BackingImageFileInfo{
						sourceBI.Name: {
							Name:  sourceBI.Name,
							UUID:  "source-backing-image-uuid",
							State: longhorn.BackingImageStateReady,
						},
					},
				},
			}
			bim, err = lhClient.LonghornV1beta2().BackingImageManagers(TestNamespace).Create(context.TODO(), bim, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = bimIndexer.Add(bim)
			c.Assert(err, IsNil)
		}
		sourceBI, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), sourceBI, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = biIndexer.Add(sourceBI)
		c.Assert(err, IsNil)

		targetBI := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeClone)
		targetBI.Spec.SourceParameters = map[string]string{longhorn.DataSourceTypeCloneParameterBackingImage: sourceBI.Name}
		targetBI.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
		targetBI, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), targetBI, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = biIndexer.Add(targetBI)
		c.Assert(err, IsNil)

		err = bic.handleBackingImageDataSource(targetBI)
		if tc.expectError {
			c.Assert(err, NotNil, Commentf("test %q: expected clone source selection error", name))
			_, getErr := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), targetBI.Name, metav1.GetOptions{})
			c.Assert(datastore.ErrorIsNotFound(getErr), Equals, true, Commentf("test %q: no BIDS should be created", name))
			continue
		}

		c.Assert(err, IsNil, Commentf("test %q: expected no error", name))
		bids, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), targetBI.Name, metav1.GetOptions{})
		c.Assert(err, IsNil, Commentf("test %q: BIDS should exist", name))
		c.Assert(bids.Spec.NodeID, Equals, TestNode2, Commentf("test %q: unexpected BIDS node", name))
		c.Assert(bids.Spec.DiskUUID, Equals, TestDiskID2, Commentf("test %q: unexpected BIDS disk", name))
	}
}

func (s *TestSuite) TestBackingImageExportDataSourceSelectionRespectsNodeSelector(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
		{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
		{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	for _, nodeName := range []string{TestNode1, TestNode2} {
		diskUUID := TestDiskID1
		if nodeName == TestNode2 {
			diskUUID = TestDiskID2
		}
		lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		kubeNode := newKubernetesNode(nodeName,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
	}

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeExportFromVolume)
	bi.Spec.SourceParameters = map[string]string{
		longhorn.DataSourceTypeExportFromVolumeParameterVolumeName: "source-volume",
	}
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	err = bic.handleBackingImageDataSource(bi)
	c.Assert(err, IsNil)
	bids, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bi.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(bids.Spec.NodeID, Equals, TestNode2)
	c.Assert(bids.Spec.DiskUUID, Equals, TestDiskID2)
	c.Assert(bids.Spec.DiskPath, Equals, TestDefaultDataPath)
}

func (s *TestSuite) TestBackingImageCloneDataSourceNotRescheduledBeforeFileTransferred(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()
	bimIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageManagers().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
		{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
		{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	for _, nodeName := range []string{TestNode1, TestNode2} {
		diskUUID := TestDiskID1
		if nodeName == TestNode2 {
			diskUUID = TestDiskID2
		}
		lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		kubeNode := newKubernetesNode(nodeName,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
	}

	sourceBI := newBackingIamge("source-backing-image", longhorn.BackingImageDataSourceTypeDownload)
	sourceBI.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
		TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
	}
	sourceBI.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStateReady},
	}
	sourceBI, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), sourceBI, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(sourceBI)
	c.Assert(err, IsNil)

	bim := &longhorn.BackingImageManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.GetBackingImageManagerName(TestBIMImage, TestDiskID1),
			Namespace: TestNamespace,
			Labels:    types.GetBackingImageManagerLabels(TestNode1, TestDiskID1),
		},
		Spec: longhorn.BackingImageManagerSpec{
			Image:         TestBIMImage,
			NodeID:        TestNode1,
			DiskUUID:      TestDiskID1,
			DiskPath:      TestDefaultDataPath,
			BackingImages: map[string]string{sourceBI.Name: "source-backing-image-uuid"},
		},
		Status: longhorn.BackingImageManagerStatus{
			BackingImageFileMap: map[string]longhorn.BackingImageFileInfo{
				sourceBI.Name: {
					Name:  sourceBI.Name,
					UUID:  "source-backing-image-uuid",
					State: longhorn.BackingImageStateReady,
				},
			},
		},
	}
	bim, err = lhClient.LonghornV1beta2().BackingImageManagers(TestNamespace).Create(context.TODO(), bim, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bimIndexer.Add(bim)
	c.Assert(err, IsNil)

	targetBI := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeClone)
	targetBI.Spec.SourceParameters = map[string]string{longhorn.DataSourceTypeCloneParameterBackingImage: sourceBI.Name}
	targetBI.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{}
	targetBI, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), targetBI, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(targetBI)
	c.Assert(err, IsNil)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:          TestNode1,
			DiskUUID:        TestDiskID1,
			DiskPath:        TestDefaultDataPath,
			SourceType:      longhorn.BackingImageDataSourceTypeClone,
			Parameters:      map[string]string{longhorn.DataSourceTypeCloneParameterBackingImage: sourceBI.Name},
			FileTransferred: false,
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	err = bic.handleBackingImageDataSource(targetBI)
	c.Assert(err, IsNil)
	updatedBIDS, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), TestBackingImage, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedBIDS.Spec.NodeID, Equals, TestNode1)
	c.Assert(updatedBIDS.Spec.DiskUUID, Equals, TestDiskID1)
}

func (s *TestSuite) TestBackingImageCopyReplenishmentRespectsNodeSelector(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
		{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
		{types.SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit, "1"},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	for _, nodeName := range []string{TestNode1, TestNode2} {
		diskUUID := TestDiskID1
		if nodeName == TestNode2 {
			diskUUID = TestDiskID2
		}
		lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		kubeNode := newKubernetesNode(nodeName,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
	}

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			FileTransferred: true,
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Spec.DataEngine = longhorn.DataEngineTypeV1
	bi.Spec.MinNumberOfCopies = 2
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
		TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
	}
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStateReady},
	}
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	err = bic.replenishBackingImageCopies(bi)
	c.Assert(err, IsNil)
	_, exists := bi.Spec.DiskFileSpecMap[TestDiskID2]
	c.Assert(exists, Equals, true)
	_, exists = bi.Spec.DiskFileSpecMap[TestDiskID1]
	c.Assert(exists, Equals, true)
	c.Assert(len(bi.Spec.DiskFileSpecMap), Equals, 2)
}

func (s *TestSuite) TestBackingImageReconcileSkipsSelectorEvictionBeforeFileTransferred(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	for _, nodeName := range []string{TestNode1, TestNode2} {
		diskUUID := TestDiskID1
		if nodeName == TestNode2 {
			diskUUID = TestDiskID2
		}
		lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		kubeNode := newKubernetesNode(nodeName,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
	}

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			DiskUUID:        TestDiskID1,
			FileTransferred: false,
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
		TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
		TestDiskID2: {DataEngine: longhorn.DataEngineTypeV1},
	}

	err = bic.requestEvictionForCopiesOutsideNodeSelector(bi)
	c.Assert(err, IsNil)
	c.Assert(bi.Spec.DiskFileSpecMap[TestDiskID1].EvictionRequested, Equals, false)
	c.Assert(bi.Spec.DiskFileSpecMap[TestDiskID2].EvictionRequested, Equals, false)
}
func (s *TestSuite) TestBackingImageReconcileRequestsEvictionForCopiesOutsideNodeSelector(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()
	bimIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageManagers().Informer().GetIndexer()
	replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()

	bic, err := newTestBackingImageController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	requiredSettings := []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameAllowEmptyNodeSelectorVolume, "true"},
		{types.SettingNameAllowEmptyDiskSelectorVolume, "true"},
		{types.SettingNameBackingImageRecoveryWaitInterval, "0"},
		{types.SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit, "1"},
	}
	for _, s := range requiredSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	for _, nodeName := range []string{TestNode1, TestNode2} {
		diskUUID := TestDiskID1
		if nodeName == TestNode2 {
			diskUUID = TestDiskID2
		}
		lhNode := newNodeForBackingImageNodeSelectorTest(nodeName, diskUUID)
		lhNode, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)

		kubeNode := newKubernetesNode(nodeName,
			corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": nodeName}
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
	}

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Status.UUID = "test-backing-image-uuid"
	bi.Spec.DataEngine = longhorn.DataEngineTypeV1
	bi.Spec.MinNumberOfCopies = 2
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
		TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1},
		TestDiskID2: {DataEngine: longhorn.DataEngineTypeV1},
	}
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStateReady},
		TestDiskID2: {State: longhorn.BackingImageStateReady},
	}
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestBackingImage,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			FileTransferred: true,
		},
		Status: longhorn.BackingImageDataSourceStatus{
			CurrentState: longhorn.BackingImageStateReady,
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	for _, diskUUID := range []string{TestDiskID1, TestDiskID2} {
		nodeName := TestNode1
		if diskUUID == TestDiskID2 {
			nodeName = TestNode2
		}
		bim := &longhorn.BackingImageManager{
			ObjectMeta: metav1.ObjectMeta{
				Name:      types.GetBackingImageManagerName(TestBIMImage, diskUUID),
				Namespace: TestNamespace,
				Labels:    types.GetBackingImageManagerLabels(nodeName, diskUUID),
			},
			Spec: longhorn.BackingImageManagerSpec{
				Image:         TestBIMImage,
				NodeID:        nodeName,
				DiskUUID:      diskUUID,
				DiskPath:      TestDefaultDataPath,
				BackingImages: map[string]string{TestBackingImage: "test-backing-image-uuid"},
			},
			Status: longhorn.BackingImageManagerStatus{
				BackingImageFileMap: map[string]longhorn.BackingImageFileInfo{
					TestBackingImage: {
						Name:  TestBackingImage,
						UUID:  "test-backing-image-uuid",
						State: longhorn.BackingImageStateReady,
					},
				},
			},
		}
		bim, err = lhClient.LonghornV1beta2().BackingImageManagers(TestNamespace).Create(context.TODO(), bim, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = bimIndexer.Add(bim)
		c.Assert(err, IsNil)
	}

	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestReplicaName,
			Namespace: TestNamespace,
			Labels: map[string]string{
				types.GetLonghornLabelKey(types.LonghornLabelBackingImage): TestBackingImage,
				types.LonghornDiskUUIDKey:                                  TestDiskID1,
			},
		},
		Spec: longhorn.ReplicaSpec{
			BackingImage: TestBackingImage,
			DiskID:       TestDiskID1,
		},
	}
	replica, err = lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), replica, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = replicaIndexer.Add(replica)
	c.Assert(err, IsNil)

	err = bic.syncBackingImage(fmt.Sprintf("%s/%s", TestNamespace, TestBackingImage))
	c.Assert(err, IsNil)
	updatedBI, err := lhClient.LonghornV1beta2().BackingImages(TestNamespace).Get(context.TODO(), TestBackingImage, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedBI.Spec.DiskFileSpecMap[TestDiskID1].EvictionRequested, Equals, true)
	c.Assert(updatedBI.Spec.DiskFileSpecMap[TestDiskID2].EvictionRequested, Equals, false)
	_, exists := updatedBI.Spec.DiskFileSpecMap[TestDiskID1]
	c.Assert(exists, Equals, true)
	_, exists = updatedBI.Spec.DiskFileSpecMap[TestDiskID2]
	c.Assert(exists, Equals, true)
}
