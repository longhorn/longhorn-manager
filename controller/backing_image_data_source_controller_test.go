package controller

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

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

	. "gopkg.in/check.v1"
)

type BackingImageDataSourceControllerTestCase struct {
	controllerID string

	// nodeSelectorSettingValue is the raw value of the
	// SystemManagedComponentsNodeSelector setting.
	nodeSelectorSettingValue string

	// existingPodNodeSelector is the NodeSelector on the pre-seeded pod.
	// A nil map means no NodeSelector (simulate a pod created before the
	// setting was applied).
	existingPodNodeSelector map[string]string

	// currentBIDSState is the BIDS status at the start of the test.
	currentBIDSState   longhorn.BackingImageState
	currentBIDSMessage string

	expectedPodCount    int
	expectedBIDSState   longhorn.BackingImageState
	expectedBIDSMessage string
}

func newTestBackingImageDataSourceController(
	lhClient *lhfake.Clientset,
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories,
	controllerID string,
) (*BackingImageDataSourceController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	logger := logrus.StandardLogger()
	proxyConnCounter := util.NewAtomicCounter()
	bidsc, err := NewBackingImageDataSourceController(
		logger, ds, scheme.Scheme, kubeClient,
		TestNamespace, controllerID, TestServiceAccount, TestBIMImage,
		proxyConnCounter,
	)
	if err != nil {
		return nil, err
	}
	fakeRecorder := record.NewFakeRecorder(100)
	bidsc.eventRecorder = fakeRecorder
	for i := range bidsc.cacheSyncs {
		bidsc.cacheSyncs[i] = alwaysReady
	}
	return bidsc, nil
}

func newBIDSForTest(name, nodeID, diskUUID, diskPath string, state longhorn.BackingImageState) *longhorn.BackingImageDataSource {
	return &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  TestNamespace,
			Finalizers: []string{longhorn.SchemeGroupVersion.Group},
		},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:     nodeID,
			DiskUUID:   diskUUID,
			DiskPath:   diskPath,
			SourceType: longhorn.BackingImageDataSourceTypeDownload,
		},
		Status: longhorn.BackingImageDataSourceStatus{
			OwnerID:      nodeID,
			CurrentState: state,
		},
	}
}

func newBIDSPodForTest(bidsName, nodeID string, nodeSelector map[string]string) *corev1.Pod {
	podName := types.GetBackingImageDataSourcePodName(bidsName)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: TestNamespace,
			Labels:    types.GetBackingImageDataSourceLabels(bidsName, nodeID, TestDiskID1),
		},
		Spec: corev1.PodSpec{
			NodeName:     nodeID,
			NodeSelector: nodeSelector,
			Containers: []corev1.Container{
				{Name: BackingImageDataSourcePodContainerName, Image: TestBIMImage},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Ready: true},
			},
		},
	}
}

func (s *TestSuite) TestSyncBackingImageDataSourceNodeSelector(c *C) {
	testCases := map[string]BackingImageDataSourceControllerTestCase{
		"running pod with stale node selector is kept": {
			controllerID:             TestNode1,
			nodeSelectorSettingValue: "kubernetes.io/hostname:does-not-exist",
			existingPodNodeSelector:  nil,
			currentBIDSState:         longhorn.BackingImageStateStarting,
			expectedPodCount:         1,
			expectedBIDSState:        longhorn.BackingImageStateStarting,
		},
		"mismatch marker does not block retry on assigned node": {
			controllerID:             TestNode1,
			nodeSelectorSettingValue: "kubernetes.io/hostname:does-not-exist",
			existingPodNodeSelector:  nil,
			currentBIDSState:         longhorn.BackingImageStateFailedAndCleanUp,
			currentBIDSMessage:       backingImageDataSourcePodSpecMismatchMessage,
			expectedPodCount:         1,
			expectedBIDSState:        "",
		},
		"new BIDS creates pod on assigned node despite selector drift": {
			controllerID:             TestNode1,
			nodeSelectorSettingValue: "kubernetes.io/hostname:does-not-exist",
			existingPodNodeSelector:  nil,
			currentBIDSState:         "",
			expectedPodCount:         1,
			expectedBIDSState:        "",
		},
		"no selector drift status message is set for assigned node": {
			controllerID:             TestNode1,
			nodeSelectorSettingValue: "kubernetes.io/hostname:does-not-exist",
			existingPodNodeSelector:  nil,
			currentBIDSState:         "",
			expectedPodCount:         1,
			expectedBIDSState:        "",
			expectedBIDSMessage:      "",
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing backing image data source: %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()
		biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

		bidsc, error := newTestBackingImageDataSourceController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(error, IsNil)

		// Settings.
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(types.SettingNameTaintToleration), ""), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)

		setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(types.SettingNameSystemManagedComponentsNodeSelector), tc.nodeSelectorSettingValue), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)

		// BackingImage (required by syncBackingImage).
		bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
		bi.Name = TestBackingImage
		bi.Status.UUID = "test-backing-image-uuid"
		bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = biIndexer.Add(bi)
		c.Assert(err, IsNil)

		// Longhorn node with ready disk (required by GetReadyDiskNode).
		lhNode := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		// Kubernetes node with a label that does not satisfy the current selector.
		kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue,
			corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": TestNode1}
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		// BIDS CR.
		bids := newBIDSForTest(TestBackingImage, TestNode1, TestDiskID1, TestDefaultDataPath, tc.currentBIDSState)
		bids.Spec.UUID = bi.Status.UUID
		bids.Status.Message = tc.currentBIDSMessage
		bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = bidsIndexer.Add(bids)
		c.Assert(err, IsNil)

		// Optionally seed an existing pod.
		if tc.existingPodNodeSelector != nil || tc.currentBIDSState == longhorn.BackingImageStateStarting {
			pod := newBIDSPodForTest(bids.Name, TestNode1, tc.existingPodNodeSelector)
			pod, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = pIndexer.Add(pod)
			c.Assert(err, IsNil)
		}

		bidsKey := fmt.Sprintf("%s/%s", TestNamespace, bids.Name)
		err = bidsc.syncBackingImageDataSource(bidsKey)
		c.Assert(err, IsNil)

		// Check pod count.
		podList, err := kubeClient.CoreV1().Pods(TestNamespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(len(podList.Items), Equals, tc.expectedPodCount, Commentf("test %q", name))

		// Check BIDS state.
		updatedBIDS, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bids.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(updatedBIDS.Status.CurrentState, Equals, tc.expectedBIDSState, Commentf("test %q", name))
		if tc.expectedBIDSMessage != "" {
			c.Assert(updatedBIDS.Status.Message, Equals, tc.expectedBIDSMessage, Commentf("test %q: wrong status message", name))
		}
	}
}

func (s *TestSuite) TestSyncBackingImageDataSourceRecreatesPodAfterSelectorMismatchReschedule(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

	bidsc, err := newTestBackingImageDataSourceController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode2)
	c.Assert(err, IsNil)

	for _, s := range []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
	} {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	lhNode := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	err = lhNodeIndexer.Add(lhNode)
	c.Assert(err, IsNil)
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	kubeNode := newKubernetesNode(TestNode2, corev1.ConditionTrue,
		corev1.ConditionFalse, corev1.ConditionFalse,
		corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	kubeNode.Labels = map[string]string{"kubernetes.io/hostname": TestNode2}
	err = kubeNodeIndexer.Add(kubeNode)
	c.Assert(err, IsNil)
	_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Name = TestBackingImage
	bi.Status.UUID = "test-backing-image-uuid"
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	bids := newBIDSForTest(TestBackingImage, TestNode2, TestDiskID1, TestDefaultDataPath, longhorn.BackingImageStateFailedAndCleanUp)
	bids.Spec.UUID = bi.Status.UUID
	bids.Spec.SourceType = longhorn.BackingImageDataSourceTypeDownload
	bids.Status.Message = backingImageDataSourcePodSpecMismatchMessage
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = bidsIndexer.Add(bids)
	c.Assert(err, IsNil)

	err = bidsc.syncBackingImageDataSource(fmt.Sprintf("%s/%s", TestNamespace, bids.Name))
	c.Assert(err, IsNil)

	podList, err := kubeClient.CoreV1().Pods(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(podList.Items), Equals, 1)
	c.Assert(podList.Items[0].Spec.NodeName, Equals, TestNode2)
	c.Assert(podList.Items[0].Spec.NodeSelector, IsNil)
	updatedBIDS, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bids.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedBIDS.Status.CurrentState, Equals, longhorn.BackingImageState(""))
	c.Assert(updatedBIDS.Status.Message, Equals, "")
}

func (s *TestSuite) TestGenerateBackingImageDataSourcePodManifestUsesAssignedNodeOnly(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

	bidsc, err := newTestBackingImageDataSourceController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	for _, s := range []struct {
		name  types.SettingName
		value string
	}{
		{types.SettingNameTaintToleration, ""},
		{types.SettingNameSystemManagedComponentsNodeSelector, "kubernetes.io/hostname:" + TestNode2},
	} {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(s.name), s.value), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	bi := newBackingIamge(TestBackingImage, longhorn.BackingImageDataSourceTypeDownload)
	bi.Name = TestBackingImage
	bi.Status.UUID = "test-backing-image-uuid"
	bi, err = lhClient.LonghornV1beta2().BackingImages(TestNamespace).Create(context.TODO(), bi, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = biIndexer.Add(bi)
	c.Assert(err, IsNil)

	bids := newBIDSForTest(TestBackingImage, TestNode1, TestDiskID1, TestDefaultDataPath, "")
	bids.Spec.UUID = bi.Status.UUID

	pod, err := bidsc.generateBackingImageDataSourcePodManifest(bids)
	c.Assert(err, IsNil)
	c.Assert(pod.Spec.NodeName, Equals, TestNode1)
	c.Assert(pod.Spec.NodeSelector, IsNil)
}
