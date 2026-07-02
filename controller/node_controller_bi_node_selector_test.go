package controller

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

	. "gopkg.in/check.v1"
)

type NodeControllerBINodeSelectorTestCase struct {
	// nodeSelectorSettingValue is the raw value of the
	// SystemManagedComponentsNodeSelector setting.
	nodeSelectorSettingValue string

	bidsFileTransferred bool

	// expectedEvictionRequested is whether the BI copy on TestDiskID1 should
	// have EvictionRequested set after syncBackingImageEvictionRequested runs.
	expectedEvictionRequested bool
}

// newBIForNodeSelectorTest creates a BackingImage with a single copy on
// TestDiskID1 that appears in both Status.DiskFileStatusMap (required by
// GetCurrentDiskBackingImageMap) and Spec.DiskFileSpecMap (where the
// EvictionRequested flag lives).
func newBIForNodeSelectorTest(name string) *longhorn.BackingImage {
	bi := newBackingIamge(name, longhorn.BackingImageDataSourceTypeDownload)
	bi.Spec.DiskFileSpecMap = map[string]*longhorn.BackingImageDiskFileSpec{
		TestDiskID1: {DataEngine: longhorn.DataEngineTypeV1, EvictionRequested: false},
	}
	bi.Status.DiskFileStatusMap = map[string]*longhorn.BackingImageDiskFileStatus{
		TestDiskID1: {State: longhorn.BackingImageStateReady},
	}
	return bi
}

func (s *TestSuite) TestSyncBackingImageEvictionRequestedForNodeSelector(c *C) {
	testCases := map[string]NodeControllerBINodeSelectorTestCase{
		"eviction set on BI copy when node excluded by selector after transfer": {
			nodeSelectorSettingValue:  "kubernetes.io/hostname:does-not-exist",
			bidsFileTransferred:       true,
			expectedEvictionRequested: true,
		},
		"eviction skipped on BI copy when node excluded by selector before transfer": {
			nodeSelectorSettingValue:  "kubernetes.io/hostname:does-not-exist",
			bidsFileTransferred:       false,
			expectedEvictionRequested: false,
		},
		"no eviction on BI copy when node matches selector": {
			nodeSelectorSettingValue:  "kubernetes.io/hostname:test-node-name-1",
			bidsFileTransferred:       true,
			expectedEvictionRequested: false,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing node controller BI node selector: %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		biIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()
		bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()

		fakeRecorder := record.NewFakeRecorder(100)
		nc, err := newTestNodeController(lhClient, kubeClient, extensionsClient, informerFactories, fakeRecorder, TestNode1)
		c.Assert(err, IsNil)

		// Settings: node selector.
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(
			context.TODO(), newSetting(string(types.SettingNameSystemManagedComponentsNodeSelector), tc.nodeSelectorSettingValue), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)

		// Kubernetes node: TestNode1 with hostname label matching its name.
		kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue,
			corev1.ConditionFalse, corev1.ConditionFalse,
			corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Labels = map[string]string{"kubernetes.io/hostname": TestNode1}
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)

		// Backing image: one copy on TestDiskID1.
		bi := newBIForNodeSelectorTest(TestBackingImage)
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
				FileTransferred: tc.bidsFileTransferred,
			},
		}
		bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = bidsIndexer.Add(bids)
		c.Assert(err, IsNil)

		// Longhorn node: TestNode1 with TestDiskID1.
		node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")

		err = nc.syncBackingImageEvictionRequested(node)
		c.Assert(err, IsNil)

		// Verify the BI copy's eviction flag.
		updatedBI, err := lhClient.LonghornV1beta2().BackingImages(TestNamespace).Get(context.TODO(), bi.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		diskFileSpec, ok := updatedBI.Spec.DiskFileSpecMap[TestDiskID1]
		c.Assert(ok, Equals, true, Commentf("test %q: disk spec missing", name))
		c.Assert(diskFileSpec.EvictionRequested, Equals, tc.expectedEvictionRequested, Commentf("test %q", name))
	}
}
