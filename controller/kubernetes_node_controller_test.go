package controller

import (
	"context"

	"github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newTestKubernetesNodeController(
	lhClient *lhfake.Clientset,
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories,
	controllerID string,
) (*KubernetesNodeController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	knc, err := NewKubernetesNodeController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, controllerID)
	if err != nil {
		return nil, err
	}
	knc.eventRecorder = record.NewFakeRecorder(100)
	for index := range knc.cacheSyncs {
		knc.cacheSyncs[index] = alwaysReady
	}

	return knc, nil
}

func (s *TestSuite) TestKubernetesNodeControllerUnschedulesMissingNodeBeforeDeletion(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

	knc, err := newTestKubernetesNodeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonKubernetesNodeGone)
	node, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(nodeIndexer.Add(node), IsNil)

	// The first reconciliation must make the missing node unschedulable. If it
	// deletes immediately, the admission webhook rejects the request.
	err = knc.syncKubernetesNode(getKey(node, c))
	c.Assert(err, IsNil)
	updatedNode, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedNode.Spec.AllowScheduling, Equals, false)

	// Simulate the informer observing the update. The next reconciliation can
	// now delete the empty Longhorn node without violating webhook policy.
	c.Assert(nodeIndexer.Update(updatedNode), IsNil)
	err = knc.syncKubernetesNode(getKey(updatedNode, c))
	c.Assert(err, IsNil)
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)
}

func (s *TestSuite) TestKubernetesNodeControllerConcurrentNodeDeletionIsNoop(c *C) {
	testCases := []struct {
		name            string
		addToIndexer    bool
		allowScheduling bool
	}{
		{
			name: "deleted before lookup",
		},
		{
			name:            "deleted before scheduling update",
			addToIndexer:    true,
			allowScheduling: true,
		},
		{
			name:         "deleted before cleanup",
			addToIndexer: true,
		},
	}

	for _, testCase := range testCases {
		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
		nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

		knc, err := newTestKubernetesNodeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil, Commentf(testCase.name))

		if testCase.addToIndexer {
			node := newNode(TestNode1, TestNamespace, testCase.allowScheduling, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonKubernetesNodeGone)
			c.Assert(nodeIndexer.Add(node), IsNil, Commentf(testCase.name))
		}

		err = knc.syncKubernetesNode(TestNode1)
		c.Assert(err, IsNil, Commentf(testCase.name))
	}
}
