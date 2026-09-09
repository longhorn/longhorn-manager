package controller

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
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
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

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

func useKubernetesNodeControllerTestClock(knc *KubernetesNodeController) *clocktesting.FakeClock {
	knc.queue.ShutDown()
	clock := clocktesting.NewFakeClock(time.Now())
	knc.queue = workqueue.NewTypedRateLimitingQueueWithConfig[any](EnhancedDefaultControllerRateLimiter(),
		workqueue.TypedRateLimitingQueueConfig[any]{Clock: clock})
	return clock
}

func advanceKubernetesNodeControllerQueue(c *C, knc *KubernetesNodeController, clock *clocktesting.FakeClock, delay time.Duration) {
	// Advance virtual time while the delaying queue's goroutine registers and
	// consumes timers. A timer registered after one tick must not make the
	// test depend on real-time sleeps or goroutine scheduling.
	c.Assert(wait.PollUntilContextTimeout(context.TODO(), time.Millisecond, 5*time.Second, true,
		func(context.Context) (bool, error) {
			clock.Step(delay)
			return knc.queue.Len() > 0, nil
		}), IsNil)
}

func (s *TestSuite) TestKubernetesNodeControllerRetriesDelayedResourceCleanup(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	replicaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
	engineIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()

	knc, err := newTestKubernetesNodeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)
	clock := useKubernetesNodeControllerTestClock(knc)
	defer knc.queue.ShutDown()

	node := newNode(TestNode1, TestNamespace, false, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonKubernetesNodeGone)
	node, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(nodeIndexer.Add(node), IsNil)
	replica := &longhorn.Replica{ObjectMeta: metav1.ObjectMeta{
		Name: "remaining-replica", Namespace: TestNamespace, Labels: map[string]string{types.LonghornNodeKey: node.Name},
	}}
	engine := &longhorn.Engine{ObjectMeta: metav1.ObjectMeta{
		Name: "remaining-engine", Namespace: TestNamespace, Labels: map[string]string{types.LonghornNodeKey: node.Name},
	}}
	c.Assert(replicaIndexer.Add(replica), IsNil)
	c.Assert(engineIndexer.Add(engine), IsNil)

	// Fake clients do not run admission. Mirror the node deletion webhook's
	// rejection while any replicas or engines remain on the missing node.
	deleteAttempts := 0
	lhClient.PrependReactor("delete", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		deleteAttempts++
		replicas, err := knc.ds.ListReplicasByNodeRO(node.Name)
		c.Assert(err, IsNil)
		engines, err := knc.ds.ListEnginesByNodeRO(node.Name)
		c.Assert(err, IsNil)
		if len(replicas) > 0 || len(engines) > 0 {
			return true, nil, apierrors.NewInvalid(longhorn.SchemeGroupVersion.WithKind("Node").GroupKind(), node.Name, nil)
		}
		return false, nil, nil
	})

	key := getKey(node, c)
	knc.queue.Add(key)
	exhaustErrorRetries := func() {
		for attempt := 0; attempt <= maxRetries; attempt++ {
			c.Assert(knc.queue.Len(), Equals, 1)
			c.Assert(knc.processNextWorkItem(), Equals, true)
			if attempt < maxRetries {
				advanceKubernetesNodeControllerQueue(c, knc, clock, time.Second)
			}
		}
		c.Assert(knc.queue.NumRequeues(key), Equals, 0)
		_, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
	}

	// Neither resource is removed during the short error-retry window. A
	// delayed retry must still arrive after handleErr has forgotten the key.
	exhaustErrorRetries()
	advanceKubernetesNodeControllerQueue(c, knc, clock, kubernetesNodeCleanupRetryInterval)
	exhaustErrorRetries()
	c.Assert(deleteAttempts, Equals, 2*(maxRetries+1))

	// Removing only the replica is not sufficient. The engine still protects
	// the node, including across another exhausted error-retry budget.
	c.Assert(replicaIndexer.Delete(replica), IsNil)
	advanceKubernetesNodeControllerQueue(c, knc, clock, kubernetesNodeCleanupRetryInterval)
	exhaustErrorRetries()
	c.Assert(deleteAttempts, Equals, 3*(maxRetries+1))

	// No Kubernetes/Longhorn node event accompanies the final resource cleanup.
	// The delayed retry alone must now delete the empty Longhorn node.
	c.Assert(engineIndexer.Delete(engine), IsNil)
	advanceKubernetesNodeControllerQueue(c, knc, clock, kubernetesNodeCleanupRetryInterval)
	c.Assert(knc.processNextWorkItem(), Equals, true)
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)
	c.Assert(deleteAttempts, Equals, 3*(maxRetries+1)+1)
	c.Assert(knc.queue.NumRequeues(key), Equals, 0)
}

func (s *TestSuite) TestKubernetesNodeControllerCleanupRetryRechecksKubernetesNode(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	knc, err := newTestKubernetesNodeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode2)
	c.Assert(err, IsNil)
	clock := useKubernetesNodeControllerTestClock(knc)
	defer knc.queue.ShutDown()

	node := newNode(TestNode1, TestNamespace, false, longhorn.ConditionStatusFalse, longhorn.NodeConditionReasonKubernetesNodeGone)
	node, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(nodeIndexer.Add(node), IsNil)
	deleteAttempts := 0
	lhClient.PrependReactor("delete", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		deleteAttempts++
		return true, nil, apierrors.NewServiceUnavailable("temporary deletion failure")
	})

	err = knc.syncKubernetesNode(getKey(node, c))
	c.Assert(err, NotNil)
	c.Assert(deleteAttempts, Equals, 1)

	// A delayed retry must observe a returned Kubernetes node, not blindly
	// repeat the stale deletion decision from the previous reconciliation.
	c.Assert(kubeNodeIndexer.Add(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node.Name}}), IsNil)
	advanceKubernetesNodeControllerQueue(c, knc, clock, kubernetesNodeCleanupRetryInterval)
	c.Assert(knc.processNextWorkItem(), Equals, true)
	c.Assert(deleteAttempts, Equals, 1)
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Get(context.TODO(), node.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
}
