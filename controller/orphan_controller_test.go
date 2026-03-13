package controller

import (
	"context"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

	. "gopkg.in/check.v1"
)

// OrphanControllerSuite holds the fake clients and indexers needed for testing
// the OrphanController in isolation.
type OrphanControllerSuite struct {
	kubeClient       *fake.Clientset
	lhClient         *lhfake.Clientset
	extensionsClient *apiextensionsfake.Clientset

	informerFactories *util.InformerFactories

	lhInstanceManagerIndexer cache.Indexer
	lhOrphanIndexer          cache.Indexer

	controller *OrphanController
}

var _ = Suite(&OrphanControllerSuite{})

func (s *OrphanControllerSuite) SetUpTest(c *C) {
	s.kubeClient = fake.NewSimpleClientset()                    // nolint: staticcheck
	s.lhClient = lhfake.NewSimpleClientset()                    // nolint: staticcheck
	s.extensionsClient = apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	s.informerFactories = util.NewInformerFactories(TestNamespace, s.kubeClient, s.lhClient, controller.NoResyncPeriodFunc())

	s.lhInstanceManagerIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	s.lhOrphanIndexer = s.informerFactories.LhInformerFactory.Longhorn().V1beta2().Orphans().Informer().GetIndexer()

	var err error
	s.controller, err = newTestOrphanController(s.lhClient, s.kubeClient, s.extensionsClient, s.informerFactories, TestNode1)
	c.Assert(err, IsNil)
}

func newTestOrphanController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*OrphanController, error) {

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	oc, err := NewOrphanController(logger, ds, scheme.Scheme, kubeClient, controllerID, TestNamespace)
	if err != nil {
		return nil, err
	}

	fakeRecorder := record.NewFakeRecorder(eventRecorderBufferSize)
	oc.eventRecorder = fakeRecorder
	for index := range oc.cacheSyncs {
		oc.cacheSyncs[index] = alwaysReady
	}

	return oc, nil
}

// TestGetRunningInstanceManagerClientNotFound verifies that
// getRunningInstanceManagerClient returns (nil, nil) when the named instance
// manager does not exist in the datastore. This is the "clean" path: there is
// nothing to connect to and nothing to delete.
func (s *OrphanControllerSuite) TestGetRunningInstanceManagerClientNotFound(c *C) {
	// The indexer is empty, so GetInstanceManagerRO will return NotFound.
	imc, err := s.controller.getRunningInstanceManagerClient("nonexistent-im")

	c.Assert(err, IsNil)
	c.Assert(imc, IsNil)
}

// TestGetRunningInstanceManagerClientNotRunning verifies that
// getRunningInstanceManagerClient returns (nil, error) when the named instance
// manager exists but is not in Running state. NewInstanceManagerClient rejects
// non-running instance managers, so the caller logs a warning and continues
// finalizing the orphan without deleting the instance.
func (s *OrphanControllerSuite) TestGetRunningInstanceManagerClientNotRunning(c *C) {
	// Build an IM that is in Error state and has no IP assigned.
	// NewInstanceManagerClient will reject it because IP is empty.
	errorIM := newInstanceManager(
		"error-state-im",
		longhorn.InstanceManagerStateError,
		TestOwnerID1, TestNode1, "", // empty IP
		map[string]longhorn.InstanceProcess{},
		map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeV1,
		TestInstanceManagerImage,
		false,
	)

	im, err := s.lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
		context.TODO(), errorIM, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhInstanceManagerIndexer.Add(im), IsNil)

	imc, err := s.controller.getRunningInstanceManagerClient("error-state-im")

	// NewInstanceManagerClient must reject the IM (empty IP / wrong state).
	c.Assert(err, NotNil)
	c.Assert(imc, IsNil)
}

// TestGetRunningInstanceManagerClientByName verifies the core fix for issue
// #12786: getRunningInstanceManagerClient looks up the instance manager
// directly by name rather than fetching whatever IM happens to be running on
// the node. Prior to the fix, the controller called
// GetRunningInstanceManagerByNodeRO and then verified the returned IM's name
// matched the orphan's IM name. If the node had a different running IM
// (e.g., after a restart), the check silently skipped cleanup even though the
// target IM might still be reachable.
//
// This test places two IMs in the datastore:
//   - "running-im"  -- the current running IM on the node (running state, IP set)
//   - "specific-im" -- the IM recorded in the orphan (error state, empty IP)
//
// With the new code, getRunningInstanceManagerClient("specific-im") must look
// up "specific-im" directly and return a non-nil error from
// NewInstanceManagerClient (because the IM is not running), demonstrating
// that the name-based lookup is used. With the old code, the function would
// have fetched "running-im", found that names differ, and silently returned
// (nil, nil).
func (s *OrphanControllerSuite) TestGetRunningInstanceManagerClientByName(c *C) {
	// "running-im" is the healthy IM currently active on the node.
	runningIM := newInstanceManager(
		"running-im",
		longhorn.InstanceManagerStateRunning,
		TestOwnerID1, TestNode1, TestIP1,
		map[string]longhorn.InstanceProcess{},
		map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeV1,
		TestInstanceManagerImage,
		false,
	)
	im1, err := s.lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
		context.TODO(), runningIM, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhInstanceManagerIndexer.Add(im1), IsNil)

	// "specific-im" is the IM recorded in the orphan; it is in error state.
	specificIM := newInstanceManager(
		"specific-im",
		longhorn.InstanceManagerStateError,
		TestOwnerID1, TestNode1, "", // empty IP -> NewInstanceManagerClient will fail
		map[string]longhorn.InstanceProcess{},
		map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeV1,
		TestExtraInstanceManagerImage,
		false,
	)
	im2, err := s.lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
		context.TODO(), specificIM, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(s.lhInstanceManagerIndexer.Add(im2), IsNil)

	// New code: looks up "specific-im" by name.
	// Because "specific-im" exists but is not running, NewInstanceManagerClient
	// returns an error -- confirming that the name-based lookup was used and
	// NOT the node-level running IM lookup.
	imc, err := s.controller.getRunningInstanceManagerClient("specific-im")

	c.Assert(err, NotNil, Commentf("expected error from NewInstanceManagerClient for non-running IM"))
	c.Assert(imc, IsNil)
}
