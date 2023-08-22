package controller

import (
	"fmt"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	"github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kubernetes/pkg/controller"
)

const (
	TestObjectEndpointName    = "test-object-endpoint"
	TestObjectEndpointPVName  = "pv-test-object-endpoint"
	TestObjectEndpointPVCName = "pvc-test-object-endpoint"
	TestObjectEndpointImage   = "quay.io/s3gw/s3gw:latest"
)

type ObjectEndpointTestCase struct {
	state               longhorn.ObjectEndpointState
	expectedState       longhorn.ObjectEndpointState
	controllerID        string
	objectEndpointNames []string
}

func (s *TestSuite) TestReconcileObjectEndpoint(c *C) {
	rolloutOwnerID := TestNode1

	testCases := map[string]ObjectEndpointTestCase{
		"create object endpoint": {
			state:         longhorn.ObjectEndpointStateUnknown,
			expectedState: longhorn.ObjectEndpointStateStarting,
		},
		"started object endpoint": {
			state:         longhorn.ObjectEndpointStateStarting,
			expectedState: longhorn.ObjectEndpointStateRunning,
		},
	}

	for name, tc := range testCases {
		if len(tc.objectEndpointNames) == 0 {
			tc.objectEndpointNames = append(tc.objectEndpointNames, TestObjectEndpointName)
		}

		if tc.controllerID == "" {
			tc.controllerID = rolloutOwnerID
		}

		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		extensionsClient := apiextensionsfake.NewSimpleClientset()

		objectEndpointController := newFakeObjectEndpointController(
			kubeClient, kubeInformerFactory,
			lhClient, lhInformerFactory,
			extensionsClient,
			tc.controllerID,
		)

		err := objectEndpointController.syncObjectEndpoint(tc.objectEndpointNames[0])
		c.Assert(err, IsNil)
	}
}

func newFakeObjectEndpointController(
	kubeClient *fake.Clientset,
	kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset,
	lhInformerFactory lhinformers.SharedInformerFactory,
	extensionClient *apiextensionsfake.Clientset,
	controllerID string) *ObjectEndpointController {

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionClient, TestNamespace)

	c := NewObjectEndpointController(logger, ds, scheme.Scheme, kubeClient, controllerID, TestNamespace, TestObjectEndpointImage)

	return c
}
