package controller

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	NonExistingInstance = "nil-instance"
	ExistingInstance    = "existing-instance"
)

type MockInstanceManagerHandler struct{}

func (imh *MockInstanceManagerHandler) GetInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return nil, fmt.Errorf("cannot find")
	}
	return &longhorn.InstanceProcess{}, nil
}

func (imh *MockInstanceManagerHandler) CreateInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return &longhorn.InstanceProcess{}, nil
	}
	return nil, fmt.Errorf("already exists")
}

func (imh *MockInstanceManagerHandler) DeleteInstance(obj interface{}) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return fmt.Errorf("cannot find")
	}
	return nil
}

func (imh *MockInstanceManagerHandler) LogInstance(obj interface{}) (*imapi.LogStream, error) {
	return nil, fmt.Errorf("LogInstance is not mocked")
}

func newEngine(name, currentImage, imName, nodeName, ip string, port int, started bool, currentState, desireState longhorn.InstanceState) *longhorn.Engine {
	return &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels:    types.GetVolumeLabels(TestVolumeName),
		},
		Spec: longhorn.EngineSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName:  TestVolumeName,
				VolumeSize:  TestVolumeSize,
				DesireState: desireState,
				NodeID:      nodeName,
				EngineImage: TestEngineImage,
			},
		},
		Status: longhorn.EngineStatus{
			InstanceStatus: longhorn.InstanceStatus{
				OwnerID:             TestOwnerID1,
				CurrentState:        currentState,
				CurrentImage:        currentImage,
				InstanceManagerName: imName,
				IP:                  ip,
				Port:                port,
				Started:             started,
			},
		},
	}
}

func (s *TestSuite) TestReconcileInstanceState(c *C) {
	testCases := map[string]struct {
		imType longhorn.InstanceManagerType
		//instance manager setup
		instanceManager *longhorn.InstanceManager

		obj runtime.Object

		//status expectation
		expectedObj runtime.Object
		errorOut    bool
	}{
		// 1. keep stopped
		"engine keeps stopped": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
		// 2. desire state becomes running
		"engine desire state becomes running": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			false,
		},
		// 3.1.1. become starting
		"engine becomes starting": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateStarting,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, false, longhorn.InstanceStateStarting, longhorn.InstanceStateRunning),
			false,
		},
		// 3.1.3. become running from starting
		"engine becomes running from starting state": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, false, longhorn.InstanceStateStarting, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		// 3.2. become running from stopped
		"engine becomes running from stopped state": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		// 4. keep running
		"engine keeps running": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		// 5. desire state becomes stopped
		"engine desire state becomes stopped": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			false,
		},
		// 6. wait for update
		"stopping engine waits for im update": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			false,
		},
		// 7.1.1. become stopping
		"engine becomes stopping": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateStopping,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			false,
		},
		// 7.1.2. still stopping
		"engine is still stopping": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateStopping,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			false,
		},
		// 7.1.3. become stopped from stopping
		"engine becomes stopped from stopping state": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
		// 7.2. become stopped from running
		"engine becomes stopped from running state": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, "", TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},

		// corner case1: invalid desireState
		"engine gets invalid desire state": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopping),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopping),
			true,
		},
		// corner case2: the instance currentState is running but the related instance manager is being deleting
		"engine keeps running but instance manager is being deleting": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{
					ExistingInstance: {
						Spec: longhorn.InstanceProcessSpec{
							Name: ExistingInstance,
						},
						Status: longhorn.InstanceProcessStatus{
							State:     longhorn.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, true),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, true, longhorn.InstanceStateError, longhorn.InstanceStateRunning),
			false,
		},
		// corner case3: the instance is stopped and the related instance manager is being deleting
		"engine keeps stopped and instance manager is being deleting": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, true),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
		// corner case4: the instance currentState is running but the related instance manager is starting
		"engine keeps running but instance manager somehow is starting": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateStarting, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{}, false),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, TestNode1, "", 0, true, longhorn.InstanceStateError, longhorn.InstanceStateRunning),
			false,
		},
		// corner case5: the node is down
		"engine node is down": {
			longhorn.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerStateUnknown, TestOwnerID1, TestNode1, TestIP1, map[string]longhorn.InstanceProcess{
				ExistingInstance: {
					Spec: longhorn.InstanceProcessSpec{
						Name: ExistingInstance,
					},
					Status: longhorn.InstanceProcessStatus{
						State:     longhorn.InstanceStateRunning,
						PortStart: TestPort1,
					},
				},
			}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode1, "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			false,
		},
		// corner case6: engine node is deleted
		"engine keeps running but the node is deleted": {
			longhorn.InstanceManagerTypeEngine,
			nil,
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode2, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestNode2, "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			false,
		},
		// corner case7
		"engine desire state becomes stopped after the node is deleted": {
			longhorn.InstanceManagerTypeEngine,
			nil,
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
	}
	for name, tc := range testCases {
		fmt.Printf("testing instance handler: %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		eiIndexer := lhInformerFactory.Longhorn().V1beta1().EngineImages().Informer().GetIndexer()
		sIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()

		h := newTestInstanceHandler(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		ei, err := lhClient.LonghornV1beta1().EngineImages(TestNamespace).Create(context.TODO(), newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err = lhClient.LonghornV1beta1().Settings(TestNamespace).Create(context.TODO(), imImageSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		if tc.instanceManager != nil {
			im, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Create(context.TODO(), tc.instanceManager, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			imIndexer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers().Informer().GetIndexer()
			err = imIndexer.Add(im)
			c.Assert(err, IsNil)
		}

		node, err := lhClient.LonghornV1beta1().Nodes(TestNamespace).Create(context.TODO(), newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		nodeIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()
		err = nodeIndexer.Add(node)
		c.Assert(err, IsNil)

		var spec *longhorn.InstanceSpec
		var status *longhorn.InstanceStatus
		if tc.imType == longhorn.InstanceManagerTypeEngine {
			e, ok := tc.obj.(*longhorn.Engine)
			c.Assert(ok, Equals, true)
			spec = &e.Spec.InstanceSpec
			status = &e.Status.InstanceStatus
		} else {
			r, ok := tc.obj.(*longhorn.Replica)
			c.Assert(ok, Equals, true)
			spec = &r.Spec.InstanceSpec
			status = &r.Status.InstanceStatus
		}
		err = h.ReconcileInstanceState(tc.obj, spec, status)
		if tc.errorOut {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(tc.obj, DeepEquals, tc.expectedObj)
		}
	}
}

func newTestInstanceHandler(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *InstanceHandler {
	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, TestNamespace)
	fakeRecorder := record.NewFakeRecorder(100)
	return NewInstanceHandler(ds, &MockInstanceManagerHandler{}, fakeRecorder)
}
