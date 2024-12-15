package controller

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	imtypes "github.com/longhorn/longhorn-instance-manager/pkg/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

	. "gopkg.in/check.v1"
)

const (
	NonExistingInstance = "nil-instance"
	ExistingInstance    = "existing-instance"
)

type MockInstanceManagerHandler struct{}

func (imh *MockInstanceManagerHandler) GetInstance(obj interface{}, isInstanceOnRemoteNode bool) (*longhorn.InstanceProcess, error) {
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

func (imh *MockInstanceManagerHandler) CreateInstance(obj interface{}, isInstanceOnRemoteNode bool) (*longhorn.InstanceProcess, error) {
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

func (imh *MockInstanceManagerHandler) SuspendInstance(obj interface{}) error {
	return fmt.Errorf("SuspendInstance is not mocked")
}

func (imh *MockInstanceManagerHandler) ResumeInstance(obj interface{}) error {
	return fmt.Errorf("ResumeInstance is not mocked")
}

func (imh *MockInstanceManagerHandler) SwitchOverTarget(obj interface{}) error {
	return fmt.Errorf("SwitchOverTarget is not mocked")
}

func (imh *MockInstanceManagerHandler) DeleteTarget(obj interface{}) error {
	return fmt.Errorf("DeleteTarget is not mocked")
}

func (imh *MockInstanceManagerHandler) IsEngine(obj interface{}) bool {
	_, ok := obj.(*longhorn.Engine)
	return ok
}

func (imh *MockInstanceManagerHandler) LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error) {
	return nil, nil, fmt.Errorf("LogInstance is not mocked")
}

func (imh *MockInstanceManagerHandler) RequireRemoteTargetInstance(obj interface{}) (bool, error) {
	return false, nil
}

func newEngine(name, currentImage, imName, nodeName, ip string, port int, started bool, currentState, desireState longhorn.InstanceState) *longhorn.Engine {
	var conditions []longhorn.Condition
	conditions = types.SetCondition(conditions,
		longhorn.InstanceConditionTypeInstanceCreation, longhorn.ConditionStatusTrue,
		"", "")

	conditions = types.SetCondition(conditions,
		imtypes.EngineConditionFilesystemReadOnly, longhorn.ConditionStatusFalse,
		"", "")

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
				Image:       TestEngineImage,
			},
		},
		Status: longhorn.EngineStatus{
			InstanceStatus: longhorn.InstanceStatus{
				OwnerID:             TestOwnerID1,
				CurrentState:        currentState,
				CurrentImage:        currentImage,
				InstanceManagerName: imName,
				IP:                  ip,
				TargetIP:            ip,
				StorageIP:           ip,
				StorageTargetIP:     ip,
				Port:                port,
				TargetPort:          0, // v1 volume doesn't set target port
				Started:             started,
				Conditions:          conditions,
			},
		},
	}
}

func (s *TestSuite) TestReconcileInstanceState(c *C) {
	testCases := map[string]struct {
		instanceType longhorn.InstanceType
		//instance manager setup
		instanceManager *longhorn.InstanceManager

		obj runtime.Object

		//status expectation
		expectedObj runtime.Object
		errorOut    bool
	}{
		// 1. keep stopped
		"engine keeps stopped": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
		// 2. desire state becomes running
		"engine desire state becomes running": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(NonExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			false,
		},
		// 3.1.1. become starting
		"engine becomes starting": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, "", TestInstanceManagerName, TestNode1, "", 0, false, longhorn.InstanceStateStarting, longhorn.InstanceStateRunning),
			false,
		},
		// 3.1.3. become running from starting
		"engine becomes running from starting state": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, "", TestInstanceManagerName, TestNode1, "", 0, false, longhorn.InstanceStateStarting, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		// 3.2. become running from stopped
		"engine becomes running from stopped state": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		// 4. keep running
		"engine keeps running": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		// 5. desire state becomes stopped
		"engine desire state becomes stopped": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, "", TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			false,
		},
		// 6. wait for update
		"stopping engine waits for im update": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			false,
		},
		// 7.1.1. become stopping
		"engine becomes stopping": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, "", TestIP1, TestPort1, false, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			false,
		},
		// 7.1.2. still stopping
		"engine is still stopping": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, "", TestInstanceManagerName, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			false,
		},
		// 7.1.3. become stopped from stopping
		"engine becomes stopped from stopping state": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(NonExistingInstance, "", TestInstanceManagerName, "", "", 0, false, longhorn.InstanceStateStopping, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
		// 7.2. become stopped from running
		"engine becomes stopped from running state": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName, "", TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},

		// corner case1: invalid desireState
		"engine gets invalid desire state": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopping),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopping),
			true,
		},
		// corner case2: the instance currentState is running but the related instance manager is being deleting
		"engine keeps running but instance manager is being deleting": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				true,
			),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName, TestNode1, "", 0, true, longhorn.InstanceStateError, longhorn.InstanceStateRunning),
			false,
		},
		// corner case3: the instance is stopped and the related instance manager is being deleting
		"engine keeps stopped and instance manager is being deleting": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				true,
			),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
		// corner case4: the instance currentState is running but the related instance manager is starting
		"engine keeps running but instance manager somehow is starting": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateStarting,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName, TestNode1, "", 0, true, longhorn.InstanceStateError, longhorn.InstanceStateRunning),
			false,
		},
		// corner case5: the node is down
		"engine node is down": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateUnknown,
				TestOwnerID1, TestNode1, TestIP1,
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
				},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			false,
		},
		// corner case6: engine node is deleted
		"engine keeps running but the node is deleted": {
			longhorn.InstanceTypeEngine,
			nil,
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode2, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode2, "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			false,
		},
		// corner case7
		"engine desire state becomes stopped after the node is deleted": {
			longhorn.InstanceTypeEngine,
			nil,
			newEngine(NonExistingInstance, "", TestInstanceManagerName, "", "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateStopped),
			false,
		},
	}
	for name, tc := range testCases {
		fmt.Printf("testing instance handler: %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset()
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		h := newTestInstanceHandler(lhClient, kubeClient, extensionsClient, informerFactories)

		ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), imImageSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		if tc.instanceManager != nil {
			im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), tc.instanceManager, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
			err = imIndexer.Add(im)
			c.Assert(err, IsNil)

			pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
			err = pIndexer.Add(pod)
			c.Assert(err, IsNil)
			_, err = kubeClient.CoreV1().Pods(im.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
			c.Assert(err, IsNil)
		}

		node, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		err = nodeIndexer.Add(node)
		c.Assert(err, IsNil)

		var spec *longhorn.InstanceSpec
		var status *longhorn.InstanceStatus
		if tc.instanceType == longhorn.InstanceTypeEngine {
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

func newTestInstanceHandler(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset, informerFactories *util.InformerFactories) *InstanceHandler {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	fakeRecorder := record.NewFakeRecorder(100)
	return NewInstanceHandler(ds, &MockInstanceManagerHandler{}, fakeRecorder)
}
