package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	. "gopkg.in/check.v1"

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

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
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

func (imh *MockInstanceManagerHandler) LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error) {
	return nil, nil, fmt.Errorf("LogInstance is not mocked")
}

type failingCreateInstanceManagerHandler struct {
	MockInstanceManagerHandler
	createErr error
}

func (imh *failingCreateInstanceManagerHandler) GetInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	return nil, fmt.Errorf("cannot find")
}

func (imh *failingCreateInstanceManagerHandler) CreateInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	return nil, imh.createErr
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
				StorageIP:           ip,
				Port:                port,
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
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, "", "", TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		"engine frontend stays suspended during switchover": {
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
							State:     longhorn.InstanceStateSuspended,
							PortStart: TestPort1,
						},
					},
				},
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV2,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateSuspended, longhorn.InstanceStateRunning),
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
		"running local engine becomes unknown while instance manager is deleting": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeLocal,
				TestInstanceManagerImage,
				true,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			false,
		},
		"running local engine becomes unknown without a replacement instance manager": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateError,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeLocal,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, "", 0, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			false,
		},
		"running local engine recovers through direct lookup before instance list": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeLocal,
				TestInstanceManagerImage,
				false,
			),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
			false,
		},
		"missing local engine becomes error after direct lookup": {
			longhorn.InstanceTypeEngine,
			newInstanceManager(
				TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeLocal,
				TestInstanceManagerImage,
				false,
			),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName, TestNode1, "", 0, true, longhorn.InstanceStateError, longhorn.InstanceStateRunning),
			false,
		},
	}
	for name, tc := range testCases {
		fmt.Printf("testing instance handler: %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

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

		if tc.instanceManager != nil && types.IsDataEngineLocal(tc.instanceManager.Spec.DataEngine) {
			tc.obj.(*longhorn.Engine).Spec.DataEngine = longhorn.DataEngineTypeLocal
			tc.expectedObj.(*longhorn.Engine).Spec.DataEngine = longhorn.DataEngineTypeLocal
		}

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
	h := NewInstanceHandler(ds, &MockInstanceManagerHandler{}, fakeRecorder)
	h.instanceGetter = func(_ *longhorn.InstanceManager, dataEngine longhorn.DataEngineType, instanceName string, _ runtime.Object) (*longhorn.InstanceProcess, error) {
		if strings.Contains(instanceName, NonExistingInstance) {
			return nil, fmt.Errorf("cannot find instance %v", instanceName)
		}
		return &longhorn.InstanceProcess{
			Spec: longhorn.InstanceProcessSpec{
				Name:       instanceName,
				DataEngine: dataEngine,
			},
			Status: longhorn.InstanceProcessStatus{
				State:     longhorn.InstanceStateRunning,
				PortStart: TestPort1,
			},
		}, nil
	}
	return h
}

func (s *TestSuite) TestCreateInstanceRecordsFailedStartingEvent(c *C) {
	fakeRecorder := record.NewFakeRecorder(5)
	h := &InstanceHandler{
		instanceManagerHandler: &failingCreateInstanceManagerHandler{
			createErr: fmt.Errorf("engine frontend create failed"),
		},
		eventRecorder: fakeRecorder,
	}

	ef := &longhorn.EngineFrontend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      NonExistingInstance,
			Namespace: TestNamespace,
		},
	}

	err := h.createInstance(NonExistingInstance, longhorn.DataEngineTypeV2, ef)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, ".*engine frontend create failed.*")

	select {
	case event := <-fakeRecorder.Events:
		c.Assert(strings.Contains(event, corev1.EventTypeWarning), Equals, true)
		c.Assert(strings.Contains(event, constant.EventReasonFailedStarting), Equals, true)
		c.Assert(strings.Contains(event, "Error starting "+NonExistingInstance+": engine frontend create failed"), Equals, true)
	default:
		c.Fatal("expected one FailedStarting event")
	}
}

// stubInstanceManagerHandler is a configurable InstanceManagerHandler stub that records
// whether CreateInstance and DeleteInstance were invoked.
type stubInstanceManagerHandler struct {
	getInstance  *longhorn.InstanceProcess
	getErr       error
	createCalled bool
	deleteCalled bool
	createErr    error
	deleteErr    error
}

func (h *stubInstanceManagerHandler) GetInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	return h.getInstance, h.getErr
}

func (h *stubInstanceManagerHandler) CreateInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	h.createCalled = true
	if h.createErr != nil {
		return nil, h.createErr
	}
	return &longhorn.InstanceProcess{}, nil
}

func (h *stubInstanceManagerHandler) DeleteInstance(obj interface{}) error {
	h.deleteCalled = true
	return h.deleteErr
}

func (h *stubInstanceManagerHandler) LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error) {
	return nil, nil, fmt.Errorf("LogInstance is not mocked")
}

// TestCreateInstanceReapsStaleStoppedV1Record covers longhorn/longhorn#13687: a stale
// `stopped` v1 process record makes GetInstance succeed, which must not turn createInstance
// into a silent no-op. The stale record has to be reaped so the next reconcile can recreate.
func (s *TestSuite) TestCreateInstanceReapsStaleStoppedV1Record(c *C) {
	newEngineObj := func() *longhorn.Engine {
		return &longhorn.Engine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ExistingInstance,
				Namespace: TestNamespace,
			},
		}
	}

	testCases := map[string]struct {
		dataEngine   longhorn.DataEngineType
		getInstance  *longhorn.InstanceProcess
		getErr       error
		expectDelete bool
		expectCreate bool
		expectReaped bool
	}{
		"v1 stale stopped record is reaped, not created": {
			dataEngine:   longhorn.DataEngineTypeV1,
			getInstance:  &longhorn.InstanceProcess{Status: longhorn.InstanceProcessStatus{State: longhorn.InstanceStateStopped}},
			getErr:       nil,
			expectDelete: true,
			expectCreate: false,
			expectReaped: true,
		},
		"v1 running record is left untouched": {
			dataEngine:   longhorn.DataEngineTypeV1,
			getInstance:  &longhorn.InstanceProcess{Status: longhorn.InstanceProcessStatus{State: longhorn.InstanceStateRunning}},
			getErr:       nil,
			expectDelete: false,
			expectCreate: false,
		},
		"v1 missing record proceeds to create": {
			dataEngine:   longhorn.DataEngineTypeV1,
			getInstance:  nil,
			getErr:       fmt.Errorf("cannot find process"),
			expectDelete: false,
			expectCreate: true,
		},
		"v2 stopped instance proceeds to create": {
			dataEngine:   longhorn.DataEngineTypeV2,
			getInstance:  nil,
			getErr:       fmt.Errorf("instance is stopped"),
			expectDelete: false,
			expectCreate: true,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing createInstance stale stopped record: %v\n", name)

		stub := &stubInstanceManagerHandler{
			getInstance: tc.getInstance,
			getErr:      tc.getErr,
		}
		h := &InstanceHandler{
			instanceManagerHandler: stub,
			eventRecorder:          record.NewFakeRecorder(10),
		}

		err := h.createInstance(ExistingInstance, tc.dataEngine, newEngineObj())
		if tc.expectReaped {
			c.Assert(errors.Is(err, errStaleInstanceReaped), Equals, true)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(stub.deleteCalled, Equals, tc.expectDelete)
		c.Assert(stub.createCalled, Equals, tc.expectCreate)
	}
}

// registryInstanceManagerHandler models a v1 process registry: GetInstance answers for a
// recorded instance (a stopped record answers successfully, matching ProcessGet), DeleteInstance
// removes the record, and CreateInstance registers a running one but fails if the name is still
// taken (matching registerProcess returning AlreadyExists). It is used to drive the full recovery
// sequence across successive reconciles.
type registryInstanceManagerHandler struct {
	instance *longhorn.InstanceProcess // nil means the process is not registered
	calls    []string
}

func (h *registryInstanceManagerHandler) GetInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	if h.instance == nil {
		return nil, fmt.Errorf("cannot find process")
	}
	return h.instance, nil
}

func (h *registryInstanceManagerHandler) CreateInstance(obj interface{}) (*longhorn.InstanceProcess, error) {
	if h.instance != nil {
		return nil, fmt.Errorf("already exists")
	}
	h.instance = &longhorn.InstanceProcess{Status: longhorn.InstanceProcessStatus{State: longhorn.InstanceStateRunning}}
	h.calls = append(h.calls, "create")
	return h.instance, nil
}

func (h *registryInstanceManagerHandler) DeleteInstance(obj interface{}) error {
	h.instance = nil
	h.calls = append(h.calls, "delete")
	return nil
}

func (h *registryInstanceManagerHandler) LogInstance(ctx context.Context, obj interface{}) (*engineapi.InstanceManagerClient, *imapi.LogStream, error) {
	return nil, nil, fmt.Errorf("LogInstance is not mocked")
}

// TestCreateInstanceRecoversFromStaleStoppedV1Record drives the end-to-end recovery sequence for
// longhorn/longhorn#13687: starting from a leaked `stopped` v1 process record, the stale record is
// reaped, the following reconcile recreates the engine successfully, and a healthy instance then
// stays a stable no-op. A regression in the reap-then-recreate wiring would leave the engine
// wedged and fail these assertions.
func (s *TestSuite) TestCreateInstanceRecoversFromStaleStoppedV1Record(c *C) {
	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ExistingInstance,
			Namespace: TestNamespace,
		},
	}
	// Start with a leaked stopped v1 process record in the registry.
	stub := &registryInstanceManagerHandler{
		instance: &longhorn.InstanceProcess{Status: longhorn.InstanceProcessStatus{State: longhorn.InstanceStateStopped}},
	}
	h := &InstanceHandler{
		instanceManagerHandler: stub,
		eventRecorder:          record.NewFakeRecorder(10),
	}

	// Reconcile 1: the stale stopped record is reaped instead of being treated as already created.
	c.Assert(errors.Is(h.createInstance(ExistingInstance, longhorn.DataEngineTypeV1, engine), errStaleInstanceReaped), Equals, true)
	c.Assert(stub.instance, IsNil)
	c.Assert(stub.calls, DeepEquals, []string{"delete"})

	// Reconcile 2: with the record gone, the engine is recreated successfully.
	c.Assert(h.createInstance(ExistingInstance, longhorn.DataEngineTypeV1, engine), IsNil)
	c.Assert(stub.instance, NotNil)
	c.Assert(stub.instance.Status.State, Equals, longhorn.InstanceStateRunning)
	c.Assert(stub.calls, DeepEquals, []string{"delete", "create"})

	// Reconcile 3: a running instance is a stable no-op, so the loop converges without churning.
	c.Assert(h.createInstance(ExistingInstance, longhorn.DataEngineTypeV1, engine), IsNil)
	c.Assert(stub.calls, DeepEquals, []string{"delete", "create"})
}

// TestReconcileInstanceStateKeepsSalvageRequestedWhenReapingStaleStoppedV1Record drives the reap
// through the full ReconcileInstanceState path to prove the salvage flag is preserved: when a stale
// stopped v1 record coexists with SalvageRequested=true, the first reconcile only reaps the record
// and must leave SalvageExecuted=false (so the volume controller does not clear SalvageRequested
// before the salvaged instance exists); the next reconcile recreates the instance and only then
// sets SalvageExecuted=true. A regression that set SalvageExecuted after a reap would fail here.
func (s *TestSuite) TestReconcileInstanceStateKeepsSalvageRequestedWhenReapingStaleStoppedV1Record(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer().Add(ei)
	c.Assert(err, IsNil)

	imImageSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), newDefaultInstanceManagerImageSetting(), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(imImageSetting)
	c.Assert(err, IsNil)

	im, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), newInstanceManager(
		TestInstanceManagerName, longhorn.InstanceManagerStateRunning,
		TestOwnerID1, TestNode1, TestIP1,
		map[string]longhorn.InstanceProcess{},
		map[string]longhorn.InstanceProcess{},
		map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeV1,
		TestInstanceManagerImage,
		false,
	), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer().Add(im)
	c.Assert(err, IsNil)

	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
	err = informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod)
	c.Assert(err, IsNil)
	_, err = kubeClient.CoreV1().Pods(im.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	node, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer().Add(node)
	c.Assert(err, IsNil)

	h := newTestInstanceHandler(lhClient, kubeClient, extensionsClient, informerFactories)
	// Start with a leaked stopped v1 process record so the first createInstance reaps it.
	registry := &registryInstanceManagerHandler{
		instance: &longhorn.InstanceProcess{Status: longhorn.InstanceProcessStatus{State: longhorn.InstanceStateStopped}},
	}
	h.instanceManagerHandler = registry

	engine := newEngine(ExistingInstance, "", TestInstanceManagerName, TestNode1, "", 0, false, longhorn.InstanceStateStopped, longhorn.InstanceStateRunning)
	engine.Spec.SalvageRequested = true

	// Reconcile 1: the stale stopped record is reaped, and SalvageExecuted must stay false so the
	// salvage request survives until the instance is actually recreated.
	c.Assert(h.ReconcileInstanceState(engine, &engine.Spec.InstanceSpec, &engine.Status.InstanceStatus), IsNil)
	c.Assert(registry.calls, DeepEquals, []string{"delete"})
	c.Assert(engine.Status.SalvageExecuted, Equals, false)
	c.Assert(engine.Spec.SalvageRequested, Equals, true)
	c.Assert(engine.Status.CurrentState, Equals, longhorn.InstanceStateStopped)

	// Reconcile 2: with the record gone the instance is recreated, and only now is SalvageExecuted set.
	c.Assert(h.ReconcileInstanceState(engine, &engine.Spec.InstanceSpec, &engine.Status.InstanceStatus), IsNil)
	c.Assert(registry.calls, DeepEquals, []string{"delete", "create"})
	c.Assert(engine.Status.SalvageExecuted, Equals, true)
}

func (s *TestSuite) TestRunningLocalEngineRebindsToReplacementInstanceManager(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	h := newTestInstanceHandler(lhClient, kubeClient, extensionsClient, informerFactories)
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	podIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	oldIM := newInstanceManager(
		"old-local-im", longhorn.InstanceManagerStateError,
		TestOwnerID1, TestNode1, TestIP1,
		map[string]longhorn.InstanceProcess{}, nil, map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeLocal, TestInstanceManagerImage, true)
	replacementIM := newInstanceManager(
		"replacement-local-im", longhorn.InstanceManagerStateRunning,
		TestOwnerID1, TestNode1, TestIP2,
		map[string]longhorn.InstanceProcess{
			ExistingInstance: {
				Spec: longhorn.InstanceProcessSpec{Name: ExistingInstance},
				Status: longhorn.InstanceProcessStatus{
					State:     longhorn.InstanceStateRunning,
					PortStart: TestPort1,
				},
			},
		}, nil, map[string]longhorn.InstanceProcess{},
		longhorn.DataEngineTypeLocal, TestInstanceManagerImage, false)

	for _, im := range []*longhorn.InstanceManager{oldIM, replacementIM} {
		created, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(imIndexer.Add(created), IsNil)
	}

	pod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, replacementIM.Name, replacementIM.Namespace, replacementIM.Spec.NodeID)
	_, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(podIndexer.Add(pod), IsNil)

	node, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""), metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(nodeIndexer.Add(node), IsNil)

	e := newEngine(ExistingInstance, TestEngineImage, oldIM.Name, TestNode1, TestIP1, TestPort1, true, longhorn.InstanceStateUnknown, longhorn.InstanceStateRunning)
	e.Spec.DataEngine = longhorn.DataEngineTypeLocal
	err = h.ReconcileInstanceState(e, &e.Spec.InstanceSpec, &e.Status.InstanceStatus)
	c.Assert(err, IsNil)
	c.Assert(e.Status.CurrentState, Equals, longhorn.InstanceStateRunning)
	c.Assert(e.Status.InstanceManagerName, Equals, replacementIM.Name)
	c.Assert(e.Status.IP, Equals, TestIP2)
}
