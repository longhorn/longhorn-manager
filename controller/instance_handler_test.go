package controller

import (
	"fmt"
	"strings"

	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	imapi "github.com/longhorn/longhorn-instance-manager/api"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

const (
	NonExistingInstance = "nil-instance"
	ExistingInstance    = "existing-instance"
)

type MockInstanceManagerHandler struct{}

func (imh *MockInstanceManagerHandler) GetInstance(obj interface{}) (*types.InstanceProcess, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return nil, fmt.Errorf("cannot find")
	}
	return &types.InstanceProcess{
		Spec: types.InstanceProcessSpec{
			Name:      ExistingInstance,
			CreatedAt: getTestNow(),
		},
		Status: types.InstanceProcessStatus{
			State: types.InstanceStateRunning,
		},
	}, nil
}

func (imh *MockInstanceManagerHandler) CreateInstance(obj interface{}, uuid string) (*types.InstanceProcess, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return &types.InstanceProcess{}, nil
	}
	return nil, fmt.Errorf("already exists")
}

func (imh *MockInstanceManagerHandler) DeleteInstance(obj interface{}) (*types.InstanceProcess, error) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := metadata.GetName()
	if strings.Contains(name, NonExistingInstance) {
		return nil, fmt.Errorf("cannot find")
	}
	return &types.InstanceProcess{}, nil
}

func (imh *MockInstanceManagerHandler) LogInstance(obj interface{}) (*imapi.LogStream, error) {
	return nil, fmt.Errorf("LogInstance is not mocked")
}

func newEngine(name, currentImage, imName, ip string, port int, started bool, currentState, desireState types.InstanceState) *longhorn.Engine {
	return &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: types.EngineSpec{
			InstanceSpec: types.InstanceSpec{
				VolumeName:  TestVolumeName,
				VolumeSize:  TestVolumeSize,
				DesireState: desireState,
				OwnerID:     TestOwnerID1,
				NodeID:      TestNode1,
				EngineImage: TestEngineImage,
			},
		},
		Status: types.EngineStatus{
			InstanceStatus: types.InstanceStatus{
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
		imType types.InstanceManagerType
		//instance manager setup
		instanceManager *longhorn.InstanceManager

		obj runtime.Object

		//status expectation
		expectedObj runtime.Object
		// nil means there is no need to check instance map
		expectedInstanceMap map[string]types.InstanceProcess
		errorOut            bool
	}{
		// 1. keep stopped
		"engine keeps stopped": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			nil,
			false,
		},
		// 2. desire state becomes running. the instance map of instance manager will be updated here hence we need to check it.
		"engine desire state becomes running": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			map[string]types.InstanceProcess{
				NonExistingInstance: types.InstanceProcess{
					Spec: types.InstanceProcessSpec{
						Name:      NonExistingInstance,
						CreatedAt: getTestNow(),
						UUID:      getInstanceUUID(),
					},
					Status: types.InstanceProcessStatus{
						State: types.InstanceStateStarting,
					},
				},
			},
			false,
		},
		// 3. wait for im update
		"starting engine waits for im update": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{
				NonExistingInstance: types.InstanceProcess{
					Spec: types.InstanceProcessSpec{
						Name:      NonExistingInstance,
						CreatedAt: getTestNow(),
					},
					Status: types.InstanceProcessStatus{
						State: types.InstanceStateStarting,
					},
				},
			}, false),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			nil,
			false,
		},
		// 3.1. become starting with `PortStart` set
		"engine becomes starting with PortStart set": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateStarting,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			nil,
			false,
		},
		// 4. become running from starting state
		"engine becomes running from starting state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStarting, types.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			nil,
			false,
		},
		// 5. keep running
		"engine keeps running": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			nil,
			false,
		},
		// 6. desire state becomes stopped. the instance map of instance manager will be updated here hence we need to check it.
		"engine desire state becomes stopped": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			map[string]types.InstanceProcess{
				ExistingInstance: types.InstanceProcess{
					Spec: types.InstanceProcessSpec{
						Name:      ExistingInstance,
						CreatedAt: getTestNow(),
						DeletedAt: getTestNow(),
					},
					Status: types.InstanceProcessStatus{
						State:     types.InstanceStateRunning,
						PortStart: TestPort1,
					},
				},
			},
			false,
		},
		// 7. wait for update
		"stopping engine waits for im update": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
							DeletedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			nil,
			false,
		},
		// 8.1.1. become stopping
		"engine becomes stopping": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
							DeletedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateStopping,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, false, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			nil,
			false,
		},
		// 8.1.2. still stopping
		"engine is still stopping": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
							DeletedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateStopping,
							PortStart: TestPort1,
						},
					},
				}, false),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			newEngine(ExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			nil,
			false,
		},
		// 8.1.3. become stopped from stopping
		"engine becomes stopped from stopping state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", TestInstanceManagerName1, "", 0, false, types.InstanceStateStopping, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			nil,
			false,
		},
		// 8.2. become stopped from running
		"engine becomes stopped from running state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			nil,
			false,
		},

		// corner case1: invalid desireState
		"engine gets invalid desire state": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopping),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopping),
			nil,
			true,
		},
		// corner case2: the instance currentState is running but the related instance manager is being deleting
		"engine keeps running but instance manager is being deleting": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1,
				map[string]types.InstanceProcess{
					ExistingInstance: types.InstanceProcess{
						Spec: types.InstanceProcessSpec{
							Name:      ExistingInstance,
							CreatedAt: getTestNow(),
						},
						Status: types.InstanceProcessStatus{
							State:     types.InstanceStateRunning,
							PortStart: TestPort1,
						},
					},
				}, true),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(NonExistingInstance, "", "", "", 0, true, types.InstanceStateError, types.InstanceStateRunning),
			nil,
			false,
		},
		// corner case3: the instance is stopped and the related instance manager is being deleting
		"engine keeps stopped and instance manager is being deleting": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, true),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			newEngine(NonExistingInstance, "", "", "", 0, false, types.InstanceStateStopped, types.InstanceStateStopped),
			nil,
			false,
		},
		// corner case4: the instance currentState is running but the related instance manager is starting
		"engine keeps running but instance manager somehow is starting": {
			types.InstanceManagerTypeEngine,
			newInstanceManager(TestInstanceManagerName1, types.InstanceManagerTypeEngine, types.InstanceManagerStateStarting, TestOwnerID1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
			newEngine(NonExistingInstance, TestEngineImage, TestInstanceManagerName1, TestIP1, TestPort1, true, types.InstanceStateRunning, types.InstanceStateRunning),
			newEngine(NonExistingInstance, "", "", "", 0, true, types.InstanceStateError, types.InstanceStateRunning),
			nil,
			false,
		},
	}
	for name, tc := range testCases {
		fmt.Printf("testing instance handler: %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		h := newTestInstanceHandler(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		ei, err := lhClient.LonghornV1alpha1().EngineImages(TestNamespace).Create(newEngineImage(types.EngineImageStateReady))
		c.Assert(err, IsNil)
		eiIndexer := lhInformerFactory.Longhorn().V1alpha1().EngineImages().Informer().GetIndexer()
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		im, err := lhClient.LonghornV1alpha1().InstanceManagers(TestNamespace).Create(tc.instanceManager)
		c.Assert(err, IsNil)
		imIndexer := lhInformerFactory.Longhorn().V1alpha1().InstanceManagers().Informer().GetIndexer()
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)

		ima, err := h.ds.GetInstanceManagerBySelector(TestNode1, getTestEngineImageName(), string(tc.imType))
		c.Assert(err, IsNil)
		c.Assert(ima, NotNil)
		c.Assert(ima, DeepEquals, im)

		var spec *types.InstanceSpec
		var status *types.InstanceStatus
		if tc.imType == types.InstanceManagerTypeEngine {
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
		err = h.ReconcileInstanceState(tc.obj, spec, status, tc.imType)
		if tc.errorOut {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(tc.obj, DeepEquals, tc.expectedObj)
			if tc.expectedInstanceMap != nil {
				im, err := lhClient.LonghornV1alpha1().InstanceManagers(TestNamespace).Get(TestInstanceManagerName1, metav1.GetOptions{})
				c.Assert(err, IsNil)
				c.Assert(im.Status.Instances, DeepEquals, tc.expectedInstanceMap)
			}
		}
	}
}

func newTestInstanceHandler(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *InstanceHandler {
	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1alpha1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1alpha1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1alpha1().Settings()
	imInformer := lhInformerFactory.Longhorn().V1alpha1().InstanceManagers()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer, imInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer,
		kubeClient, TestNamespace)
	fakeRecorder := record.NewFakeRecorder(100)

	ih := NewInstanceHandler(ds, &MockInstanceManagerHandler{}, fakeRecorder, podInformer, kubeClient, TestNamespace)
	ih.nowHandler = getTestNow
	ih.uuidGenerator = getInstanceUUID
	return ih
}
