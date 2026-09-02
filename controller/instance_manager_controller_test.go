package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

type InstanceManagerTestCase struct {
	controllerID string
	nodeDown     bool
	nodeID       string

	currentPodStatus *corev1.PodStatus
	currentOwnerID   string
	currentState     longhorn.InstanceManagerState
	currentEngines   map[string]longhorn.InstanceProcess
	currentReplicas  map[string]longhorn.InstanceProcess

	expectedPodCount int
	expectedStatus   longhorn.InstanceManagerStatus
}

func newTolerationSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameTaintToleration),
		},
		Value: "",
	}
}

func newSystemManagedComponentsNodeSelectorSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameSystemManagedComponentsNodeSelector),
		},
		Value: "",
	}
}

func newGuaranteedInstanceManagerCPUSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameGuaranteedInstanceManagerCPU),
		},
		Value: "{\"v1\":\"12\",\"v2\":\"12\"}",
	}
}

func newPriorityClassSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNamePriorityClass),
		},
		Value: "",
	}
}

func newStorageNetworkSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameStorageNetwork),
		},
		Value: "",
	}
}

func newV1DataEngineSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameV1DataEngine),
		},
		Value: "true",
	}
}

func newV2DataEngineSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameV2DataEngine),
		},
		Value: "false",
	}
}

func newInstanceManagerPodLivenessProbeTimeoutSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameInstanceManagerPodLivenessProbeTimeout),
		},
		Value: "0",
	}
}

func newLogPathSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameLogPath),
		},
		Value: "/var/lib/longhorn/logs/",
	}
}

func newDataEngineInterruptModeEnabledSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameDataEngineInterruptModeEnabled),
		},
		Value: "{\"v2\":\"false\"}",
	}
}

func newDataEngineHugepageEnabledSetting() *longhorn.Setting {
	return newSetting(string(types.SettingNameDataEngineHugepageEnabled), `{"v2":"true"}`)
}

func newDataEngineMemorySizeSetting() *longhorn.Setting {
	return newSetting(string(types.SettingNameDataEngineMemorySize), `{"v2":"1024"}`)
}
func newDataEngineIPFamilySetting(value string) *longhorn.Setting {
	return newSetting(string(types.SettingNamePreferredDataEngineIPFamily), value)
}

func newDataEngineIPFamilyTestController(c *C) (*InstanceManagerController, *lhfake.Clientset, *fake.Clientset, cache.Indexer, cache.Indexer, cache.Indexer, cache.Indexer, cache.Indexer, cache.Indexer) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	podIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)
	return imc, lhClient, kubeClient, imIndexer, podIndexer, settingIndexer, volumeIndexer, kubeNodeIndexer, lhNodeIndexer
}
func addDataEngineIPFamilyTestSetting(c *C, lhClient *lhfake.Clientset, settingIndexer cache.Indexer, setting *longhorn.Setting) {
	setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(settingIndexer.Add(setting), IsNil)
}

func fakeInstanceManagerVersionUpdater(im *longhorn.InstanceManager) error {
	im.Status.APIMinVersion = engineapi.MinInstanceManagerAPIVersion
	im.Status.APIVersion = engineapi.CurrentInstanceManagerAPIVersion
	return nil
}

func newTestInstanceManagerController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*InstanceManagerController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()

	proxyConnCounter := util.NewAtomicCounter()
	imc, err := NewInstanceManagerController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID, TestServiceAccount, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	fakeRecorder := record.NewFakeRecorder(100)
	imc.eventRecorder = fakeRecorder
	for index := range imc.cacheSyncs {
		imc.cacheSyncs[index] = alwaysReady
	}
	imc.versionUpdater = fakeInstanceManagerVersionUpdater

	return imc, nil
}

func (s *TestSuite) TestSyncInstanceManager(c *C) {
	var err error

	testCases := map[string]InstanceManagerTestCase{
		"instance manager change ownership": {
			TestNode1, false, TestNode1,
			&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning},
			TestNode2, longhorn.InstanceManagerStateUnknown, nil, nil, 1,
			longhorn.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  longhorn.InstanceManagerStateRunning,
				IP:            TestIP1,
				APIMinVersion: engineapi.MinInstanceManagerAPIVersion,
				APIVersion:    engineapi.CurrentInstanceManagerAPIVersion,
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusTrue,
						Reason: longhorn.InstanceManagerConditionReasonPodRunning,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusTrue,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeSettingSynced,
						Status: longhorn.ConditionStatusTrue,
					},
				},
			},
		},
		"instance manager error then restart immediately": {
			TestNode1, false, TestNode1,
			&corev1.PodStatus{PodIP: "", Phase: corev1.PodFailed},
			TestNode1, longhorn.InstanceManagerStateRunning,
			map[string]longhorn.InstanceProcess{ // Process information will be erased in the next reconcile loop.
				TestEngineName: {
					Spec: longhorn.InstanceProcessSpec{
						Name: TestEngineName,
					},
					Status: longhorn.InstanceProcessStatus{
						State:     longhorn.InstanceStateRunning,
						PortStart: 1000,
					},
				},
			},
			map[string]longhorn.InstanceProcess{ // Process information will be erased in the next reconcile loop.
				TestReplicaName: {
					Spec: longhorn.InstanceProcessSpec{
						Name: TestReplicaName,
					},
					Status: longhorn.InstanceProcessStatus{
						State:     longhorn.InstanceStateRunning,
						PortStart: 1000,
					},
				},
			},
			1,
			longhorn.InstanceManagerStatus{
				OwnerID:                 TestNode1,
				CurrentState:            longhorn.InstanceManagerStateError, // The state will become InstanceManagerStateStarting in the next reconcile loop
				IP:                      TestIP1,
				APIMinVersion:           0,
				APIVersion:              0,
				InstanceEngines:         nil, // Transition to InstanceManagerStateError erases process information.
				InstanceEngineFrontends: nil, // Transition to InstanceManagerStateError erases process information.
				InstanceReplicas:        nil, // Transition to InstanceManagerStateError erases process information.
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusFalse,
						Reason: longhorn.InstanceManagerConditionReasonPodFailed,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusTrue,
					},
				},
			},
		},
		"instance manager node down": {
			TestNode2, true, TestNode1,
			&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning},
			TestNode2, longhorn.InstanceManagerStateRunning, nil, nil, 1,
			longhorn.InstanceManagerStatus{
				OwnerID:       TestNode2,
				CurrentState:  longhorn.InstanceManagerStateUnknown,
				IP:            TestIP1,
				APIMinVersion: engineapi.MinInstanceManagerAPIVersion,
				APIVersion:    engineapi.CurrentInstanceManagerAPIVersion,
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusTrue,
						Reason: longhorn.InstanceManagerConditionReasonPodRunning,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusFalse,
						Reason: longhorn.InstanceManagerConditionReasonNodeDown,
					},
				},
			},
		},
		"instance manager restarting after error": {
			TestNode1, false, TestNode1,
			&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodPending},
			TestNode1, longhorn.InstanceManagerStateError, nil, nil, 1,
			longhorn.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  longhorn.InstanceManagerStateStarting,
				APIMinVersion: 0,
				APIVersion:    0,
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusTrue,
					},
				},
			},
		},
		"instance manager running": {
			TestNode1, false, TestNode1,
			&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning},
			TestNode1, longhorn.InstanceManagerStateStarting, nil, nil, 1,
			longhorn.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  longhorn.InstanceManagerStateRunning,
				IP:            TestIP1,
				APIMinVersion: engineapi.MinInstanceManagerAPIVersion,
				APIVersion:    engineapi.CurrentInstanceManagerAPIVersion,
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusTrue,
						Reason: longhorn.InstanceManagerConditionReasonPodRunning,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusTrue,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeSettingSynced,
						Status: longhorn.ConditionStatusTrue,
					},
				},
			},
		},
		"instance manager starting engine": {
			TestNode1, false, TestNode1,
			nil,
			TestNode1, longhorn.InstanceManagerStateStopped, nil, nil, 1,
			longhorn.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  longhorn.InstanceManagerStateStopped, // The state will become InstanceManagerStateStarting in the next reconcile loop
				APIMinVersion: 0,
				APIVersion:    0,
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusFalse,
						Reason: longhorn.InstanceManagerConditionReasonPodNotFound,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusTrue,
					},
				},
			},
		},
		"instance manager starting replica": {
			TestNode1, false, TestNode1,
			nil,
			TestNode1, longhorn.InstanceManagerStateStopped, nil, nil, 1,
			longhorn.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  longhorn.InstanceManagerStateStopped, // The state will become InstanceManagerStateStarting in the next reconcile loop
				APIMinVersion: 0,
				APIVersion:    0,
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusFalse,
						Reason: longhorn.InstanceManagerConditionReasonPodNotFound,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusTrue,
					},
				},
			},
		},
		"instance manager sync IP": {
			TestNode1, false, TestNode1,
			&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning},
			TestNode1, longhorn.InstanceManagerStateRunning, nil, nil, 1,
			longhorn.InstanceManagerStatus{
				OwnerID:       TestNode1,
				CurrentState:  longhorn.InstanceManagerStateRunning,
				IP:            TestIP2,
				APIMinVersion: engineapi.MinInstanceManagerAPIVersion,
				APIVersion:    engineapi.CurrentInstanceManagerAPIVersion,
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusTrue,
						Reason: longhorn.InstanceManagerConditionReasonPodRunning,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeNodeReady,
						Status: longhorn.ConditionStatusTrue,
					},
					{
						Type:   longhorn.InstanceManagerConditionTypeSettingSynced,
						Status: longhorn.ConditionStatusTrue,
					},
				},
			},
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

		imc, error := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(error, IsNil)

		// Controller logic depends on the existence of DefaultInstanceManagerImage Setting and Danger Zone Settings.
		tolerationSetting := newTolerationSetting()
		tolerationSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), tolerationSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(tolerationSetting)
		c.Assert(err, IsNil)

		guaranteedInstanceManagerCPUSetting := newGuaranteedInstanceManagerCPUSetting()
		guaranteedInstanceManagerCPUSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), guaranteedInstanceManagerCPUSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(guaranteedInstanceManagerCPUSetting)
		c.Assert(err, IsNil)
		storageNetworkSetting := newStorageNetworkSetting()
		storageNetworkSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), storageNetworkSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(storageNetworkSetting)
		c.Assert(err, IsNil)
		priorityClassSetting := newPriorityClassSetting()
		priorityClassSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), priorityClassSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(priorityClassSetting)
		c.Assert(err, IsNil)
		v1DataEngineSetting := newV1DataEngineSetting()
		v1DataEngineSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), v1DataEngineSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(v1DataEngineSetting)
		c.Assert(err, IsNil)
		v2DataEngineSetting := newV2DataEngineSetting()
		v2DataEngineSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), v2DataEngineSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(v2DataEngineSetting)
		c.Assert(err, IsNil)
		instanceManagerPodLivenessProbeTimeoutSetting := newInstanceManagerPodLivenessProbeTimeoutSetting()
		instanceManagerPodLivenessProbeTimeoutSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), instanceManagerPodLivenessProbeTimeoutSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(instanceManagerPodLivenessProbeTimeoutSetting)
		c.Assert(err, IsNil)
		logPathSetting := newLogPathSetting()
		logPathSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), logPathSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(logPathSetting)
		c.Assert(err, IsNil)
		dataEngineInterruptModeEnabledSetting := newDataEngineInterruptModeEnabledSetting()
		dataEngineInterruptModeEnabledSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), dataEngineInterruptModeEnabledSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(dataEngineInterruptModeEnabledSetting)
		c.Assert(err, IsNil)

		systemManagedComponentsNodeSelectorSetting := newSystemManagedComponentsNodeSelectorSetting()
		systemManagedComponentsNodeSelectorSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), systemManagedComponentsNodeSelectorSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(systemManagedComponentsNodeSelectorSetting)
		c.Assert(err, IsNil)

		imImageSetting := newDefaultInstanceManagerImageSetting()
		imImageSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), imImageSetting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(imImageSetting)
		c.Assert(err, IsNil)

		// Create Nodes for test. Conditionally add the first Node.
		if !tc.nodeDown {
			kubeNode1 := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
			kubeNode1.Status.Allocatable = corev1.ResourceList{"cpu": resource.MustParse("4")}
			err = kubeNodeIndexer.Add(kubeNode1)
			c.Assert(err, IsNil)
			_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode1, metav1.CreateOptions{})
			c.Assert(err, IsNil)

			lhNode1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
			err = lhNodeIndexer.Add(lhNode1)
			c.Assert(err, IsNil)
			_, err = lhClient.LonghornV1beta2().Nodes(lhNode1.Namespace).Create(context.TODO(), lhNode1, metav1.CreateOptions{})
			c.Assert(err, IsNil)
		}

		kubeNode2 := newKubernetesNode(TestNode2, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode2.Status.Allocatable = corev1.ResourceList{"cpu": resource.MustParse("4")}
		err = kubeNodeIndexer.Add(kubeNode2)
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode2, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		lhNode2 := newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, "")
		err = lhNodeIndexer.Add(lhNode2)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta2().Nodes(lhNode2.Namespace).Create(context.TODO(), lhNode2, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		currentIP := ""
		if tc.currentState == longhorn.InstanceManagerStateRunning || tc.currentState == longhorn.InstanceManagerStateStarting {
			currentIP = TestIP1
		}
		im := newInstanceManager(
			TestInstanceManagerName, tc.currentState,
			tc.currentOwnerID, tc.nodeID, currentIP,
			tc.currentEngines, nil, tc.currentReplicas,
			longhorn.DataEngineTypeV1,
			TestInstanceManagerImage,
			false,
		)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta2().InstanceManagers(im.Namespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		if tc.currentPodStatus != nil {
			pod := newPod(tc.currentPodStatus, im.Name, im.Namespace, im.Spec.NodeID)
			var containers []corev1.Container
			containers = append(containers, corev1.Container{
				Name:      "instance-manager",
				Args:      []string{"daemon", "--ip-family", types.DataEngineIPFamilyIPv4},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"cpu": resource.MustParse("480m")}}},
			)
			pod.Spec.Containers = containers
			err = pIndexer.Add(pod)
			c.Assert(err, IsNil)
			_, err = kubeClient.CoreV1().Pods(im.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
			c.Assert(err, IsNil)
		}

		err = imc.syncInstanceManager(getKey(im, c))
		c.Assert(err, IsNil)
		podList, err := kubeClient.CoreV1().Pods(im.Namespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(podList.Items, HasLen, tc.expectedPodCount)

		// Check the Pod that was created by the Instance Manager.
		if tc.currentPodStatus == nil {
			pod, err := kubeClient.CoreV1().Pods(im.Namespace).Get(context.TODO(), im.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			c.Assert(pod.Spec.Containers[0].Name, Equals, "instance-manager")
		}

		// Skip checking imc.instanceManagerMonitorMap since the monitor doesn't work in the unit test.

		updatedIM, err := lhClient.LonghornV1beta2().InstanceManagers(im.Namespace).Get(context.TODO(), im.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		if tc.expectedStatus.CurrentState != longhorn.InstanceManagerStateError &&
			tc.expectedStatus.CurrentState != longhorn.InstanceManagerStateUnknown {
			appliedFamily := types.DataEngineIPFamilyDefault
			tc.expectedStatus.IPFamily = &appliedFamily
		}
		for i, condition := range updatedIM.Status.Conditions {
			tc.expectedStatus.Conditions[i].LastTransitionTime = condition.LastTransitionTime
			tc.expectedStatus.Conditions[i].LastProbeTime = condition.LastProbeTime
			tc.expectedStatus.Conditions[i].Message = condition.Message
		}
		c.Assert(updatedIM.Status, DeepEquals, tc.expectedStatus)
	}
}

// createDangerZoneSettingsForV2 creates all the danger zone settings required by areDangerZoneSettingsSyncedToIMPod
// with V2 data engine enabled. It auto-generates settings from types.GetDangerZoneSettings() using their
// default values, then overrides specific settings that need non-default values for V2 to function.
// This ensures new danger zone settings are automatically included without manual maintenance.
func createDangerZoneSettingsForV2(c *C, lhClient *lhfake.Clientset, sIndexer cache.Indexer) {
	// Auto-populate all danger zone settings with their definition defaults.
	for settingName := range types.GetDangerZoneSettings() {
		definition, ok := types.GetSettingDefinition(settingName)
		c.Assert(ok, Equals, true, Commentf("missing definition for danger zone setting %q", settingName))
		setting := newSetting(string(settingName), definition.Default)
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}

	// Override settings that need specific values for V2 IM tests.
	overrides := map[types.SettingName]string{
		types.SettingNameV2DataEngine:                "true",
		types.SettingNameDataEngineHugepageEnabled:   `{"v2":"true"}`,
		types.SettingNameDataEngineMemorySize:        `{"v2":"1024"}`,
		types.SettingNameDataEngineCPUMask:           `{"v2":"0x1"}`,
		types.SettingNamePreferredDataEngineIPFamily: types.DataEngineIPFamilyIPv4,
	}
	for name, value := range overrides {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(name), metav1.GetOptions{})
		c.Assert(err, IsNil)
		setting.Value = value
		setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Update(context.TODO(), setting, metav1.UpdateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Update(setting)
		c.Assert(err, IsNil)
	}

	// Also add non-danger-zone settings that the IM controller needs.
	extraSettings := []*longhorn.Setting{
		newDefaultInstanceManagerImageSetting(),
	}
	for _, setting := range extraSettings {
		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
	}
}

func (s *TestSuite) TestHugepagePodDeletionDecision(c *C) {
	type testCase struct {
		nodeHugepageCapacity  string // hugepages-2Mi capacity on the kube node
		podHugepageLimit      string // current hugepages-2Mi limit on the IM pod
		podMemorySize         string // current --spdk-memory-size in the IM pod args
		hugepageDisabled      bool   // if true, override hugepage-enabled setting to false
		extraUnsyncedSetting  bool   // if true, add an extra unsynced setting (priority class)
		expectedPodCount      int    // 0 = pod deleted without recreation, 1 = pod kept or recreated
		expectedHugepageLimit string
	}

	testCases := map[string]testCase{
		"hugepage mismatch, node has enough hugepages, no instances - pod should be deleted and recreated": {
			nodeHugepageCapacity:  "2Gi",
			podHugepageLimit:      "512Mi", // mismatches the setting value of 1024Mi
			podMemorySize:         "512",
			expectedPodCount:      1, // deleted then recreated (net 1)
			expectedHugepageLimit: "1Gi",
		},
		"hugepage mismatch, node has insufficient hugepages, no instances - pod should NOT be deleted": {
			nodeHugepageCapacity:  "256Mi",
			podHugepageLimit:      "512Mi",
			podMemorySize:         "512",
			expectedPodCount:      1, // kept as-is
			expectedHugepageLimit: "512Mi",
		},
		"hugepage mismatch + other unsynced setting, node has insufficient hugepages - pod should NOT be deleted": {
			nodeHugepageCapacity:  "256Mi",
			podHugepageLimit:      "512Mi",
			podMemorySize:         "512",
			extraUnsyncedSetting:  true,
			expectedPodCount:      1, // guard blocks deletion; pod stays as-is
			expectedHugepageLimit: "512Mi",
		},
		"hugepage matches, node has enough hugepages - pod should NOT be deleted": {
			nodeHugepageCapacity:  "2Gi",
			podHugepageLimit:      "1024Mi", // matches the setting value
			podMemorySize:         "1024",
			expectedPodCount:      1, // kept as-is
			expectedHugepageLimit: "1Gi",
		},
		"memory size mismatch with hugepages disabled - pod should be deleted and recreated": {
			nodeHugepageCapacity:  "2Gi",
			podHugepageLimit:      "0",   // hugepages disabled, limit is 0
			podMemorySize:         "512", // stale --spdk-memory-size, setting is 1024
			hugepageDisabled:      true,
			expectedPodCount:      1, // deleted then recreated (net 1)
			expectedHugepageLimit: "0",
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

		imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)

		createDangerZoneSettingsForV2(c, lhClient, sIndexer)

		if tc.hugepageDisabled {
			hpSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(types.SettingNameDataEngineHugepageEnabled), metav1.GetOptions{})
			c.Assert(err, IsNil)
			hpSetting.Value = `{"v2":"false"}`
			hpSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Update(context.TODO(), hpSetting, metav1.UpdateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Update(hpSetting)
			c.Assert(err, IsNil)
		}

		// If we want an extra unsynced setting, override priority class to a value that won't match the pod.
		if tc.extraUnsyncedSetting {
			pcSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(types.SettingNamePriorityClass), metav1.GetOptions{})
			c.Assert(err, IsNil)
			pcSetting.Value = "system-cluster-critical"
			pcSetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Update(context.TODO(), pcSetting, metav1.UpdateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Update(pcSetting)
			c.Assert(err, IsNil)
		}

		// Create kubernetes node with hugepage capacity.
		kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Status.Allocatable = corev1.ResourceList{
			"cpu":                                resource.MustParse("4"),
			corev1.ResourceName("hugepages-2Mi"): resource.MustParse(tc.nodeHugepageCapacity),
		}
		kubeNode.Status.Capacity = corev1.ResourceList{
			corev1.ResourceName("hugepages-2Mi"): resource.MustParse(tc.nodeHugepageCapacity),
		}
		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		lhNode := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
		err = lhNodeIndexer.Add(lhNode)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta2().Nodes(lhNode.Namespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		// Create a V2 running IM with no instances.
		im := newInstanceManager(
			TestInstanceManagerName,
			longhorn.InstanceManagerStateRunning,
			TestNode1, TestNode1, TestIP1,
			nil, nil, nil,
			longhorn.DataEngineTypeV2,
			TestInstanceManagerImage,
			false,
		)
		// Set DataEngineStatus to match settings so CPU mask and interrupt mode are "applied".
		im.Status.DataEngineStatus.V2.CPUMask = "0x1"
		im.Status.DataEngineStatus.V2.InterruptModeEnabled = "false"
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
		_, err = lhClient.LonghornV1beta2().InstanceManagers(im.Namespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		// Create a pod with the specified hugepage limit and memory size (may or may not match the setting).
		pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
		pod.Spec.Containers = []corev1.Container{{
			Name:    "instance-manager",
			Command: []string{"instance-manager"},
			Args:    []string{"--spdk-memory-size", tc.podMemorySize},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{"cpu": resource.MustParse("480m")},
				Limits: corev1.ResourceList{
					corev1.ResourceName("hugepages-2Mi"): resource.MustParse(tc.podHugepageLimit),
				},
			},
		}}
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().Pods(im.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = imc.syncInstanceManager(getKey(im, c))
		c.Assert(err, IsNil)

		podList, err := kubeClient.CoreV1().Pods(im.Namespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(podList.Items, HasLen, tc.expectedPodCount, Commentf("test case: %v", name))
		if tc.expectedPodCount == 1 {
			hugepageLimit := podList.Items[0].Spec.Containers[0].Resources.Limits[corev1.ResourceName("hugepages-2Mi")]
			c.Assert(hugepageLimit.String(), Equals, tc.expectedHugepageLimit, Commentf("test case: %v", name))
		}
	}
}

func (s *TestSuite) TestNodeHasEnoughHugepageTotalCapacity(c *C) {
	type testCase struct {
		allocatable string
		capacity    string
		expected    bool
	}

	testCases := map[string]testCase{
		"prefer allocatable when present": {
			allocatable: "512Mi",
			capacity:    "2Gi",
			expected:    false,
		},
		"fallback to capacity when allocatable missing": {
			capacity: "2Gi",
			expected: true,
		},
		"allocatable enough": {
			allocatable: "2Gi",
			capacity:    "512Mi",
			expected:    true,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
		kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

		imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)

		for _, setting := range []*longhorn.Setting{
			newDataEngineHugepageEnabledSetting(),
			newDataEngineMemorySizeSetting(),
		} {
			setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}

		kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
		kubeNode.Status.Capacity = corev1.ResourceList{}
		if tc.capacity != "" {
			kubeNode.Status.Capacity[corev1.ResourceName("hugepages-2Mi")] = resource.MustParse(tc.capacity)
		}
		kubeNode.Status.Allocatable = corev1.ResourceList{}
		if tc.allocatable != "" {
			kubeNode.Status.Allocatable[corev1.ResourceName("hugepages-2Mi")] = resource.MustParse(tc.allocatable)
		}

		err = kubeNodeIndexer.Add(kubeNode)
		c.Assert(err, IsNil)
		_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		im := newInstanceManager(
			TestInstanceManagerName,
			longhorn.InstanceManagerStateRunning,
			TestNode1,
			TestNode1,
			TestIP1,
			nil,
			nil,
			nil,
			longhorn.DataEngineTypeV2,
			TestInstanceManagerImage,
			false,
		)

		ok, err := imc.nodeHasEnoughHugepageTotalCapacity(im)
		c.Assert(err, IsNil)
		c.Assert(ok, Equals, tc.expected)
	}
}

func (s *TestSuite) TestIsDataEngineIPFamilySynced(c *C) {
	testCases := []struct {
		name           string
		settingValue   string
		containers     []corev1.Container
		attached       bool
		expectedSynced bool
		expectedError  string
	}{
		{
			name:         "authoritative container matches",
			settingValue: types.DataEngineIPFamilyIPv6,
			containers: []corev1.Container{{
				Name: "instance-manager",
				Args: []string{"--ip-family", types.DataEngineIPFamilyIPv6},
			}},
			expectedSynced: true,
		},
		{
			name:         "sidecar value is ignored",
			settingValue: types.DataEngineIPFamilyIPv6,
			containers: []corev1.Container{{
				Name: "sidecar",
				Args: []string{"--ip-family", types.DataEngineIPFamilyIPv6},
			}},
			expectedSynced: false,
		},
		{
			name:         "mismatched family while detached",
			settingValue: types.DataEngineIPFamilyIPv6,
			containers: []corev1.Container{{
				Name: "instance-manager",
				Args: []string{"--ip-family", types.DataEngineIPFamilyIPv4},
			}},
			expectedSynced: false,
		},
		{
			name:         "mismatched family while attached",
			settingValue: types.DataEngineIPFamilyIPv6,
			containers: []corev1.Container{{
				Name: "instance-manager",
				Args: []string{"--ip-family", types.DataEngineIPFamilyIPv4},
			}},
			attached:      true,
			expectedError: "failed to apply preferred-data-engine-ip-family setting to Longhorn components when there are attached volumes. It will be eventually applied",
		},
		{
			name: "default setting matches omitted flag",
			containers: []corev1.Container{{
				Name: "instance-manager",
				Args: []string{"daemon"},
			}},
			expectedSynced: true,
		},
		{
			name:         "default setting rejects legacy explicit IPv4",
			settingValue: types.DataEngineIPFamilyDefault,
			containers: []corev1.Container{{
				Name: "instance-manager",
				Args: []string{"--ip-family", types.DataEngineIPFamilyIPv4},
			}},
			expectedSynced: false,
		},
		{
			name: "blank setting requires authoritative container",
			containers: []corev1.Container{{
				Name: "sidecar",
				Args: []string{"daemon"},
			}},
			expectedSynced: false,
		},
	}

	for _, tc := range testCases {
		imc, lhClient, _, _, _, settingIndexer, volumeIndexer, _, _ := newDataEngineIPFamilyTestController(c)
		addDataEngineIPFamilyTestSetting(c, lhClient, settingIndexer, newDataEngineIPFamilySetting(tc.settingValue))
		if tc.attached {
			volume := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "attached-volume", Namespace: TestNamespace},
				Status:     longhorn.VolumeStatus{State: longhorn.VolumeStateAttached},
			}
			volume, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(volumeIndexer.Add(volume), IsNil)
		}

		setting := newDataEngineIPFamilySetting(tc.settingValue)
		pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: tc.containers}}
		synced, err := imc.isSettingDataEngineIPFamilySynced(setting, pod)
		if tc.expectedError == "" {
			c.Assert(err, IsNil, Commentf("test case: %s", tc.name))
			c.Assert(synced, Equals, tc.expectedSynced, Commentf("test case: %s", tc.name))
			continue
		}

		var invalidState *types.ErrorInvalidState
		c.Assert(errors.As(err, &invalidState), Equals, true, Commentf("test case: %s", tc.name))
		c.Assert(invalidState.Reason, Equals, tc.expectedError, Commentf("test case: %s", tc.name))
	}
}

func (s *TestSuite) TestCreateV2InstanceManagerPodSpecUsesDualStackListener(c *C) {
	imc, lhClient, kubeClient, imIndexer, _, settingIndexer, _, kubeNodeIndexer, lhNodeIndexer := newDataEngineIPFamilyTestController(c)
	createDangerZoneSettingsForV2(c, lhClient, settingIndexer)
	kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	kubeNode.Status.Allocatable = corev1.ResourceList{"cpu": resource.MustParse("4")}
	kubeNode.Status.Capacity = corev1.ResourceList{"cpu": resource.MustParse("4")}
	c.Assert(kubeNodeIndexer.Add(kubeNode), IsNil)
	_, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	lhNode := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	c.Assert(lhNodeIndexer.Add(lhNode), IsNil)
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	var instanceManagers []*longhorn.InstanceManager
	for _, dataEngine := range []longhorn.DataEngineType{longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
		im := newInstanceManager(
			TestInstanceManagerName+"-"+string(dataEngine),
			longhorn.InstanceManagerStateStopped,
			TestNode1,
			TestNode1,
			"",
			nil,
			nil,
			nil,
			dataEngine,
			TestInstanceManagerImage,
			false,
		)
		if types.IsDataEngineV2(dataEngine) {
			im.Spec.DataEngineSpec.V2.CPUMask = "0x1"
		}
		createdIM, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(imIndexer.Add(createdIM), IsNil)
		im = createdIM
		instanceManagers = append(instanceManagers, im)

		pod, err := imc.createInstanceManagerPodSpec(im, nil, "", nil, dataEngine)
		if err != nil {
			c.Fatalf("failed to create pod spec for data engine %s: %v", dataEngine, err)
		}
		c.Assert(pod.Spec.Containers, HasLen, 1)

		family, specified, valid := types.ParseDataEngineIPFamilyArgs(pod.Spec.Containers[0].Args)
		c.Assert(valid, Equals, true, Commentf("data engine: %s", dataEngine))
		c.Assert(specified, Equals, false, Commentf("data engine: %s", dataEngine))
		c.Assert(family, Equals, "", Commentf("data engine: %s", dataEngine))
		for _, env := range pod.Spec.Containers[0].Env {
			c.Assert(env.Name, Not(Equals), "DATA_ENGINE_IP_FAMILY")
		}

		if types.IsDataEngineV2(dataEngine) {
			listen := ""
			for i := 0; i+1 < len(pod.Spec.Containers[0].Args); i++ {
				if pod.Spec.Containers[0].Args[i] == "--listen" {
					listen = pod.Spec.Containers[0].Args[i+1]
					break
				}
			}
			c.Assert(listen, Equals, ":8500")
		}
	}

	familySetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(
		context.TODO(), string(types.SettingNamePreferredDataEngineIPFamily), metav1.GetOptions{})
	c.Assert(err, IsNil)
	familySetting.Value = types.DataEngineIPFamilyDefault
	familySetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Update(
		context.TODO(), familySetting, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(settingIndexer.Update(familySetting), IsNil)

	for _, im := range instanceManagers {
		pod, err := imc.createInstanceManagerPodSpec(im, nil, "", nil, im.Spec.DataEngine)
		c.Assert(err, IsNil)
		family, specified, valid := types.ParseDataEngineIPFamilyArgs(pod.Spec.Containers[0].Args)
		c.Assert(valid, Equals, true, Commentf("data engine: %s", im.Spec.DataEngine))
		c.Assert(specified, Equals, false, Commentf("data engine: %s", im.Spec.DataEngine))
		c.Assert(family, Equals, "", Commentf("data engine: %s", im.Spec.DataEngine))
	}
}

func (s *TestSuite) TestDataEngineIPFamilyStorageNetworkCompatibility(c *C) {
	const (
		storageNetwork = "longhorn-system/ipv4-only"
		oldStorageIP   = "192.0.2.10"
		clusterIP      = "10.0.0.10"
	)

	imc, lhClient, kubeClient, _, podIndexer, settingIndexer, _, _, _ := newDataEngineIPFamilyTestController(c)
	storageSetting := newSetting(string(types.SettingNameStorageNetwork), storageNetwork)
	addDataEngineIPFamilyTestSetting(c, lhClient, settingIndexer, storageSetting)
	addDataEngineIPFamilyTestSetting(c, lhClient, settingIndexer, newDataEngineIPFamilySetting(types.DataEngineIPFamilyIPv6))

	im := newInstanceManager(
		"instance-manager-aio-v1-node1",
		longhorn.InstanceManagerStateRunning,
		TestNode1,
		TestNode1,
		oldStorageIP,
		nil,
		nil,
		nil,
		longhorn.DataEngineTypeV1,
		TestInstanceManagerImage,
		false,
	)
	pod := newPod(&corev1.PodStatus{
		PodIP:             clusterIP,
		PodIPs:            []corev1.PodIP{{IP: clusterIP}},
		Phase:             corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{{Name: "instance-manager", Ready: true}},
	}, im.Name, im.Namespace, im.Spec.NodeID)
	pod.UID = "original-pod-uid"
	pod.Spec.Containers = []corev1.Container{{
		Name: "instance-manager",
		Args: []string{"daemon"},
	}}
	pod.Annotations = map[string]string{
		string(types.CNIAnnotationNetworks):      types.CreateCniAnnotationFromSetting(storageSetting, types.StorageNetworkInterface),
		string(types.CNIAnnotationNetworkStatus): fmt.Sprintf(`[{"name":"%s","interface":"lhnet1","ips":["%s"]}]`, storageNetwork, oldStorageIP),
	}
	c.Assert(podIndexer.Add(pod), IsNil)
	_, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	familySetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(
		context.TODO(), string(types.SettingNamePreferredDataEngineIPFamily), metav1.GetOptions{})
	c.Assert(err, IsNil)
	familySetting.Value = types.DataEngineIPFamilyDefault
	familySetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Update(
		context.TODO(), familySetting, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(settingIndexer.Update(familySetting), IsNil)
	appliedFamily := types.DataEngineIPFamilyDefault
	im.Status.IPFamily = &appliedFamily
	blocked, err := imc.checkDataEngineIPFamilyForStorageNetwork(im, pod)
	c.Assert(err, IsNil)
	c.Assert(blocked, Equals, false)

	familySetting.Value = types.DataEngineIPFamilyIPv6
	familySetting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Update(
		context.TODO(), familySetting, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(settingIndexer.Update(familySetting), IsNil)
	appliedFamily = types.DataEngineIPFamilyIPv6
	blocked, err = imc.checkDataEngineIPFamilyForStorageNetwork(im, pod)
	c.Assert(err, IsNil)
	c.Assert(blocked, Equals, true)
	err = imc.handlePod(im)
	c.Assert(err, IsNil)

	updatedPod, err := kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(fmt.Sprint(updatedPod.UID), Equals, fmt.Sprint(pod.UID))
	c.Assert(im.Status.IP, Equals, oldStorageIP)
	condition := types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced)
	c.Assert(condition.Status, Equals, longhorn.ConditionStatusFalse)
	c.Assert(condition.Reason, Equals, longhorn.InstanceManagerConditionReasonSettingNotSynced)
	expectedMessage := fmt.Sprintf("Settings [preferred-data-engine-ip-family] are not synced: storage network longhorn-system/ipv4-only cannot provide an address in family ipv6 for pod %s/instance-manager-aio-v1-node1", TestNamespace)
	if condition.Message != expectedMessage {
		c.Fatalf("unexpected setting condition message: got %q, want %q", condition.Message, expectedMessage)
	}

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.IP, Equals, "")
	condition = types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced)
	c.Assert(condition.Status, Equals, longhorn.ConditionStatusFalse)
	c.Assert(condition.Reason, Equals, longhorn.InstanceManagerConditionReasonSettingNotSynced)
	c.Assert(condition.Message, Equals, fmt.Sprintf("Settings [preferred-data-engine-ip-family] are not synced: pod %s/instance-manager-aio-v1-node1 cannot provide an address in family ipv6", TestNamespace))

	appliedFamily = types.DataEngineIPFamilyIPv4
	c.Assert(podIndexer.Update(pod), IsNil)
	im.Status.IP = oldStorageIP
	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.IP, Equals, clusterIP)

	recoveryIMC, recoveryLHClient, recoveryKubeClient, recoveryIMIndexer, recoveryPodIndexer, recoverySettingIndexer, _, kubeNodeIndexer, lhNodeIndexer := newDataEngineIPFamilyTestController(c)
	createDangerZoneSettingsForV2(c, recoveryLHClient, recoverySettingIndexer)
	recoveryStorageSetting, err := recoveryLHClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(types.SettingNameStorageNetwork), metav1.GetOptions{})
	c.Assert(err, IsNil)
	recoveryStorageSetting.Value = "longhorn-system/dual-stack"
	recoveryStorageSetting, err = recoveryLHClient.LonghornV1beta2().Settings(TestNamespace).Update(context.TODO(), recoveryStorageSetting, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(recoverySettingIndexer.Update(recoveryStorageSetting), IsNil)
	recoveryFamilySetting, err := recoveryLHClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(types.SettingNamePreferredDataEngineIPFamily), metav1.GetOptions{})
	c.Assert(err, IsNil)
	recoveryFamilySetting.Value = types.DataEngineIPFamilyIPv6
	recoveryFamilySetting, err = recoveryLHClient.LonghornV1beta2().Settings(TestNamespace).Update(context.TODO(), recoveryFamilySetting, metav1.UpdateOptions{})
	c.Assert(err, IsNil)
	c.Assert(recoverySettingIndexer.Update(recoveryFamilySetting), IsNil)

	kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	kubeNode.Status.Allocatable = corev1.ResourceList{"cpu": resource.MustParse("4")}
	kubeNode.Status.Capacity = corev1.ResourceList{"cpu": resource.MustParse("4")}
	c.Assert(kubeNodeIndexer.Add(kubeNode), IsNil)
	_, err = recoveryKubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	lhNode := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	c.Assert(lhNodeIndexer.Add(lhNode), IsNil)
	_, err = recoveryLHClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	recoveryIM := newInstanceManager(
		"instance-manager-recovery",
		longhorn.InstanceManagerStateRunning,
		TestNode1,
		TestNode1,
		oldStorageIP,
		nil,
		nil,
		nil,
		longhorn.DataEngineTypeV1,
		TestInstanceManagerImage,
		false,
	)
	recoveryIM.Status.Conditions = types.SetCondition(nil, longhorn.InstanceManagerConditionTypeSettingSynced,
		longhorn.ConditionStatusFalse, longhorn.InstanceManagerConditionReasonSettingNotSynced, "old failure")
	recoveryIM, err = recoveryLHClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), recoveryIM, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(recoveryIMIndexer.Add(recoveryIM), IsNil)
	recoveryPod, err := recoveryIMC.createInstanceManagerPodSpec(recoveryIM, nil, "", nil, longhorn.DataEngineTypeV1)
	c.Assert(err, IsNil)
	recoveryPod.Status = corev1.PodStatus{
		PodIP:             clusterIP,
		PodIPs:            []corev1.PodIP{{IP: clusterIP}, {IP: "2001:db8::10"}},
		Phase:             corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{{Name: "instance-manager", Ready: true}},
	}
	recoveryPod.Annotations[string(types.CNIAnnotationNetworkStatus)] = `[{"name":"longhorn-system/dual-stack","interface":"lhnet1","ips":["192.0.2.20","2001:db8::10"]}]`
	c.Assert(recoveryPodIndexer.Add(recoveryPod), IsNil)
	_, err = recoveryKubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), recoveryPod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = recoveryIMC.handlePod(recoveryIM)
	c.Assert(err, IsNil)
	condition = types.GetCondition(recoveryIM.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced)
	c.Assert(condition.Status, Equals, longhorn.ConditionStatusTrue)
}
