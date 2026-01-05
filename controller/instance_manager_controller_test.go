package controller

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

	. "gopkg.in/check.v1"
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
				OwnerID:          TestNode1,
				CurrentState:     longhorn.InstanceManagerStateError, // The state will become InstanceManagerStateStarting in the next reconcile loop
				IP:               TestIP1,
				APIMinVersion:    0,
				APIVersion:       0,
				InstanceEngines:  nil, // Transition to InstanceManagerStateError erases process information.
				InstanceReplicas: nil, // Transition to InstanceManagerStateError erases process information.
				Conditions: []longhorn.Condition{
					{
						Type:   longhorn.InstanceManagerConditionTypePodReady,
						Status: longhorn.ConditionStatusFalse,
						Reason: longhorn.InstanceManagerConditionReasonPodRestarting,
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
						Reason: longhorn.InstanceManagerConditionReasonPodRestarting,
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
						Reason: longhorn.InstanceManagerConditionReasonPodRestarting,
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
			tc.currentEngines, tc.currentReplicas,
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
		for i, condition := range updatedIM.Status.Conditions {
			tc.expectedStatus.Conditions[i].LastTransitionTime = condition.LastTransitionTime
			tc.expectedStatus.Conditions[i].LastProbeTime = condition.LastProbeTime
			tc.expectedStatus.Conditions[i].Message = condition.Message
		}
		c.Assert(updatedIM.Status, DeepEquals, tc.expectedStatus)
	}
}
