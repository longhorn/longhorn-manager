package controller

import (
	"context"
	"errors"
	"strings"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestShouldExpandEngineFrontend(c *C) {
	ef := &longhorn.EngineFrontend{}
	v := &longhorn.Volume{}
	e := &longhorn.Engine{}

	c.Assert(shouldExpandEngineFrontend(nil, v, e), Equals, false)
	c.Assert(shouldExpandEngineFrontend(ef, nil, e), Equals, false)
	c.Assert(shouldExpandEngineFrontend(ef, v, nil), Equals, false)

	ef.Spec.VolumeSize = 0
	v.Status.ExpansionRequired = true
	c.Assert(shouldExpandEngineFrontend(ef, v, e), Equals, false)

	ef.Spec.VolumeSize = TestVolumeSize
	v.Status.ExpansionRequired = false
	c.Assert(shouldExpandEngineFrontend(ef, v, e), Equals, false)

	v.Status.ExpansionRequired = true
	e.Status.IsExpanding = true
	c.Assert(shouldExpandEngineFrontend(ef, v, e), Equals, false)

	e.Status.IsExpanding = false
	e.Status.CurrentSize = TestVolumeSize
	c.Assert(shouldExpandEngineFrontend(ef, v, e), Equals, false)

	e.Status.CurrentSize = TestVolumeSize - 1
	c.Assert(shouldExpandEngineFrontend(ef, v, e), Equals, true)
}

func (s *TestSuite) TestIsEngineFrontendTargetInitialized(c *C) {
	c.Assert(isEngineFrontendTargetInitialized("", 0), Equals, false)
	c.Assert(isEngineFrontendTargetInitialized("10.0.0.1", 0), Equals, false)
	c.Assert(isEngineFrontendTargetInitialized("", 9502), Equals, false)
	c.Assert(isEngineFrontendTargetInitialized("10.0.0.1", 9502), Equals, true)
}

func newTestEngineFrontendController(
	lhClient *lhfake.Clientset,
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories,
	controllerID string,
) (*EngineFrontendController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	efc, err := NewEngineFrontendController(
		logrus.StandardLogger(),
		ds,
		scheme.Scheme,
		kubeClient,
		TestNamespace,
		controllerID,
		util.NewAtomicCounter(),
		NewSnapshotConcurrentLimiter(),
	)
	if err != nil {
		return nil, err
	}

	efc.eventRecorder = record.NewFakeRecorder(100)
	for index := range efc.cacheSyncs {
		efc.cacheSyncs[index] = alwaysReady
	}

	return efc, nil
}

func (s *TestSuite) TestSyncEngineFrontendInitializesIncompleteTargetStatus(c *C) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

	efc, err := newTestEngineFrontendController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	efIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineFrontends().Informer().GetIndexer()
	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
	podIndexer := informerFactories.KubeNamespaceFilteredInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	defaultEngineImageSetting := newSetting(string(types.SettingNameDefaultEngineImage), TestEngineImage)
	v2DataEngineSetting := newSetting(string(types.SettingNameV2DataEngine), "true")
	for _, setting := range []*longhorn.Setting{defaultEngineImageSetting, v2DataEngineSetting} {
		createdSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(settingIndexer.Add(createdSetting), IsNil)
	}

	volume := newVolume(TestVolumeName, 1)
	volume.Spec.DataEngine = longhorn.DataEngineTypeV2
	volume.Status.CurrentImage = TestEngineImage
	createdVolume, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(volumeIndexer.Add(createdVolume), IsNil)

	kubeNode := newKubernetesNode(TestOwnerID1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	createdKubeNode, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(kubeNodeIndexer.Add(createdKubeNode), IsNil)

	imPod := newPod(&corev1.PodStatus{Phase: corev1.PodRunning, PodIP: TestIP1}, TestInstanceManagerName, TestNamespace, TestOwnerID1)
	createdPod, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), imPod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(podIndexer.Add(createdPod), IsNil)

	ef := &longhorn.EngineFrontend{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-engine-frontend",
			Namespace: TestNamespace,
		},
		Spec: longhorn.EngineFrontendSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName:  volume.Name,
				VolumeSize:  volume.Spec.Size,
				NodeID:      TestOwnerID1,
				Image:       TestEngineImage,
				DesireState: longhorn.InstanceStateRunning,
				DataEngine:  longhorn.DataEngineTypeV2,
			},
			EngineName: "test-engine",
			TargetIP:   TestIP2,
			TargetPort: TestPort1,
		},
		Status: longhorn.EngineFrontendStatus{
			InstanceStatus: longhorn.InstanceStatus{
				OwnerID:             TestOwnerID1,
				InstanceManagerName: TestInstanceManagerName,
				CurrentState:        longhorn.InstanceStateSuspended,
				Started:             true,
			},
			TargetIP:   TestIP2,
			TargetPort: 0,
		},
	}

	instanceManager := newInstanceManager(
		TestInstanceManagerName,
		longhorn.InstanceManagerStateRunning,
		TestOwnerID1,
		TestOwnerID1,
		TestIP1,
		nil,
		map[string]longhorn.InstanceProcess{
			ef.Name: {
				Spec: longhorn.InstanceProcessSpec{
					Name:       ef.Name,
					DataEngine: longhorn.DataEngineTypeV2,
				},
				Status: longhorn.InstanceProcessStatus{
					State:     longhorn.InstanceStateSuspended,
					PortStart: int32(TestPort1),
					UUID:      "test-uuid",
				},
			},
		},
		nil,
		longhorn.DataEngineTypeV2,
		TestInstanceManagerImage,
		false,
	)
	createdIM, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), instanceManager, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(imIndexer.Add(createdIM), IsNil)

	createdEF, err := lhClient.LonghornV1beta2().EngineFrontends(TestNamespace).Create(context.TODO(), ef, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(efIndexer.Add(createdEF), IsNil)

	err = efc.syncEngineFrontend(TestNamespace + "/" + ef.Name)
	c.Assert(err, IsNil)

	updatedEF, err := lhClient.LonghornV1beta2().EngineFrontends(TestNamespace).Get(context.TODO(), ef.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedEF.Status.TargetIP, Equals, ef.Spec.TargetIP)
	c.Assert(updatedEF.Status.TargetPort, Equals, ef.Spec.TargetPort)
	c.Assert(updatedEF.Status.CurrentState, Equals, longhorn.InstanceStateSuspended)
	_, exists := efc.engineFrontendMonitorMap[ef.Name]
	c.Assert(exists, Equals, false)
}

func (s *TestSuite) TestGetReplicaRebuildCandidate(c *C) {
	engine := &longhorn.Engine{}
	engine.Spec.VolumeName = TestVolumeName
	engine.Status.CurrentReplicaAddressMap = map[string]string{
		"replica-1": "10.0.0.1:10000",
	}

	replicaName, addr, needRebuild := getReplicaRebuildCandidate(engine, logrus.StandardLogger())
	c.Assert(needRebuild, Equals, true)
	c.Assert(replicaName, Equals, "replica-1")
	c.Assert(addr, Equals, "10.0.0.1:10000")

	engine.Status.CurrentReplicaAddressMap["replica-2"] = "10.0.0.2:10000"
	engine.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{
		"replica-1": longhorn.ReplicaModeRW,
		"replica-2": longhorn.ReplicaModeWO,
	}
	replicaName, addr, needRebuild = getReplicaRebuildCandidate(engine, logrus.StandardLogger())
	c.Assert(needRebuild, Equals, false)
	c.Assert(replicaName, Equals, "")
	c.Assert(addr, Equals, "")
}

type fakeEngineFrontendSwitchoverClient struct {
	callOrder        []string
	suspendErr       error
	switchErr        error
	resumeErr        error
	suspendCallCount int
	switchCallCount  int
	resumeCallCount  int

	suspendDataEngine longhorn.DataEngineType
	suspendName       string

	switchDataEngine    longhorn.DataEngineType
	switchName          string
	switchTargetAddress string
	switchEngineName    string

	resumeDataEngine longhorn.DataEngineType
	resumeName       string
}

func (f *fakeEngineFrontendSwitchoverClient) EngineFrontendSuspend(dataEngine longhorn.DataEngineType, name string) error {
	f.callOrder = append(f.callOrder, "suspend")
	f.suspendCallCount++
	f.suspendDataEngine = dataEngine
	f.suspendName = name
	return f.suspendErr
}

func (f *fakeEngineFrontendSwitchoverClient) EngineFrontendSwitchOverTarget(dataEngine longhorn.DataEngineType, name, targetAddress, engineName string) error {
	f.callOrder = append(f.callOrder, "switch")
	f.switchCallCount++
	f.switchDataEngine = dataEngine
	f.switchName = name
	f.switchTargetAddress = targetAddress
	f.switchEngineName = engineName
	return f.switchErr
}

func (f *fakeEngineFrontendSwitchoverClient) EngineFrontendResume(dataEngine longhorn.DataEngineType, name string) error {
	f.callOrder = append(f.callOrder, "resume")
	f.resumeCallCount++
	f.resumeDataEngine = dataEngine
	f.resumeName = name
	return f.resumeErr
}

func (s *TestSuite) TestSwitchEngineFrontendTarget(c *C) {
	targetAddress := "tcp://10.0.0.1:10000"

	testCases := []struct {
		name                 string
		suspendErr           error
		switchErr            error
		resumeErr            error
		expectedFailureType  switchoverFailureType
		expectedErrorPattern string
		expectedCallOrder    []string
		expectedSuspendCalls int
		expectedSwitchCalls  int
		expectedResumeCalls  int
	}{
		{
			name:                 "success",
			expectedFailureType:  switchoverFailureType(""),
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
		{
			name:                 "switch failure with recovery resume",
			switchErr:            errors.New("switch failed"),
			expectedFailureType:  switchoverFailureSwitch,
			expectedErrorPattern: ".*failed to switch over target for engine frontend ef-1.*",
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
		{
			name:                 "suspend failure",
			suspendErr:           errors.New("suspend failed"),
			expectedFailureType:  switchoverFailureSuspend,
			expectedErrorPattern: ".*failed to suspend engine frontend ef-1 before switchover.*",
			expectedCallOrder:    []string{"suspend"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  0,
			expectedResumeCalls:  0,
		},
		{
			name:                 "switch and resume failure",
			switchErr:            errors.New("switch failed"),
			resumeErr:            errors.New("resume failed"),
			expectedFailureType:  switchoverFailureSwitchAndResume,
			expectedErrorPattern: ".*failed to switch over target for engine frontend ef-1, then failed to resume:.*",
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
		{
			name:                 "resume failure after successful switch",
			resumeErr:            errors.New("resume failed"),
			expectedFailureType:  switchoverFailureResume,
			expectedErrorPattern: ".*failed to resume engine frontend ef-1 after switchover.*",
			expectedCallOrder:    []string{"suspend", "switch", "resume"},
			expectedSuspendCalls: 1,
			expectedSwitchCalls:  1,
			expectedResumeCalls:  1,
		},
	}

	for _, tc := range testCases {
		ef := &longhorn.EngineFrontend{}
		ef.Name = "ef-1"
		ef.Spec.DataEngine = longhorn.DataEngineTypeV2

		client := &fakeEngineFrontendSwitchoverClient{
			suspendErr: tc.suspendErr,
			switchErr:  tc.switchErr,
			resumeErr:  tc.resumeErr,
		}

		failureType, err := switchEngineFrontendTarget(client, ef, targetAddress)
		caseInfo := Commentf("case=%s", tc.name)

		if tc.expectedErrorPattern == "" {
			c.Assert(err, IsNil, caseInfo)
		} else {
			c.Assert(err, NotNil, caseInfo)
			c.Assert(err.Error(), Matches, tc.expectedErrorPattern, caseInfo)
		}

		c.Assert(failureType, Equals, tc.expectedFailureType, caseInfo)
		c.Assert(client.callOrder, DeepEquals, tc.expectedCallOrder, caseInfo)
		c.Assert(client.suspendCallCount, Equals, tc.expectedSuspendCalls, caseInfo)
		c.Assert(client.switchCallCount, Equals, tc.expectedSwitchCalls, caseInfo)
		c.Assert(client.resumeCallCount, Equals, tc.expectedResumeCalls, caseInfo)

		if tc.expectedSwitchCalls > 0 {
			c.Assert(client.switchDataEngine, Equals, longhorn.DataEngineTypeV2, caseInfo)
			c.Assert(client.switchName, Equals, "ef-1", caseInfo)
			c.Assert(client.switchTargetAddress, Equals, targetAddress, caseInfo)
			c.Assert(client.switchEngineName, Equals, "", caseInfo)
		}
	}
}

func (s *TestSuite) TestGetEngineFrontendSwitchoverFailureEventMessage(c *C) {
	targetAddress := "10.1.2.3:9502"
	baseErr := errors.New("rpc failed")

	testCases := []struct {
		name            string
		failureType     switchoverFailureType
		expectedPattern string
	}{
		{
			name:            "suspend failure message",
			failureType:     switchoverFailureSuspend,
			expectedPattern: ".*Failed to suspend engine frontend before switchover to 10\\.1\\.2\\.3:9502: rpc failed.*",
		},
		{
			name:            "switch and resume failure message",
			failureType:     switchoverFailureSwitchAndResume,
			expectedPattern: ".*Failed to switch over target to 10\\.1\\.2\\.3:9502 and failed to resume engine frontend: rpc failed.*",
		},
		{
			name:            "resume failure message",
			failureType:     switchoverFailureResume,
			expectedPattern: ".*Switched over target to 10\\.1\\.2\\.3:9502 but failed to resume engine frontend: rpc failed.*",
		},
		{
			name:            "default switch failure message",
			failureType:     switchoverFailureSwitch,
			expectedPattern: ".*Failed to switch over target to 10\\.1\\.2\\.3:9502: rpc failed.*",
		},
	}

	for _, tc := range testCases {
		msg := getEngineFrontendSwitchoverFailureEventMessage(tc.failureType, targetAddress, baseErr)
		c.Assert(msg, Matches, tc.expectedPattern, Commentf("case=%s", tc.name))
	}
}

func (s *TestSuite) TestRecordEngineFrontendSwitchoverFailureEvent(c *C) {
	ef := &longhorn.EngineFrontend{}
	ef.Name = "ef-1"
	targetAddress := "10.1.2.3:9502"
	baseErr := errors.New("rpc failed")

	testCases := []struct {
		name            string
		failureType     switchoverFailureType
		expectedMessage string
	}{
		{
			name:            "suspend failure event",
			failureType:     switchoverFailureSuspend,
			expectedMessage: "Failed to suspend engine frontend before switchover to 10.1.2.3:9502: rpc failed",
		},
		{
			name:            "switch and resume failure event",
			failureType:     switchoverFailureSwitchAndResume,
			expectedMessage: "Failed to switch over target to 10.1.2.3:9502 and failed to resume engine frontend: rpc failed",
		},
		{
			name:            "resume failure event",
			failureType:     switchoverFailureResume,
			expectedMessage: "Switched over target to 10.1.2.3:9502 but failed to resume engine frontend: rpc failed",
		},
		{
			name:            "default switch failure event",
			failureType:     switchoverFailureSwitch,
			expectedMessage: "Failed to switch over target to 10.1.2.3:9502: rpc failed",
		},
	}

	for _, tc := range testCases {
		fakeRecorder := record.NewFakeRecorder(5)
		recordEngineFrontendSwitchoverFailureEvent(fakeRecorder, ef, tc.failureType, targetAddress, baseErr)

		select {
		case event := <-fakeRecorder.Events:
			caseInfo := Commentf("case=%s event=%s", tc.name, event)
			c.Assert(strings.Contains(event, corev1.EventTypeWarning), Equals, true, caseInfo)
			c.Assert(strings.Contains(event, constant.EventReasonFailedSwitchover), Equals, true, caseInfo)
			c.Assert(strings.Contains(event, tc.expectedMessage), Equals, true, caseInfo)
		default:
			c.Fatalf("case=%s expected one recorded event", tc.name)
		}
	}
}
