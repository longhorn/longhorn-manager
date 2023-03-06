package controller

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"

	. "gopkg.in/check.v1"
)

type EngineImageControllerTestCase struct {
	// For DaemonSet related tests
	node *longhorn.Node

	// For ref count check
	volume *longhorn.Volume
	engine *longhorn.Engine

	// For ref count related test
	upgradedEngineImage *longhorn.EngineImage

	// For expired engine image cleanup
	defaultEngineImage string

	currentEngineImage  *longhorn.EngineImage
	currentDaemonSet    *appv1.DaemonSet
	currentDaemonSetPod *corev1.Pod

	expectedEngineImage *longhorn.EngineImage
	expectedDaemonSet   *appv1.DaemonSet
}

func newTestEngineImageController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset) *EngineImageController {

	// Skip the Lister check that occurs on creation of an Instance Manager.
	datastore.SkipListerCheck = true

	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionsClient, TestNamespace)

	logger := logrus.StandardLogger()
	ic := NewEngineImageController(
		logger,
		ds, scheme.Scheme,
		kubeClient, TestNamespace, TestNode1, TestServiceAccount)

	fakeRecorder := record.NewFakeRecorder(100)
	ic.eventRecorder = fakeRecorder
	for index := range ic.cacheSyncs {
		ic.cacheSyncs[index] = alwaysReady
	}
	ic.nowHandler = getTestNow
	ic.engineBinaryChecker = fakeEngineBinaryChecker
	ic.engineImageVersionUpdater = fakeEngineImageUpdater

	return ic
}

func getEngineImageControllerTestTemplate() *EngineImageControllerTestCase {
	tc := &EngineImageControllerTestCase{
		node:                newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
		volume:              newVolume(TestVolumeName, 2),
		engine:              newEngine(TestEngineName, TestEngineImage, TestInstanceManagerName, TestNode1, TestIP1, 0, true, longhorn.InstanceStateRunning, longhorn.InstanceStateRunning),
		upgradedEngineImage: newEngineImage(TestUpgradedEngineImage, longhorn.EngineImageStateDeployed),

		defaultEngineImage: TestEngineImage,

		currentEngineImage: newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed),
		currentDaemonSet:   newEngineImageDaemonSet(),
	}

	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.currentEngineImage.Status.RefCount = 2

	return tc
}

func (tc *EngineImageControllerTestCase) copyCurrentToExpected() {
	if tc.currentEngineImage != nil {
		tc.expectedEngineImage = tc.currentEngineImage.DeepCopy()
	}
	if tc.currentDaemonSet != nil {
		tc.expectedDaemonSet = tc.currentDaemonSet.DeepCopy()
	}
}

func createEngineImageDaemonSetPod(name string, containerReadyStatus bool, nodeID string) *corev1.Pod {
	pod := newPod(
		&corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Ready: containerReadyStatus}}},
		name,
		TestNamespace,
		nodeID,
	)
	pod.Labels = types.GetEIDaemonSetLabelSelector(getTestEngineImageName())
	return pod
}

func generateEngineImageControllerTestCases() map[string]*EngineImageControllerTestCase {
	var tc *EngineImageControllerTestCase
	testCases := map[string]*EngineImageControllerTestCase{}
	invalidEngineVersion := -1

	// The TestNode2 is a non-existing node and is just used for node down test.
	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.OwnerID = TestNode2
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.OwnerID = TestNode1
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["Engine image ownerID node is down"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.State = longhorn.EngineImageStateDeploying
	tc.currentDaemonSet = nil
	tc.copyCurrentToExpected()
	tc.expectedDaemonSet = newEngineImageDaemonSet()
	tc.expectedDaemonSet.Status.NumberAvailable = 0
	tc.expectedEngineImage.Status.Conditions = types.SetConditionWithoutTimestamp(tc.expectedEngineImage.Status.Conditions, longhorn.EngineImageConditionTypeReady, longhorn.ConditionStatusFalse, longhorn.EngineImageConditionTypeReadyReasonDaemonSet, "")
	testCases["Engine image DaemonSet creation"] = tc

	// The DaemonSet is not ready hence the engine image will become state `deploying`
	tc = getEngineImageControllerTestTemplate()
	tc.currentDaemonSet.Status.NumberAvailable = 0
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = longhorn.EngineImageStateDeploying
	tc.expectedEngineImage.Status.Conditions = types.SetConditionWithoutTimestamp(tc.expectedEngineImage.Status.Conditions, longhorn.EngineImageConditionTypeReady, longhorn.ConditionStatusFalse, longhorn.EngineImageConditionTypeReadyReasonDaemonSet, "")
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: false}
	testCases["Engine Image DaemonSet pods are suddenly removed"] = tc

	// `ei.Status.refCount` should become 2 (1 volume and 1 engine are using it) and `Status.NoRefSince` should be unset
	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.RefCount = 0
	tc.currentEngineImage.Status.NoRefSince = getTestNow()
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.RefCount = 2
	tc.expectedEngineImage.Status.NoRefSince = ""
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["One volume starts to use the engine image"] = tc

	// No volume is using the current engine image.
	tc = getEngineImageControllerTestTemplate()
	tc.volume = nil
	tc.engine = nil
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.RefCount = 0
	tc.expectedEngineImage.Status.NoRefSince = getTestNow()
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["The default engine image won't be cleaned up even if there is no volume using it"] = tc

	tc = getEngineImageControllerTestTemplate()
	incompatibleVersion := longhorn.EngineVersionDetails{
		Version:                 "ei.Spec.Image",
		GitCommit:               "unknown",
		BuildDate:               "unknown",
		CLIAPIVersion:           invalidEngineVersion,
		CLIAPIMinVersion:        invalidEngineVersion,
		ControllerAPIVersion:    invalidEngineVersion,
		ControllerAPIMinVersion: invalidEngineVersion,
		DataFormatVersion:       invalidEngineVersion,
		DataFormatMinVersion:    invalidEngineVersion,
	}
	tc.currentEngineImage.Status.EngineVersionDetails = incompatibleVersion
	tc.currentDaemonSetPod = createEngineImageDaemonSetPod(getTestEngineImageDaemonSetName()+TestPod1, true, TestNode1)
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = longhorn.EngineImageStateIncompatible
	tc.expectedEngineImage.Status.Conditions = types.SetConditionWithoutTimestamp(tc.expectedEngineImage.Status.Conditions, longhorn.EngineImageConditionTypeReady, longhorn.ConditionStatusFalse, longhorn.EngineImageConditionTypeReadyReasonBinary, "")
	tc.expectedEngineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true}
	testCases["Incompatible engine image"] = tc

	return testCases
}

func (s *TestSuite) TestEngineImage(c *C) {
	testCases := generateEngineImageControllerTestCases()
	for name, tc := range testCases {
		var err error
		logrus.Debugf("Testing engine image controller: %v", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		extentionClient := apiextensionsfake.NewSimpleClientset()

		dsIndexer := kubeInformerFactory.Apps().V1().DaemonSets().Informer().GetIndexer()
		podIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		nodeIndexer := lhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		settingIndexer := lhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		eiIndexer := lhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		vIndexer := lhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
		eIndexer := lhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()

		ic := newTestEngineImageController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, extentionClient)

		setting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), newSetting(string(types.SettingNameDefaultEngineImage), tc.defaultEngineImage), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = settingIndexer.Add(setting)
		c.Assert(err, IsNil)
		// For DaemonSet creation test
		setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), newSetting(string(types.SettingNameTaintToleration), ""), metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = settingIndexer.Add(setting)
		c.Assert(err, IsNil)

		node, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), tc.node, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nodeIndexer.Add(node)
		c.Assert(err, IsNil)

		ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), tc.currentEngineImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)
		ei, err = lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), tc.upgradedEngineImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		if tc.volume != nil {
			v, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), tc.volume, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = vIndexer.Add(v)
			c.Assert(err, IsNil)
		}
		if tc.engine != nil {
			e, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Create(context.TODO(), tc.engine, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = eIndexer.Add(e)
			c.Assert(err, IsNil)
		}
		if tc.currentDaemonSet != nil {
			ds, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Create(context.TODO(), tc.currentDaemonSet, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = dsIndexer.Add(ds)
			c.Assert(err, IsNil)
		}
		if tc.currentDaemonSetPod != nil {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), tc.currentDaemonSetPod, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = podIndexer.Add(p)
			c.Assert(err, IsNil)
		}
		engineImageControllerKey := fmt.Sprintf("%s/%s", TestNamespace, getTestEngineImageName())
		err = ic.syncEngineImage(engineImageControllerKey)
		c.Assert(err, IsNil)

		ei, err = lhClient.LonghornV1beta2().EngineImages(TestNamespace).Get(context.TODO(), getTestEngineImageName(), metav1.GetOptions{})
		if tc.expectedEngineImage == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			for k, v := range ei.Status.Conditions {
				v.LastTransitionTime = ""
				v.Message = ""
				ei.Status.Conditions[k] = v
			}
			c.Assert(ei.Status, DeepEquals, tc.expectedEngineImage.Status)
		}

		ds, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Get(context.TODO(), getTestEngineImageDaemonSetName(), metav1.GetOptions{})
		if tc.expectedDaemonSet == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			// For the DaemonSet created by the fake k8s client, the field `Status.DesiredNumberScheduled` won't be set automatically.
			ds.Status.DesiredNumberScheduled = 1
			c.Assert(ds.Status, DeepEquals, tc.expectedDaemonSet.Status)
		}
	}
}
