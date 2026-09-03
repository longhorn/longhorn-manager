package controller

import (
	"context"
	"strconv"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newTestBackupVolumeController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*BackupVolumeController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	proxyConnCounter := util.NewAtomicCounter()
	bvc, err := NewBackupVolumeController(logger, ds, scheme.Scheme, kubeClient, controllerID, TestNamespace, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	fakeRecorder := record.NewFakeRecorder(100)
	bvc.eventRecorder = fakeRecorder
	for index := range bvc.cacheSyncs {
		bvc.cacheSyncs[index] = alwaysReady
	}
	return bvc, nil
}

// TestBackupVolumeIsResponsibleFor verifies that backup volume ownership is transferred to a node
// with a running instance manager when the current owner is drained (its engine image DaemonSet
// keeps running while the instance manager pod is evicted). Otherwise the BackupVolume status is
// never synced. Ref: https://github.com/longhorn/longhorn/issues/13775
func (s *TestSuite) TestBackupVolumeIsResponsibleFor(c *C) {
	testCases := map[string]struct {
		controllerID       string
		ownerID            string
		nodesWithRunningIM []string
		engineImageNodes   []string
		v1Enabled          bool
		v2Enabled          bool
		imDataEngine       longhorn.DataEngineType
		deleting           bool
		expected           bool
	}{
		"node with running instance manager takes over from drained owner": {
			controllerID:       TestNode2,
			ownerID:            TestNode1,
			nodesWithRunningIM: []string{TestNode2},
			expected:           true,
		},
		"drained owner without running instance manager backs off": {
			controllerID:       TestNode1,
			ownerID:            TestNode1,
			nodesWithRunningIM: []string{TestNode2},
			expected:           false,
		},
		"owner with running instance manager keeps ownership": {
			controllerID:       TestNode2,
			ownerID:            TestNode2,
			nodesWithRunningIM: []string{TestNode2},
			expected:           true,
		},
		"full outage falls back to engine image only behavior": {
			controllerID:       TestNode1,
			ownerID:            TestNode1,
			nodesWithRunningIM: []string{},
			expected:           true,
		},
		"v2 data engine node with running instance manager takes over": {
			controllerID:       TestNode2,
			ownerID:            TestNode1,
			nodesWithRunningIM: []string{TestNode2},
			v1Enabled:          false,
			v2Enabled:          true,
			imDataEngine:       longhorn.DataEngineTypeV2,
			expected:           true,
		},
		"instance manager node without engine image falls back to engine image only behavior": {
			controllerID:       TestNode1,
			ownerID:            TestNode1,
			nodesWithRunningIM: []string{TestNode2},
			engineImageNodes:   []string{TestNode1},
			expected:           true,
		},
		"deleting backup volume ignores instance manager availability": {
			controllerID:       TestNode1,
			ownerID:            TestNode1,
			nodesWithRunningIM: []string{TestNode2},
			deleting:           true,
			expected:           true,
		},
	}

	for name, tc := range testCases {
		c.Logf("testing %v", name)

		if tc.engineImageNodes == nil {
			tc.engineImageNodes = []string{TestNode1, TestNode2}
		}
		if !tc.v1Enabled && !tc.v2Enabled {
			tc.v1Enabled = true
		}
		if tc.imDataEngine == "" {
			tc.imDataEngine = longhorn.DataEngineTypeV1
		}

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()

		bvc, err := newTestBackupVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(err, IsNil)

		for _, setting := range []*longhorn.Setting{
			newSetting(string(types.SettingNameDefaultEngineImage), TestEngineImage),
			newSetting(string(types.SettingNameDefaultInstanceManagerImage), TestInstanceManagerImage),
			newSetting(string(types.SettingNameV1DataEngine), strconv.FormatBool(tc.v1Enabled)),
			newSetting(string(types.SettingNameV2DataEngine), strconv.FormatBool(tc.v2Enabled)),
		} {
			setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(settingIndexer.Add(setting), IsNil)
		}

		// Both nodes are Ready. The engine image is deployed on engineImageNodes, mimicking a node
		// drained with --ignore-daemonsets that keeps the engine image DaemonSet running.
		engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
		engineImage.Status.NodeDeploymentMap = map[string]bool{}
		for _, nodeName := range tc.engineImageNodes {
			engineImage.Status.NodeDeploymentMap[nodeName] = true
		}
		engineImage, err = lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(eiIndexer.Add(engineImage), IsNil)

		for _, nodeName := range []string{TestNode1, TestNode2} {
			node := newNode(nodeName, TestNamespace, true, longhorn.ConditionStatusTrue, "")
			node, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(nodeIndexer.Add(node), IsNil)
		}

		for _, nodeName := range tc.nodesWithRunningIM {
			im := newInstanceManager("instance-manager-"+nodeName, longhorn.InstanceManagerStateRunning,
				nodeName, nodeName, randomIP(), nil, nil, nil, tc.imDataEngine, TestInstanceManagerImage, false)
			im, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(imIndexer.Add(im), IsNil)
		}

		backupVolume := &longhorn.BackupVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      TestVolumeName,
				Namespace: TestNamespace,
			},
			Spec: longhorn.BackupVolumeSpec{
				VolumeName: TestVolumeName,
			},
			Status: longhorn.BackupVolumeStatus{
				OwnerID: tc.ownerID,
			},
		}
		if tc.deleting {
			now := metav1.Now()
			backupVolume.DeletionTimestamp = &now
		}

		isResponsible, err := bvc.isResponsibleFor(backupVolume, TestEngineImage)
		c.Assert(err, IsNil)
		c.Assert(isResponsible, Equals, tc.expected)
	}
}
