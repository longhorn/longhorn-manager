package controller

import (
	"context"
	"testing"
	"time"

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

func TestIsBackupTargetSyncRequired(t *testing.T) {
	now := time.Now().UTC()

	testCases := map[string]struct {
		backupTarget *longhorn.BackupTarget
		expected     bool
	}{
		"nil backup target": {
			backupTarget: nil,
			expected:     false,
		},
		"empty backup target URL bypasses stale sync gate": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: "",
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: true,
		},
		"stale sync request is skipped for configured target": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					Available:    true,
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: false,
		},
		"newer sync request is processed": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now},
				},
				Status: longhorn.BackupTargetStatus{
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: true,
		},
		"unavailable target with non-empty URL defers to timer": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					Available:    false,
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: false,
		},
		"first sync after URL transition from empty": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
				},
				Status: longhorn.BackupTargetStatus{
					Available: false,
				},
			},
			expected: true,
		},
		"available target with stale sync is skipped": {
			backupTarget: &longhorn.BackupTarget{
				Spec: longhorn.BackupTargetSpec{
					BackupTargetURL: TestBackupTarget,
					SyncRequestedAt: metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				Status: longhorn.BackupTargetStatus{
					Available:    true,
					LastSyncedAt: metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := isBackupTargetSyncRequired(tc.backupTarget)
			if actual != tc.expected {
				t.Fatalf("unexpected sync requirement: got %v, want %v", actual, tc.expected)
			}
		})
	}
}

func newTestBackupTargetController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*BackupTargetController, error) {
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	proxyConnCounter := util.NewAtomicCounter()
	btc, err := NewBackupTargetController(logger, ds, scheme.Scheme, kubeClient, controllerID, TestNamespace, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	fakeRecorder := record.NewFakeRecorder(100)
	btc.eventRecorder = fakeRecorder
	for index := range btc.cacheSyncs {
		btc.cacheSyncs[index] = alwaysReady
	}
	return btc, nil
}

// TestBackupTargetIsResponsibleFor verifies that backup target ownership is transferred to a node
// with a running instance manager when the current owner is drained (its engine image DaemonSet
// keeps running while the instance manager pod is evicted). Otherwise the BackupTarget is never
// synced and BackupVolume CRs are never created. Ref: https://github.com/longhorn/longhorn/issues/13775
func (s *TestSuite) TestBackupTargetIsResponsibleFor(c *C) {
	testCases := map[string]struct {
		controllerID       string
		ownerID            string
		nodesWithRunningIM []string
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
	}

	for name, tc := range testCases {
		c.Logf("testing %v", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
		eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()

		btc, err := newTestBackupTargetController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(err, IsNil)

		for _, setting := range []*longhorn.Setting{
			newSetting(string(types.SettingNameDefaultEngineImage), TestEngineImage),
			newSetting(string(types.SettingNameDefaultInstanceManagerImage), TestInstanceManagerImage),
			newSetting(string(types.SettingNameV1DataEngine), "true"),
			newSetting(string(types.SettingNameV2DataEngine), "false"),
		} {
			setting, err = lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(settingIndexer.Add(setting), IsNil)
		}

		// Both nodes are Ready and both have the engine image deployed, mimicking a node drained with
		// --ignore-daemonsets that keeps the engine image DaemonSet running.
		engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
		engineImage.Status.NodeDeploymentMap = map[string]bool{TestNode1: true, TestNode2: true}
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
				nodeName, nodeName, randomIP(), nil, nil, nil, longhorn.DataEngineTypeV1, TestInstanceManagerImage, false)
			im, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(imIndexer.Add(im), IsNil)
		}

		backupTarget := &longhorn.BackupTarget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      types.DefaultBackupTargetName,
				Namespace: TestNamespace,
			},
			Status: longhorn.BackupTargetStatus{
				OwnerID: tc.ownerID,
			},
		}

		isResponsible, err := btc.isResponsibleFor(backupTarget, TestEngineImage)
		c.Assert(err, IsNil)
		c.Assert(isResponsible, Equals, tc.expected)
	}
}
