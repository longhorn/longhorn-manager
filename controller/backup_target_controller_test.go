package controller

import (
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
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

func newTestInstanceManager(name, nodeID string, imType longhorn.InstanceManagerType, dataEngine longhorn.DataEngineType, state longhorn.InstanceManagerState, terminating bool) *longhorn.InstanceManager {
	im := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels:    types.GetInstanceManagerLabels(nodeID, "", imType, dataEngine),
		},
		Spec: longhorn.InstanceManagerSpec{
			NodeID:     nodeID,
			Type:       imType,
			DataEngine: dataEngine,
		},
		Status: longhorn.InstanceManagerStatus{
			CurrentState: state,
		},
	}
	if terminating {
		now := metav1.Now()
		im.DeletionTimestamp = &now
	}
	return im
}

func TestBackupControllerIsInstanceManagerRunningOnNode(t *testing.T) {
	testCases := map[string]struct {
		instanceManagers []*longhorn.InstanceManager
		nodeID           string
		dataEngine       longhorn.DataEngineType
		expected         bool
	}{
		"running instance manager on the node": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-node1", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			nodeID:     TestNode1,
			dataEngine: longhorn.DataEngineTypeV1,
			expected:   true,
		},
		"no instance manager on the node": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-node1", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			nodeID:     TestNode2,
			dataEngine: longhorn.DataEngineTypeV1,
			expected:   false,
		},
		"instance manager on the node is not running": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-node1", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateStopped, false),
			},
			nodeID:     TestNode1,
			dataEngine: longhorn.DataEngineTypeV1,
			expected:   false,
		},
		"instance manager data engine mismatch": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-node1", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning, false),
			},
			nodeID:     TestNode1,
			dataEngine: longhorn.DataEngineTypeV1,
			expected:   false,
		},
		"terminating instance manager is excluded even if still running": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-node1", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, true),
			},
			nodeID:     TestNode1,
			dataEngine: longhorn.DataEngineTypeV1,
			expected:   false,
		},
		"healthy instance manager is selected over a terminating one on the same node": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-node1-terminating", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, true),
				newTestInstanceManager("im-node1-healthy", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			nodeID:     TestNode1,
			dataEngine: longhorn.DataEngineTypeV1,
			expected:   true,
		},
		"empty node id": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-node1", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			nodeID:     "",
			dataEngine: longhorn.DataEngineTypeV1,
			expected:   false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
			imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
			ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

			for _, im := range tc.instanceManagers {
				if err := imIndexer.Add(im); err != nil {
					t.Fatalf("failed to seed instance manager %v: %v", im.Name, err)
				}
			}

			bc := &BackupController{ds: ds}
			got, err := bc.isInstanceManagerRunningOnNode(tc.nodeID, tc.dataEngine)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.expected {
				t.Fatalf("unexpected result: got %v, want %v", got, tc.expected)
			}
		})
	}
}
