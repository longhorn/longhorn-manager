package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
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

func TestGetFallbackRunningInstanceManager(t *testing.T) {
	testCases := map[string]struct {
		instanceManagers []*longhorn.InstanceManager
		dataEngine       longhorn.DataEngineType
		// expectedName is the name of the instance manager expected to be selected.
		// An empty string means no eligible instance manager is expected (nil result).
		expectedName string
	}{
		"selects the only running all-in-one instance manager matching the data engine": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-v1", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeV1,
			expectedName: "im-v1",
		},
		"skips non-running candidates": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-stopped", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateStopped, false),
				newTestInstanceManager("im-error", TestNode2, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateError, false),
				newTestInstanceManager("im-running", "test-node-name-3", longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeV1,
			expectedName: "im-running",
		},
		"skips instance managers with a different data engine": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-v2", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeV1,
			expectedName: "",
		},
		"supports DataEngineTypeAll": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-v2", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeAll,
			expectedName: "im-v2",
		},
		"skips deprecated engine and replica instance manager types": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-engine", TestNode1, longhorn.InstanceManagerTypeEngine, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
				newTestInstanceManager("im-replica", TestNode2, longhorn.InstanceManagerTypeReplica, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
				newTestInstanceManager("im-aio", "test-node-name-3", longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeV1,
			expectedName: "im-aio",
		},
		"skips terminating instance managers": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-terminating", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, true),
				newTestInstanceManager("im-healthy", TestNode2, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeV1,
			expectedName: "im-healthy",
		},
		"returns nil when no eligible instance manager exists": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-stopped", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateStopped, false),
				newTestInstanceManager("im-engine", TestNode2, longhorn.InstanceManagerTypeEngine, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeV1,
			expectedName: "",
		},
		"selects deterministically by name when multiple candidates are eligible": {
			instanceManagers: []*longhorn.InstanceManager{
				newTestInstanceManager("im-b", TestNode2, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
				newTestInstanceManager("im-a", TestNode1, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
				newTestInstanceManager("im-c", "test-node-name-3", longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV1, longhorn.InstanceManagerStateRunning, false),
			},
			dataEngine:   longhorn.DataEngineTypeV1,
			expectedName: "im-a",
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

			im, err := getFallbackRunningInstanceManager(ds, tc.dataEngine)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.expectedName == "" {
				if im != nil {
					t.Fatalf("expected no eligible instance manager, got %v", im.Name)
				}
				return
			}

			if im == nil {
				t.Fatalf("expected instance manager %v, got nil", tc.expectedName)
			}
			if im.Name != tc.expectedName {
				t.Fatalf("unexpected instance manager: got %v, want %v", im.Name, tc.expectedName)
			}
		})
	}
}
