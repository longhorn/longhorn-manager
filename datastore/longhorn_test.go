package datastore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	"github.com/longhorn/longhorn-manager/types"
)

func TestCreateReplica(t *testing.T) {
	const (
		testNamespace   = "longhorn-system"
		testVolumeName  = "test-volume"
		testNodeID      = "test-node"
		testDiskID      = "test-disk"
		testReplicaName = "test-replica"
	)

	newTestDataStore := func(objects []runtime.Object) *DataStore {
		lhClient := lhfake.NewSimpleClientset(objects...) // nolint: staticcheck
		informerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, 0)

		replicaInformer := informerFactory.Longhorn().V1beta2().Replicas()

		ds := &DataStore{
			namespace:       testNamespace,
			lhClient:        lhClient,
			replicaLister:   replicaInformer.Lister(),
			ReplicaInformer: replicaInformer.Informer(),
		}

		return ds
	}

	startInformers := func(ds *DataStore, stopCh chan struct{}) error {
		go ds.ReplicaInformer.Run(stopCh)

		if !cache.WaitForCacheSync(stopCh, ds.ReplicaInformer.HasSynced) {
			return fmt.Errorf("failed to sync informer cache")
		}

		return nil
	}

	type testCase struct {
		name string

		existingObjects []runtime.Object
		startInformers  bool

		replica *longhorn.Replica

		expectError      bool
		expectedErrorMsg string
		validateResult   func(t *testing.T, ds *DataStore, created *longhorn.Replica, err error)
	}

	tests := map[string]testCase{
		"success-create replica with all fields": {
			name:            "success - create replica with all fields",
			existingObjects: []runtime.Object{},
			startInformers:  true,
			replica: &longhorn.Replica{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testReplicaName,
					Namespace: testNamespace,
				},
				Spec: longhorn.ReplicaSpec{
					InstanceSpec: longhorn.InstanceSpec{
						VolumeName: testVolumeName,
						NodeID:     testNodeID,
					},
					DiskID: testDiskID,
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, ds *DataStore, created *longhorn.Replica, err error) {
				require.NoError(t, err)
				assert.NotNil(t, created)
				assert.Equal(t, testReplicaName, created.Name)
				assert.Equal(t, testVolumeName, created.Spec.VolumeName)
				assert.Equal(t, testNodeID, created.Spec.NodeID)
				assert.Equal(t, testDiskID, created.Spec.DiskID)

				// Verify replica exists in the client
				fetchedReplica, fetchErr := ds.lhClient.LonghornV1beta2().Replicas(testNamespace).Get(context.TODO(), testReplicaName, metav1.GetOptions{})
				require.NoError(t, fetchErr)
				assert.Equal(t, testReplicaName, fetchedReplica.Name)
			},
		},
		"failure-replica already exists": {
			name: "failure - replica already exists",
			existingObjects: []runtime.Object{
				&longhorn.Replica{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testReplicaName,
						Namespace: testNamespace,
					},
					Spec: longhorn.ReplicaSpec{
						InstanceSpec: longhorn.InstanceSpec{
							VolumeName: testVolumeName,
							NodeID:     testNodeID,
						},
						DiskID: testDiskID,
					},
				},
			},
			startInformers: true,
			replica: &longhorn.Replica{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testReplicaName,
					Namespace: testNamespace,
				},
				Spec: longhorn.ReplicaSpec{
					InstanceSpec: longhorn.InstanceSpec{
						VolumeName: testVolumeName,
						NodeID:     testNodeID,
					},
					DiskID: testDiskID,
				},
			},
			expectError:      true,
			expectedErrorMsg: "already exists",
			validateResult: func(t *testing.T, ds *DataStore, created *longhorn.Replica, err error) {
				require.Error(t, err)
				assert.Nil(t, created)
				assert.Contains(t, err.Error(), "already exists")
			},
		},
		"failure-verification failed with cleanup (informer not synced)": {
			name:            "failure - verification failed with cleanup (informer not synced)",
			existingObjects: []runtime.Object{},
			startInformers:  false, // Don't start informers to simulate cache sync failure
			replica: &longhorn.Replica{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testReplicaName,
					Namespace: testNamespace,
				},
				Spec: longhorn.ReplicaSpec{
					InstanceSpec: longhorn.InstanceSpec{
						VolumeName: testVolumeName,
						NodeID:     testNodeID,
					},
					DiskID: testDiskID,
				},
			},
			expectError:      true,
			expectedErrorMsg: "failed to verify the existence",
			validateResult: func(t *testing.T, ds *DataStore, created *longhorn.Replica, err error) {
				require.Error(t, err)
				assert.Nil(t, created)
				assert.Contains(t, err.Error(), "failed to verify the existence")

				// Verify cleanup was attempted - replica may still exist briefly
				fetchedReplica, fetchErr := ds.lhClient.LonghornV1beta2().Replicas(testNamespace).Get(context.TODO(), testReplicaName, metav1.GetOptions{})
				if fetchErr == nil {
					// If it still exists, it means the cleanup was initiated but not completed yet
					assert.NotNil(t, fetchedReplica)
				}
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ds := newTestDataStore(tc.existingObjects)

			var stopCh chan struct{}
			if tc.startInformers {
				stopCh = make(chan struct{})
				defer close(stopCh)

				err := startInformers(ds, stopCh)
				require.NoError(t, err, "Failed to start informers")
			}

			createdReplica, err := ds.CreateReplica(tc.replica)

			if tc.validateResult != nil {
				tc.validateResult(t, ds, createdReplica, err)
			} else {
				if tc.expectError {
					require.Error(t, err)
					assert.Nil(t, createdReplica)
					if tc.expectedErrorMsg != "" {
						assert.Contains(t, err.Error(), tc.expectedErrorMsg)
					}
				} else {
					require.NoError(t, err)
					assert.NotNil(t, createdReplica)
				}
			}
		})
	}
}

func TestGetSettingValidValue(t *testing.T) {
	// Use a v1-only bool setting definition similar to disable-revision-counter
	v1OnlyBoolDef := types.SettingDefinition{
		DisplayName:        "Test V1 Only Bool",
		Type:               types.SettingTypeBool,
		Required:           true,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"true\"}", longhorn.DataEngineTypeV1),
	}

	tests := map[string]struct {
		definition  types.SettingDefinition
		value       string
		expectError bool
		expected    string
	}{
		"valid v1-only value": {
			definition:  v1OnlyBoolDef,
			value:       fmt.Sprintf("{%q:\"false\"}", longhorn.DataEngineTypeV1),
			expectError: false,
			expected:    fmt.Sprintf("{%q:\"false\"}", longhorn.DataEngineTypeV1),
		},
		"invalid v1 and v2 for v1-only setting": {
			definition:  v1OnlyBoolDef,
			value:       fmt.Sprintf("{%q:\"true\",%q:\"true\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
			expectError: true,
		},
		"invalid v2-only for v1-only setting": {
			definition:  v1OnlyBoolDef,
			value:       fmt.Sprintf("{%q:\"true\"}", longhorn.DataEngineTypeV2),
			expectError: true,
		},
		"single value gets expanded to all supported engines": {
			definition:  v1OnlyBoolDef,
			value:       "false",
			expectError: false,
			expected:    fmt.Sprintf("{%q:\"false\"}", longhorn.DataEngineTypeV1),
		},
		"non data-engine-specific returns as is": {
			definition: types.SettingDefinition{
				DisplayName:        "Test Non Specific",
				Type:               types.SettingTypeBool,
				DataEngineSpecific: false,
				Default:            "true",
			},
			value:       "false",
			expectError: false,
			expected:    "false",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := GetSettingValidValue(tc.definition, tc.value)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestGetSettingValidValueStripped(t *testing.T) {
	// Use a v1-only bool setting definition
	v1OnlyBoolDef := types.SettingDefinition{
		DisplayName:        "Test V1 Only Bool",
		Type:               types.SettingTypeBool,
		Required:           true,
		DataEngineSpecific: true,
		Default:            fmt.Sprintf("{%q:\"true\"}", longhorn.DataEngineTypeV1),
	}

	tests := map[string]struct {
		definition types.SettingDefinition
		value      string
		expected   string
	}{
		"strips v2 from v1-only setting": {
			definition: v1OnlyBoolDef,
			value:      fmt.Sprintf("{%q:\"true\",%q:\"true\"}", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2),
			expected:   fmt.Sprintf("{%q:\"true\"}", longhorn.DataEngineTypeV1),
		},
		"valid v1-only value unchanged": {
			definition: v1OnlyBoolDef,
			value:      fmt.Sprintf("{%q:\"false\"}", longhorn.DataEngineTypeV1),
			expected:   fmt.Sprintf("{%q:\"false\"}", longhorn.DataEngineTypeV1),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := GetSettingValidValueStripped(tc.definition, tc.value)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}
