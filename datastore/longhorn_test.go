package datastore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
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

func TestGetVolumeCurrentEngineFrontendReturnsErrorWhenMissing(t *testing.T) {
	const (
		testNamespace  = "longhorn-system"
		testVolumeName = "test-volume"
	)

	lhClient := lhfake.NewSimpleClientset(&longhorn.Volume{ // nolint: staticcheck
		ObjectMeta: metav1.ObjectMeta{
			Name:      testVolumeName,
			Namespace: testNamespace,
		},
	}) // nolint: staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactories.Start(stopCh)

	require.True(t, cache.WaitForCacheSync(stopCh,
		ds.VolumeInformer.HasSynced,
		ds.EngineFrontendInformer.HasSynced,
	))

	ef, err := ds.GetVolumeCurrentEngineFrontend(testVolumeName)
	require.Error(t, err)
	require.Nil(t, ef)
	require.Contains(t, err.Error(), "cannot find the current engine frontend")
}

func TestGetCurrentEngineAndExtrasIgnoresDeletingActiveEngine(t *testing.T) {
	deletionTime := metav1.Now()
	deletingEngine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleting-engine",
			DeletionTimestamp: &deletionTime,
		},
		Spec: longhorn.EngineSpec{Active: true},
	}
	currentEngine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{Name: "current-engine"},
		Spec:       longhorn.EngineSpec{Active: true},
	}

	engine, extras, err := GetCurrentEngineAndExtras(&longhorn.Volume{}, map[string]*longhorn.Engine{
		deletingEngine.Name: deletingEngine,
		currentEngine.Name:  currentEngine,
	})
	require.NoError(t, err)
	assert.Same(t, currentEngine, engine)
	require.Len(t, extras, 1)
	assert.Same(t, deletingEngine, extras[0])
}

func TestGetVolumeCurrentEngineDoesNotPromoteCachedEngine(t *testing.T) {
	const namespace = "longhorn-system"
	volume := &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "volume", Namespace: namespace},
		Spec: longhorn.VolumeSpec{
			DataEngine:   longhorn.DataEngineTypeV2,
			NodeID:       "old-node",
			EngineNodeID: "target-node",
		},
		Status: longhorn.VolumeStatus{CurrentNodeID: "old-node", CurrentEngineNodeID: "old-node"},
	}
	deletionTime := metav1.Now()
	oldEngine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name: "old-engine", Namespace: namespace,
			Labels: types.GetVolumeLabels(volume.Name), DeletionTimestamp: &deletionTime,
		},
		Spec: longhorn.EngineSpec{Active: true, InstanceSpec: longhorn.InstanceSpec{NodeID: "old-node"}},
	}
	targetEngine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name: "target-engine", Namespace: namespace, Labels: types.GetVolumeLabels(volume.Name),
		},
		Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{NodeID: "target-node"}},
	}
	factory := lhinformerfactory.NewSharedInformerFactory(lhfake.NewSimpleClientset(), 0) // nolint: staticcheck
	engineInformer := factory.Longhorn().V1beta2().Engines()
	volumeInformer := factory.Longhorn().V1beta2().Volumes()
	require.NoError(t, engineInformer.Informer().GetIndexer().Add(oldEngine))
	require.NoError(t, engineInformer.Informer().GetIndexer().Add(targetEngine))
	require.NoError(t, volumeInformer.Informer().GetIndexer().Add(volume))
	ds := &DataStore{namespace: namespace, engineLister: engineInformer.Lister(), volumeLister: volumeInformer.Lister()}

	engine, err := ds.GetVolumeCurrentEngine(volume.Name)
	require.NoError(t, err)
	assert.Equal(t, targetEngine.Name, engine.Name)
	assert.False(t, engine.Spec.Active)
	assert.False(t, targetEngine.Spec.Active, "selection must not promote the informer object")
	assert.True(t, oldEngine.Spec.Active)
	assert.Equal(t, "old-node", volume.Status.CurrentEngineNodeID)
}

func TestCurrentEngineSelectionWithSingleActiveEngineWithoutNodeID(t *testing.T) {
	for name, selectEngine := range map[string]func(*longhorn.Volume, map[string]*longhorn.Engine) (*longhorn.Engine, []*longhorn.Engine, error){
		"GetCurrentEngineAndExtras":    GetCurrentEngineAndExtras,
		"GetNewCurrentEngineAndExtras": GetNewCurrentEngineAndExtras,
	} {
		t.Run(name, func(t *testing.T) {
			for _, deleting := range []bool{false, true} {
				t.Run(fmt.Sprintf("deleting=%t", deleting), func(t *testing.T) {
					engine := &longhorn.Engine{
						ObjectMeta: metav1.ObjectMeta{Name: "engine"},
						Spec:       longhorn.EngineSpec{Active: true},
					}
					if deleting {
						deletionTime := metav1.Now()
						engine.DeletionTimestamp = &deletionTime
					}

					currentEngine, extras, err := selectEngine(&longhorn.Volume{}, map[string]*longhorn.Engine{
						engine.Name: engine,
					})
					if deleting {
						require.Error(t, err)
						assert.Contains(t, err.Error(), "cannot find the current engine")
						assert.Nil(t, currentEngine)
						return
					}
					require.NoError(t, err)
					assert.Same(t, engine, currentEngine)
					assert.Empty(t, extras)
				})
			}
		})
	}
}
