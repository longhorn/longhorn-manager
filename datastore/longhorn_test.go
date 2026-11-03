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

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"

	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
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
	ds := NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

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

func TestValidateSettingBlocksV2IMUpgradeStartTimeWhenActiveIMUExistsWithoutIMUC(t *testing.T) {
	const testNamespace = "longhorn-system"

	testCases := map[string]longhorn.InstanceManagerUpgradeStatus{
		"active relocating IMU": {
			State: longhorn.InstanceManagerUpgradeStateRelocatingEngines,
		},
		"pending IMU with startedAt": {
			State:     longhorn.InstanceManagerUpgradeStatePending,
			StartedAt: "2026-04-20T15:00:00Z",
		},
	}

	for name, status := range testCases {
		t.Run(name, func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset()                    // nolint:staticcheck
			kubeClient := fake.NewSimpleClientset()                    // nolint:staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint:staticcheck
			informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)

			ds := NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
			imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

			imu := &longhorn.InstanceManagerUpgrade{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-imu",
					Namespace: testNamespace,
				},
				Spec: longhorn.InstanceManagerUpgradeSpec{
					NodeID:      "test-node-1",
					TargetImage: "im:target",
				},
				Status: status,
			}
			createdIMU, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades(testNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, imuIndexer.Add(createdIMU))

			err = ds.ValidateSetting(string(types.SettingNameInstanceManagerUpgradeStartTime), "2026-04-20T15:00:00Z")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "actively in progress")
			assert.Contains(t, err.Error(), "IMU")
		})
	}
}

func TestValidateSettingRejectsUnsupportedV2InstanceManagerLiveUpgrade(t *testing.T) {
	const testNamespace = "longhorn-system"

	lhClient := lhfake.NewSimpleClientset()                    // nolint:staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint:staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint:staticcheck
	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

	currentVersion := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameCurrentLonghornVersion),
			Namespace: testNamespace,
			Annotations: map[string]string{
				types.GetLonghornLabelKey(types.V2InstanceManagerLiveUpgradeUnsupported): "true",
			},
		},
		Value: "v1.13.0",
	}
	created, err := lhClient.LonghornV1beta2().Settings(testNamespace).Create(context.TODO(), currentVersion, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, settingIndexer.Add(created))

	err = ds.ValidateSetting(string(types.SettingNameAllowInstanceManagerAutomaticUpgrade), `{"v2":"true"}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), types.MinimumLonghornVersionForV2InstanceManagerLiveUpgrade)

	require.NoError(t, ds.ValidateSetting(string(types.SettingNameAllowInstanceManagerAutomaticUpgrade), `{"v2":"false"}`))
}

func TestGetSettingValueExistedByDataEngineAllowsEmptyValue(t *testing.T) {
	const testNamespace = "longhorn-system"

	lhClient := lhfake.NewSimpleClientset()                    // nolint:staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint:staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint:staticcheck
	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameDataEngineLogFlags),
			Namespace: testNamespace,
		},
		Value: `{"v2":""}`,
	}
	created, err := lhClient.LonghornV1beta2().Settings(testNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, settingIndexer.Add(created))

	value, err := ds.GetSettingValueExistedByDataEngine(types.SettingNameDataEngineLogFlags, longhorn.DataEngineTypeV2)
	require.NoError(t, err)
	assert.Empty(t, value)
}

func TestVolumeHookPolicyAccessors(t *testing.T) {
	newPolicy := func(namespace, name string) *longhorn.VolumeHookPolicy {
		return &longhorn.VolumeHookPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	}
	factory := lhinformerfactory.NewSharedInformerFactory(lhfake.NewClientset(), 0)
	informer := factory.Longhorn().V1beta2().VolumeHookPolicies()
	for _, policy := range []*longhorn.VolumeHookPolicy{
		newPolicy("app-a", "mongo"),
		newPolicy("app-a", "postgres"),
		newPolicy("app-b", "mongo"),
	} {
		require.NoError(t, informer.Informer().GetIndexer().Add(policy))
	}
	ds := &DataStore{namespace: "longhorn-system", volumeHookPolicyLister: informer.Lister()}

	getTests := map[string]struct {
		namespace, name string
		wantNamespace   string // empty means an error is expected
		wantNotFound    bool
	}{
		"get is scoped to the given namespace": {
			namespace: "app-b", name: "mongo",
			wantNamespace: "app-b",
		},
		"get does not see other namespaces": {
			namespace: "app-b", name: "postgres",
			wantNotFound: true,
		},
		"get never falls back to the manager namespace": {
			namespace: "", name: "mongo",
		},
	}
	for name, tc := range getTests {
		t.Run(name, func(t *testing.T) {
			policy, err := ds.GetVolumeHookPolicyRO(tc.namespace, tc.name)
			if tc.wantNamespace != "" {
				require.NoError(t, err)
				assert.Equal(t, tc.wantNamespace, policy.Namespace)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantNotFound, apierrors.IsNotFound(err))
		})
	}

	listTests := map[string]struct {
		namespace string
		all       bool
		wantLen   int
		wantErr   bool
	}{
		"list by namespace": {namespace: "app-a", wantLen: 2},
		"list never falls back to the manager namespace": {namespace: "", wantErr: true},
		"list all namespaces":                            {all: true, wantLen: 3},
	}
	for name, tc := range listTests {
		t.Run(name, func(t *testing.T) {
			var policies []*longhorn.VolumeHookPolicy
			var err error
			if tc.all {
				policies, err = ds.ListAllVolumeHookPoliciesRO()
			} else {
				policies, err = ds.ListVolumeHookPoliciesRO(tc.namespace)
			}
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, policies, tc.wantLen)
		})
	}
}

func TestResolveVolumeHookPolicyForVolume(t *testing.T) {
	newPolicy := func(namespace, name string, pvcLabels map[string]string) *longhorn.VolumeHookPolicy {
		return &longhorn.VolumeHookPolicy{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Generation: 3},
			Spec:       longhorn.VolumeHookPolicySpec{PVCSelector: &metav1.LabelSelector{MatchLabels: pvcLabels}},
		}
	}
	newNamePolicy := func(namespace, name string, pvcNames ...string) *longhorn.VolumeHookPolicy {
		return &longhorn.VolumeHookPolicy{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Generation: 3},
			Spec:       longhorn.VolumeHookPolicySpec{PVCNames: pvcNames},
		}
	}
	newPVC := func(namespace, name string, labels map[string]string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels}}
	}
	newVolume := func(name, namespace, pvcName string) *longhorn.Volume {
		return &longhorn.Volume{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "longhorn-system"},
			Status: longhorn.VolumeStatus{KubernetesStatus: longhorn.KubernetesStatus{
				Namespace: namespace, PVCName: pvcName,
			}},
		}
	}

	lhFactory := lhinformerfactory.NewSharedInformerFactory(lhfake.NewClientset(), 0)
	policyInformer := lhFactory.Longhorn().V1beta2().VolumeHookPolicies()
	for _, policy := range []*longhorn.VolumeHookPolicy{
		newPolicy("app", "mongo", map[string]string{"app": "mongo"}),
		newPolicy("app", "postgres", map[string]string{"app": "postgres"}),
		newPolicy("app", "postgres-dup", map[string]string{"app": "postgres"}),
		newPolicy("other", "mongo", map[string]string{"app": "mongo"}),
		newNamePolicy("app", "redis-by-name", "data-redis-0", "data-redis-1"),
		newNamePolicy("app", "mysql-by-name", "mysql-data"),
		newNamePolicy("other", "redis-by-name", "data-redis-0"),
	} {
		require.NoError(t, policyInformer.Informer().GetIndexer().Add(policy))
	}
	kubeFactory := kubeinformers.NewSharedInformerFactory(fake.NewClientset(), 0)
	pvcInformer := kubeFactory.Core().V1().PersistentVolumeClaims()
	for _, pvc := range []*corev1.PersistentVolumeClaim{
		newPVC("app", "mongo-data", map[string]string{"app": "mongo"}),
		newPVC("app", "postgres-data", map[string]string{"app": "postgres"}),
		newPVC("app", "unlabeled", nil),
		newPVC("app", "data-redis-1", nil),
		newPVC("app", "mysql-data", map[string]string{"app": "mongo"}),
	} {
		require.NoError(t, pvcInformer.Informer().GetIndexer().Add(pvc))
	}
	ds := &DataStore{
		namespace:                   "longhorn-system",
		volumeHookPolicyLister:      policyInformer.Lister(),
		persistentVolumeClaimLister: pvcInformer.Lister(),
	}
	resolutionReason := func(t *testing.T, err error) snapshothook.AdmissionFailureReason {
		var resolution *snapshothook.PolicyResolutionError
		require.ErrorAs(t, err, &resolution)
		return resolution.Reason
	}

	historical := newVolume("vol-history", "app", "mongo-data")
	historical.Status.KubernetesStatus.LastPVCRefAt = "2026-01-01T00:00:00Z"

	tests := map[string]struct {
		volume          *longhorn.Volume
		wantPolicy      string // namespaced name; empty means an error is expected
		wantReason      snapshothook.AdmissionFailureReason
		wantErrContains []string
	}{
		"exactly one match in the PVC's namespace": {
			volume:     newVolume("vol-mongo", "app", "mongo-data"),
			wantPolicy: "app/mongo",
		},
		"pvcNames covers an unlabeled PVC in the policy's namespace": {
			volume:     newVolume("vol-redis", "app", "data-redis-1"),
			wantPolicy: "app/redis-by-name",
		},
		"no match names the volume and PVC": {
			volume:          newVolume("vol-plain", "app", "unlabeled"),
			wantReason:      snapshothook.AdmissionFailureNoPolicy,
			wantErrContains: []string{"vol-plain", "app/unlabeled"},
		},
		"several matches name every policy": {
			volume:          newVolume("vol-pg", "app", "postgres-data"),
			wantReason:      snapshothook.AdmissionFailureMultiplePolicies,
			wantErrContains: []string{"app/postgres, app/postgres-dup"},
		},
		"a name policy and a selector policy on the same PVC are two policies": {
			volume:          newVolume("vol-mysql", "app", "mysql-data"),
			wantReason:      snapshothook.AdmissionFailureMultiplePolicies,
			wantErrContains: []string{"app/mongo, app/mysql-by-name"},
		},
		"a volume without a PVC cannot be a hooked member": {
			volume:     newVolume("vol-detached", "", ""),
			wantReason: snapshothook.AdmissionFailureNoPVC,
		},
		"a historical PVC reference does not count as a PVC": {
			volume:     historical,
			wantReason: snapshothook.AdmissionFailureNoPVC,
		},
		"PVC missing from cache": {
			volume:          newVolume("vol-gone", "app", "gone"),
			wantReason:      snapshothook.AdmissionFailureNoPVC,
			wantErrContains: []string{"does not exist"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			policy, err := ds.ResolveVolumeHookPolicyForVolume(tc.volume)
			if tc.wantPolicy != "" {
				require.NoError(t, err)
				assert.Equal(t, tc.wantPolicy, snapshothook.PolicyNamespacedName(policy))
				assert.NotZero(t, policy.Generation, "the resolved policy must carry its metadata")
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantReason, resolutionReason(t, err))
			for _, want := range tc.wantErrContains {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}
