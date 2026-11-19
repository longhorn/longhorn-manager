package datastore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lhns "github.com/longhorn/go-common-libs/ns"

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

func TestIsSameImageReference(t *testing.T) {
	tests := map[string]struct {
		running string
		target  string
		expect  bool
	}{
		"same reference":        {"longhornio/im:v1", "longhornio/im:v1", true},
		"normalized repository": {"docker.io/longhornio/im:v1", "longhornio/im:v1", true},
		"another tag":           {"longhornio/im:v1-head", "longhornio/im:v1", false},
		"another repository":    {"registry.example.com/longhornio/im:v1", "longhornio/im:v1", false},
		"image ID":              {"sha256:" + strings.Repeat("a", 64), "longhornio/im:v1", false},
	}
	for name, tt := range tests {
		assert.Equal(t, tt.expect, isSameImageReference(tt.running, tt.target), name)
	}
}

// hostlessJoiner runs host namespace functions in the test process, so the
// filesystem-type default disk can be created under t.TempDir().
type hostlessJoiner struct{}

func (hostlessJoiner) Revert() error { return nil }

func (hostlessJoiner) Run(fn func() (any, error)) (any, error) { return fn() }

func TestCreateDefaultDisks(t *testing.T) {
	origJoiner := lhns.NewJoiner
	t.Cleanup(func() { lhns.NewJoiner = origJoiner })
	lhns.NewJoiner = func(string, time.Duration) (lhns.JoinerInterface, error) {
		return hostlessJoiner{}, nil
	}

	const bdfAddress = "0000:00:1e.0"
	// Does not exist on the test host; IsPotentialBlockDisk falls back to the /dev/ prefix.
	const missingDevice = "/dev/disk/by-id/longhorn-test-missing-device"

	for _, tc := range []struct {
		name string
		// Setting values; an empty string leaves the setting unset so the default applies.
		v2Enabled   string
		v2DataPath  string
		reservedPct string
		// v1DataPath overrides the temp directory used as default-data-path.
		v1DataPath func(t *testing.T, dir string) string
		wantBlock  string // path of the expected block-type disk; empty means none
		wantErr    string
	}{
		{name: "defaults create filesystem disk only"},
		{name: "V2 path ignored when V2 disabled", v2Enabled: "false", v2DataPath: bdfAddress},
		{name: "V2 enabled without V2 path", v2Enabled: "true"},
		{name: "V2 enabled with BDF address", v2Enabled: "true", v2DataPath: bdfAddress, wantBlock: bdfAddress},
		{name: "V2 enabled with device path", v2Enabled: "true", v2DataPath: missingDevice, wantBlock: missingDevice},
		{name: "V2 path that is a directory on the node is skipped", v2Enabled: "true",
			v2DataPath: "<dir>"},
		{name: "zero storage reserved percentage", reservedPct: "0"},
		{name: "filesystem disk creation failure", v2Enabled: "true", v2DataPath: bdfAddress,
			v1DataPath: func(t *testing.T, dir string) string {
				file := filepath.Join(dir, "not-a-directory")
				require.NoError(t, os.WriteFile(file, nil, 0644))
				return file
			},
			wantErr: "failed to create default disk for path"},
		{name: "invalid storage reserved percentage", reservedPct: "invalid", wantErr: "invalid syntax"},
		{name: "invalid V2 Data Engine setting", v2Enabled: "invalid", wantErr: "invalid syntax"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds, _, indexer := newSettingTestDataStore(t, nil)
			dir := t.TempDir()
			v1DataPath := dir
			if tc.v1DataPath != nil {
				v1DataPath = tc.v1DataPath(t, dir)
			}
			v2DataPath := tc.v2DataPath
			if v2DataPath == "<dir>" {
				v2DataPath = t.TempDir()
			}

			for name, value := range map[types.SettingName]string{
				types.SettingNameDefaultDataPath:                         v1DataPath,
				types.SettingNameV2DataEngine:                            tc.v2Enabled,
				types.SettingNameV2DefaultDataPath:                       v2DataPath,
				types.SettingNameStorageReservedPercentageForDefaultDisk: tc.reservedPct,
			} {
				if value == "" {
					continue
				}
				require.NoError(t, indexer.Add(&longhorn.Setting{
					ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: ds.namespace},
					Value:      value,
				}))
			}

			disks, err := ds.CreateDefaultDisks()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, disks)
				return
			}
			require.NoError(t, err)

			wantCount := 1
			if tc.wantBlock != "" {
				wantCount = 2
			}
			require.Len(t, disks, wantCount)

			var fsDisks, blockDisks []longhorn.DiskSpec
			for name, disk := range disks {
				require.True(t, strings.HasPrefix(name, types.DefaultDiskPrefix), "disk name %v", name)
				require.True(t, disk.AllowScheduling, "disk %v", name)
				require.Equal(t, []string{}, disk.Tags, "disk %v", name)
				switch disk.Type {
				case longhorn.DiskTypeFilesystem:
					fsDisks = append(fsDisks, disk)
				case longhorn.DiskTypeBlock:
					blockDisks = append(blockDisks, disk)
				default:
					t.Fatalf("unexpected disk type %v for disk %v", disk.Type, name)
				}
			}

			// The V1 Data Engine is always enabled, so a filesystem disk always exists.
			require.Len(t, fsDisks, 1)
			require.Equal(t, v1DataPath, fsDisks[0].Path)
			require.Equal(t, longhorn.DiskDriverNone, fsDisks[0].DiskDriver)
			require.DirExists(t, filepath.Join(v1DataPath, util.ReplicaDirectory))
			if tc.reservedPct == "0" {
				require.Zero(t, fsDisks[0].StorageReserved)
			} else {
				// Default storage-reserved-percentage-for-default-disk is 30.
				require.Positive(t, fsDisks[0].StorageReserved)
			}

			if tc.wantBlock == "" {
				require.Empty(t, blockDisks)
				return
			}
			require.Len(t, blockDisks, 1)
			require.Equal(t, tc.wantBlock, blockDisks[0].Path)
			require.Equal(t, longhorn.DiskDriverAuto, blockDisks[0].DiskDriver)
			require.Zero(t, blockDisks[0].StorageReserved)
		})
	}
}

func TestValidateSettingDefaultDataPaths(t *testing.T) {
	ds, _, _ := newSettingTestDataStore(t, nil)

	for _, tc := range []struct {
		name       string
		setting    types.SettingName
		value      string
		wantErr    bool
		wantErrMsg string
	}{
		{name: "V1 directory", setting: types.SettingNameDefaultDataPath, value: "/var/lib/longhorn/"},
		{name: "V1 empty", setting: types.SettingNameDefaultDataPath, value: "",
			wantErr: true, wantErrMsg: "shouldn't be empty"},
		{name: "V1 device path", setting: types.SettingNameDefaultDataPath, value: "/dev/sdb",
			wantErr: true, wantErrMsg: "block devices are not supported"},
		{name: "V1 BDF address", setting: types.SettingNameDefaultDataPath, value: "0000:00:1e.0",
			wantErr: true, wantErrMsg: "block devices are not supported"},
		{name: "V2 empty", setting: types.SettingNameV2DefaultDataPath, value: ""},
		{name: "V2 device path", setting: types.SettingNameV2DefaultDataPath, value: "/dev/sdb"},
		{name: "V2 BDF address", setting: types.SettingNameV2DefaultDataPath, value: "0000:00:1e.0"},
		{name: "V2 directory", setting: types.SettingNameV2DefaultDataPath, value: "/mnt/disk",
			wantErr: true, wantErrMsg: "must be empty, a block device path under /dev/, or a PCI BDF address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ds.ValidateSetting(string(tc.setting), tc.value)
			if tc.wantErr {
				require.ErrorContains(t, err, tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
