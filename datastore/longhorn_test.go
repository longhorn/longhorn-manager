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

func TestValidateSettingDefaultControlPath(t *testing.T) {
	const testNamespace = "longhorn-system"

	baseSetting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameDefaultControlPath),
			Namespace: testNamespace,
		},
		Value: types.DefaultControlPath,
	}

	newNode := func(name string) *longhorn.Node {
		return &longhorn.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace,
			},
		}
	}

	tests := map[string]struct {
		existingObjects []runtime.Object
		newValue        string
		expectError     string
	}{
		"same path with trailing slash normalization is allowed": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "/var/lib/longhorn/",
		},
		"changing path before initialization is allowed": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "/control/longhorn",
		},
		"changing path after initialization is rejected": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy(), newNode("node-1")},
			newValue:        "/control/longhorn",
			expectError:     "cannot change default-control-path after Longhorn has been initialized",
		},
		"legacy cluster rejects control path change to customized data path": {
			existingObjects: []runtime.Object{
				&longhorn.Setting{
					ObjectMeta: metav1.ObjectMeta{
						Name:      string(types.SettingNameDefaultDataPath),
						Namespace: testNamespace,
					},
					Value: "/data/longhorn",
				},
				newNode("node-1"),
			},
			newValue:    "/data/longhorn",
			expectError: "cannot change default-control-path after Longhorn has been initialized",
		},
		"legacy cluster keeps historical default control path": {
			existingObjects: []runtime.Object{
				&longhorn.Setting{
					ObjectMeta: metav1.ObjectMeta{
						Name:      string(types.SettingNameDefaultDataPath),
						Namespace: testNamespace,
					},
					Value: "/data/longhorn",
				},
				newNode("node-1"),
			},
			newValue: types.DefaultControlPath,
		},
		"block device path is rejected": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "/dev/nvme0n1",
			expectError:     "the value of default-control-path is invalid",
		},
		"root path is rejected": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "/",
			expectError:     "the value of default-control-path is invalid",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset(tc.existingObjects...) // nolint: staticcheck
			kubeClient := fake.NewSimpleClientset()                      // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset()   // nolint: staticcheck
			informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
			ds := NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

			stopCh := make(chan struct{})
			defer close(stopCh)
			informerFactories.Start(stopCh)

			require.True(t, cache.WaitForCacheSync(stopCh,
				ds.SettingInformer.HasSynced,
				ds.NodeInformer.HasSynced,
			))

			err := ds.ValidateSetting(string(types.SettingNameDefaultControlPath), tc.newValue)
			if tc.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectError)
			}
		})
	}
}

func TestValidateSettingDefaultDataPathImmutability(t *testing.T) {
	const testNamespace = "longhorn-system"

	baseSetting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameDefaultDataPath),
			Namespace: testNamespace,
		},
		Value: "/var/lib/longhorn",
	}

	newNode := func(name string) *longhorn.Node {
		return &longhorn.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace,
			},
		}
	}

	tests := map[string]struct {
		existingObjects []runtime.Object
		newValue        string
		expectError     string
	}{
		"relative path is rejected": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "relative/path",
			expectError:     "the value of default-data-path is invalid",
		},
		"root path is rejected": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "/",
			expectError:     "the value of default-data-path is invalid",
		},
		"same path with trailing slash normalization is allowed": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "/var/lib/longhorn/",
		},
		"same path with whitespace normalization is allowed (even after initialization)": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy(), newNode("node-1")},
			newValue:        " /var/lib/longhorn/ ",
		},
		"changing path before initialization is allowed": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "/data/longhorn",
		},
		"bare pci identifier is rejected": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy()},
			newValue:        "0000:00:1e.0",
			expectError:     "the value of default-data-path is invalid",
		},
		"changing path after initialization is rejected": {
			existingObjects: []runtime.Object{baseSetting.DeepCopy(), newNode("node-1")},
			newValue:        "/data/longhorn",
			expectError:     "cannot change default-data-path after Longhorn has been initialized",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset(tc.existingObjects...) // nolint: staticcheck
			kubeClient := fake.NewSimpleClientset()                      // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset()   // nolint: staticcheck
			informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
			ds := NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

			stopCh := make(chan struct{})
			defer close(stopCh)
			informerFactories.Start(stopCh)

			require.True(t, cache.WaitForCacheSync(stopCh,
				ds.SettingInformer.HasSynced,
				ds.NodeInformer.HasSynced,
			))

			err := ds.ValidateSetting(string(types.SettingNameDefaultDataPath), tc.newValue)
			if tc.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectError)
			}
		})
	}
}

func TestUpdateCustomizedSettingsForInstallTimePathsOnUpgrade(t *testing.T) {
	const (
		testNamespace            = "longhorn-system"
		configMapResourceVersion = "3113"
	)

	newNode := func(name string) *longhorn.Node {
		return &longhorn.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace,
			},
		}
	}

	newSetting := func(name types.SettingName, value string) *longhorn.Setting {
		return &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(name),
				Namespace: testNamespace,
			},
			Value: value,
		}
	}

	newDefaultSettingConfigMap := func(dataPath, controlPath string) *corev1.ConfigMap {
		data := ""
		if dataPath != "" {
			data += fmt.Sprintf("default-data-path: %q\n", dataPath)
		}
		if controlPath != "" {
			data += fmt.Sprintf("default-control-path: %q\n", controlPath)
		}
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            types.DefaultDefaultSettingConfigMapName,
				Namespace:       testNamespace,
				ResourceVersion: configMapResourceVersion,
			},
			Data: map[string]string{
				types.DefaultSettingYAMLFileName: data,
			},
		}
	}

	tests := map[string]struct {
		existingObjects             []runtime.Object
		defaultSettingConfigMap     *corev1.ConfigMap
		expectedDataPathValue       string
		expectedControlPathValue    string
		expectControlPathToBeCreate bool
	}{
		"upgrade from historical default path succeeds": {
			existingObjects: []runtime.Object{
				newNode("node-1"),
				newSetting(types.SettingNameDefaultDataPath, types.DefaultDataPath),
			},
			defaultSettingConfigMap:     newDefaultSettingConfigMap(types.DefaultDataPath, types.DefaultControlPath),
			expectedDataPathValue:       types.DefaultDataPath,
			expectedControlPathValue:    types.DefaultControlPath,
			expectControlPathToBeCreate: true,
		},
		"upgrade from customized data path bootstraps historical control path": {
			existingObjects: []runtime.Object{
				newNode("node-1"),
				newSetting(types.SettingNameDefaultDataPath, "/data/longhorn"),
			},
			defaultSettingConfigMap:     newDefaultSettingConfigMap("/data/longhorn", types.DefaultControlPath),
			expectedDataPathValue:       "/data/longhorn",
			expectedControlPathValue:    types.DefaultControlPath,
			expectControlPathToBeCreate: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset(tc.existingObjects...)      // nolint: staticcheck
			kubeClient := fake.NewSimpleClientset(tc.defaultSettingConfigMap) // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset()        // nolint: staticcheck
			informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
			ds := NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

			stopCh := make(chan struct{})
			defer close(stopCh)
			informerFactories.Start(stopCh)

			require.True(t, cache.WaitForCacheSync(stopCh,
				ds.SettingInformer.HasSynced,
				ds.NodeInformer.HasSynced,
				ds.ConfigMapInformer.HasSynced,
			))

			err := ds.UpdateCustomizedSettings(nil)
			require.NoError(t, err)

			dataSetting, err := ds.lhClient.LonghornV1beta2().Settings(testNamespace).Get(context.TODO(), string(types.SettingNameDefaultDataPath), metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.expectedDataPathValue, dataSetting.Value)

			controlSetting, err := ds.lhClient.LonghornV1beta2().Settings(testNamespace).Get(context.TODO(), string(types.SettingNameDefaultControlPath), metav1.GetOptions{})
			if tc.expectControlPathToBeCreate {
				require.NoError(t, err)
				require.Equal(t, tc.expectedControlPathValue, controlSetting.Value)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestValidateCustomizedDefaultDataAndControlPathSettings(t *testing.T) {
	const (
		testNamespace            = "longhorn-system"
		configMapResourceVersion = "3113"
	)

	newNode := func(name string) *longhorn.Node {
		return &longhorn.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace,
			},
		}
	}

	newSetting := func(name types.SettingName, value string) *longhorn.Setting {
		return &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(name),
				Namespace: testNamespace,
			},
			Value: value,
		}
	}

	newDefaultSettingConfigMap := func(dataPath, controlPath string) *corev1.ConfigMap {
		data := ""
		if dataPath != "" {
			data += fmt.Sprintf("default-data-path: %q\n", dataPath)
		}
		if controlPath != "" {
			data += fmt.Sprintf("default-control-path: %q\n", controlPath)
		}
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            types.DefaultDefaultSettingConfigMapName,
				Namespace:       testNamespace,
				ResourceVersion: configMapResourceVersion,
			},
			Data: map[string]string{
				types.DefaultSettingYAMLFileName: data,
			},
		}
	}

	tests := map[string]struct {
		existingObjects         []runtime.Object
		defaultSettingConfigMap *corev1.ConfigMap
		expectError             string
	}{
		"customized paths are allowed when unchanged": {
			existingObjects: []runtime.Object{
				newNode("node-1"),
				newSetting(types.SettingNameDefaultDataPath, "/data/longhorn"),
				newSetting(types.SettingNameDefaultControlPath, types.DefaultControlPath),
			},
			defaultSettingConfigMap: newDefaultSettingConfigMap("/data/longhorn", types.DefaultControlPath),
		},
		"rejects default data path change after initialization": {
			existingObjects: []runtime.Object{
				newNode("node-1"),
				newSetting(types.SettingNameDefaultDataPath, "/data/longhorn"),
			},
			defaultSettingConfigMap: newDefaultSettingConfigMap(types.DefaultDataPath, types.DefaultControlPath),
			expectError:             "cannot change default-data-path after Longhorn has been initialized",
		},
		"rejects default control path change after initialization": {
			existingObjects: []runtime.Object{
				newNode("node-1"),
				newSetting(types.SettingNameDefaultDataPath, types.DefaultDataPath),
				newSetting(types.SettingNameDefaultControlPath, types.DefaultControlPath),
			},
			defaultSettingConfigMap: newDefaultSettingConfigMap(types.DefaultDataPath, "/control/longhorn"),
			expectError:             "cannot change default-control-path after Longhorn has been initialized",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset(tc.existingObjects...)      // nolint: staticcheck
			kubeClient := fake.NewSimpleClientset(tc.defaultSettingConfigMap) // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset()        // nolint: staticcheck
			informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
			ds := NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

			stopCh := make(chan struct{})
			defer close(stopCh)
			informerFactories.Start(stopCh)

			require.True(t, cache.WaitForCacheSync(stopCh,
				ds.SettingInformer.HasSynced,
				ds.NodeInformer.HasSynced,
				ds.ConfigMapInformer.HasSynced,
			))

			err := ds.ValidateCustomizedDefaultDataAndControlPathSettings()
			if tc.expectError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectError)
			}
		})
	}
}
