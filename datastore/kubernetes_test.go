package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhlisters "github.com/longhorn/longhorn-manager/k8s/pkg/client/listers/longhorn/v1beta2"
)

func TestNewPVCManifestForVolume(t *testing.T) {
	tests := map[string]struct {
		volume             *longhorn.Volume
		expectedAccessMode corev1.PersistentVolumeAccessMode
	}{
		"read write once": {
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:       1024 * 1024 * 1024, // 1Gi
					AccessMode: longhorn.AccessModeReadWriteOnce,
				},
			},
			expectedAccessMode: corev1.ReadWriteOnce,
		},
		"read write many": {
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:       1024 * 1024 * 1024, // 1Gi
					AccessMode: longhorn.AccessModeReadWriteMany,
				},
			},
			expectedAccessMode: corev1.ReadWriteMany,
		},
		"read write once pod": {
			volume: &longhorn.Volume{
				Spec: longhorn.VolumeSpec{
					Size:       1024 * 1024 * 1024, // 1Gi
					AccessMode: longhorn.AccessModeReadWriteOncePod,
				},
			},
			expectedAccessMode: corev1.ReadWriteOncePod,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pvc := NewPVCManifestForVolume(tc.volume, "pv-name", "default", "pvc-name", "longhorn")
			require.NotNil(t, pvc)
			assert.Equal(t, []corev1.PersistentVolumeAccessMode{tc.expectedAccessMode}, pvc.Spec.AccessModes)
		})
	}
}

func TestNewPVManifestForVolumeAttributesAndAccessModes(t *testing.T) {
	newVolume := func(mode longhorn.AccessMode, migratable, encrypted bool, replicas, srt int, diskSel, nodeSel []string) *longhorn.Volume {
		return &longhorn.Volume{
			Spec: longhorn.VolumeSpec{
				Size:                2 * 1024 * 1024 * 1024, // 2Gi
				AccessMode:          mode,
				Migratable:          migratable,
				Encrypted:           encrypted,
				NumberOfReplicas:    replicas,
				StaleReplicaTimeout: srt,
				DiskSelector:        diskSel,
				NodeSelector:        nodeSel,
			},
		}
	}

	t.Run("rwop volume manifest attributes", func(t *testing.T) {
		v := newVolume(longhorn.AccessModeReadWriteOncePod, false, true, 3, 2880, []string{"ssd"}, []string{"fast"})
		pv := NewPVManifestForVolume(v, "pv-rwop", "longhorn", "ext4")
		require.NotNil(t, pv)
		assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}, pv.Spec.AccessModes)
		attrs := pv.Spec.CSI.VolumeAttributes
		require.NotNil(t, attrs)
		assert.Equal(t, "ssd", attrs["diskSelector"])
		assert.Equal(t, "fast", attrs["nodeSelector"])
		assert.Equal(t, "3", attrs["numberOfReplicas"])
		assert.Equal(t, "2880", attrs["staleReplicaTimeout"])
		assert.Equal(t, "true", attrs["encrypted"])
		_, hasMigratable := attrs["migratable"]
		assert.False(t, hasMigratable)
	})

	t.Run("rwx volume manifest attributes", func(t *testing.T) {
		v := newVolume(longhorn.AccessModeReadWriteMany, true, false, 2, 1440, []string{"nvme", "hot"}, []string{"zone-a"})
		pv := NewPVManifestForVolume(v, "pv-rwx", "longhorn", "ext4")
		require.NotNil(t, pv)
		assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, pv.Spec.AccessModes)
		attrs := pv.Spec.CSI.VolumeAttributes
		require.NotNil(t, attrs)
		assert.Equal(t, "nvme,hot", attrs["diskSelector"])
		assert.Equal(t, "zone-a", attrs["nodeSelector"])
		assert.Equal(t, "2", attrs["numberOfReplicas"])
		assert.Equal(t, "1440", attrs["staleReplicaTimeout"])
		assert.Equal(t, "true", attrs["migratable"])
		_, hasEncrypted := attrs["encrypted"]
		assert.False(t, hasEncrypted)
	})
}

func TestLeaseInformerIsNamespaceScoped(t *testing.T) {
	const longhornNamespace = "longhorn-system"

	shareManagerLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "test-volume", Namespace: longhornNamespace},
	}
	// kubelet renews one of these per node every 10 seconds, so caching them
	// costs watch traffic for objects no Lease read path ever asks for.
	kubeNodeLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node", Namespace: "kube-node-lease"},
	}

	kubeClient := fake.NewSimpleClientset(shareManagerLease, kubeNodeLease) // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                                 // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset()              // nolint: staticcheck

	informerFactories := util.NewInformerFactories(longhornNamespace, kubeClient, lhClient, 0)
	ds := NewDataStore(longhornNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactories.Start(stopCh)
	require.True(t, cache.WaitForCacheSync(stopCh, ds.cacheSyncs...))

	// A cluster-scoped List goes to the whole cache, not just s.namespace, so it
	// reports what the informer actually watches.
	cached, err := ds.leaseLister.List(labels.Everything())
	require.NoError(t, err)
	require.Len(t, cached, 1)
	assert.Equal(t, longhornNamespace, cached[0].Namespace)
	assert.Equal(t, shareManagerLease.Name, cached[0].Name)
}

const testDataEngineNamespace = "longhorn-system"

func newDataEngineTestDataStore(t *testing.T, storageNetwork string) *DataStore {
	t.Helper()

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if storageNetwork != "" {
		setting := &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(types.SettingNameStorageNetwork),
				Namespace: testDataEngineNamespace,
			},
			Value: storageNetwork,
		}
		require.NoError(t, indexer.Add(setting))
	}

	return &DataStore{
		namespace:     testDataEngineNamespace,
		settingLister: lhlisters.NewSettingLister(indexer),
	}
}

func newDataEngineTestPod(instanceManagerFamily, sidecarFamily *string, podIPs []string, podIP string) *corev1.Pod {
	containers := []corev1.Container{{Name: "instance-manager"}}
	if instanceManagerFamily != nil {
		containers[0].Args = []string{"--ip-family", *instanceManagerFamily}
	}
	if sidecarFamily != nil {
		containers = append(containers, corev1.Container{
			Name: "sidecar",
			Args: []string{"--ip-family", *sidecarFamily},
		})
	}

	statusPodIPs := make([]corev1.PodIP, 0, len(podIPs))
	for _, ip := range podIPs {
		statusPodIPs = append(statusPodIPs, corev1.PodIP{IP: ip})
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "instance-manager-a",
			Namespace: testDataEngineNamespace,
		},
		Spec: corev1.PodSpec{Containers: containers},
		Status: corev1.PodStatus{
			PodIP:  podIP,
			PodIPs: statusPodIPs,
		},
	}
}

func dataEngineFamilyPtr(family string) *string {
	return &family
}

func dataEngineCNIStatus(t *testing.T, networks ...types.CniNetwork) string {
	t.Helper()
	status, err := json.Marshal(networks)
	require.NoError(t, err)
	return string(status)
}

func requireDataEngineInvalidState(t *testing.T, err error, network, family string, pod *corev1.Pod) {
	t.Helper()

	require.Error(t, err)
	var invalidState *types.ErrorInvalidState
	require.True(t, errors.As(err, &invalidState))
	assert.Equal(t,
		fmt.Sprintf("storage network %s cannot provide an address in family %s for pod %s/%s",
			network, family, pod.Namespace, pod.Name),
		invalidState.Reason)
}

func TestGetDataEngineIPFromPod(t *testing.T) {
	tests := []struct {
		name            string
		instanceManager *string
		podIPs          []string
		podIP           string
		expected        string
	}{
		{
			name:            "ipv4 selects first requested family from pod ips",
			instanceManager: dataEngineFamilyPtr(types.DataEngineIPFamilyIPv4),
			podIPs:          []string{"2001:db8::10", "192.0.2.10"},
			podIP:           "192.0.2.10",
			expected:        "192.0.2.10",
		},
		{
			name:            "ipv6 selects first requested family from pod ips",
			instanceManager: dataEngineFamilyPtr(types.DataEngineIPFamilyIPv6),
			podIPs:          []string{"192.0.2.10", "2001:db8::10"},
			podIP:           "192.0.2.10",
			expected:        "2001:db8::10",
		},
		{
			name:            "ipv4 selects first ipv4 when pod ips are ipv6 first",
			instanceManager: dataEngineFamilyPtr(types.DataEngineIPFamilyIPv4),
			podIPs:          []string{"2001:db8::20", "2001:db8::10", "192.0.2.20", "192.0.2.10"},
			podIP:           "2001:db8::20",
			expected:        "192.0.2.20",
		},
		{
			name:            "ipv6 selects first ipv6 when pod ips are ipv4 first",
			instanceManager: dataEngineFamilyPtr(types.DataEngineIPFamilyIPv6),
			podIPs:          []string{"192.0.2.20", "192.0.2.10", "2001:db8::20", "2001:db8::10"},
			podIP:           "192.0.2.20",
			expected:        "2001:db8::20",
		},
		{
			name:            "requested family unavailable falls back to pod ip",
			instanceManager: dataEngineFamilyPtr(types.DataEngineIPFamilyIPv6),
			podIPs:          []string{"192.0.2.10"},
			podIP:           "192.0.2.10",
			expected:        "192.0.2.10",
		},
		{
			name:     "missing authoritative args keeps legacy pod ip fallback",
			podIPs:   []string{"2001:db8::10", "192.0.2.10"},
			podIP:    "192.0.2.10",
			expected: "192.0.2.10",
		},
		{
			name:            "mixed-case authoritative args select requested family",
			instanceManager: dataEngineFamilyPtr("IPv6"),
			podIPs:          []string{"2001:db8::10", "192.0.2.10"},
			podIP:           "192.0.2.10",
			expected:        "2001:db8::10",
		},
		{
			name:            "invalid authoritative args keeps legacy pod ip fallback",
			instanceManager: dataEngineFamilyPtr(" ipv6"),
			podIPs:          []string{"2001:db8::10", "192.0.2.10"},
			podIP:           "192.0.2.10",
			expected:        "192.0.2.10",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			pod := newDataEngineTestPod(tc.instanceManager, nil, tc.podIPs, tc.podIP)
			ds := newDataEngineTestDataStore(t, "")
			assert.Equal(t, tc.expected, ds.GetDataEngineIPFromPod(pod))
		})
	}
}

func TestGetDataEngineIPFromPodByCNISetting(t *testing.T) {
	const storageNetwork = "longhorn-system/dual-stack"

	t.Run("current annotation preserves requested family order", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			family   string
			expected string
		}{
			{name: "ipv4", family: types.DataEngineIPFamilyIPv4, expected: "192.0.2.20"},
			{name: "ipv6", family: types.DataEngineIPFamilyIPv6, expected: "2001:db8::20"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				pod := newDataEngineTestPod(dataEngineFamilyPtr(tc.family), nil, nil, "10.0.0.1")
				pod.Annotations = map[string]string{
					string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
						Name: storageNetwork,
						IPs:  []string{"2001:db8::20", "2001:db8::10", "192.0.2.20", "192.0.2.10"},
					}),
				}

				got, err := newDataEngineTestDataStore(t, storageNetwork).
					GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			})
		}
	})

	t.Run("current annotation takes precedence over deprecated annotation", func(t *testing.T) {
		pod := newDataEngineTestPod(dataEngineFamilyPtr(types.DataEngineIPFamilyIPv4), nil, nil, "10.0.0.1")
		pod.Annotations = map[string]string{
			string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"192.0.2.20"},
			}),
			string(types.CNIAnnotationNetworksStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"192.0.2.10"},
			}),
		}

		got, err := newDataEngineTestDataStore(t, storageNetwork).
			GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
		require.NoError(t, err)
		assert.Equal(t, "192.0.2.20", got)
	})

	t.Run("deprecated annotation is used when current annotation is absent", func(t *testing.T) {
		pod := newDataEngineTestPod(dataEngineFamilyPtr(types.DataEngineIPFamilyIPv6), nil, nil, "10.0.0.1")
		pod.Annotations = map[string]string{
			string(types.CNIAnnotationNetworksStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"2001:db8::20", "2001:db8::10"},
			}),
		}

		got, err := newDataEngineTestDataStore(t, storageNetwork).
			GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
		require.NoError(t, err)
		assert.Equal(t, "2001:db8::20", got)
	})

	strictFailureCases := []struct {
		name        string
		family      string
		annotations map[string]string
	}{
		{
			name:   "missing current and deprecated annotations",
			family: types.DataEngineIPFamilyIPv4,
		},
		{
			name:   "malformed current annotation does not use deprecated annotation",
			family: types.DataEngineIPFamilyIPv4,
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): "{",
				string(types.CNIAnnotationNetworksStatus): dataEngineCNIStatus(t, types.CniNetwork{
					Name: storageNetwork,
					IPs:  []string{"192.0.2.10"},
				}),
			},
		},
		{
			name:   "no matching network",
			family: types.DataEngineIPFamilyIPv4,
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
					Name: "longhorn-system/other",
					IPs:  []string{"192.0.2.10"},
				}),
			},
		},
		{
			name:   "matching network has empty ips",
			family: types.DataEngineIPFamilyIPv4,
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
					Name: storageNetwork,
					IPs:  []string{},
				}),
			},
		},
		{
			name:   "matching network has no requested family",
			family: types.DataEngineIPFamilyIPv6,
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
					Name: storageNetwork,
					IPs:  []string{"192.0.2.10"},
				}),
			},
		},
	}

	for _, tc := range strictFailureCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := newDataEngineTestPod(dataEngineFamilyPtr(tc.family), nil, nil, "10.0.0.1")
			pod.Annotations = tc.annotations

			got, err := newDataEngineTestDataStore(t, storageNetwork).
				GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
			assert.Empty(t, got)
			requireDataEngineInvalidState(t, err, storageNetwork, tc.family, pod)
		})
	}

	t.Run("invalid authoritative args delegates to generic selector", func(t *testing.T) {
		pod := newDataEngineTestPod(dataEngineFamilyPtr(""), dataEngineFamilyPtr(types.DataEngineIPFamilyIPv6), nil, "10.0.0.1")
		pod.Annotations = map[string]string{
			string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"192.0.2.20", "192.0.2.10"},
			}),
		}

		got, err := newDataEngineTestDataStore(t, storageNetwork).
			GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
		require.NoError(t, err)
		assert.Equal(t, "192.0.2.10", got)
	})

	t.Run("missing authoritative args ignores a conflicting sidecar", func(t *testing.T) {
		pod := newDataEngineTestPod(nil, dataEngineFamilyPtr(types.DataEngineIPFamilyIPv6), nil, "10.0.0.1")
		pod.Spec.Containers = pod.Spec.Containers[1:]
		pod.Annotations = map[string]string{
			string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"192.0.2.20", "192.0.2.10"},
			}),
		}

		got, err := newDataEngineTestDataStore(t, storageNetwork).
			GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
		require.NoError(t, err)
		assert.Equal(t, "192.0.2.10", got)
	})
	t.Run("setting lookup failure is an ordinary error", func(t *testing.T) {
		pod := newDataEngineTestPod(dataEngineFamilyPtr(types.DataEngineIPFamilyIPv4), nil, nil, "10.0.0.1")

		got, err := newDataEngineTestDataStore(t, "").
			GetDataEngineIPFromPodByCNISetting(pod, types.SettingName("not-a-setting"))
		assert.Empty(t, got)
		require.Error(t, err)
		var invalidState *types.ErrorInvalidState
		assert.False(t, errors.As(err, &invalidState))
	})
}

func TestValidateDataEngineIPFamilyForStorageNetwork(t *testing.T) {
	const storageNetwork = "longhorn-system/ipv4-only"
	const family = types.DataEngineIPFamilyIPv6

	t.Run("valid requested family", func(t *testing.T) {
		const dualStackNetwork = "longhorn-system/dual-stack"
		pod := newDataEngineTestPod(nil, nil, nil, "10.0.0.1")
		pod.Annotations = map[string]string{
			string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: dualStackNetwork,
				IPs:  []string{"192.0.2.10", "2001:db8::10"},
			}),
		}

		err := newDataEngineTestDataStore(t, dualStackNetwork).
			ValidateDataEngineIPFamilyForStorageNetwork(pod, types.DataEngineIPFamilyIPv6)
		require.NoError(t, err)
	})

	strictFailureCases := []struct {
		name        string
		annotations map[string]string
	}{
		{
			name: "missing current and deprecated annotations",
		},
		{
			name: "malformed current annotation",
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): "{",
			},
		},
		{
			name: "unmatched network",
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
					Name: "longhorn-system/other",
					IPs:  []string{"2001:db8::10"},
				}),
			},
		},
		{
			name: "empty ips",
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
					Name: storageNetwork,
					IPs:  []string{},
				}),
			},
		},
		{
			name: "ipv4-only network requested as ipv6",
			annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
					Name: storageNetwork,
					IPs:  []string{"192.0.2.10"},
				}),
			},
		},
	}

	for _, tc := range strictFailureCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := newDataEngineTestPod(nil, nil, nil, "10.0.0.1")
			pod.Annotations = tc.annotations

			err := newDataEngineTestDataStore(t, storageNetwork).
				ValidateDataEngineIPFamilyForStorageNetwork(pod, family)
			requireDataEngineInvalidState(t, err, storageNetwork, family, pod)
		})
	}

	t.Run("deprecated annotation is accepted", func(t *testing.T) {
		pod := newDataEngineTestPod(nil, nil, nil, "10.0.0.1")
		pod.Annotations = map[string]string{
			string(types.CNIAnnotationNetworksStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"2001:db8::20", "2001:db8::10"},
			}),
		}

		err := newDataEngineTestDataStore(t, storageNetwork).
			ValidateDataEngineIPFamilyForStorageNetwork(pod, family)
		require.NoError(t, err)
	})

	t.Run("current annotation takes precedence", func(t *testing.T) {
		pod := newDataEngineTestPod(nil, nil, nil, "10.0.0.1")
		pod.Annotations = map[string]string{
			string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"2001:db8::10"},
			}),
			string(types.CNIAnnotationNetworksStatus): dataEngineCNIStatus(t, types.CniNetwork{
				Name: storageNetwork,
				IPs:  []string{"192.0.2.10"},
			}),
		}

		err := newDataEngineTestDataStore(t, storageNetwork).
			ValidateDataEngineIPFamilyForStorageNetwork(pod, family)
		require.NoError(t, err)
	})
}

const containerIPTestNamespace = "longhorn-system"

func newContainerIPTestPod(containerName string, command, args []string, podIP string, podIPs ...string) *corev1.Pod {
	statusPodIPs := make([]corev1.PodIP, 0, len(podIPs))
	for _, ip := range podIPs {
		statusPodIPs = append(statusPodIPs, corev1.PodIP{IP: ip})
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-ip-test",
			Namespace: containerIPTestNamespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    containerName,
				Command: command,
				Args:    args,
			}},
		},
		Status: corev1.PodStatus{
			PodIP:  podIP,
			PodIPs: statusPodIPs,
		},
	}
}

func addContainerIPTestSidecar(pod *corev1.Pod, command, args []string) {
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:    "sidecar",
		Command: command,
		Args:    args,
	})
}

func newContainerIPTestDataStore(t *testing.T, storageNetwork string) *DataStore {
	t.Helper()

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameStorageNetwork),
			Namespace: containerIPTestNamespace,
		},
		Value: storageNetwork,
	}
	require.NoError(t, indexer.Add(setting))

	return &DataStore{
		namespace:     containerIPTestNamespace,
		settingLister: lhlisters.NewSettingLister(indexer),
	}
}

func setContainerIPTestNetworkStatus(t *testing.T, pod *corev1.Pod, network string, ips ...string) {
	t.Helper()

	status, err := json.Marshal([]types.CniNetwork{{Name: network, IPs: ips}})
	require.NoError(t, err)
	pod.Annotations = map[string]string{
		string(types.CNIAnnotationNetworkStatus): string(status),
	}
}

func TestGetDataEngineIPFromPodForContainer(t *testing.T) {
	tests := []struct {
		name          string
		containerName string
		command       []string
		args          []string
		podIP         string
		podIPs        []string
		expected      string
	}{
		{
			name:          "backing image manager ipv4",
			containerName: "backing-image-manager",
			command:       []string{"--ip-family", types.DataEngineIPFamilyIPv4},
			podIP:         "2001:db8::20",
			podIPs:        []string{"2001:db8::20", "192.0.2.20"},
			expected:      "192.0.2.20",
		},
		{
			name:          "backing image manager ipv6",
			containerName: "backing-image-manager",
			command:       []string{"--ip-family=ipv6"},
			podIP:         "192.0.2.20",
			podIPs:        []string{"192.0.2.20", "2001:db8::20"},
			expected:      "2001:db8::20",
		},
		{
			name:          "backing image data source ipv4",
			containerName: "backing-image-data-source",
			command:       []string{"--ip-family=ipv4"},
			podIP:         "2001:db8::30",
			podIPs:        []string{"2001:db8::30", "192.0.2.30"},
			expected:      "192.0.2.30",
		},
		{
			name:          "backing image data source ipv6",
			containerName: "backing-image-data-source",
			command:       []string{"--ip-family", "ipv6"},
			podIP:         "192.0.2.30",
			podIPs:        []string{"192.0.2.30", "2001:db8::30"},
			expected:      "2001:db8::30",
		},
		{
			name:          "blank family uses primary pod ip",
			containerName: "backing-image-manager",
			podIP:         "192.0.2.40",
			podIPs:        []string{"2001:db8::40", "192.0.2.40"},
			expected:      "192.0.2.40",
		},
		{
			name:          "blank family uses IPv6 primary pod IP",
			containerName: "backing-image-manager",
			podIP:         "2001:db8::41",
			podIPs:        []string{"2001:db8::41", "192.0.2.41"},
			expected:      "2001:db8::41",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newContainerIPTestPod(tc.containerName, tc.command, tc.args, tc.podIP, tc.podIPs...)
			got, err := newContainerIPTestDataStore(t, "").GetDataEngineIPFromPodForContainer(pod, tc.containerName)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}

	t.Run("sidecar family is ignored", func(t *testing.T) {
		pod := newContainerIPTestPod("backing-image-manager", nil, nil, "192.0.2.50", "192.0.2.50", "2001:db8::50")
		addContainerIPTestSidecar(pod, []string{"--ip-family", "ipv6"}, nil)

		got, err := newContainerIPTestDataStore(t, "").GetDataEngineIPFromPodForContainer(pod, "backing-image-manager")
		require.NoError(t, err)
		assert.Equal(t, "192.0.2.50", got)
	})

	for _, tc := range []struct {
		name    string
		command []string
		args    []string
	}{
		{
			name:    "malformed family",
			command: []string{"--ip-family", "ipv3"},
		},
		{
			name:    "duplicate family across command and args",
			command: []string{"--ip-family=ipv4"},
			args:    []string{"--ip-family", "ipv6"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := newContainerIPTestPod("backing-image-manager", tc.command, tc.args, "192.0.2.60", "192.0.2.60")
			got, err := newContainerIPTestDataStore(t, "").GetDataEngineIPFromPodForContainer(pod, "backing-image-manager")
			assert.Empty(t, got)
			require.Error(t, err)
		})
	}

	t.Run("requested family absent is a strict error", func(t *testing.T) {
		pod := newContainerIPTestPod("backing-image-data-source", []string{"--ip-family", "ipv6"}, nil, "192.0.2.70", "192.0.2.70")
		got, err := (&DataStore{}).GetDataEngineIPFromPodForContainer(pod, "backing-image-data-source")
		assert.Empty(t, got)
		require.Error(t, err)
		var invalidState *types.ErrorInvalidState
		assert.True(t, errors.As(err, &invalidState))
	})
}

func TestGetDataEngineIPFromPodForContainerInstanceManagerWrapper(t *testing.T) {
	pod := newContainerIPTestPod("instance-manager", nil, []string{"--ip-family", "ipv6"}, "192.0.2.80", "192.0.2.80", "2001:db8::80")

	assert.Equal(t, "2001:db8::80", (&DataStore{}).GetDataEngineIPFromPod(pod))
}

func TestGetDataEngineIPFromPodByCNISettingForContainer(t *testing.T) {
	const storageNetwork = "longhorn-system/dual-stack"

	t.Run("named BIM selects requested family", func(t *testing.T) {
		pod := newContainerIPTestPod("backing-image-manager", []string{"--ip-family", "ipv6"}, nil, "192.0.2.90")
		setContainerIPTestNetworkStatus(t, pod, storageNetwork, "192.0.2.90", "2001:db8::90")

		got, err := newContainerIPTestDataStore(t, storageNetwork).
			GetDataEngineIPFromPodByCNISettingForContainer(pod, types.SettingNameStorageNetwork, "backing-image-manager")
		require.NoError(t, err)
		assert.Equal(t, "2001:db8::90", got)
	})

	t.Run("requested family absent is a strict error", func(t *testing.T) {
		pod := newContainerIPTestPod("backing-image-data-source", []string{"--ip-family", "ipv6"}, nil, "192.0.2.91")
		setContainerIPTestNetworkStatus(t, pod, storageNetwork, "192.0.2.91")

		got, err := newContainerIPTestDataStore(t, storageNetwork).
			GetDataEngineIPFromPodByCNISettingForContainer(pod, types.SettingNameStorageNetwork, "backing-image-data-source")
		assert.Empty(t, got)
		require.Error(t, err)
		var invalidState *types.ErrorInvalidState
		assert.True(t, errors.As(err, &invalidState))
	})
}

func TestGetDataEngineIPFromPodByCNISettingInstanceManagerWrapper(t *testing.T) {
	const storageNetwork = "longhorn-system/dual-stack"
	pod := newContainerIPTestPod("instance-manager", nil, []string{"--ip-family", "ipv4"}, "2001:db8::100")
	setContainerIPTestNetworkStatus(t, pod, storageNetwork, "2001:db8::100", "192.0.2.100")

	got, err := newContainerIPTestDataStore(t, storageNetwork).
		GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.100", got)
}
