package datastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
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
	ds := NewDataStoreForGlobal(longhornNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

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

func TestOutdatedManagerPods(t *testing.T) {
	daemonSet := &appsv1.DaemonSet{}
	daemonSet.Spec.Template.Spec.Containers = []corev1.Container{{Name: types.LonghornManagerContainerName, Image: "longhorn-manager:v2"}}

	pod := func(name, image string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: types.LonghornManagerContainerName, Image: image}}},
			Status:     corev1.PodStatus{Phase: phase},
		}
	}
	stray := pod("node-a", "longhorn-manager:v2", corev1.PodRunning)
	stray.Spec.Containers[0].Name = "sidecar"

	tests := map[string]struct {
		daemonSet *appsv1.DaemonSet
		pods      []*corev1.Pod
		expected  []string
	}{
		"pods on the DaemonSet image are current": {
			daemonSet: daemonSet,
			pods: []*corev1.Pod{
				pod("node-a", "longhorn-manager:v2", corev1.PodRunning),
				pod("node-b", "longhorn-manager:v2", corev1.PodPending),
			},
		},
		"pods on another image are named, sorted": {
			daemonSet: daemonSet,
			pods: []*corev1.Pod{
				pod("node-c", "longhorn-manager:v1", corev1.PodRunning),
				pod("node-b", "longhorn-manager:v2", corev1.PodRunning),
				pod("node-a", "longhorn-manager:v1", corev1.PodPending),
			},
			expected: []string{"node-a", "node-c"},
		},
		"exited pods on the old image do not count": {
			daemonSet: daemonSet,
			pods: []*corev1.Pod{
				pod("node-a", "longhorn-manager:v1", corev1.PodSucceeded),
				pod("node-b", "longhorn-manager:v1", corev1.PodFailed),
			},
		},
		"a pod without the manager container is outdated": {
			daemonSet: daemonSet,
			pods:      []*corev1.Pod{stray},
			expected:  []string{"node-a"},
		},
		"a DaemonSet without the manager container checks nothing": {
			daemonSet: &appsv1.DaemonSet{},
			pods: []*corev1.Pod{
				pod("node-a", "longhorn-manager:v1", corev1.PodRunning),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, outdatedManagerPods(tc.daemonSet, tc.pods))
		})
	}
}

func TestOutdatedManagerPodsMessage(t *testing.T) {
	assert.Equal(t, "", OutdatedManagerPodsMessage(nil))
	assert.Equal(t, "hooks: command is refused until the longhorn-manager rollout completes: pods node-a, node-c still run the previous image and would snapshot without running the hooks", OutdatedManagerPodsMessage([]string{"node-a", "node-c"}))
}
