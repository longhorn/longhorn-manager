package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestPrimaryDataEngineIPUsesPodPrimaryAddress(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): `[{"name":"pod-net","interface":"eth0","ips":["10.0.0.2"]},{"name":"storage-net","interface":"lhnet1","ips":["2001:db8::20","2001:db8::21"]}]`,
			},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.2"},
	}
	require.Equal(t, "10.0.0.2", getPrimaryDataEngineIP(pod))

	pod.Annotations = nil
	require.Equal(t, "10.0.0.2", getPrimaryDataEngineIP(pod))
}

func TestDataEngineIPFamilyRequiresUsableAddress(t *testing.T) {
	require.True(t, isDataEngineIPFamily("192.0.2.10", types.DataEngineIPFamilyIPv4))
	require.True(t, isDataEngineIPFamily("2001:db8::10", types.DataEngineIPFamilyIPv6))
	require.False(t, isDataEngineIPFamily("127.0.0.1", types.DataEngineIPFamilyIPv4))
	require.False(t, isDataEngineIPFamily("fe80::1", types.DataEngineIPFamilyIPv6))
	require.False(t, isDataEngineIPFamily("2001:db8::10", types.DataEngineIPFamilyIPv4))
}

func TestGetDataEngineIPFromPodByCNISettingRejectsInvalidFamilyArguments(t *testing.T) {
	const namespace = "longhorn-system"
	kubeClient := fake.NewSimpleClientset()
	lhClient := lhfake.NewClientset()
	factories := util.NewInformerFactories(namespace, kubeClient, lhClient, 0)
	ds := NewDataStoreForGlobal(namespace, lhClient, kubeClient, apiextensionsfake.NewSimpleClientset(), factories)
	require.NoError(t, ds.SettingInformer.GetStore().Add(&longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{Name: string(types.SettingNameStorageNetwork), Namespace: namespace},
		Value:      types.CniNetworkNone,
	}))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "instance-manager", Namespace: namespace},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "instance-manager"}}},
		Status:     corev1.PodStatus{PodIP: "192.0.2.10"},
	}
	address, err := ds.GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
	require.NoError(t, err)
	require.Equal(t, pod.Status.PodIP, address)
	for _, args := range [][]string{
		{"--ip-family=invalid"},
		{"--ip-family", "ipv4", "--ip-family", "ipv6"},
		{"--ip-family"},
	} {
		pod.Spec.Containers[0].Args = args
		address, err = ds.GetDataEngineIPFromPodByCNISetting(pod, types.SettingNameStorageNetwork)
		var invalidState *types.ErrorInvalidState
		require.ErrorAs(t, err, &invalidState, "args: %v", args)
		require.Empty(t, address)
	}
}
