package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestDataEngineAndBackingImageIPSelectionIsStrict(t *testing.T) {
	for _, containerName := range []string{"instance-manager", "backing-image-manager", "backing-image-data-source"} {
		for _, tc := range []struct {
			name       string
			family     string
			network    string
			podIPs     []corev1.PodIP
			storageIPs string
			wantIP     string
			wantError  string
		}{
			{name: "secondary pod IPv6", family: "ipv6", podIPs: []corev1.PodIP{{IP: "192.0.2.10"}, {IP: "2001:db8::10"}}, wantIP: "2001:db8::10"},
			{name: "missing pod IPv6", family: "ipv6", wantError: "cannot provide an address in family ipv6"},
			{name: "storage IPv6", family: "ipv6", network: "storage-net", storageIPs: `["192.0.2.20","2001:db8:1::20"]`, wantIP: "2001:db8:1::20"},
			{name: "storage IPv4", family: "ipv4", network: "storage-net", storageIPs: `["2001:db8:1::20","192.0.2.20"]`, wantIP: "192.0.2.20"},
			{name: "opposite storage with matching pod", family: "ipv6", network: "storage-net", storageIPs: `["192.0.2.20"]`, podIPs: []corev1.PodIP{{IP: "2001:db8::10"}}, wantError: "storage network storage-net cannot provide an address in family ipv6"},
			{name: "unusable storage", family: "ipv6", network: "storage-net", storageIPs: `["fe80::1","::1"]`, wantError: "storage network storage-net cannot provide an address in family ipv6"},
			{name: "default primary", wantIP: "192.0.2.10"},
		} {
			t.Run(containerName+"/"+tc.name, func(t *testing.T) {
				const namespace = "longhorn-system"
				kubeClient := fake.NewSimpleClientset()
				lhClient := lhfake.NewClientset()
				factories := util.NewInformerFactories(namespace, kubeClient, lhClient, 0)
				ds := NewDataStoreForGlobal(namespace, lhClient, kubeClient, apiextensionsfake.NewSimpleClientset(), factories)
				setting := &longhorn.Setting{Value: tc.network}
				setting.Name = string(types.SettingNameStorageNetwork)
				setting.Namespace = namespace
				require.NoError(t, ds.SettingInformer.GetStore().Add(setting))
				pod := &corev1.Pod{}
				pod.Name = "test-pod"
				pod.Namespace = namespace
				pod.Spec.Containers = []corev1.Container{{Name: containerName}}
				if tc.family != "" {
					pod.Spec.Containers[0].Args = []string{"--ip-family", tc.family}
				}
				pod.Status.PodIP = "192.0.2.10"
				pod.Status.PodIPs = tc.podIPs
				if tc.network != "" {
					pod.Annotations = map[string]string{string(types.CNIAnnotationNetworkStatus): `[{"name":"storage-net","interface":"lhnet1","ips":` + tc.storageIPs + `}]`}
				}
				address, err := ds.GetDataEngineIPFromPodByCNISettingForContainer(pod, types.SettingNameStorageNetwork, containerName)
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
					require.Empty(t, address)
					var invalidState *types.ErrorInvalidState
					require.ErrorAs(t, err, &invalidState)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.wantIP, address)
				}
			})
		}
	}
}
