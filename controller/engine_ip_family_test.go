package controller

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"

	commonnet "github.com/longhorn/go-common-libs/net"
	iscsidevtypes "github.com/longhorn/go-iscsi-helper/types"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestEnginePreferredIPFamilyUsesSetting(t *testing.T) {
	for _, tc := range []struct {
		setting string
		want    commonnet.IPFamily
	}{
		{"", commonnet.IPFamilyUnspecified},
		{types.DataEngineIPFamilyDefault, commonnet.IPFamilyUnspecified},
		{types.DataEngineIPFamilyIPv4, commonnet.IPFamilyIPv4},
		{types.DataEngineIPFamilyIPv6, commonnet.IPFamilyIPv6},
	} {
		t.Run(tc.setting, func(t *testing.T) {
			imc := newInstanceManagerIPFamilyOptionController(t, tc.setting)
			family, err := getPreferredDataEngineIPFamily(imc.ds)
			require.NoError(t, err)
			require.Equal(t, tc.want, family)
		})
	}
}

func TestEngineDataEndpointUsesPodNetwork(t *testing.T) {
	for _, tc := range []struct {
		name      string
		family    string
		primary   string
		podIPs    []corev1.PodIP
		engineIP  string
		storageIP string
		omitPod   bool
		wantIP    string
		wantError string
	}{
		{
			name: "default retains primary IPv4", family: types.DataEngineIPFamilyDefault,
			primary: "192.0.2.10", storageIP: "2001:db8:1::10", omitPod: true, wantIP: "192.0.2.10",
		},
		{
			name: "omitted retains primary IPv6", primary: "2001:db8::10",
			storageIP: "192.0.2.20", omitPod: true, wantIP: "2001:db8::10",
		},
		{
			name: "default preserves status IP exactly", primary: "192.0.2.10",
			engineIP: "2001:db8::10", storageIP: "192.0.2.20", wantIP: "2001:db8::10",
		},
		{
			name: "explicit IPv6 selects secondary pod address", family: types.DataEngineIPFamilyIPv6,
			primary: "192.0.2.10", podIPs: []corev1.PodIP{{IP: "192.0.2.10"}, {IP: "2001:db8::10"}},
			wantIP: "2001:db8::10",
		},
		{
			name: "explicit IPv4 selects secondary pod address", family: types.DataEngineIPFamilyIPv4,
			primary: "2001:db8::10", podIPs: []corev1.PodIP{{IP: "2001:db8::10"}, {IP: "192.0.2.10"}},
			wantIP: "192.0.2.10",
		},
		{
			name: "explicit IPv6 ignores matching storage IP", family: types.DataEngineIPFamilyIPv6,
			primary: "192.0.2.10", podIPs: []corev1.PodIP{{IP: "192.0.2.10"}, {IP: "2001:db8::10"}},
			storageIP: "2001:db8:1::10", wantIP: "2001:db8::10",
		},
		{
			name: "explicit IPv4 ignores matching storage IP", family: types.DataEngineIPFamilyIPv4,
			primary: "2001:db8::10", podIPs: []corev1.PodIP{{IP: "2001:db8::10"}, {IP: "192.0.2.10"}},
			storageIP: "192.0.2.20", wantIP: "192.0.2.10",
		},
		{
			name: "explicit IPv6 uses matching pod without PodIPs", family: types.DataEngineIPFamilyIPv6,
			primary: "2001:db8::10", storageIP: "192.0.2.20", wantIP: "2001:db8::10",
		},
		{
			name: "missing pod IPv6 never uses primary or matching storage", family: types.DataEngineIPFamilyIPv6,
			primary: "192.0.2.10", podIPs: []corev1.PodIP{{IP: "192.0.2.10"}},
			storageIP: "2001:db8:1::10", wantError: "cannot provide an address in family ipv6",
		},
		{
			name: "missing pod IPv4 never uses primary or matching storage", family: types.DataEngineIPFamilyIPv4,
			primary: "2001:db8::10", podIPs: []corev1.PodIP{{IP: "2001:db8::10"}},
			storageIP: "192.0.2.20", wantError: "cannot provide an address in family ipv4",
		},
		{
			name: "missing IM pod never uses status IP", family: types.DataEngineIPFamilyIPv6,
			primary: "2001:db8::10", storageIP: "2001:db8:1::10", omitPod: true,
			wantError: "pod is nil, cannot find container instance-manager",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			imc := newInstanceManagerIPFamilyOptionController(t, tc.family)
			require.NoError(t, imc.ds.SettingInformer.GetStore().Add(newSetting(string(types.SettingNameStorageNetwork), "storage-net")))
			pod := newInstanceManagerIPFamilyOptionPod("test-im", tc.family)
			pod.Status.PodIP = tc.primary
			pod.Status.PodIPs = tc.podIPs
			pod.Annotations = map[string]string{
				string(types.CNIAnnotationNetworkStatus): `[{"name":"storage-net","interface":"lhnet1","ips":["` + tc.storageIP + `"]}]`,
			}
			if !tc.omitPod {
				require.NoError(t, imc.ds.PodInformer.GetStore().Add(pod))
			}
			engine := &longhorn.Engine{}
			engine.Name = "test-engine"
			engine.Status.InstanceManagerName = pod.Name
			engine.Status.IP = tc.primary
			if tc.engineIP != "" {
				engine.Status.IP = tc.engineIP
			}
			engine.Status.StorageIP = tc.storageIP
			monitor := &EngineMonitor{ds: imc.ds}
			endpoint, err := monitor.getEngineDataEndpoint(engine, iscsidevtypes.FrontendTGTISCSI, "iqn.test")
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.ErrorContains(t, err, "engine test-engine")
				require.Empty(t, endpoint)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "iscsi://"+net.JoinHostPort(tc.wantIP, "3260")+"/iqn.test/1", endpoint)
		})
	}
}

func TestEngineDataEndpointPreservesProcessEndpoints(t *testing.T) {
	// No datastore: non-iSCSI frontends must not resolve settings or pod IPs.
	monitor := &EngineMonitor{}
	engine := &longhorn.Engine{}
	engine.Status.StorageIP = "2001:db8:1::10"
	for _, tc := range []struct {
		frontend string
		endpoint string
	}{
		{iscsidevtypes.FrontendTGTBlockDev, "/dev/longhorn/test-volume"},
		{"spdk-tcp-blockdev", "/dev/longhorn/test-volume"},
		{"spdk-tcp-nvmf", "nvmf://2001:db8:1::10:4420/nqn.test"},
		{"ublk", "/dev/ublkb0"},
		{"", ""},
	} {
		t.Run(tc.frontend, func(t *testing.T) {
			endpoint, err := monitor.getEngineDataEndpoint(engine, tc.frontend, tc.endpoint)
			require.NoError(t, err)
			require.Equal(t, tc.endpoint, endpoint)
		})
	}
}
