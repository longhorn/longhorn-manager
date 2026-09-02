package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/longhorn/longhorn-manager/types"
)

func TestDataEngineIPFamilyEndpointSelection(t *testing.T) {
	tests := []struct {
		name      string
		family    string
		podIPs    []string
		primaryIP string
		want      string
		wantErr   bool
	}{
		{
			name:      "ipv4 selects ipv4 endpoint",
			family:    types.DataEngineIPFamilyIPv4,
			podIPs:    []string{"2001:db8::10", "192.0.2.10"},
			primaryIP: "2001:db8::10",
			want:      "192.0.2.10",
		},
		{
			name:      "ipv6 selects ipv6 endpoint",
			family:    types.DataEngineIPFamilyIPv6,
			podIPs:    []string{"192.0.2.10", "2001:db8::10"},
			primaryIP: "192.0.2.10",
			want:      "2001:db8::10",
		},
		{
			name:      "unavailable family clears endpoint",
			family:    types.DataEngineIPFamilyIPv6,
			podIPs:    []string{"192.0.2.10"},
			primaryIP: "192.0.2.10",
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newDataEngineTestPod(dataEngineFamilyPtr(tc.family), nil, tc.podIPs, tc.primaryIP)
			got, err := newDataEngineTestDataStore(t, "").GetDataEngineIPFromPodForContainer(pod, "instance-manager")
			if tc.wantErr {
				require.Error(t, err)
				require.Empty(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDataEngineIPFamilyEndpointRejectsInvalidAuthoritativeSelection(t *testing.T) {
	pod := newDataEngineTestPod(dataEngineFamilyPtr("ipv3"), nil, []string{"192.0.2.10"}, "192.0.2.10")

	got, err := newDataEngineTestDataStore(t, "").GetDataEngineIPFromPodForContainer(pod, "instance-manager")
	require.Error(t, err)
	require.Empty(t, got)
	require.Contains(t, err.Error(), "invalid data engine IP family arguments")
}

func TestDataEngineIPFamilyClassifierRejectsMalformedAddress(t *testing.T) {
	require.False(t, isDataEngineIPFamily("not-an-ip", types.DataEngineIPFamilyIPv4))
	require.False(t, isDataEngineIPFamily("192.0.2.10", types.DataEngineIPFamilyIPv6))
	require.True(t, isDataEngineIPFamily("192.0.2.10", types.DataEngineIPFamilyIPv4))
	require.True(t, isDataEngineIPFamily("2001:db8::10", types.DataEngineIPFamilyIPv6))
}

func TestGetDataEngineIPFromPodForIPFamilyUsesAppliedFamily(t *testing.T) {
	pod := newDataEngineTestPod(nil, nil, []string{"2001:db8::10", "192.0.2.10"}, "192.0.2.10")
	ds := newDataEngineTestDataStore(t, "")

	got, err := ds.GetDataEngineIPFromPodForIPFamily(pod, types.DataEngineIPFamilyIPv6)
	require.NoError(t, err)
	require.Equal(t, "2001:db8::10", got)

	got, err = ds.GetDataEngineIPFromPodForIPFamily(pod, types.DataEngineIPFamilyDefault)
	require.NoError(t, err)
	require.Equal(t, "192.0.2.10", got)

	got, err = ds.GetDataEngineIPFromPodForIPFamily(pod, "")
	require.NoError(t, err)
	require.Equal(t, "192.0.2.10", got)

}
func TestGetDataEngineIPFromPodForIPFamilyClearsUnavailableEndpoint(t *testing.T) {
	pod := newDataEngineTestPod(nil, nil, []string{"192.0.2.10"}, "192.0.2.10")

	got, err := newDataEngineTestDataStore(t, "").GetDataEngineIPFromPodForIPFamily(
		pod, types.DataEngineIPFamilyIPv6)
	require.Error(t, err)
	require.Empty(t, got)
	require.Contains(t, err.Error(), "cannot provide an address in family ipv6")
}

func TestGetDataEngineIPFromPodForIPFamilyRejectsUnknownFamily(t *testing.T) {
	pod := newDataEngineTestPod(nil, nil, []string{"192.0.2.10"}, "192.0.2.10")

	got, err := newDataEngineTestDataStore(t, "").GetDataEngineIPFromPodForIPFamily(pod, "ipv3")
	require.Error(t, err)
	require.Empty(t, got)
	require.Contains(t, err.Error(), "invalid data engine IP family")
}

func TestGetDataEngineIPFromPodByCNISettingForIPFamilyIsStrict(t *testing.T) {
	const storageNetwork = "longhorn-system/dual-stack"
	pod := newDataEngineTestPod(nil, nil, nil, "192.0.2.10")
	pod.Annotations = map[string]string{
		string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
			Name: storageNetwork,
			IPs:  []string{"192.0.2.10"},
		}),
	}
	ds := newDataEngineTestDataStore(t, storageNetwork)

	got, err := ds.GetDataEngineIPFromPodByCNISettingForIPFamily(
		pod, types.SettingNameStorageNetwork, types.DataEngineIPFamilyIPv4)
	require.NoError(t, err)
	require.Equal(t, "192.0.2.10", got)

	got, err = ds.GetDataEngineIPFromPodByCNISettingForIPFamily(
		pod, types.SettingNameStorageNetwork, types.DataEngineIPFamilyDefault)
	require.NoError(t, err)
	require.Equal(t, "192.0.2.10", got)

	got, err = ds.GetDataEngineIPFromPodByCNISettingForIPFamily(
		pod, types.SettingNameStorageNetwork, types.DataEngineIPFamilyIPv6)
	require.Error(t, err)
	require.Empty(t, got)
	require.Contains(t, err.Error(), "cannot provide an address in family ipv6")
}
