package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/longhorn/longhorn-manager/types"
)

func TestCommonIPFamilyRejectsUnusableCandidates(t *testing.T) {
	for _, candidate := range []string{
		"127.0.0.1",
		"169.254.1.1",
		"224.0.0.1",
		"0.0.0.0",
		"not-an-ip",
		"2001:db8::10",
	} {
		require.False(t, isDataEngineIPFamily(candidate, types.DataEngineIPFamilyIPv4), candidate)
	}
	for _, candidate := range []string{
		"::1",
		"fe80::1",
		"ff02::1",
		"::",
		"not-an-ip",
		"192.0.2.10",
	} {
		require.False(t, isDataEngineIPFamily(candidate, types.DataEngineIPFamilyIPv6), candidate)
	}
}

func TestOrderedCNISelectionUsesCommonNetworkPreference(t *testing.T) {
	const storageNetwork = "longhorn-system/dual-stack"
	pod := newDataEngineTestPod(nil, nil, nil, "192.0.2.99")
	pod.Annotations = map[string]string{
		string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
			Name: storageNetwork,
			IPs:  []string{"2001:db8::20", "192.0.2.10"},
		}),
	}

	ds := newDataEngineTestDataStore(t, storageNetwork)
	got, err := ds.GetIPFromPodByCNISettingOrdered(pod, types.SettingNameStorageNetwork)
	require.NoError(t, err)
	require.Equal(t, "2001:db8::20", got)
}

func TestOrderedCNISelectionSkipsUnusableCandidatesAndFailsClosed(t *testing.T) {
	const storageNetwork = "longhorn-system/dual-stack"
	ds := newDataEngineTestDataStore(t, storageNetwork)

	pod := newDataEngineTestPod(nil, nil, nil, "192.0.2.99")
	pod.Annotations = map[string]string{
		string(types.CNIAnnotationNetworkStatus): dataEngineCNIStatus(t, types.CniNetwork{
			Name: storageNetwork,
			IPs:  []string{"127.0.0.1", "fe80::1", "not-an-ip", "192.0.2.10"},
		}),
	}
	got, err := ds.GetIPFromPodByCNISettingOrdered(pod, types.SettingNameStorageNetwork)
	require.NoError(t, err)
	require.Equal(t, "192.0.2.10", got)

	pod.Annotations[string(types.CNIAnnotationNetworkStatus)] = dataEngineCNIStatus(t, types.CniNetwork{
		Name: storageNetwork,
		IPs:  []string{"127.0.0.1", "fe80::1", "not-an-ip"},
	})
	got, err = ds.GetIPFromPodByCNISettingOrdered(pod, types.SettingNameStorageNetwork)
	require.Error(t, err)
	require.Empty(t, got)
}
