package engineapi

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	commonnet "github.com/longhorn/go-common-libs/net"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestV1EngineIPFamilyArguments(t *testing.T) {
	versions := []int{-1, 0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
	families := []commonnet.IPFamily{"", "default", commonnet.IPFamilyIPv4, commonnet.IPFamilyIPv6}
	for _, version := range versions {
		for _, family := range families {
			for _, upgrade := range []bool{false, true} {
				t.Run(fmt.Sprintf("cli-%d/family-%s/upgrade-%t", version, family, upgrade), func(t *testing.T) {
					engine := &longhorn.Engine{}
					engine.Name = "test-engine"
					engine.Spec.DataEngine = longhorn.DataEngineTypeV1
					engine.Spec.Image = "longhornio/longhorn-engine:test"
					engine.Spec.VolumeName = "test-volume"
					engine.Spec.VolumeSize = 1 << 30
					engine.Status.CurrentSize = engine.Spec.VolumeSize
					engine.Status.CurrentReplicaAddressMap = map[string]string{"replica": "[2001:db8::20]:10000"}
					engine.Spec.UpgradedReplicaAddressMap = engine.Status.CurrentReplicaAddressMap
					build := func(f commonnet.IPFamily) (string, []string, error) {
						if upgrade {
							return getBinaryAndArgsForEngineProcessUpgrade(&EngineInstanceUpgradeRequest{
								Engine: engine, EngineCLIAPIVersion: version, IPFamily: f,
								VolumeFrontend: longhorn.VolumeFrontendBlockDev,
							})
						}
						return getBinaryAndArgsForEngineInstanceCreation(&EngineInstanceCreateRequest{
							Engine: engine, EngineCLIAPIVersion: version, IPFamily: f,
						}, "tgt-blockdev")
					}
					binary, args, err := build(family)
					explicit := family == commonnet.IPFamilyIPv4 || family == commonnet.IPFamilyIPv6
					if explicit && version < CLIAPIVersionForIPFamily {
						require.ErrorContains(t, err, string(types.SettingNamePreferredDataEngineIPFamily)+"="+string(family))
						require.ErrorContains(t, err, fmt.Sprintf("reports CLI API version %d", version))
						require.ErrorContains(t, err, "requires engine CLI API version 13 or later")
						require.ErrorContains(t, err, "upgrade the engine image")
						require.ErrorContains(t, err, engine.Spec.Image)
						require.Empty(t, binary)
						require.Empty(t, args)
						return
					}
					require.NoError(t, err)
					baseBinary, baseArgs, err := build(commonnet.IPFamilyUnspecified)
					require.NoError(t, err)
					require.Equal(t, baseBinary, binary)
					require.Contains(t, args, "controller")
					require.Contains(t, args, "tcp://[2001:db8::20]:10000")
					require.Equal(t, upgrade, slices.Contains(args, "--upgrade"))
					if explicit {
						require.Equal(t, append(baseArgs, "--ip-family", string(family)), args)
					} else {
						require.Equal(t, baseArgs, args)
						require.NotContains(t, args, "--ip-family")
					}
				})
			}
		}
	}
}

func TestV1EngineIPFamilyRejectsUnsupportedRequestsBeforeRPC(t *testing.T) {
	client := &InstanceManagerClient{apiMinVersion: 1, apiVersion: CurrentInstanceManagerAPIVersion}
	engine := &longhorn.Engine{}
	engine.Spec.DataEngine = longhorn.DataEngineTypeV1
	for _, family := range []commonnet.IPFamily{commonnet.IPFamilyIPv4, commonnet.IPFamilyIPv6, "invalid"} {
		t.Run(string(family), func(t *testing.T) {
			_, err := client.EngineInstanceCreate(&EngineInstanceCreateRequest{
				Engine: engine, EngineCLIAPIVersion: 12, IPFamily: family,
			})
			require.Error(t, err)
			_, err = client.EngineInstanceUpgrade(&EngineInstanceUpgradeRequest{
				Engine: engine, EngineCLIAPIVersion: 12, IPFamily: family,
			})
			require.Error(t, err)
		})
	}
}

func TestEngineIPFamilyLeavesV2AndReplicaArgumentsUnchanged(t *testing.T) {
	for _, version := range []int{7, 12, 13, 14} {
		for _, family := range []commonnet.IPFamily{"", "default", commonnet.IPFamilyIPv4, commonnet.IPFamilyIPv6} {
			t.Run(fmt.Sprintf("cli-%d/family-%s", version, family), func(t *testing.T) {
				engine := &longhorn.Engine{}
				engine.Spec.DataEngine = longhorn.DataEngineTypeV2
				binary, args, err := getBinaryAndArgsForEngineInstanceCreation(&EngineInstanceCreateRequest{
					Engine: engine, EngineCLIAPIVersion: version, IPFamily: family,
				}, "")
				require.NoError(t, err)
				require.Empty(t, binary)
				require.Empty(t, args)
				client := &InstanceManagerClient{}
				_, err = client.EngineInstanceUpgrade(&EngineInstanceUpgradeRequest{Engine: engine, IPFamily: family})
				require.EqualError(t, err, "SPDK engine upgrade is not supported yet")

				replica := &longhorn.Replica{}
				replica.Spec.DataEngine = longhorn.DataEngineTypeV1
				replica.Spec.VolumeSize = 1 << 30
				_, replicaArgs, err := getBinaryAndArgsForReplicaProcessCreation(replica,
					"/replica", "", longhorn.DataLocalityDisabled, DefaultReplicaPortCountV1, version, false)
				require.NoError(t, err)
				require.Contains(t, replicaArgs, "replica")
				require.NotContains(t, replicaArgs, "--ip-family")
				require.Contains(t, replicaArgs, "--sync-agent-port-count")
			})
		}
	}
}
