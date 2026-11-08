package monitor

import (
	"strings"

	"github.com/longhorn/longhorn-manager/types"
)

func (m *EnvironmentCheckMonitor) requiresV2EnvironmentChecks() bool {
	enabled, err := m.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
	if err != nil {
		m.logger.WithError(err).Debug("Failed to fetch v2-data-engine setting")
		return m.ds.DataEngineMode() == types.DataEngineModeV2Only
	}
	return m.ds.DataEngineMode() == types.DataEngineModeV2Only || enabled
}

func requiredPackageProbeExecutables(osImage string, mode types.DataEngineMode) map[string]string {
	osImage = strings.ToLower(osImage)
	probes := make(map[string]string)
	switch {
	case strings.Contains(osImage, "ubuntu"), strings.Contains(osImage, "debian"):
		probes["nfs-common"] = "mount.nfs"
		probes["open-iscsi"] = "iscsiadm"
		probes["cryptsetup"] = "cryptsetup"
		probes["dmsetup"] = "dmsetup"
	case strings.Contains(osImage, "centos"), strings.Contains(osImage, "fedora"), strings.Contains(osImage, "red hat"), strings.Contains(osImage, "rocky"), strings.Contains(osImage, "ol"):
		probes["nfs-utils"] = "mount.nfs"
		probes["iscsi-initiator-utils"] = "iscsiadm"
		probes["cryptsetup"] = "cryptsetup"
		probes["device-mapper"] = "dmsetup"
	case strings.Contains(osImage, "suse"):
		probes["nfs-client"] = "mount.nfs"
		probes["open-iscsi"] = "iscsiadm"
		probes["cryptsetup"] = "cryptsetup"
		probes["device-mapper"] = "dmsetup"
	case strings.Contains(osImage, "arch"):
		probes["nfs-utils"] = "mount.nfs"
		probes["open-iscsi"] = "iscsiadm"
		probes["cryptsetup"] = "cryptsetup"
		probes["device-mapper"] = "dmsetup"
	case strings.Contains(osImage, "gentoo"):
		probes["net-fs/nfs-utils"] = "mount.nfs"
		probes["sys-block/open-iscsi"] = "iscsiadm"
		probes["sys-fs/cryptsetup"] = "cryptsetup"
		probes["sys-fs/lvm2"] = "dmsetup"
	default:
		return nil
	}
	if !mode.RequiresISCSI() {
		for name, binary := range probes {
			if binary == "iscsiadm" {
				delete(probes, name)
			}
		}
	}
	return probes
}
