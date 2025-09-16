package types

type NvmeTransportType string

const (
	NvmeTransportTypeTCP  = NvmeTransportType("tcp")
	NvmeTransportTypeRDMA = NvmeTransportType("rdma")
	NvmeTransportTypePCIe = NvmeTransportType("pcie")
)

type NvmeAddressFamily string

const (
	NvmeAddressFamilyIPv4      = NvmeAddressFamily("ipv4")
	NvmeAddressFamilyIPv6      = NvmeAddressFamily("ipv6")
	NvmeAddressFamilyIB        = NvmeAddressFamily("ib")
	NvmeAddressFamilyFC        = NvmeAddressFamily("fc")
	NvmeAddressFamilyIntraHost = NvmeAddressFamily("intra_host")
)

type NvmeMultipathBehavior string

const (
	NvmeMultipathBehaviorDisable   = "disable"
	NvmeMultipathBehaviorFailover  = "failover"
	NvmeMultipathBehaviorMultipath = "multipath"
)

type BdevDriverSpecificNvme []NvmeNamespaceInfo

type NvmeNamespaceInfo struct {
	PciAddress string             `json:"pci_address,omitempty"`
	CtrlrData  NvmeCtrlrData      `json:"ctrlr_data"`
	NsData     NvmeNsData         `json:"ns_data"`
	Trid       NvmeTransportID    `json:"trid"`
	VS         NvmeVendorSpecific `json:"vs"`
}

type NvmeCtrlrData struct {
	AnaReporting     bool   `json:"ana_reporting"`
	Cntlid           uint16 `json:"cntlid"`
	FirmwareRevision string `json:"firmware_revision"`
	ModelNumber      string `json:"model_number"`
	MultiCtrlr       bool   `json:"multi_ctrlr"`
	Oacs             struct {
		Firmware uint32 `json:"firmware"`
		Format   uint32 `json:"format"`
		NsManage uint32 `json:"ns_manage"`
		Security uint32 `json:"security"`
	} `json:"oacs"`
	SerialNumber string `json:"serial_number"`
	Subnqn       string `json:"subnqn"`
	VendorID     string `json:"vendor_id"`
}

type NvmeNsData struct {
	ID       uint32 `json:"id"`
	CanShare bool   `json:"can_share"`
	AnaState string `json:"ana_state,omitempty"`
}

type NvmeTransportID struct {
	Trtype  NvmeTransportType `json:"trtype,omitempty"`
	Adrfam  NvmeAddressFamily `json:"adrfam,omitempty"`
	Traddr  string            `json:"traddr,omitempty"`
	Trsvcid string            `json:"trsvcid,omitempty"`
	Subnqn  string            `json:"subnqn,omitempty"`
}

type NvmeVendorSpecific struct {
	NvmeVersion string `json:"nvme_version"`
}

type BdevNvmeMultipathPolicy string

const (
	BdevNvmeMultipathPolicyActivePassive = "active_passive"
	BdevNvmeMultipathPolicyActiveActive  = "active_active"
)

type BdevNvmeControllerInfo struct {
	Name   string               `json:"name"`
	Ctrlrs []NvmeControllerInfo `json:"ctrlrs"`
}

type NvmeControllerInfo struct {
	State  string             `json:"state"`
	Cntlid uint16             `json:"cntlid"`
	Trid   NvmeTransportID    `json:"trid"`
	Host   NvmeControllerHost `json:"host"`
}

type NvmeControllerHost struct {
	Nqn   string `json:"nqn"`
	Addr  string `json:"addr"`
	Svcid string `json:"svcid"`
}

type BdevNvmeAttachControllerRequest struct {
	Name string `json:"name"`

	NvmeTransportID

	Hostaddr  string `json:"hostaddr,omitempty"`
	Hostsvcid string `json:"hostsvcid,omitempty"`

	CtrlrLossTimeoutSec  int32 `json:"ctrlr_loss_timeout_sec"`
	ReconnectDelaySec    int32 `json:"reconnect_delay_sec"`
	FastIOFailTimeoutSec int32 `json:"fast_io_fail_timeout_sec"`

	Multipath string `json:"multipath,omitempty"`
}

type BdevNvmeDetachControllerRequest struct {
	Name string `json:"name"`

	NvmeTransportID

	Hostaddr  string `json:"hostaddr,omitempty"`
	Hostsvcid string `json:"hostsvcid,omitempty"`
}

type BdevNvmeSetOptionsRequest struct {
	CtrlrLossTimeoutSec  int32 `json:"ctrlr_loss_timeout_sec"`
	ReconnectDelaySec    int32 `json:"reconnect_delay_sec"`
	FastIOFailTimeoutSec int32 `json:"fast_io_fail_timeout_sec"`
	TransportAckTimeout  int32 `json:"transport_ack_timeout"`
	KeepAliveTimeoutMs   int32 `json:"keep_alive_timeout_ms"`
}

type BdevNvmeGetControllersRequest struct {
	Name string `json:"name,omitempty"`
}

// type BdevNvmeControllerHealthInfo struct {
//	ModelNumber                             string  `json:"model_number"`
//	SerialNumber                            string  `json:"serial_number"`
//	FirmwareRevision                        string  `json:"firmware_revision"`
//	Traddr                                  string  `json:"traddr"`
//	TemperatureCelsius                      uint64  `json:"temperature_celsius"`
//	AvailableSparePercentage                uint64  `json:"available_spare_percentage"`
//	AvailableSpareThresholdPercentage       uint64  `json:"available_spare_threshold_percentage"`
//	PercentageUsed                          uint64  `json:"percentage_used"`
//	DataUnitsRead                           uint128 `json:"data_units_read"`
//	DataUnitsWritten                        uint128 `json:"data_units_written"`
//	HostReadCommands                        uint128 `json:"host_read_commands"`
//	HostWriteCommands                       uint128 `json:"host_write_commands"`
//	ControllerBusyTime                      uint128 `json:"controller_busy_time"`
//	PowerCycles                             uint128 `json:"power_cycles"`
//	PowerOnHours                            uint128 `json:"power_on_hours"`
//	UnsafeShutdowns                         uint128 `json:"unsafe_shutdowns"`
//	MediaErrors                             uint128 `json:"media_errors"`
//	NumErrLogEntries                        uint128 `json:"num_err_log_entries"`
//	WarningTemperatureTimeMinutes           uint64  `json:"warning_temperature_time_minutes"`
//	CriticalCompositeTemperatureTimeMinutes uint64  `json:"critical_composite_temperature_time_minutes"`
// }
//
// type BdevNvmeGetControllerHealthInfoRequest struct {
//	Name string `json:"name"`
// }
