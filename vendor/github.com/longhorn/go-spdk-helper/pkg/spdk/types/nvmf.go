package types

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type NvmfANAGroupID string

const DefaultNvmfANAGroupID uint32 = 1

func (groupID *NvmfANAGroupID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*groupID = ""
		return nil
	}

	var stringValue string
	if err := json.Unmarshal(data, &stringValue); err == nil {
		*groupID = NvmfANAGroupID(stringValue)
		return nil
	}

	var numericValue uint32
	if err := json.Unmarshal(data, &numericValue); err == nil {
		*groupID = NvmfANAGroupID(strconv.FormatUint(uint64(numericValue), 10))
		return nil
	}

	return fmt.Errorf("failed to unmarshal ANA group ID %s", string(data))
}

type NvmfCreateTransportRequest struct {
	Trtype NvmeTransportType `json:"trtype"`
}

type NvmfGetTransportRequest struct {
	TgtName string            `json:"tgt_name,omitempty"`
	Trtype  NvmeTransportType `json:"trtype,omitempty"`
}

type NvmfTransport struct {
	Trtype              NvmeTransportType `json:"trtype"`
	MaxQueueDepth       uint32            `json:"max_queue_depth"`
	MaxIoQpairsPerCtrlr uint32            `json:"max_io_qpairs_per_ctrlr"`
	InCapsuleDataSize   uint32            `json:"in_capsule_data_size"`
	MaxIoSize           uint32            `json:"max_io_size"`
	IoUnitSize          uint32            `json:"io_unit_size"`
	MaxAqDepth          uint32            `json:"max_aq_depth"`
	NumSharedBuffers    uint32            `json:"num_shared_buffers"`
	BufCacheSize        uint32            `json:"buf_cache_size"`
	SockPriority        uint32            `json:"sock_priority"`
	AbortTimeoutSec     uint32            `json:"abort_timeout_sec"`
	DifInsertOrStrip    bool              `json:"dif_insert_or_strip"`
	Zcopy               bool              `json:"zcopy"`
	C2HSuccess          bool              `json:"c2h_success"`
}

type NvmfCreateSubsystemRequest struct {
	Nqn string `json:"nqn"`

	TgtName       string `json:"tgt_name,omitempty"`
	SerialNumber  string `json:"serial_number,omitempty"`
	ModelNumber   string `json:"model_number,omitempty"`
	AllowAnyHost  bool   `json:"allow_any_host,omitempty"`
	AnaReporting  bool   `json:"ana_reporting,omitempty"`
	MaxNamespaces uint32 `json:"max_namespaces,omitempty"`
	MinCntlid     uint16 `json:"min_cntlid,omitempty"`
	MaxCntlid     uint16 `json:"max_cntlid,omitempty"`
}

type NvmfDeleteSubsystemRequest struct {
	Nqn     string `json:"nqn"`
	TgtName string `json:"tgt_name,omitempty"`
}

type NvmfGetSubsystemsRequest struct {
	Nqn     string `json:"nqn,omitempty"`
	TgtName string `json:"tgt_name,omitempty"`
}

type NvmfSubsystem struct {
	Nqn             string                       `json:"nqn"`
	Subtype         string                       `json:"subtype"`
	ListenAddresses []NvmfSubsystemListenAddress `json:"listen_addresses"`
	AllowAnyHost    bool                         `json:"allow_any_host"`
	Hosts           []NvmfSubsystemHost          `json:"hosts"`
	SerialNumber    string                       `json:"serial_number,omitempty"`
	ModelNumber     string                       `json:"model_number,omitempty"`
	MaxNamespaces   uint32                       `json:"max_namespaces,omitempty"`
	MinCntlid       uint16                       `json:"min_cntlid,omitempty"`
	MaxCntlid       uint16                       `json:"max_cntlid,omitempty"`
	Namespaces      []NvmfSubsystemNamespace     `json:"namespaces"`
}

type NvmfSubsystemListenAddress struct {
	Trtype  NvmeTransportType `json:"trtype"`
	Adrfam  NvmeAddressFamily `json:"adrfam"`
	Traddr  string            `json:"traddr"`
	Trsvcid string            `json:"trsvcid"`
}

type NvmfSubsystemNamespace struct {
	Nsid     uint32 `json:"nsid,omitempty"`
	BdevName string `json:"bdev_name"`
	Nguid    string `json:"nguid,omitempty"`
	Eui64    string `json:"eui64,omitempty"`
	UUID     string `json:"uuid,omitempty"`
	Anagrpid string `json:"anagrpid,omitempty"`
	PtplFile string `json:"ptpl_file,omitempty"`
}

// UnmarshalJSON handles SPDK returning anagrpid as either a string or a number.
func (ns *NvmfSubsystemNamespace) UnmarshalJSON(data []byte) error {
	type Alias NvmfSubsystemNamespace
	aux := &struct {
		Anagrpid json.RawMessage `json:"anagrpid,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(ns),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if len(aux.Anagrpid) > 0 && string(aux.Anagrpid) != "null" {
		var s string
		if err := json.Unmarshal(aux.Anagrpid, &s); err == nil {
			ns.Anagrpid = s
		} else {
			var n uint32
			if err := json.Unmarshal(aux.Anagrpid, &n); err == nil {
				ns.Anagrpid = strconv.FormatUint(uint64(n), 10)
			} else {
				return fmt.Errorf("failed to unmarshal anagrpid: %s", string(aux.Anagrpid))
			}
		}
	}
	return nil
}

type NvmfSubsystemHost struct {
	Nqn string `json:"nqn"`
}

type NvmfSubsystemAddNsRequest struct {
	Nqn       string                 `json:"nqn"`
	Namespace NvmfSubsystemNamespace `json:"namespace"`
	TgtName   string                 `json:"tgt_name,omitempty"`
}

type NvmfSubsystemRemoveNsRequest struct {
	Nqn     string `json:"nqn"`
	Nsid    uint32 `json:"nsid"`
	TgtName string `json:"tgt_name,omitempty"`
}

type NvmfSubsystemAddListenerRequest struct {
	Nqn           string                     `json:"nqn"`
	ListenAddress NvmfSubsystemListenAddress `json:"listen_address"`

	TgtName string `json:"tgt_name,omitempty"`
}

type NvmfSubsystemRemoveListenerRequest struct {
	Nqn           string                     `json:"nqn"`
	ListenAddress NvmfSubsystemListenAddress `json:"listen_address"`

	TgtName string `json:"tgt_name,omitempty"`
}

type NvmfSubsystemListenerSetANAStateRequest struct {
	Nqn           string                        `json:"nqn"`
	ListenAddress NvmfSubsystemListenAddress    `json:"listen_address"`
	AnaState      NvmfSubsystemListenerAnaState `json:"ana_state"`
	AnaGrpid      uint32                        `json:"anagrpid,omitempty"`

	TgtName string `json:"tgt_name,omitempty"`
}

type NvmfSubsystemGetListenersRequest struct {
	Nqn string `json:"nqn"`

	TgtName string `json:"tgt_name,omitempty"`
}

type NvmfSubsystemListenerAnaState string

const (
	NvmfSubsystemListenerAnaStateOptimized    = "optimized"
	NvmfSubsystemListenerAnaStateNonOptimized = "non_optimized"
	NvmfSubsystemListenerAnaStateInaccessible = "inaccessible"

	// Deprecated: NvmfSubsystemListenerAnaStatePersistentLoss is kept for
	// backward compatibility with older Longhorn versions that reference it.
	NvmfSubsystemListenerAnaStatePersistentLoss = "persistent_loss"
	// Deprecated: NvmfSubsystemListenerAnaStateChange is kept for backward
	// compatibility with older Longhorn versions that reference it.
	NvmfSubsystemListenerAnaStateChange = "change"
)

type NvmfSubsystemListener struct {
	Address  NvmfSubsystemListenAddress    `json:"address"`
	AnaState NvmfSubsystemListenerAnaState `json:"ana_state"`
}
