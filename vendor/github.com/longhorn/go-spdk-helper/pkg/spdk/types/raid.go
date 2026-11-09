package types

type BdevRaidLevel string

const (
	BdevRaidLevel0      = BdevRaidLevel("0")
	BdevRaidLevelRaid0  = BdevRaidLevel("raid0")
	BdevRaidLevel1      = BdevRaidLevel("1")
	BdevRaidLevelRaid1  = BdevRaidLevel("raid1")
	BdevRaidLevel5f     = BdevRaidLevel("5f")
	BdevRaidLevelRaid5f = BdevRaidLevel("raid5f")
	BdevRaidLevelConcat = BdevRaidLevel("concat")
)

type BdevRaidInfo struct {
	Name                    string        `json:"name,omitempty"`
	StripSizeKb             uint32        `json:"strip_size_kb"`
	State                   string        `json:"state"`
	RaidLevel               BdevRaidLevel `json:"raid_level"`
	NumBaseBdevs            uint8         `json:"num_base_bdevs"`
	NumBaseBdevsDiscovered  uint8         `json:"num_base_bdevs_discovered"`
	NumBaseBdevsOperational uint8         `json:"num_base_bdevs_operational,omitempty"`
	BaseBdevsList           []BaseBdev    `json:"base_bdevs_list"`
	Superblock              bool          `json:"superblock"`
	DisableAutoResize       bool          `json:"disable_auto_resize"`
}

type BaseBdev struct {
	Name         string `json:"name"`
	UUID         string `json:"uuid"`
	IsConfigured bool   `json:"is_configured"`
	// RemoveScheduled is reported by the Longhorn SPDK: a base bdev stays configured until
	// its asynchronous removal completes. It is false with an SPDK that does not report it.
	RemoveScheduled bool   `json:"remove_scheduled"`
	DataOffset      uint64 `json:"data_offset"`
	DataSize        uint64 `json:"data_size"`
}

type BdevRaidCreateRequest struct {
	Name        string        `json:"name"`
	RaidLevel   BdevRaidLevel `json:"raid_level"`
	StripSizeKb uint32        `json:"strip_size_kb"`
	BaseBdevs   []string      `json:"base_bdevs"`
	UUID        string        `json:"uuid,omitempty"`
	// DisableAutoResize is omitted when false, because SPDK rejects unknown parameters.
	DisableAutoResize bool `json:"disable_auto_resize,omitempty"`
}

// BdevRaidCreateOptions holds the optional bdev_raid_create parameters of the Longhorn SPDK.
type BdevRaidCreateOptions struct {
	// DisableAutoResize makes the RAID bdev ignore base bdev resize events, so that it grows
	// only through bdev_raid_resize.
	DisableAutoResize bool
}

type BdevRaidDeleteRequest struct {
	Name string `json:"name"`
}

type BdevRaidCategory string

const (
	BdevRaidCategoryAll         = "all"
	BdevRaidCategoryOnline      = "online"
	BdevRaidCategoryOffline     = "offline"
	BdevRaidCategoryConfiguring = "configuring"
)

type BdevRaidGetBdevsRequest struct {
	Category BdevRaidCategory `json:"category"`
}

type BdevRaidRemoveBaseBdevRequest struct {
	Name string `json:"name"`
}

type BdevRaidGrowBaseBdevRequest struct {
	RaidName string `json:"raid_name"`
	BaseName string `json:"base_name"`
}

// BdevRaidResizeRequest is the request for bdev_raid_resize.
type BdevRaidResizeRequest struct {
	Name              string `json:"name"`
	ExpectedSizeInMib uint64 `json:"expected_size_in_mib,omitempty"`
}

// BdevRaidResizeResponse is the response for bdev_raid_resize.
type BdevRaidResizeResponse struct {
	Name        string `json:"name"`
	OldBlockcnt uint64 `json:"old_blockcnt"`
	NewBlockcnt uint64 `json:"new_blockcnt"`
	Resized     bool   `json:"resized"`
}
