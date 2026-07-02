package types

type BdevEcState string

const (
	BdevEcStateOnline   = BdevEcState("online")
	BdevEcStateDegraded = BdevEcState("degraded")
	BdevEcStateOffline  = BdevEcState("offline")
)

type BdevEcSlotState string

const (
	BdevEcSlotStateNormal    = BdevEcSlotState("normal")
	BdevEcSlotStateFailed    = BdevEcSlotState("failed")
	BdevEcSlotStateReplacing = BdevEcSlotState("replacing")
)

type BdevEcSlotRole string

const (
	BdevEcSlotRoleData   = BdevEcSlotRole("data")
	BdevEcSlotRoleParity = BdevEcSlotRole("parity")
)

type BdevEcRebuildState string

const (
	BdevEcRebuildStateIdle    = BdevEcRebuildState("idle")
	BdevEcRebuildStateRunning = BdevEcRebuildState("running")
	BdevEcRebuildStateDone    = BdevEcRebuildState("done")
	BdevEcRebuildStateError   = BdevEcRebuildState("error")
)

// EcBaseBdev represents one base device slot in the EC array.
type EcBaseBdev struct {
	Name         string          `json:"name"`
	Slot         uint32          `json:"slot"`
	Role         BdevEcSlotRole  `json:"role"`
	State        BdevEcSlotState `json:"state"`
	NeedsRebuild bool            `json:"needs_rebuild,omitempty"`
}

// BdevEcInfo is the response element for bdev_ec_get_bdevs.
// State is not returned by the SPDK JSON-RPC; it is derived by the Go layer
// from FailedCount and Offline: offline → "offline", failed_count > 0 → "degraded", else → "online".
type BdevEcInfo struct {
	Name              string `json:"name"`
	DataChunks        uint32 `json:"k"`
	ParityChunks      uint32 `json:"m"`
	TotalChunks       uint32 `json:"n"` // total number of base devices (data + parity)
	StripSizeKB       uint32 `json:"strip_size_kb"`
	FailedCount       uint32 `json:"failed_count"`
	Offline           bool   `json:"offline"`
	ReplaceInProgress bool   `json:"replace_in_progress"`
	RebuildInProgress bool   `json:"rebuild_in_progress"`
	// DirtyStripes counts how many stripes are currently claimed by a write
	// (RMW, full-stripe write, UNMAP, or rebuild). It is a current count,
	// not a running total.
	DirtyStripes               uint64                 `json:"dirty_stripes"`
	RmwInFlight                uint32                 `json:"rmw_in_flight"`
	RmwTotal                   uint64                 `json:"rmw_total"`
	RmwDeferredScrub           uint64                 `json:"rmw_deferred_scrub"`
	RmwDeferredDirty           uint64                 `json:"rmw_deferred_dirty"`
	RmwDeferredInflight        uint64                 `json:"rmw_deferred_inflight"`
	FullStripeWrites           uint64                 `json:"full_stripe_writes"`
	FullStripeWritesDeferred   uint64                 `json:"full_stripe_writes_deferred"`
	UnmapsSubmitted            uint64                 `json:"unmaps_submitted"`
	UnmapsCompleted            uint64                 `json:"unmaps_completed"`
	UnmapsDeferredBusy         uint64                 `json:"unmaps_deferred_busy"`
	UnmapsViaWriteZeros        uint64                 `json:"unmaps_via_write_zeros"`
	UnmapsFailed               uint64                 `json:"unmaps_failed"`
	UnmapFanoutMisses          uint64                 `json:"unmap_fanout_misses"`
	UnmappedReadsSynthesized   uint64                 `json:"unmapped_reads_synthesized"`
	WritesIntoUnmapped         uint64                 `json:"writes_into_unmapped"`
	WritesIntoUnmappedFailed   uint64                 `json:"writes_into_unmapped_failed"`
	UnmappedStripes            uint64                 `json:"unmapped_stripes"`
	DegradedReadsReconstructed uint64                 `json:"degraded_reads_reconstructed"`
	DegradedReadEioDirty       uint64                 `json:"degraded_read_eio_dirty"`
	RebuildProgress            *BdevEcRebuildProgress `json:"rebuild_progress,omitempty"`
	BaseBdevs                  []EcBaseBdev           `json:"base_bdevs"`
	State                      BdevEcState            `json:"-"`
}

// BdevEcCreateRequest is the request for bdev_ec_create.
//
// SalvageRequested gates the in-band unmapped-bitmap load behavior when no
// on-disk bitmap copy is found at create time. With false (fresh create),
// SPDK persists an all-unmapped bitmap and the EC bdev comes up empty.
// With true (operator-driven salvage), SPDK refuses to fresh-zero a torn
// bitmap and surfaces the failure so the operator can decide.
type BdevEcCreateRequest struct {
	Name             string   `json:"name"`
	DataChunks       uint32   `json:"data_chunk_count"`
	ParityChunks     uint32   `json:"parity_chunk_count"`
	StripSizeKB      uint32   `json:"strip_size_kb"`
	BaseBdevs        []string `json:"base_bdevs"`
	SalvageRequested bool     `json:"salvage_requested,omitempty"`
}

// BdevEcDeleteRequest is the request for bdev_ec_delete.
type BdevEcDeleteRequest struct {
	Name string `json:"name"`
}

// BdevEcGetBdevsRequest is the request for bdev_ec_get_bdevs.
// An empty Name lists all EC bdevs.
type BdevEcGetBdevsRequest struct {
	Name string `json:"name,omitempty"`
}

// BdevEcReplaceBaseBdevRequest is the request for bdev_ec_replace_base_bdev.
type BdevEcReplaceBaseBdevRequest struct {
	Name        string `json:"ec_name"`
	Slot        uint32 `json:"slot"`
	NewBdevName string `json:"new_bdev_name"`
}

// BdevEcReplaceBaseBdevResponse is the response for bdev_ec_replace_base_bdev.
// State is always "replacing" on success; NeedsRebuild is always true on success.
type BdevEcReplaceBaseBdevResponse struct {
	EcName       string          `json:"ec_name"`
	Slot         uint32          `json:"slot"`
	NewBdevName  string          `json:"new_bdev_name"`
	State        BdevEcSlotState `json:"state"`
	NeedsRebuild bool            `json:"needs_rebuild"`
}

// BdevEcStartRebuildRequest is the request for bdev_ec_start_rebuild.
type BdevEcStartRebuildRequest struct {
	Name string `json:"ec_name"`
}

// BdevEcStartRebuildResponse is the response for bdev_ec_start_rebuild.
type BdevEcStartRebuildResponse struct {
	EcName     string `json:"ec_name"`
	NumStripes uint64 `json:"num_stripes"`
	FirstSlot  uint32 `json:"first_slot"`
}

// BdevEcGetRebuildProgressRequest is the request for bdev_ec_get_rebuild_progress.
type BdevEcGetRebuildProgressRequest struct {
	Name string `json:"ec_name"`
}

// BdevEcStopRebuildRequest is the request for bdev_ec_stop_rebuild.
type BdevEcStopRebuildRequest struct {
	Name string `json:"ec_name"`
}

// BdevEcSetRebuildQosRequest is the request for bdev_ec_set_rebuild_qos.
type BdevEcSetRebuildQosRequest struct {
	Name             string `json:"ec_name"`
	MaxStripesPerSec uint32 `json:"max_stripes_per_sec"`
	Paused           bool   `json:"paused"`
}

// BdevEcResizeRequest is the request for bdev_ec_resize.
type BdevEcResizeRequest struct {
	Name string `json:"ec_name"`
}

// BdevEcResizeResponse is the response for bdev_ec_resize.
type BdevEcResizeResponse struct {
	EcName      string `json:"ec_name"`
	OldBlockcnt uint64 `json:"old_blockcnt"`
	NewBlockcnt uint64 `json:"new_blockcnt"`
}

// BdevEcGetWibStatusRequest is the request for bdev_ec_get_wib_status.
type BdevEcGetWibStatusRequest struct {
	Name string `json:"ec_name"`
}

// BdevEcGetUnmapStatusRequest is the request for bdev_ec_get_unmap_status.
type BdevEcGetUnmapStatusRequest struct {
	Name string `json:"ec_name"`
}

// BdevEcGetScrubProgressRequest is the request for bdev_ec_get_scrub_progress.
type BdevEcGetScrubProgressRequest struct {
	Name string `json:"ec_name"`
}

// BdevEcRebuildProgress is the response for bdev_ec_get_rebuild_progress.
// It is also embedded in BdevEcInfo when RebuildInProgress is true.
// RebuildState is not returned by the SPDK JSON-RPC; it is derived by the Go layer:
// -ENOENT → "idle", percent_complete == 100 → "done", non-ENOENT error → "error", otherwise → "running".
type BdevEcRebuildProgress struct {
	EcName          string             `json:"ec_name"`
	CurrentSlot     uint32             `json:"current_slot"`
	CurrentStripe   uint64             `json:"current_stripe"`
	NumStripes      uint64             `json:"num_stripes"`
	StripesRebuilt  uint64             `json:"stripes_rebuilt"`
	SlotsToRebuild  uint32             `json:"slots_to_rebuild"`
	PercentComplete uint32             `json:"percent_complete"`
	RebuildState    BdevEcRebuildState `json:"-"`
}

// BdevEcWibStatus is the response for bdev_ec_get_wib_status.
type BdevEcWibStatus struct {
	EcName         string `json:"ec_name"`
	NumRegions     uint32 `json:"num_regions"`
	DirtyRegions   uint32 `json:"dirty_regions"`
	Generation     uint64 `json:"generation"`
	PersistPending bool   `json:"persist_pending"`
}

// BdevEcUnmapStatus is the response for bdev_ec_get_unmap_status. It reports
// the on-disk state of the in-band unmapped bitmap, which is provisioned at
// bdev_ec_create time and persists for the bdev's lifetime.
type BdevEcUnmapStatus struct {
	EcName          string `json:"ec_name"`
	NumStripes      uint64 `json:"num_stripes"`
	UnmappedStripes uint64 `json:"unmapped_stripes"`
	BlobBytes       uint64 `json:"blob_bytes"`
	Generation      uint64 `json:"generation"`
	// ActiveCopy is the double-buffer slot the bitmap was loaded from at
	// startup (0 or 1). Surfaced for diagnostics; consumers should not
	// dispatch on it without a documented need.
	ActiveCopy     uint32 `json:"active_copy"`
	PersistPending bool   `json:"persist_pending"`
}

// BdevEcScrubProgress is the response for bdev_ec_get_scrub_progress.
type BdevEcScrubProgress struct {
	EcName            string `json:"ec_name"`
	CurrentRegion     uint32 `json:"current_region"`
	NumRegions        uint32 `json:"num_regions"`
	TotalDirtyRegions uint32 `json:"total_dirty_regions"`
	CurrentStripe     uint64 `json:"current_stripe"`
	StripesScrubbed   uint64 `json:"stripes_scrubbed"`
	RegionsScrubbed   uint64 `json:"regions_scrubbed"`
	PercentComplete   uint32 `json:"percent_complete"`
}
