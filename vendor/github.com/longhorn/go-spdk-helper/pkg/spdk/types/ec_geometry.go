package types

// SPDK bdev_ec on-disk metadata constants, mirrored from longhorn/spdk. The front
// reservation recomputes ec_compute_geometry, so these must stay in sync with SPDK.
const (
	ecWibHeaderBytes     = 24   // sizeof(struct ec_wib_header)
	ecBitmapHeaderBytes  = 40   // sizeof(struct ec_bitmap_header)
	ecCRCBytes           = 4    // uint32 crc trailer
	ecWibRegionStripes   = 1024 // EC_WIB_REGION_STRIPES
	ecWibStrips          = 2    // double-buffered WIB, reserved on every disk
	ecCommitRecordStrips = 2    // commit record (stamp): one strip per double-buffer copy
)

// EcFrontReservationStrips returns the strips SPDK bdev_ec keeps at the front of each base
// disk for metadata (the double-buffered unmapped bitmap, the commit record, and the WIB)
// before user data, recomputing SPDK's ec_compute_geometry. Assumes a valid strip size
// (power of two, 4..1024 KiB); a smaller value underflows the WIB-payload subtraction.
func EcFrontReservationStrips(stripSizeKB uint32) uint64 {
	stripBytes := uint64(stripSizeKB) * 1024
	wibPayload := stripBytes - ecWibHeaderBytes - ecCRCBytes
	maxStripes := (wibPayload / 8) * 64 * ecWibRegionStripes
	blobBytes := ecBitmapHeaderBytes + ((maxStripes+63)/64)*8
	slotStrips := (blobBytes + ecCRCBytes + stripBytes - 1) / stripBytes
	return slotStrips*2 + ecWibStrips + ecCommitRecordStrips
}

// EcFrontReservationBytes returns the per-disk front reservation, in bytes, to add on
// top of ceil(volumeSize/k) when sizing an EC shard lvol.
func EcFrontReservationBytes(stripSizeKB uint32) uint64 {
	return EcFrontReservationStrips(stripSizeKB) * uint64(stripSizeKB) * 1024
}
