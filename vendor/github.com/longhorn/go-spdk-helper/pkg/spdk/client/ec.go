package client

import (
	"encoding/json"
	"fmt"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

// BdevEcCreate creates an EC bdev backed by k+m base bdevs.
//
//	"name": Required. Name for the new EC bdev.
//	"dataChunks": Required. Number of data chunks per stripe (Reed-Solomon k).
//	"parityChunks": Required. Number of parity chunks per stripe (Reed-Solomon m).
//	"stripSizeKB": Required. Chunk size in KiB (e.g. 64).
//	"baseBdevs": Required. Ordered list of k+m base bdev names.
//	"salvageRequested": Optional. When true, SPDK refuses to fresh-zero a
//	    torn on-disk unmapped bitmap and surfaces the failure so the
//	    operator can decide. Set on operator-driven recovery; leave false
//	    on normal create.
func (c *Client) BdevEcCreate(name string, dataChunks, parityChunks, stripSizeKB uint32, baseBdevs []string, salvageRequested bool) (bdevName string, err error) {
	req := spdktypes.BdevEcCreateRequest{
		Name:             name,
		DataChunks:       dataChunks,
		ParityChunks:     parityChunks,
		StripSizeKB:      stripSizeKB,
		BaseBdevs:        baseBdevs,
		SalvageRequested: salvageRequested,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_create", req)
	if err != nil {
		return "", err
	}

	// bdev_ec_create returns true on success, not the bdev name.
	var created bool
	if err := json.Unmarshal(cmdOutput, &created); err != nil {
		return "", err
	}
	if !created {
		return "", fmt.Errorf("bdev_ec_create returned false for %s", name)
	}
	return name, nil
}

// BdevEcDelete deletes an EC bdev by name.
func (c *Client) BdevEcDelete(name string) (deleted bool, err error) {
	req := spdktypes.BdevEcDeleteRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_delete", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// BdevEcGetBdevs lists EC bdevs. If name is empty, all EC bdevs are returned.
// The State field of each BdevEcInfo is derived from FailedCount and Offline
// after unmarshaling, since SPDK does not return it directly.
func (c *Client) BdevEcGetBdevs(name string) (bdevEcInfoList []spdktypes.BdevEcInfo, err error) {
	req := spdktypes.BdevEcGetBdevsRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_get_bdevs", req)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(cmdOutput, &bdevEcInfoList); err != nil {
		return nil, err
	}

	for i := range bdevEcInfoList {
		bdevEcInfoList[i].State = bdevEcState(&bdevEcInfoList[i])
		if bdevEcInfoList[i].RebuildInProgress && bdevEcInfoList[i].RebuildProgress != nil {
			if bdevEcInfoList[i].RebuildProgress.PercentComplete == 100 {
				bdevEcInfoList[i].RebuildProgress.RebuildState = spdktypes.BdevEcRebuildStateDone
			} else {
				bdevEcInfoList[i].RebuildProgress.RebuildState = spdktypes.BdevEcRebuildStateRunning
			}
		}
	}

	return bdevEcInfoList, nil
}

// bdevEcState derives the BdevEcState from the FailedCount and Offline fields.
func bdevEcState(info *spdktypes.BdevEcInfo) spdktypes.BdevEcState {
	if info.Offline {
		return spdktypes.BdevEcStateOffline
	}
	if info.FailedCount > 0 {
		return spdktypes.BdevEcStateDegraded
	}
	return spdktypes.BdevEcStateOnline
}

// BdevEcReplaceBaseBdev hot-swaps a failed base bdev slot with a new bdev.
// The slot is identified by its index. The slot transitions FAILED -> REPLACING
// immediately and the new bdev starts receiving foreground writes. A subsequent
// BdevEcStartRebuild call is required to reconstruct the missing data.
func (c *Client) BdevEcReplaceBaseBdev(name string, slot uint32, newBdevName string) (resp spdktypes.BdevEcReplaceBaseBdevResponse, err error) {
	req := spdktypes.BdevEcReplaceBaseBdevRequest{
		Name:        name,
		Slot:        slot,
		NewBdevName: newBdevName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_replace_base_bdev", req)
	if err != nil {
		return resp, err
	}

	return resp, json.Unmarshal(cmdOutput, &resp)
}

// BdevEcStartRebuild starts the background rebuild poller for all REPLACING
// slots in the EC bdev. Must be called after BdevEcReplaceBaseBdev.
func (c *Client) BdevEcStartRebuild(name string) (resp spdktypes.BdevEcStartRebuildResponse, err error) {
	req := spdktypes.BdevEcStartRebuildRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_start_rebuild", req)
	if err != nil {
		return resp, err
	}

	return resp, json.Unmarshal(cmdOutput, &resp)
}

// BdevEcGetRebuildProgress queries the rebuild progress of an EC bdev.
// RebuildState is derived by the Go layer: SPDK returns -ENOENT when no rebuild
// is active (idle), percent_complete=100 means done, otherwise running.
func (c *Client) BdevEcGetRebuildProgress(name string) (progress spdktypes.BdevEcRebuildProgress, err error) {
	req := spdktypes.BdevEcGetRebuildProgressRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_get_rebuild_progress", req)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoEntry(err) {
			return spdktypes.BdevEcRebuildProgress{RebuildState: spdktypes.BdevEcRebuildStateIdle}, nil
		}
		return spdktypes.BdevEcRebuildProgress{RebuildState: spdktypes.BdevEcRebuildStateError}, err
	}

	if err := json.Unmarshal(cmdOutput, &progress); err != nil {
		return progress, err
	}

	if progress.PercentComplete == 100 {
		progress.RebuildState = spdktypes.BdevEcRebuildStateDone
	} else {
		progress.RebuildState = spdktypes.BdevEcRebuildStateRunning
	}

	return progress, nil
}

// BdevEcStopRebuild stops a running rebuild. Returns an error if no rebuild
// is in progress (-ENOENT). The in-progress rebuild drains its current stripe
// I/O before finishing; REPLACING slots are not reverted to FAILED.
func (c *Client) BdevEcStopRebuild(name string) (stopped bool, err error) {
	req := spdktypes.BdevEcStopRebuildRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_stop_rebuild", req)
	if err != nil {
		return false, err
	}

	return stopped, json.Unmarshal(cmdOutput, &stopped)
}

// BdevEcSetRebuildQos sets the rebuild rate limit in stripes/sec.
// maxStripesPerSec=0 means unlimited. paused=true suspends the rebuild
// poller without cancelling it. Applied immediately to any in-progress rebuild.
func (c *Client) BdevEcSetRebuildQos(name string, maxStripesPerSec uint32, paused bool) (set bool, err error) {
	req := spdktypes.BdevEcSetRebuildQosRequest{
		Name:             name,
		MaxStripesPerSec: maxStripesPerSec,
		Paused:           paused,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_set_rebuild_qos", req)
	if err != nil {
		return false, err
	}

	return set, json.Unmarshal(cmdOutput, &set)
}

// BdevEcResize performs an in-place capacity expansion of an EC bdev.
// All k+m base bdevs must have been resized before calling this.
// No data movement occurs; only geometry and WIB metadata are updated.
// When the base bdevs have not grown, the call is an idempotent no-op:
// it returns success with Resized false and the current block count.
func (c *Client) BdevEcResize(name string) (resp spdktypes.BdevEcResizeResponse, err error) {
	req := spdktypes.BdevEcResizeRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_resize", req)
	if err != nil {
		return resp, err
	}

	return resp, json.Unmarshal(cmdOutput, &resp)
}

// BdevEcGetWibStatus queries the Write-Intent Bitmap (WIB) state of an EC bdev.
func (c *Client) BdevEcGetWibStatus(name string) (status spdktypes.BdevEcWibStatus, err error) {
	req := spdktypes.BdevEcGetWibStatusRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_get_wib_status", req)
	if err != nil {
		return status, err
	}

	return status, json.Unmarshal(cmdOutput, &status)
}

// BdevEcGetUnmapStatus queries the in-band unmapped-bitmap state of an EC
// bdev. The bitmap is lifecycle-bound to the bdev (provisioned at create
// time, persists for the bdev's lifetime), so a successful call always
// returns a populated status. A non-nil error means the bdev itself is
// gone (-ENODEV) or the RPC failed.
func (c *Client) BdevEcGetUnmapStatus(name string) (status spdktypes.BdevEcUnmapStatus, err error) {
	req := spdktypes.BdevEcGetUnmapStatusRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_get_unmap_status", req)
	if err != nil {
		return status, err
	}

	return status, json.Unmarshal(cmdOutput, &status)
}

// BdevEcGetScrubProgress queries the startup scrub progress of an EC bdev.
// Returns nil, nil when SPDK signals -ENOENT (no scrub is active).
// A non-nil pointer means a scrub is currently running.
func (c *Client) BdevEcGetScrubProgress(name string) (*spdktypes.BdevEcScrubProgress, error) {
	req := spdktypes.BdevEcGetScrubProgressRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_ec_get_scrub_progress", req)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoEntry(err) {
			return nil, nil
		}
		return nil, err
	}

	var progress spdktypes.BdevEcScrubProgress
	if err := json.Unmarshal(cmdOutput, &progress); err != nil {
		return nil, err
	}
	return &progress, nil
}
