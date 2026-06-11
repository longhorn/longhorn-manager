package client

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

// ShardGroupCreate provisions a ShardGroup process: NVMe-attaches to k+m
// shards, creates bdev_ec, lvstore, head lvol, and exposes via NVMe-oF.
// salvageRequested=true selects the recovery path that re-discovers the
// existing lvstore on bdev_ec.
func (c *SPDKClient) ShardGroupCreate(name, volumeName string, specSize uint64, dataChunks, parityChunks, stripSizeKb uint32, shards map[string]*spdkrpc.ShardEndpoint, portCount int32, salvageRequested bool) (*spdkrpc.ShardGroup, error) {
	if name == "" || volumeName == "" {
		return nil, fmt.Errorf("failed to create SPDK shardgroup: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupCreate(ctx, &spdkrpc.ShardGroupCreateRequest{
		Name:       name,
		VolumeName: volumeName,
		SpecSize:   specSize,
		PortCount:  portCount,
		Spec: &spdkrpc.ShardGroupSpec{
			DataChunks:       dataChunks,
			ParityChunks:     parityChunks,
			StripSizeKb:      stripSizeKb,
			Shards:           shards,
			SalvageRequested: salvageRequested,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create SPDK shardgroup")
	}
	return resp, nil
}

// ShardGroupDelete tears down the ShardGroup process. cleanupRequired=true is
// the volume-deletion path (deletes lvstore + head lvol). cleanupRequired=false
// is the detach path (preserves lvstore + head lvol on the encoded blocks).
func (c *SPDKClient) ShardGroupDelete(name string, cleanupRequired bool) error {
	if name == "" {
		return fmt.Errorf("failed to delete SPDK shardgroup: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardGroupDelete(ctx, &spdkrpc.ShardGroupDeleteRequest{
		Name:            name,
		CleanupRequired: cleanupRequired,
	})
	return errors.Wrapf(err, "failed to delete SPDK shardgroup %v", name)
}

func (c *SPDKClient) ShardGroupGet(name string) (*spdkrpc.ShardGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get SPDK shardgroup: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupGet(ctx, &spdkrpc.ShardGroupGetRequest{Name: name})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK shardgroup %v", name)
	}
	return resp, nil
}

func (c *SPDKClient) ShardGroupList() (map[string]*spdkrpc.ShardGroup, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK shardgroups")
	}
	return resp.ShardGroups, nil
}

func (c *SPDKClient) ShardGroupWatch(ctx context.Context) (spdkrpc.SPDKService_ShardGroupWatchClient, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.ShardGroupWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to watch SPDK shardgroups")
	}
	return stream, nil
}

func (c *SPDKClient) ShardGroupExpand(name string, size uint64) error {
	if name == "" {
		return fmt.Errorf("failed to expand SPDK shardgroup: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardGroupExpand(ctx, &spdkrpc.ShardGroupExpandRequest{
		Name: name,
		Size: size,
	})
	return errors.Wrapf(err, "failed to expand SPDK shardgroup %v", name)
}

func (c *SPDKClient) ShardGroupExpandPrecheck(name string, size uint64) (bool, error) {
	if name == "" {
		return false, fmt.Errorf("failed to precheck expand SPDK shardgroup: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupExpandPrecheck(ctx, &spdkrpc.ShardGroupExpandPrecheckRequest{
		Name: name,
		Size: size,
	})
	if err != nil {
		return false, errors.Wrapf(err, "failed to precheck expand SPDK shardgroup %v", name)
	}
	return resp.ExpansionRequired, nil
}

func (c *SPDKClient) ShardGroupShardReplace(shardGroupName, shardName, shardAddress string) (spdkrpc.EcSlotState, error) {
	if shardGroupName == "" || shardName == "" || shardAddress == "" {
		return spdkrpc.EcSlotState_EC_SLOT_STATE_NORMAL, fmt.Errorf("failed to replace shard: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupShardReplace(ctx, &spdkrpc.ShardGroupShardReplaceRequest{
		ShardGroupName: shardGroupName,
		ShardName:      shardName,
		ShardAddress:   shardAddress,
	})
	if err != nil {
		return spdkrpc.EcSlotState_EC_SLOT_STATE_NORMAL, errors.Wrapf(err, "failed to replace shard %v in shardgroup %v", shardName, shardGroupName)
	}
	return resp.SlotState, nil
}

func (c *SPDKClient) ShardGroupShardForceFail(shardGroupName, shardName string) (spdkrpc.EcSlotState, error) {
	if shardGroupName == "" || shardName == "" {
		return spdkrpc.EcSlotState_EC_SLOT_STATE_NORMAL, fmt.Errorf("failed to force-fail shard: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupShardForceFail(ctx, &spdkrpc.ShardGroupShardForceFailRequest{
		ShardGroupName: shardGroupName,
		ShardName:      shardName,
	})
	if err != nil {
		return spdkrpc.EcSlotState_EC_SLOT_STATE_NORMAL, errors.Wrapf(err, "failed to force-fail shard %v in shardgroup %v", shardName, shardGroupName)
	}
	return resp.SlotState, nil
}

func (c *SPDKClient) ShardGroupShardRebuildStart(shardGroupName string) (numStripes uint64, firstSlot uint32, err error) {
	if shardGroupName == "" {
		return 0, 0, fmt.Errorf("failed to start rebuild: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupShardRebuildStart(ctx, &spdkrpc.ShardGroupShardRebuildStartRequest{
		ShardGroupName: shardGroupName,
	})
	if err != nil {
		return 0, 0, errors.Wrapf(err, "failed to start rebuild on shardgroup %v", shardGroupName)
	}
	return resp.NumStripes, resp.FirstSlot, nil
}

func (c *SPDKClient) ShardGroupShardRebuildProgress(shardGroupName string) (*spdkrpc.ShardGroupShardRebuildProgressResponse, error) {
	if shardGroupName == "" {
		return nil, fmt.Errorf("failed to get rebuild progress: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupShardRebuildProgress(ctx, &spdkrpc.ShardGroupShardRebuildProgressRequest{
		ShardGroupName: shardGroupName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get rebuild progress for shardgroup %v", shardGroupName)
	}
	return resp, nil
}

func (c *SPDKClient) ShardGroupShardRebuildStop(shardGroupName string) error {
	if shardGroupName == "" {
		return fmt.Errorf("failed to stop rebuild: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardGroupShardRebuildStop(ctx, &spdkrpc.ShardGroupShardRebuildStopRequest{
		ShardGroupName: shardGroupName,
	})
	return errors.Wrapf(err, "failed to stop rebuild on shardgroup %v", shardGroupName)
}

func (c *SPDKClient) ShardGroupShardRebuildQosSet(shardGroupName string, maxStripesPerSec uint32, paused bool) error {
	if shardGroupName == "" {
		return fmt.Errorf("failed to set rebuild QoS: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardGroupShardRebuildQosSet(ctx, &spdkrpc.ShardGroupShardRebuildQosSetRequest{
		ShardGroupName:   shardGroupName,
		MaxStripesPerSec: maxStripesPerSec,
		Paused:           paused,
	})
	return errors.Wrapf(err, "failed to set rebuild QoS on shardgroup %v", shardGroupName)
}

func (c *SPDKClient) ShardGroupSnapshotCreate(shardGroupName, snapshotName string) (snapshotUUID string, err error) {
	if shardGroupName == "" || snapshotName == "" {
		return "", fmt.Errorf("failed to create snapshot: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGroupSnapshotCreate(ctx, &spdkrpc.ShardGroupSnapshotCreateRequest{
		ShardGroupName: shardGroupName,
		SnapshotName:   snapshotName,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to create snapshot %v on shardgroup %v", snapshotName, shardGroupName)
	}
	return resp.SnapshotUuid, nil
}

func (c *SPDKClient) ShardGroupSnapshotDelete(shardGroupName, snapshotName string) error {
	if shardGroupName == "" || snapshotName == "" {
		return fmt.Errorf("failed to delete snapshot: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardGroupSnapshotDelete(ctx, &spdkrpc.ShardGroupSnapshotDeleteRequest{
		ShardGroupName: shardGroupName,
		SnapshotName:   snapshotName,
	})
	return errors.Wrapf(err, "failed to delete snapshot %v from shardgroup %v", snapshotName, shardGroupName)
}

func (c *SPDKClient) ShardGroupSnapshotRevert(shardGroupName, snapshotName string) error {
	if shardGroupName == "" || snapshotName == "" {
		return fmt.Errorf("failed to revert snapshot: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardGroupSnapshotRevert(ctx, &spdkrpc.ShardGroupSnapshotRevertRequest{
		ShardGroupName: shardGroupName,
		SnapshotName:   snapshotName,
	})
	return errors.Wrapf(err, "failed to revert snapshot %v on shardgroup %v", snapshotName, shardGroupName)
}

func (c *SPDKClient) ShardGroupSnapshotPurge(shardGroupName string) error {
	if shardGroupName == "" {
		return fmt.Errorf("failed to purge snapshots: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardGroupSnapshotPurge(ctx, &spdkrpc.ShardGroupSnapshotPurgeRequest{
		ShardGroupName: shardGroupName,
	})
	return errors.Wrapf(err, "failed to purge snapshots on shardgroup %v", shardGroupName)
}
