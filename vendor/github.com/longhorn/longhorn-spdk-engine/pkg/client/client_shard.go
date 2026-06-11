package client

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
)

// ShardCreate creates a shard on the specified disk.
func (c *SPDKClient) ShardCreate(volumeName string, slotIndex uint32, sizeBytes uint64, lvsName, lvsUUID string, portCount int32) (*api.Shard, error) {
	if volumeName == "" || (lvsName == "" && lvsUUID == "") {
		return nil, fmt.Errorf("failed to create SPDK shard: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardCreate(ctx, &spdkrpc.ShardCreateRequest{
		VolumeName: volumeName,
		SlotIndex:  slotIndex,
		SizeBytes:  sizeBytes,
		LvsName:    lvsName,
		LvsUuid:    lvsUUID,
		PortCount:  portCount,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create SPDK shard")
	}

	return api.ProtoShardToShard(resp), nil
}

// ShardDelete deletes a shard by its instance name. When cleanupRequired is
// false the shard is detached (expose torn down, port released) but its lvol is
// preserved on disk for re-attach; when true the lvol is destroyed.
func (c *SPDKClient) ShardDelete(name string, cleanupRequired bool) error {
	if name == "" {
		return fmt.Errorf("failed to delete SPDK shard: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardDelete(ctx, &spdkrpc.ShardDeleteRequest{
		Name:            name,
		CleanupRequired: cleanupRequired,
	})
	return errors.Wrapf(err, "failed to delete SPDK shard %v", name)
}

// ShardGet returns the current state of a shard by its instance name.
func (c *SPDKClient) ShardGet(name string) (*api.Shard, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get SPDK shard: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardGet(ctx, &spdkrpc.ShardGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK shard %v", name)
	}

	return api.ProtoShardToShard(resp), nil
}

// ShardList returns all shards known to the SPDK service, keyed by instance name.
func (c *SPDKClient) ShardList() (map[string]*api.Shard, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ShardList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK shards")
	}

	res := map[string]*api.Shard{}
	for _, s := range resp.Shards {
		shard := api.ProtoShardToShard(s)
		res[shard.ShardID] = shard
	}
	return res, nil
}

// ShardWatch opens a watch stream for shard change events.
func (c *SPDKClient) ShardWatch(ctx context.Context) (*api.ShardStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.ShardWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open shard watch stream")
	}

	return api.NewShardStream(stream), nil
}

// ShardExpand expands a shard to a new size by its instance name.
func (c *SPDKClient) ShardExpand(name string, newSize uint64) error {
	if name == "" {
		return fmt.Errorf("failed to expand SPDK shard: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ShardExpand(ctx, &spdkrpc.ShardExpandRequest{
		Name: name,
		Size: newSize,
	})
	return errors.Wrapf(err, "failed to expand SPDK shard %v", name)
}
