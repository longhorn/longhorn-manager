package client

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// EngineFrontendCreate creates and starts an engine frontend for an existing engine.
func (c *SPDKClient) EngineFrontendCreate(name, volumeName, engineName, frontend string, specSize uint64, targetAddress string,
	ublkQueueDepth, ublkNumberOfQueue int32) (*api.EngineFrontend, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to start engine frontend: missing required parameter name")
	}
	if volumeName == "" {
		return nil, fmt.Errorf("failed to start engine frontend: missing required parameter volumeName")
	}
	if engineName == "" {
		return nil, fmt.Errorf("failed to start engine frontend: missing required parameter engineName")
	}
	if frontend == types.FrontendSPDKTCPBlockdev || frontend == types.FrontendSPDKTCPNvmf {
		if targetAddress == "" {
			return nil, fmt.Errorf("failed to start engine frontend: missing required parameter targetAddress")
		}
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineFrontendCreate(ctx, &spdkrpc.EngineFrontendCreateRequest{
		Name:              name,
		VolumeName:        volumeName,
		EngineName:        engineName,
		SpecSize:          specSize,
		TargetAddress:     targetAddress,
		Frontend:          frontend,
		UblkQueueDepth:    ublkQueueDepth,
		UblkNumberOfQueue: ublkNumberOfQueue,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start engine frontend")
	}

	return api.ProtoEngineFrontendToEngineFrontend(resp), nil
}

// EngineFrontendDelete deletes an engine frontend by name.
func (c *SPDKClient) EngineFrontendDelete(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete engine frontend: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendDelete(ctx, &spdkrpc.EngineFrontendDeleteRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to delete engine frontend %v", name)
}

// EngineFrontendList returns all engine frontends known to the SPDK service.
func (c *SPDKClient) EngineFrontendList() (map[string]*api.EngineFrontend, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineFrontendList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list engine frontends")
	}

	res := map[string]*api.EngineFrontend{}
	for engineFrontendName, ef := range resp.EngineFrontends {
		res[engineFrontendName] = api.ProtoEngineFrontendToEngineFrontend(ef)
	}
	return res, nil
}

// EngineFrontendWatch opens a watch stream for engine frontend change events.
func (c *SPDKClient) EngineFrontendWatch(ctx context.Context) (*api.EngineFrontendStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.EngineFrontendWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open engine frontend watch stream")
	}

	return api.NewEngineFrontendStream(stream), nil
}

// EngineFrontendGet returns the current state of an engine frontend.
func (c *SPDKClient) EngineFrontendGet(name string) (*api.EngineFrontend, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get engine frontend: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineFrontendGet(ctx, &spdkrpc.EngineFrontendGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get engine frontend %v", name)
	}
	return api.ProtoEngineFrontendToEngineFrontend(resp), nil
}

// EngineFrontendSwitchOver repoints an engine frontend to a new engine target.
// The engine IP for ANA state coordination is derived from the targetAddress
// on the server side.
func (c *SPDKClient) EngineFrontendSwitchOver(name, newEngineName, newTargetAddress, switchoverPhase string) error {
	if name == "" {
		return fmt.Errorf("failed to switch over target for engine frontend: missing required parameter name")
	}
	if newTargetAddress == "" {
		return fmt.Errorf("failed to switch over target for engine frontend: missing required parameter newTargetAddress")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendSwitchOver(ctx, &spdkrpc.EngineFrontendSwitchOverRequest{
		Name:            name,
		EngineName:      newEngineName,
		TargetAddress:   newTargetAddress,
		SwitchoverPhase: switchoverPhase,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to switch over target to %s with new engine %s for %s", newTargetAddress, newEngineName, name)
	}

	return nil
}

// EngineFrontendSuspend suspends I/O on an engine frontend.
func (c *SPDKClient) EngineFrontendSuspend(name string) error {
	if name == "" {
		return fmt.Errorf("failed to suspend engine frontend: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendSuspend(ctx, &spdkrpc.EngineFrontendSuspendRequest{
		Name: name,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to suspend engine frontend %v", name)
	}
	return nil
}

// EngineFrontendResume resumes I/O on a suspended engine frontend.
func (c *SPDKClient) EngineFrontendResume(name string) error {
	if name == "" {
		return fmt.Errorf("failed to resume engine frontend: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendResume(ctx, &spdkrpc.EngineFrontendResumeRequest{
		Name: name,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to resume engine frontend %v", name)
	}
	return nil
}

// EngineFrontendExpand expands an engine frontend and orchestrates the underlying engine expansion.
// The EngineExpand path is for internal orchestration; external callers should use EngineFrontendExpand.
func (c *SPDKClient) EngineFrontendExpand(ctx context.Context, name string, size uint64) error {
	if name == "" {
		return fmt.Errorf("failed to expand engine frontend: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(ctx, GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendExpand(ctx, &spdkrpc.EngineFrontendExpandRequest{
		Name: name,
		Size: size,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to expand engine frontend %v", name)
	}
	return nil
}

// EngineFrontendSnapshotCreate creates a snapshot through the engine frontend path.
// The EngineSnapshotCreate path is for internal orchestration; external callers should use EngineFrontendSnapshotCreate.
func (c *SPDKClient) EngineFrontendSnapshotCreate(name, snapshotName string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("failed to create snapshot: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineFrontendSnapshotCreate(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to create snapshot %s", snapshotName)
	}
	return resp.SnapshotName, nil
}

// EngineFrontendSnapshotDelete deletes a snapshot through the engine frontend path.
// The EngineSnapshotDelete path is for internal orchestration; external callers should use EngineFrontendSnapshotDelete.
func (c *SPDKClient) EngineFrontendSnapshotDelete(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to delete engine frontend snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendSnapshotDelete(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to delete engine frontend %s snapshot %s", name, snapshotName)
}

// EngineFrontendSnapshotRevert reverts an engine frontend to the specified snapshot.
func (c *SPDKClient) EngineFrontendSnapshotRevert(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to revert engine frontend snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendSnapshotRevert(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to revert engine frontend %s snapshot %s", name, snapshotName)
}

// EngineFrontendSnapshotPurge purges snapshots through the engine frontend path.
// The EngineSnapshotPurge path is for internal orchestration; external callers should use EngineFrontendSnapshotPurge.
func (c *SPDKClient) EngineFrontendSnapshotPurge(name string) error {
	if name == "" {
		return fmt.Errorf("failed to purge engine frontend: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendSnapshotPurge(ctx, &spdkrpc.SnapshotRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to purge engine frontend %s", name)
}

// EngineFrontendReplicaAdd adds a replica through the engine frontend path.
// The EngineReplicaAdd path is for internal orchestration; external callers should use EngineFrontendReplicaAdd.
func (c *SPDKClient) EngineFrontendReplicaAdd(engineFrontendName, replicaName, replicaAddress string, fastSync bool) error {
	if engineFrontendName == "" {
		return fmt.Errorf("failed to add replica for engine frontend: missing required parameter engineFrontendName")
	}
	if replicaName == "" || replicaAddress == "" {
		return fmt.Errorf("failed to add replica for engine frontend: missing required parameter replicaName or replicaAddress")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineFrontendReplicaAdd(ctx, &spdkrpc.EngineFrontendReplicaAddRequest{
		EngineFrontendName: engineFrontendName,
		ReplicaName:        replicaName,
		ReplicaAddress:     replicaAddress,
		FastSync:           fastSync,
	})
	return errors.Wrapf(err, "failed to add replica %s with address %s by engine frontend %s", replicaName, replicaAddress, engineFrontendName)
}
