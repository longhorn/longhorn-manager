package client

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

// EngineCreate creates and starts an engine instance with the requested replicas.
func (c *SPDKClient) EngineCreate(name, volumeName, frontend string, specSize uint64, replicaAddressMap map[string]string, portCount int32, salvageRequested bool) (*api.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to start engine: missing required parameter name")
	}
	if volumeName == "" {
		return nil, fmt.Errorf("failed to start engine: missing required parameter volumeName")
	}
	if len(replicaAddressMap) == 0 {
		return nil, fmt.Errorf("failed to start engine: missing required parameter replicaAddressMap")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineCreate(ctx, &spdkrpc.EngineCreateRequest{
		Name:              name,
		VolumeName:        volumeName,
		Frontend:          frontend,
		SpecSize:          specSize,
		ReplicaAddressMap: replicaAddressMap,
		PortCount:         portCount,
		SalvageRequested:  salvageRequested,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start engine")
	}

	return api.ProtoEngineToEngine(resp), nil
}

// EngineDelete deletes an engine instance by name.
func (c *SPDKClient) EngineDelete(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineDelete(ctx, &spdkrpc.EngineDeleteRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to delete engine %v", name)
}

// EngineGet returns the current state of an engine.
func (c *SPDKClient) EngineGet(name string) (*api.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineGet(ctx, &spdkrpc.EngineGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get engine %v", name)
	}
	return api.ProtoEngineToEngine(resp), nil
}

// EngineDeleteTarget deletes the exported target for an engine without deleting the engine object itself.
func (c *SPDKClient) EngineDeleteTarget(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete target for engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineDeleteTarget(ctx, &spdkrpc.EngineDeleteTargetRequest{
		Name: name,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to delete target for engine %v", name)
	}
	return nil
}

// EngineList returns all engines known to the SPDK service.
func (c *SPDKClient) EngineList() (map[string]*api.Engine, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list engines")
	}

	res := map[string]*api.Engine{}
	for engineName, e := range resp.Engines {
		res[engineName] = api.ProtoEngineToEngine(e)
	}
	return res, nil
}

// EngineWatch opens a watch stream for engine change events.
func (c *SPDKClient) EngineWatch(ctx context.Context) (*api.EngineStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.EngineWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open engine watch stream")
	}

	return api.NewEngineStream(stream), nil
}

// EngineExpand requests an online expansion of the specified engine.
func (c *SPDKClient) EngineExpand(ctx context.Context, name string, size uint64) error {
	if name == "" {
		return fmt.Errorf("failed to expand engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(ctx, GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineExpand(ctx, &spdkrpc.EngineExpandRequest{
		Name: name,
		Size: size,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to expand engine %v", name)
	}
	return nil
}

// EngineExpandPrecheck validates whether the specified engine can be expanded to the requested size.
func (c *SPDKClient) EngineExpandPrecheck(ctx context.Context, name string, size uint64) error {
	if name == "" {
		return fmt.Errorf("failed to expand engine precheck: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(ctx, GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineExpandPrecheck(ctx, &spdkrpc.EngineExpandPrecheckRequest{
		Name: name,
		Size: size,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to expand engine %v", name)
	}
	return nil
}

// EngineSnapshotCreate creates a snapshot directly on the engine.
func (c *SPDKClient) EngineSnapshotCreate(name, snapshotName string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("failed to create engine snapshot: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineSnapshotCreate(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to create engine %s snapshot %s", name, snapshotName)
	}
	return resp.SnapshotName, nil
}

// EngineSnapshotDelete deletes a snapshot directly from the engine.
func (c *SPDKClient) EngineSnapshotDelete(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to delete engine snapshot: missing required parameter name or snapshotName")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotDelete(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to delete engine %s snapshot %s", name, snapshotName)
}

// EngineSnapshotRevert reverts an engine directly to the specified snapshot.
func (c *SPDKClient) EngineSnapshotRevert(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to revert engine snapshot: missing required parameter name or snapshotName")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotRevert(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to revert engine %s snapshot %s", name, snapshotName)
}

// EngineSnapshotPurge purges purgeable snapshots directly on the engine.
func (c *SPDKClient) EngineSnapshotPurge(name string) error {
	if name == "" {
		return fmt.Errorf("failed to purge engine: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotPurge(ctx, &spdkrpc.SnapshotRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to purge engine %s", name)
}

// EngineSnapshotHash starts or re-runs checksum generation for an engine snapshot.
func (c *SPDKClient) EngineSnapshotHash(name, snapshotName string, rehash bool) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to hash engine snapshot: missing required parameter name or snapshotName")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotHash(ctx, &spdkrpc.SnapshotHashRequest{
		Name:         name,
		SnapshotName: snapshotName,
		Rehash:       rehash,
	})
	return errors.Wrapf(err, "failed to hash engine %s snapshot %s", name, snapshotName)
}

// EngineSnapshotHashStatus returns the current checksum status for an engine snapshot.
func (c *SPDKClient) EngineSnapshotHashStatus(name, snapshotName string) (response *spdkrpc.EngineSnapshotHashStatusResponse, err error) {
	if name == "" || snapshotName == "" {
		return nil, fmt.Errorf("failed to check hash status for engine snapshot: missing required parameter name or snapshotName")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.EngineSnapshotHashStatus(ctx, &spdkrpc.SnapshotHashStatusRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
}

// EngineSnapshotClone clones a snapshot from a source engine into the target engine.
func (c *SPDKClient) EngineSnapshotClone(name, snapshotName, srcEngineName, srcEngineAddress string, cloneMode spdkrpc.CloneMode) error {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: name},
		util.Param{Name: "snapshotName", Value: snapshotName},
		util.Param{Name: "srcEngineName", Value: srcEngineName},
		util.Param{Name: "srcEngineAddress", Value: srcEngineAddress},
	); err != nil {
		return errors.Wrapf(err, "failed to clone snapshot for engine %s, snapshotName %s, srcEngineName %s, srcEngineAddress %s",
			name, snapshotName, srcEngineName, srcEngineAddress)
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotClone(ctx, &spdkrpc.EngineSnapshotCloneRequest{
		Name:             name,
		SnapshotName:     snapshotName,
		SrcEngineName:    srcEngineName,
		SrcEngineAddress: srcEngineAddress,
		CloneMode:        cloneMode,
	})
	return errors.Wrapf(err, "failed to clone snapshot for engine %s, snapshotName %s, srcEngineName %s, srcEngineAddress %s",
		name, snapshotName, srcEngineName, srcEngineAddress)
}

// EngineReplicaAdd calls the full-flow EngineReplicaAdd gRPC on the Engine node.
// When efName and efAddress are non-empty, they are set on the request so
// Engine can call back to the EngineFrontend for suspend/resume.
func (c *SPDKClient) EngineReplicaAdd(engineName, replicaName, replicaAddress string, fastSync bool, efName, efAddress string) error {
	if engineName == "" {
		return fmt.Errorf("failed to add replica for engine: missing required parameter engineName")
	}
	if replicaName == "" || replicaAddress == "" {
		return fmt.Errorf("failed to add replica for engine: missing required parameter replicaName or replicaAddress")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	req := &spdkrpc.EngineReplicaAddRequest{
		EngineName:            engineName,
		ReplicaName:           replicaName,
		ReplicaAddress:        replicaAddress,
		FastSync:              fastSync,
		EngineFrontendName:    efName,
		EngineFrontendAddress: efAddress,
	}

	_, err := client.EngineReplicaAdd(ctx, req)
	return errors.Wrapf(err, "failed to add replica %s with address %s to engine %s", replicaName, replicaAddress, engineName)
}

// EngineReplicaList returns the replicas currently attached to an engine.
func (c *SPDKClient) EngineReplicaList(engineName string) (map[string]*api.Replica, error) {
	if engineName == "" {
		return nil, fmt.Errorf("failed to list replica for engine: missing required parameter engineName")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	resp, err := client.EngineReplicaList(ctx, &spdkrpc.EngineReplicaListRequest{
		EngineName: engineName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list replica for engine: %s", engineName)
	}
	res := map[string]*api.Replica{}
	for replicaName, r := range resp.Replicas {
		res[replicaName] = api.ProtoReplicaToReplica(r)
	}
	return res, nil
}

// EngineReplicaDelete detaches a replica from an engine.
func (c *SPDKClient) EngineReplicaDelete(engineName, replicaName, replicaAddress string) error {
	if engineName == "" {
		return fmt.Errorf("failed to delete replica from engine: missing required parameter engineName")
	}
	if replicaName == "" && replicaAddress == "" {
		return fmt.Errorf("failed to delete replica from engine: missing required parameter replicaName or replicaAddress, at least one of them is required")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineReplicaDelete(ctx, &spdkrpc.EngineReplicaDeleteRequest{
		EngineName:     engineName,
		ReplicaName:    replicaName,
		ReplicaAddress: replicaAddress,
	})
	return errors.Wrapf(err, "failed to delete replica %s with address %s to engine %s", replicaName, replicaAddress, engineName)
}

// EngineBackupCreate starts a backup from the specified engine snapshot.
func (c *SPDKClient) EngineBackupCreate(req *BackupCreateRequest) (*spdkrpc.BackupCreateResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.EngineBackupCreate(ctx, &spdkrpc.BackupCreateRequest{
		SnapshotName:         req.SnapshotName,
		BackupTarget:         req.BackupTarget,
		VolumeName:           req.VolumeName,
		EngineName:           req.EngineName,
		Labels:               req.Labels,
		Credential:           req.Credential,
		BackingImageName:     req.BackingImageName,
		BackingImageChecksum: req.BackingImageChecksum,
		BackupName:           req.BackupName,
		CompressionMethod:    req.CompressionMethod,
		ConcurrentLimit:      req.ConcurrentLimit,
		StorageClassName:     req.StorageClassName,
	})
}

// EngineBackupStatus returns the status of an engine backup.
func (c *SPDKClient) EngineBackupStatus(backupName, engineName, replicaAddress string) (*spdkrpc.BackupStatusResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.EngineBackupStatus(ctx, &spdkrpc.BackupStatusRequest{
		Backup:         backupName,
		EngineName:     engineName,
		ReplicaAddress: replicaAddress,
	})
}

// EngineBackupRestore restores backup data into an engine.
func (c *SPDKClient) EngineBackupRestore(req *BackupRestoreRequest) error {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	recv, err := client.EngineBackupRestore(ctx, &spdkrpc.EngineBackupRestoreRequest{
		BackupUrl:       req.BackupUrl,
		EngineName:      req.EngineName,
		SnapshotName:    req.SnapshotName,
		Credential:      req.Credential,
		ConcurrentLimit: req.ConcurrentLimit,
	})
	if err != nil {
		return err
	}

	if len(recv.Errors) == 0 {
		return nil
	}

	taskErr := util.NewTaskError()
	for replicaAddress, replicaErr := range recv.Errors {
		replicaURL := "tcp://" + replicaAddress
		taskErr.Append(util.NewReplicaError(replicaURL, errors.New(replicaErr)))
	}

	return taskErr
}

// EngineRestoreStatus returns the current restore status for an engine.
func (c *SPDKClient) EngineRestoreStatus(engineName string) (*spdkrpc.RestoreStatusResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.EngineRestoreStatus(ctx, &spdkrpc.RestoreStatusRequest{
		EngineName: engineName,
	})
}
