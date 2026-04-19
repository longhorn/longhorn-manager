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

// ReplicaCreate creates and starts a replica in the specified lvstore.
func (c *SPDKClient) ReplicaCreate(name, lvsName, lvsUUID string, specSize uint64, portCount int32, backingImageName string) (*api.Replica, error) {
	if name == "" || lvsName == "" || lvsUUID == "" {
		return nil, fmt.Errorf("failed to start SPDK replica: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaCreate(ctx, &spdkrpc.ReplicaCreateRequest{
		Name:             name,
		LvsName:          lvsName,
		LvsUuid:          lvsUUID,
		SpecSize:         specSize,
		PortCount:        portCount,
		BackingImageName: backingImageName,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start SPDK replica")
	}

	return api.ProtoReplicaToReplica(resp), nil
}

// ReplicaDelete deletes a replica, optionally cleaning up related SPDK resources.
func (c *SPDKClient) ReplicaDelete(name string, cleanupRequired bool) error {
	if name == "" {
		return fmt.Errorf("failed to delete SPDK replica: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaDelete(ctx, &spdkrpc.ReplicaDeleteRequest{
		Name:            name,
		CleanupRequired: cleanupRequired,
	})
	return errors.Wrapf(err, "failed to delete SPDK replica %v", name)
}

// ReplicaGet returns the current state of a replica.
func (c *SPDKClient) ReplicaGet(name string) (*api.Replica, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get SPDK replica: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaGet(ctx, &spdkrpc.ReplicaGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK replica %v", name)
	}
	return api.ProtoReplicaToReplica(resp), nil
}

// ReplicaExpand requests an online expansion of the specified replica.
func (c *SPDKClient) ReplicaExpand(name string, size uint64) error {
	if name == "" {
		return fmt.Errorf("failed to expand replica: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaExpand(ctx, &spdkrpc.ReplicaExpandRequest{
		Name: name,
		Size: size,
	})
	return errors.Wrapf(err, "failed to expand replica %v", name)
}

// ReplicaList returns all replicas known to the SPDK service.
func (c *SPDKClient) ReplicaList() (map[string]*api.Replica, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK replicas")
	}

	res := map[string]*api.Replica{}
	for replicaName, r := range resp.Replicas {
		res[replicaName] = api.ProtoReplicaToReplica(r)
	}
	return res, nil
}

// ReplicaWatch opens a watch stream for replica change events.
func (c *SPDKClient) ReplicaWatch(ctx context.Context) (*api.ReplicaStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.ReplicaWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open replica watch stream")
	}

	return api.NewReplicaStream(stream), nil
}

// ReplicaSnapshotCreate creates a snapshot directly on a replica.
func (c *SPDKClient) ReplicaSnapshotCreate(name, snapshotName string, opts *api.SnapshotOptions) error {
	if name == "" || snapshotName == "" || opts == nil {
		return fmt.Errorf("failed to create SPDK replica snapshot: missing required parameter name, snapshot name or opts")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	snapshotRequest := spdkrpc.SnapshotRequest{
		Name:              name,
		SnapshotName:      snapshotName,
		UserCreated:       opts.UserCreated,
		SnapshotTimestamp: opts.Timestamp,
	}

	_, err := client.ReplicaSnapshotCreate(ctx, &snapshotRequest)

	return errors.Wrapf(err, "failed to create SPDK replica %s snapshot %s", name, snapshotName)
}

// ReplicaSnapshotDelete deletes a snapshot directly from a replica.
func (c *SPDKClient) ReplicaSnapshotDelete(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to delete SPDK replica snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaSnapshotDelete(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to delete SPDK replica %s snapshot %s", name, snapshotName)
}

// ReplicaSnapshotRevert reverts a replica directly to the specified snapshot.
func (c *SPDKClient) ReplicaSnapshotRevert(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to revert SPDK replica snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaSnapshotRevert(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to revert SPDK replica %s snapshot %s", name, snapshotName)
}

// ReplicaSnapshotPurge purges purgeable snapshots directly on a replica.
func (c *SPDKClient) ReplicaSnapshotPurge(name string) error {
	if name == "" {
		return fmt.Errorf("failed to purge SPDK replica: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaSnapshotPurge(ctx, &spdkrpc.SnapshotRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to purge SPDK replica %s", name)
}

// ReplicaSnapshotHash starts or re-runs checksum generation for a replica snapshot.
func (c *SPDKClient) ReplicaSnapshotHash(name, snapshotName string, rehash bool) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to hash SPDK replica snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaSnapshotHash(ctx, &spdkrpc.SnapshotHashRequest{
		Name:         name,
		SnapshotName: snapshotName,
		Rehash:       rehash,
	})
	return errors.Wrapf(err, "failed to hash SPDK replica %s snapshot %s", name, snapshotName)
}

// ReplicaSnapshotHashStatus returns the current checksum status for a replica snapshot.
func (c *SPDKClient) ReplicaSnapshotHashStatus(name, snapshotName string) (*spdkrpc.ReplicaSnapshotHashStatusResponse, error) {
	if name == "" || snapshotName == "" {
		return nil, fmt.Errorf("failed to check hash status for SPDK replica snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaSnapshotHashStatus(ctx, &spdkrpc.SnapshotHashStatusRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
}

// ReplicaSnapshotCloneDstStart starts snapshot clone preparation on the destination replica.
func (c *SPDKClient) ReplicaSnapshotCloneDstStart(name, snapshotName, srcReplicaName, srcReplicaAddress string, cloneMode spdkrpc.CloneMode) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to do ReplicaSnapshotCloneDstStart: replica: %v, snapshot: %v", name, snapshotName)
	}()
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: name},
		util.Param{Name: "snapshotName", Value: snapshotName},
		util.Param{Name: "srcReplicaName", Value: srcReplicaName},
		util.Param{Name: "srcReplicaAddress", Value: srcReplicaAddress},
	); err != nil {
		return err
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err = client.ReplicaSnapshotCloneDstStart(ctx, &spdkrpc.ReplicaSnapshotCloneDstStartRequest{
		Name:              name,
		SnapshotName:      snapshotName,
		SrcReplicaName:    srcReplicaName,
		SrcReplicaAddress: srcReplicaAddress,
		CloneMode:         cloneMode,
	})
	return err
}

// ReplicaSnapshotCloneDstStatusCheck returns snapshot clone progress on the destination replica.
func (c *SPDKClient) ReplicaSnapshotCloneDstStatusCheck(name string) (resp *api.ReplicaSnapshotCloneDstStatus, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to do ReplicaSnapshotCloneDstStatusCheck: replica name %v", name)
	}()
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: name},
	); err != nil {
		return nil, err
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	rpcResp, err := client.ReplicaSnapshotCloneDstStatusCheck(ctx, &spdkrpc.ReplicaSnapshotCloneDstStatusCheckRequest{
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	return api.ProtoReplicaSnapshotCloneDstStatusCheckResponseToSnapshotCloneDstStatus(rpcResp), nil
}

// ReplicaSnapshotCloneSrcStart starts snapshot clone work on the source replica.
func (c *SPDKClient) ReplicaSnapshotCloneSrcStart(name, snapshotName, dstReplicaName, dstCloningLvolAddress string, mode spdkrpc.CloneMode) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to do ReplicaSnapshotCloneSrcStart. Replica name: %v, snapshot name: %v", name, snapshotName)
	}()
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: name},
		util.Param{Name: "snapshotName", Value: snapshotName},
		util.Param{Name: "dstReplicaName", Value: dstReplicaName},
	); err != nil {
		return err
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err = client.ReplicaSnapshotCloneSrcStart(ctx, &spdkrpc.ReplicaSnapshotCloneSrcStartRequest{
		Name:                  name,
		SnapshotName:          snapshotName,
		DstReplicaName:        dstReplicaName,
		DstCloningLvolAddress: dstCloningLvolAddress,
		CloneMode:             mode,
	})
	return err
}

// ReplicaSnapshotCloneSrcStatusCheck returns snapshot clone progress on the source replica.
func (c *SPDKClient) ReplicaSnapshotCloneSrcStatusCheck(name, snapshotName, dstReplicaName string) (resp *api.ReplicaSnapshotCloneSrcStatus, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to do ReplicaSnapshotCloneSrcStatusCheck. Replica name: %v, snapshot name: %v", name, snapshotName)
	}()
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: name},
		util.Param{Name: "snapshotName", Value: snapshotName},
		util.Param{Name: "dstReplicaName", Value: dstReplicaName},
	); err != nil {
		return nil, err
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	rpcResp, err := client.ReplicaSnapshotCloneSrcStatusCheck(ctx, &spdkrpc.ReplicaSnapshotCloneSrcStatusCheckRequest{
		Name:           name,
		SnapshotName:   snapshotName,
		DstReplicaName: dstReplicaName,
	})
	if err != nil {
		return nil, err
	}
	return api.ProtoReplicaSnapshotCloneSrcStatusCheckResponseToSnapshotCloneSrcStatus(rpcResp), nil
}

// ReplicaSnapshotCloneSrcFinish finalizes source-side snapshot clone state and cleans up clone metadata.
func (c *SPDKClient) ReplicaSnapshotCloneSrcFinish(name, dstReplicaName string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to do ReplicaSnapshotCloneSrcFinish. replica: %v, src replica: %v", dstReplicaName, name)
	}()
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: name},
		util.Param{Name: "dstReplicaName", Value: dstReplicaName},
	); err != nil {
		return err
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err = client.ReplicaSnapshotCloneSrcFinish(ctx, &spdkrpc.ReplicaSnapshotCloneSrcFinishRequest{
		Name:           name,
		DstReplicaName: dstReplicaName,
	})
	return err
}

// ReplicaSnapshotRangeHashGet returns range hashes for the specified clusters of a replica snapshot.
func (c *SPDKClient) ReplicaSnapshotRangeHashGet(name, snapshotName string, clusterStartIndex, clusterCount uint64) (*spdkrpc.ReplicaSnapshotRangeHashGetResponse, error) {
	if name == "" || snapshotName == "" {
		return nil, fmt.Errorf("failed to get range hash for SPDK replica snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaSnapshotRangeHashGet(ctx, &spdkrpc.ReplicaSnapshotRangeHashGetRequest{
		Name:              name,
		SnapshotName:      snapshotName,
		ClusterStartIndex: clusterStartIndex,
		ClusterCount:      clusterCount,
	})
}

// ReplicaRebuildingSrcStart asks the source replica to check the parent snapshot of the head and expose it as a NVMf bdev if necessary.
// If the source replica and the destination replica have different IPs, the API will expose the snapshot lvol as a NVMf bdev and return the address <IP>:<Port>.
// Otherwise, the API will directly return the snapshot lvol alias.
func (c *SPDKClient) ReplicaRebuildingSrcStart(srcReplicaName, dstReplicaName, dstReplicaAddress, exposedSnapshotName string) (exposedSnapshotLvolAddress string, err error) {
	if srcReplicaName == "" {
		return "", fmt.Errorf("failed to start replica rebuilding src: missing required parameter src replica name")
	}
	if dstReplicaName == "" || dstReplicaAddress == "" {
		return "", fmt.Errorf("failed to start replica rebuilding src: missing required parameter dst replica name or address")
	}
	if exposedSnapshotName == "" {
		return "", fmt.Errorf("failed to start replica rebuilding src: missing required parameter exposed snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaRebuildingSrcStart(ctx, &spdkrpc.ReplicaRebuildingSrcStartRequest{
		Name:                srcReplicaName,
		DstReplicaName:      dstReplicaName,
		DstReplicaAddress:   dstReplicaAddress,
		ExposedSnapshotName: exposedSnapshotName,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to start replica rebuilding src %s for rebuilding replica %s(%s)", srcReplicaName, dstReplicaName, dstReplicaAddress)
	}
	return resp.ExposedSnapshotLvolAddress, nil
}

// ReplicaRebuildingSrcFinish asks the source replica to stop exposing the parent snapshot of the head, if needed,
// and to clean up destination-replica rebuild state. It does not detach the destination rebuilding lvol.
func (c *SPDKClient) ReplicaRebuildingSrcFinish(srcReplicaName, dstReplicaName string) error {
	if srcReplicaName == "" {
		return fmt.Errorf("failed to finish replica rebuilding src: missing required parameter src replica name")
	}
	if dstReplicaName == "" {
		return fmt.Errorf("failed to finish replica rebuilding src: missing required parameter dst replica name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaRebuildingSrcFinish(ctx, &spdkrpc.ReplicaRebuildingSrcFinishRequest{
		Name:           srcReplicaName,
		DstReplicaName: dstReplicaName,
	})
	return errors.Wrapf(err, "failed to finish replica rebuilding src %s for rebuilding replica %s", srcReplicaName, dstReplicaName)
}

// ReplicaRebuildingSrcShallowCopyStart starts a shallow copy from the source snapshot lvol to the destination rebuilding lvol.
func (c *SPDKClient) ReplicaRebuildingSrcShallowCopyStart(srcReplicaName, snapshotName, dstRebuildingLvolAddress string) error {
	if srcReplicaName == "" || snapshotName == "" {
		return fmt.Errorf("failed to start rebuilding src replica shallow copy: missing required parameter replica name or snapshot name")
	}
	if dstRebuildingLvolAddress == "" {
		return fmt.Errorf("failed to start rebuilding src replica shallow copy: missing required parameter dst rebuilding lvol address")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceMedTimeout)
	defer cancel()

	_, err := client.ReplicaRebuildingSrcShallowCopyStart(ctx, &spdkrpc.ReplicaRebuildingSrcShallowCopyStartRequest{
		Name:                     srcReplicaName,
		SnapshotName:             snapshotName,
		DstRebuildingLvolAddress: dstRebuildingLvolAddress,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to start rebuilding src replica %v shallow copy snapshot %v", srcReplicaName, snapshotName)
	}
	return nil
}

// ReplicaRebuildingSrcRangeShallowCopyStart starts a delta shallow copy for the specified source clusters.
func (c *SPDKClient) ReplicaRebuildingSrcRangeShallowCopyStart(srcReplicaName, snapshotName, dstRebuildingLvolAddress string, mismatchingClusterList []uint64) error {
	if srcReplicaName == "" || snapshotName == "" {
		return fmt.Errorf("failed to start rebuilding src replica range shallow copy: missing required parameter replica name or snapshot name")
	}
	if dstRebuildingLvolAddress == "" {
		return fmt.Errorf("failed to start rebuilding src replica range shallow copy: missing required parameter dst rebuilding lvol address")
	}
	if len(mismatchingClusterList) == 0 {
		return fmt.Errorf("failed to start rebuilding src replica range shallow copy: missing required parameter mismatching cluster list")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceMedTimeout)
	defer cancel()

	_, err := client.ReplicaRebuildingSrcRangeShallowCopyStart(ctx, &spdkrpc.ReplicaRebuildingSrcRangeShallowCopyStartRequest{
		Name:                     srcReplicaName,
		SnapshotName:             snapshotName,
		DstRebuildingLvolAddress: dstRebuildingLvolAddress,
		MismatchingClusterList:   mismatchingClusterList,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to start rebuilding src replica %v range shallow copy snapshot %v", srcReplicaName, snapshotName)
	}
	return nil
}

// ReplicaRebuildingSrcShallowCopyCheck returns source-side shallow copy progress for a rebuilding snapshot.
func (c *SPDKClient) ReplicaRebuildingSrcShallowCopyCheck(srcReplicaName, dstReplicaName, snapshotName string) (state string, handledClusters, totalClusters uint64, errorMsg string, err error) {
	if srcReplicaName == "" || dstReplicaName == "" {
		return "", 0, 0, "", fmt.Errorf("failed to check rebuilding src replica shallow copy: missing required parameter src replica name or dst replica name")
	}
	if snapshotName == "" {
		return "", 0, 0, "", fmt.Errorf("failed to check rebuilding src replica shallow copy: missing required parameter snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceMedTimeout)
	defer cancel()

	resp, err := client.ReplicaRebuildingSrcShallowCopyCheck(ctx, &spdkrpc.ReplicaRebuildingSrcShallowCopyCheckRequest{
		Name:           srcReplicaName,
		DstReplicaName: dstReplicaName,
		SnapshotName:   snapshotName,
	})
	if err != nil {
		return "", 0, 0, "", errors.Wrapf(err, "failed to check rebuilding src replica %v shallow copy snapshot %v for dst replica %s", srcReplicaName, snapshotName, dstReplicaName)
	}
	return resp.State, resp.HandledClusters, resp.TotalClusters, resp.ErrorMsg, nil
}

// ReplicaRebuildingDstStart prepares the destination replica for rebuilding from a source snapshot.
// It creates a new head lvol, exposes it as needed, and returns the destination head lvol address.
// The external snapshot address is a local alias when source and destination share the same host,
// otherwise it is the exported NVMf address of the source snapshot lvol.
func (c *SPDKClient) ReplicaRebuildingDstStart(replicaName, srcReplicaName, srcReplicaAddress, externalSnapshotName, externalSnapshotAddress string, rebuildingSnapshotList []*api.Lvol) (dstHeadLvolAddress string, err error) {
	if replicaName == "" {
		return "", fmt.Errorf("failed to start replica rebuilding dst: missing required parameter replica name")
	}
	if srcReplicaName == "" || srcReplicaAddress == "" {
		return "", fmt.Errorf("failed to start replica rebuilding dst: missing required parameter src replica name or address")
	}
	if externalSnapshotName == "" || externalSnapshotAddress == "" {
		return "", fmt.Errorf("failed to start replica rebuilding dst: missing required parameter external snapshot name or address")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	var protoRebuildingSnapshotList []*spdkrpc.Lvol
	for _, snapshot := range rebuildingSnapshotList {
		protoRebuildingSnapshotList = append(protoRebuildingSnapshotList, api.LvolToProtoLvol(snapshot))
	}
	resp, err := client.ReplicaRebuildingDstStart(ctx, &spdkrpc.ReplicaRebuildingDstStartRequest{
		Name:                    replicaName,
		SrcReplicaName:          srcReplicaName,
		SrcReplicaAddress:       srcReplicaAddress,
		ExternalSnapshotName:    externalSnapshotName,
		ExternalSnapshotAddress: externalSnapshotAddress,
		RebuildingSnapshotList:  protoRebuildingSnapshotList,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to start replica rebuilding dst %s", replicaName)
	}
	return resp.DstHeadLvolAddress, nil
}

// ReplicaRebuildingDstFinish finalizes rebuild state on the destination replica.
// It reconstructs the snapshot tree and active chain, then detaches the external source snapshot if needed.
// The caller must guarantee that there is no I/O during the parent switch.
func (c *SPDKClient) ReplicaRebuildingDstFinish(replicaName string) error {
	if replicaName == "" {
		return fmt.Errorf("failed to finish replica rebuilding dst: missing required parameter replica name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaRebuildingDstFinish(ctx, &spdkrpc.ReplicaRebuildingDstFinishRequest{
		Name: replicaName,
	})
	return errors.Wrapf(err, "failed to finish replica rebuilding dst %s", replicaName)
}

// ReplicaRebuildingDstShallowCopyStart starts shallow copy work on the destination replica.
func (c *SPDKClient) ReplicaRebuildingDstShallowCopyStart(dstReplicaName, snapshotName string, fastSync bool) error {
	if dstReplicaName == "" {
		return fmt.Errorf("failed to start rebuilding dst replica shallow copy: missing required parameter dst replica name")
	}
	if snapshotName == "" {
		return fmt.Errorf("failed to start rebuilding dst replica shallow copy: missing required parameter snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceMedTimeout)
	defer cancel()

	_, err := client.ReplicaRebuildingDstShallowCopyStart(ctx, &spdkrpc.ReplicaRebuildingDstShallowCopyStartRequest{
		Name:         dstReplicaName,
		SnapshotName: snapshotName,
		FastSync:     fastSync,
	})
	return errors.Wrapf(err, "failed to start rebuilding dst replica %v shallow copy snapshot %v", dstReplicaName, snapshotName)
}

// ReplicaRebuildingDstShallowCopyCheck returns destination-side shallow copy progress.
func (c *SPDKClient) ReplicaRebuildingDstShallowCopyCheck(dstReplicaName string) (resp *api.ReplicaRebuildingStatus, err error) {
	if dstReplicaName == "" {
		return nil, fmt.Errorf("failed to check rebuilding dst replica shallow copy: missing required parameter dst replica name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceMedTimeout)
	defer cancel()

	rpcResp, err := client.ReplicaRebuildingDstShallowCopyCheck(ctx, &spdkrpc.ReplicaRebuildingDstShallowCopyCheckRequest{
		Name: dstReplicaName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check rebuilding dst replica %v shallow copy snapshot", dstReplicaName)
	}
	return api.ProtoShallowCopyStatusToReplicaRebuildingStatus(dstReplicaName, c.serviceURL, rpcResp), nil
}

// ReplicaRebuildingDstSnapshotCreate creates a rebuilding snapshot on the destination replica.
func (c *SPDKClient) ReplicaRebuildingDstSnapshotCreate(name, snapshotName string, opts *api.SnapshotOptions) error {
	if name == "" || snapshotName == "" || opts == nil {
		return fmt.Errorf("failed to create dst SPDK replica rebuilding snapshot: missing required parameter name, snapshot name or opts")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	snapshotRequest := spdkrpc.SnapshotRequest{
		Name:              name,
		SnapshotName:      snapshotName,
		UserCreated:       opts.UserCreated,
		SnapshotTimestamp: opts.Timestamp,
	}

	_, err := client.ReplicaRebuildingDstSnapshotCreate(ctx, &snapshotRequest)
	return errors.Wrapf(err, "failed to create dst SPDK replica %s rebuilding snapshot %s", name, snapshotName)
}

// ReplicaRebuildingDstSetQosLimit sets a QoS limit (in MB/s) on the destination replica
// during the shallow copy (rebuilding) process. The limit controls write throughput to reduce rebuild impact.
// A QoS limit of 0 disables throttling (i.e., unlimited bandwidth).
func (c *SPDKClient) ReplicaRebuildingDstSetQosLimit(replicaName string, qosLimitMbps int64) error {
	if replicaName == "" {
		return fmt.Errorf("failed to set QoS on replica: missing replica name")
	}
	if qosLimitMbps < 0 {
		return fmt.Errorf("invalid QoS limit: must not be negative, got %d", qosLimitMbps)
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceMedTimeout)
	defer cancel()

	_, err := client.ReplicaRebuildingDstSetQosLimit(ctx, &spdkrpc.ReplicaRebuildingDstSetQosLimitRequest{
		Name:         replicaName,
		QosLimitMbps: qosLimitMbps,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to set QoS limit %d MB/s on replica %s", qosLimitMbps, replicaName)
	}

	return nil
}

// ReplicaBackupCreate starts a backup from the specified replica snapshot.
func (c *SPDKClient) ReplicaBackupCreate(req *BackupCreateRequest) (*spdkrpc.BackupCreateResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaBackupCreate(ctx, &spdkrpc.BackupCreateRequest{
		BackupName:           req.BackupName,
		SnapshotName:         req.SnapshotName,
		BackupTarget:         req.BackupTarget,
		VolumeName:           req.VolumeName,
		ReplicaName:          req.ReplicaName,
		Size:                 int64(req.Size),
		Labels:               req.Labels,
		Credential:           req.Credential,
		BackingImageName:     req.BackingImageName,
		BackingImageChecksum: req.BackingImageChecksum,
		CompressionMethod:    req.CompressionMethod,
		ConcurrentLimit:      req.ConcurrentLimit,
		StorageClassName:     req.StorageClassName,
	})
}

// ReplicaBackupStatus returns the status of a replica backup.
func (c *SPDKClient) ReplicaBackupStatus(backupName string) (*spdkrpc.BackupStatusResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaBackupStatus(ctx, &spdkrpc.BackupStatusRequest{
		Backup: backupName,
	})
}

// ReplicaBackupRestore restores backup data into a replica.
func (c *SPDKClient) ReplicaBackupRestore(req *BackupRestoreRequest) error {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaBackupRestore(ctx, &spdkrpc.ReplicaBackupRestoreRequest{
		BackupUrl:       req.BackupUrl,
		ReplicaName:     req.ReplicaName,
		SnapshotName:    req.SnapshotName,
		Credential:      req.Credential,
		ConcurrentLimit: req.ConcurrentLimit,
	})
	return err
}

// ReplicaRestoreStatus returns the current restore status for a replica.
func (c *SPDKClient) ReplicaRestoreStatus(replicaName string) (*spdkrpc.ReplicaRestoreStatusResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaRestoreStatus(ctx, &spdkrpc.ReplicaRestoreStatusRequest{
		ReplicaName: replicaName,
	})
}
