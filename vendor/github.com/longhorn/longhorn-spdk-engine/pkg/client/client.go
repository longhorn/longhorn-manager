package client

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

func (c *SPDKServiceContext) Close() error {
	if c.cc != nil {
		if err := c.cc.Close(); err != nil {
			return err
		}
		c.cc = nil
	}
	return nil
}

func (c *SPDKClient) getSPDKServiceClient() spdkrpc.SPDKServiceClient {
	return c.service
}

func NewSPDKClient(serviceURL string) (*SPDKClient, error) {
	getSPDKServiceContext := func(serviceUrl string) (SPDKServiceContext, error) {
		connection, err := grpc.NewClient(serviceUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return SPDKServiceContext{}, errors.Wrapf(err, "cannot connect to SPDKService %v", serviceUrl)
		}

		return SPDKServiceContext{
			cc:      connection,
			service: spdkrpc.NewSPDKServiceClient(connection),
		}, nil
	}

	serviceContext, err := getSPDKServiceContext(serviceURL)
	if err != nil {
		return nil, err
	}

	return &SPDKClient{
		serviceURL:         serviceURL,
		SPDKServiceContext: serviceContext,
	}, nil
}

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

func (c *SPDKClient) ReplicaWatch(ctx context.Context) (*api.ReplicaStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.ReplicaWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open replica watch stream")
	}

	return api.NewReplicaStream(stream), nil
}

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

// ReplicaRebuildingSrcFinish asks the source replica to stop exposing the parent snapshot of the head (if necessary) and clean up the dst replica related cache
// It's not responsible for detaching rebuilding lvol of the dst replica
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

// ReplicaRebuildingSrcShallowCopyStart asks the src replica to start a shallow copy from its snapshot lvol to the dst rebuilding lvol.
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

// ReplicaRebuildingSrcRangeShallowCopyStart asks the src replica to start a range/delta shallow copy from the specified clusters of its snapshot lvol to the dst rebuilding lvol.
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

// ReplicaRebuildingSrcShallowCopyCheck asks the src replica to check the shallow copy progress and status via the snapshot name
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

// ReplicaRebuildingDstStart asks the dst replica to create a new head lvol based on the external snapshot of the src replica and blindly expose it as a NVMf bdev.
// It returns the new head lvol address <IP>:<Port>.
// Notice that input `externalSnapshotAddress` is the alias of the src snapshot lvol if src and dst have on the same IP, otherwise it's the NVMf address of the src snapshot lvol.
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

// ReplicaRebuildingDstFinish asks the dst replica to reconstruct its snapshot tree and active chain, then detach that external src snapshot (if necessary).
// The engine should guarantee that there is no IO during the parent switch.
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

func (c *SPDKClient) ReplicaRebuildingDstShallowCopyStart(dstReplicaName, snapshotName string) error {
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
	})
	return errors.Wrapf(err, "failed to start rebuilding dst replica %v shallow copy snapshot %v", dstReplicaName, snapshotName)
}

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

func (c *SPDKClient) EngineCreate(name, volumeName, frontend string, specSize uint64, replicaAddressMap map[string]string, portCount int32,
	initiatorAddress, targetAddress string, salvageRequested bool, ublkQueueDepth, ublkNumberOfQueue int32) (*api.Engine, error) {
	if name == "" || volumeName == "" || len(replicaAddressMap) == 0 {
		return nil, fmt.Errorf("failed to start SPDK engine: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineCreate(ctx, &spdkrpc.EngineCreateRequest{
		Name:              name,
		VolumeName:        volumeName,
		SpecSize:          specSize,
		ReplicaAddressMap: replicaAddressMap,
		Frontend:          frontend,
		PortCount:         portCount,
		TargetAddress:     targetAddress,
		InitiatorAddress:  initiatorAddress,
		SalvageRequested:  salvageRequested,
		UblkQueueDepth:    ublkQueueDepth,
		UblkNumberOfQueue: ublkNumberOfQueue,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start SPDK engine")
	}

	return api.ProtoEngineToEngine(resp), nil
}

func (c *SPDKClient) EngineDelete(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete SPDK engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineDelete(ctx, &spdkrpc.EngineDeleteRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to delete SPDK engine %v", name)
}

func (c *SPDKClient) EngineGet(name string) (*api.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get SPDK engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineGet(ctx, &spdkrpc.EngineGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK engine %v", name)
	}
	return api.ProtoEngineToEngine(resp), nil
}

func (c *SPDKClient) EngineSuspend(name string) error {
	if name == "" {
		return fmt.Errorf("failed to suspend engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSuspend(ctx, &spdkrpc.EngineSuspendRequest{
		Name: name,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to suspend engine %v", name)
	}
	return nil
}

func (c *SPDKClient) EngineResume(name string) error {
	if name == "" {
		return fmt.Errorf("failed to resume engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineResume(ctx, &spdkrpc.EngineResumeRequest{
		Name: name,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to resume engine %v", name)
	}
	return nil
}

func (c *SPDKClient) EngineSwitchOverTarget(name, targetAddress string) error {
	if name == "" {
		return fmt.Errorf("failed to switch over target for engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSwitchOverTarget(ctx, &spdkrpc.EngineSwitchOverTargetRequest{
		Name:          name,
		TargetAddress: targetAddress,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to switch over target for engine %v", name)
	}
	return nil
}

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

func (c *SPDKClient) EngineList() (map[string]*api.Engine, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK engines")
	}

	res := map[string]*api.Engine{}
	for engineName, e := range resp.Engines {
		res[engineName] = api.ProtoEngineToEngine(e)
	}
	return res, nil
}

func (c *SPDKClient) EngineWatch(ctx context.Context) (*api.EngineStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.EngineWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open engine watch stream")
	}

	return api.NewEngineStream(stream), nil
}

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

func (c *SPDKClient) EngineSnapshotCreate(name, snapshotName string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("failed to create SPDK engine snapshot: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineSnapshotCreate(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to create SPDK engine %s snapshot %s", name, snapshotName)
	}
	return resp.SnapshotName, nil
}

func (c *SPDKClient) EngineSnapshotDelete(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to delete SPDK engine snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotDelete(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to delete SPDK engine %s snapshot %s", name, snapshotName)
}

func (c *SPDKClient) EngineSnapshotRevert(name, snapshotName string) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to revert SPDK engine snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotRevert(ctx, &spdkrpc.SnapshotRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
	return errors.Wrapf(err, "failed to revert SPDK engine %s snapshot %s", name, snapshotName)
}

func (c *SPDKClient) EngineSnapshotPurge(name string) error {
	if name == "" {
		return fmt.Errorf("failed to purge SPDK engine: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotPurge(ctx, &spdkrpc.SnapshotRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to purge SPDK engine %s", name)
}

func (c *SPDKClient) EngineSnapshotHash(name, snapshotName string, rehash bool) error {
	if name == "" || snapshotName == "" {
		return fmt.Errorf("failed to hash SPDK engine snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineSnapshotHash(ctx, &spdkrpc.SnapshotHashRequest{
		Name:         name,
		SnapshotName: snapshotName,
		Rehash:       rehash,
	})
	return errors.Wrapf(err, "failed to hash SPDK engine %s snapshot %s", name, snapshotName)
}

func (c *SPDKClient) EngineSnapshotHashStatus(name, snapshotName string) (response *spdkrpc.EngineSnapshotHashStatusResponse, err error) {
	if name == "" || snapshotName == "" {
		return nil, fmt.Errorf("failed to check hash status for SPDK engine snapshot: missing required parameter name or snapshot name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.EngineSnapshotHashStatus(ctx, &spdkrpc.SnapshotHashStatusRequest{
		Name:         name,
		SnapshotName: snapshotName,
	})
}

func (c *SPDKClient) EngineSnapshotClone(name, snapshotName, srcEngineName, srcEngineAddress string, cloneMode spdkrpc.CloneMode) error {
	if err := util.VerifyParams(
		util.Param{Name: "name", Value: name},
		util.Param{Name: "snapshotName", Value: snapshotName},
		util.Param{Name: "srcEngineName", Value: srcEngineName},
		util.Param{Name: "srcEngineAddress", Value: srcEngineAddress},
	); err != nil {
		return errors.Wrap(err, "failed to clone snapshot for SPDK engine")
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
	return errors.Wrapf(err, "failed to clone snapshot for SPDK engine: name %s, snapshotName %s, "+
		"srcEngineName %s, srcEngineAddress %v", name, snapshotName, srcEngineName, srcEngineAddress)
}

func (c *SPDKClient) EngineReplicaAdd(engineName, replicaName, replicaAddress string) error {
	if engineName == "" {
		return fmt.Errorf("failed to add replica for SPDK engine: missing required parameter engine name")
	}
	if replicaName == "" || replicaAddress == "" {
		return fmt.Errorf("failed to add replica for SPDK engine: missing required parameter replica name or address")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineReplicaAdd(ctx, &spdkrpc.EngineReplicaAddRequest{
		EngineName:     engineName,
		ReplicaName:    replicaName,
		ReplicaAddress: replicaAddress,
	})
	return errors.Wrapf(err, "failed to add replica %s with address %s to engine %s", replicaName, replicaAddress, engineName)
}

func (c *SPDKClient) EngineReplicaList(engineName string) (map[string]*api.Replica, error) {
	if engineName == "" {
		return nil, fmt.Errorf("failed to list replica for SPDK engine: missing required parameter engine name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	resp, err := client.EngineReplicaList(ctx, &spdkrpc.EngineReplicaListRequest{
		EngineName: engineName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list replica for SPDK engine: %s", engineName)
	}
	res := map[string]*api.Replica{}
	for replicaName, r := range resp.Replicas {
		res[replicaName] = api.ProtoReplicaToReplica(r)
	}
	return res, nil
}

func (c *SPDKClient) EngineReplicaDelete(engineName, replicaName, replicaAddress string) error {
	if engineName == "" {
		return fmt.Errorf("failed to delete replica from SPDK engine: missing required parameter engine name")
	}
	if replicaName == "" && replicaAddress == "" {
		return fmt.Errorf("failed to delete replica from SPDK engine: missing required parameter replica name or address, at least one of them is required")
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

func (c *SPDKClient) ReplicaBackupStatus(backupName string) (*spdkrpc.BackupStatusResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaBackupStatus(ctx, &spdkrpc.BackupStatusRequest{
		Backup: backupName,
	})
}

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

func (c *SPDKClient) EngineRestoreStatus(engineName string) (*spdkrpc.RestoreStatusResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.EngineRestoreStatus(ctx, &spdkrpc.RestoreStatusRequest{
		EngineName: engineName,
	})
}

func (c *SPDKClient) ReplicaRestoreStatus(replicaName string) (*spdkrpc.ReplicaRestoreStatusResponse, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.ReplicaRestoreStatus(ctx, &spdkrpc.ReplicaRestoreStatusRequest{
		ReplicaName: replicaName,
	})
}

func (c *SPDKClient) BackingImageCreate(name, backingImageUUID, lvsUUID string, size uint64, checksum string, fromAddress string, srcLvsUUID string) (*api.BackingImage, error) {
	if name == "" || backingImageUUID == "" || checksum == "" || lvsUUID == "" || size == 0 {
		return nil, fmt.Errorf("failed to start SPDK backing image: missing required parameters")
	}
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageCreate(ctx, &spdkrpc.BackingImageCreateRequest{
		Name:             name,
		BackingImageUuid: backingImageUUID,
		LvsUuid:          lvsUUID,
		Size:             size,
		Checksum:         checksum,
		FromAddress:      fromAddress,
		SrcLvsUuid:       srcLvsUUID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start SPDK backing image")
	}
	return api.ProtoBackingImageToBackingImage(resp), nil
}

func (c *SPDKClient) BackingImageDelete(name, lvsUUID string) error {
	if name == "" || lvsUUID == "" {
		return fmt.Errorf("failed to delete SPDK backingImage: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.BackingImageDelete(ctx, &spdkrpc.BackingImageDeleteRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	return errors.Wrapf(err, "failed to delete SPDK backing image %v", name)
}

func (c *SPDKClient) BackingImageGet(name, lvsUUID string) (*api.BackingImage, error) {
	if name == "" || lvsUUID == "" {
		return nil, fmt.Errorf("failed to get SPDK BackingImage: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageGet(ctx, &spdkrpc.BackingImageGetRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK backing image %v", name)
	}
	return api.ProtoBackingImageToBackingImage(resp), nil
}

func (c *SPDKClient) BackingImageList() (map[string]*api.BackingImage, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK backing images")
	}

	res := map[string]*api.BackingImage{}
	for name, backingImage := range resp.BackingImages {
		res[name] = api.ProtoBackingImageToBackingImage(backingImage)
	}
	return res, nil
}

func (c *SPDKClient) BackingImageWatch(ctx context.Context) (*api.BackingImageStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.BackingImageWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open backing image watch stream")
	}

	return api.NewBackingImageStream(stream), nil
}

func (c *SPDKClient) BackingImageExpose(name, lvsUUID string) (exposedSnapshotLvolAddress string, err error) {
	if name == "" || lvsUUID == "" {
		return "", fmt.Errorf("failed to expose SPDK backing image: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageExpose(ctx, &spdkrpc.BackingImageGetRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to expose SPDK backing image %v in lvstore: %v", name, lvsUUID)
	}
	return resp.ExposedSnapshotLvolAddress, nil
}

func (c *SPDKClient) BackingImageUnexpose(name, lvsUUID string) error {
	if name == "" || lvsUUID == "" {
		return fmt.Errorf("failed to unexpose SPDK backing image: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.BackingImageUnexpose(ctx, &spdkrpc.BackingImageGetRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to unexpose SPDK backing image %v in lvstore %v", name, lvsUUID)
	}
	return nil
}

// DiskCreate creates a disk with the given name and path.
// diskUUID is optional, if not provided, it indicates the disk is newly added.
func (c *SPDKClient) DiskCreate(diskName, diskUUID, diskPath, diskDriver string, blockSize int64) (*spdkrpc.Disk, error) {
	if diskName == "" || diskPath == "" {
		return nil, fmt.Errorf("failed to create disk: missing required parameters")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.DiskCreate(ctx, &spdkrpc.DiskCreateRequest{
		DiskName:   diskName,
		DiskUuid:   diskUUID,
		DiskPath:   diskPath,
		BlockSize:  blockSize,
		DiskDriver: diskDriver,
	})
}

func (c *SPDKClient) DiskGet(diskName, diskPath, diskDriver string) (*spdkrpc.Disk, error) {
	if diskName == "" {
		return nil, fmt.Errorf("failed to get disk info: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.DiskGet(ctx, &spdkrpc.DiskGetRequest{
		DiskName:   diskName,
		DiskPath:   diskPath,
		DiskDriver: diskDriver,
	})
}

func (c *SPDKClient) DiskDelete(diskName, diskUUID, diskPath, diskDriver string) error {
	if diskName == "" {
		return fmt.Errorf("failed to delete disk: missing required parameter disk name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.DiskDelete(ctx, &spdkrpc.DiskDeleteRequest{
		DiskName:   diskName,
		DiskUuid:   diskUUID,
		DiskPath:   diskPath,
		DiskDriver: diskDriver,
	})
	return err
}

// DiskHealthGet retrieves the health info for a specified disk.
func (c *SPDKClient) DiskHealthGet(diskName, diskPath, diskDriver string) (*spdkrpc.DiskHealthGetResponse, error) {
	if diskName == "" {
		return nil, fmt.Errorf("failed to get disk health: missing required parameter 'disk name'")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	req := &spdkrpc.DiskHealthGetRequest{
		DiskName:   diskName,
		DiskDriver: diskDriver,
		DiskPath:   diskPath,
	}
	return client.DiskHealthGet(ctx, req)
}

func (c *SPDKClient) LogSetLevel(level string) error {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.LogSetLevel(ctx, &spdkrpc.LogSetLevelRequest{
		Level: level,
	})
	return err
}

func (c *SPDKClient) LogSetFlags(flags string) error {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.LogSetFlags(ctx, &spdkrpc.LogSetFlagsRequest{
		Flags: flags,
	})
	return err
}

func (c *SPDKClient) LogGetLevel() (string, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.LogGetLevel(ctx, &emptypb.Empty{})
	if err != nil {
		return "", err
	}
	return resp.Level, nil
}

func (c *SPDKClient) LogGetFlags() (string, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.LogGetFlags(ctx, &emptypb.Empty{})
	if err != nil {
		return "", err
	}
	return resp.Flags, nil
}

func (c *SPDKClient) MetricsGet(name string) (*spdkrpc.Metrics, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get SPDK engine metrics: missing required parameter")
	}
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()
	resp, err := client.MetricsGet(ctx, &spdkrpc.MetricsRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK engine %v metrics", name)
	}
	return resp, nil
}
