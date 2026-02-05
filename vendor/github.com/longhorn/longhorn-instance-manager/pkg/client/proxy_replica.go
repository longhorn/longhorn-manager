package client

import (
	"fmt"

	"github.com/cockroachdb/errors"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func (c *ProxyClient) ReplicaAdd(dataEngine, engineName, volumeName, serviceAddress, replicaName,
	replicaAddress string, restore bool, size, currentSize int64, fileSyncHTTPClientTimeout int,
	fastSync bool, localSync *etypes.FileLocalSync, grpcTimeoutSeconds int64) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
		"serviceAddress": serviceAddress,
		"replicaName":    replicaName,
		"replicaAddress": replicaAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to add replica for volume")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return fmt.Errorf("failed to add replica for volume: invalid data engine %v", dataEngine)
	}

	defer func() {
		if restore {
			err = errors.Wrapf(err, "%v failed to add restore replica %v for volume", c.getProxyErrorPrefix(serviceAddress), replicaAddress)
		} else {
			err = errors.Wrapf(err, "%v failed to add replica %v for volume", c.getProxyErrorPrefix(serviceAddress), replicaAddress)
		}
	}()

	req := &rpc.EngineReplicaAddRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:    serviceAddress,
			EngineName: engineName,
			// nolint:all replaced with DataEngine
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			DataEngine:         rpc.DataEngine(driver),
			VolumeName:         volumeName,
		},
		ReplicaName:               replicaName,
		ReplicaAddress:            replicaAddress,
		Restore:                   restore,
		Size:                      size,
		CurrentSize:               currentSize,
		FastSync:                  fastSync,
		FileSyncHttpClientTimeout: int32(fileSyncHTTPClientTimeout),
		GrpcTimeoutSeconds:        grpcTimeoutSeconds,
	}

	if localSync != nil {
		req.LocalSync = &rpc.EngineReplicaLocalSync{
			SourcePath: localSync.SourcePath,
			TargetPath: localSync.TargetPath,
		}
	}

	ctx, cancel := getContextWithGRPCLongTimeout(c.ctx, grpcTimeoutSeconds)
	defer cancel()
	_, err = c.service.ReplicaAdd(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) ReplicaList(dataEngine, engineName, volumeName,
	serviceAddress string) (rInfoList []*etypes.ControllerReplicaInfo, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to list replicas for volume")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return nil, fmt.Errorf("failed to list replicas for volume: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to list replicas for volume", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
		Address:    serviceAddress,
		EngineName: engineName,
		// nolint:all replaced with DataEngine
		BackendStoreDriver: rpc.BackendStoreDriver(driver),
		DataEngine:         rpc.DataEngine(driver),
		VolumeName:         volumeName,
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	resp, err := c.service.ReplicaList(ctx, req)
	if err != nil {
		return nil, err
	}

	for _, cr := range resp.ReplicaList.Replicas {
		rInfoList = append(rInfoList, &etypes.ControllerReplicaInfo{
			Address: cr.Address.Address,
			Mode:    etypes.GRPCReplicaModeToReplicaMode(cr.Mode),
		})
	}

	return rInfoList, nil
}

func (c *ProxyClient) ReplicaRebuildingStatus(dataEngine, engineName, volumeName,
	serviceAddress string) (status map[string]*ReplicaRebuildStatus, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get replicas rebuilding status")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return nil, fmt.Errorf("failed to get replicas rebuilding status: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get replicas rebuilding status", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
		Address:    serviceAddress,
		EngineName: engineName,
		// nolint:all replaced with DataEngine
		BackendStoreDriver: rpc.BackendStoreDriver(driver),
		DataEngine:         rpc.DataEngine(driver),
		VolumeName:         volumeName,
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	recv, err := c.service.ReplicaRebuildingStatus(ctx, req)
	if err != nil {
		return status, err
	}

	status = make(map[string]*ReplicaRebuildStatus)
	for k, v := range recv.Status {
		status[k] = &ReplicaRebuildStatus{
			Error:                  v.Error,
			IsRebuilding:           v.IsRebuilding,
			Progress:               int(v.Progress),
			State:                  v.State,
			FromReplicaAddressList: v.FromReplicaAddressList,
		}
	}
	return status, nil
}

func (c *ProxyClient) ReplicaRebuildingQosSet(dataEngine, engineName, volumeName,
	serviceAddress string, qosLimitMbps int64) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to set replicas rebuilding qos set")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return fmt.Errorf("failed to set replicas rebuilding qos set: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to set replicas rebuilding qos set", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineReplicaRebuildingQosSetRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:    serviceAddress,
			EngineName: engineName,
			// nolint:all replaced with DataEngine
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			DataEngine:         rpc.DataEngine(driver),
			VolumeName:         volumeName,
		},
		QosLimitMbps: qosLimitMbps,
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	_, err = c.service.ReplicaRebuildingQosSet(ctx, req)
	return err
}

func (c *ProxyClient) ReplicaVerifyRebuild(dataEngine, engineName, volumeName, serviceAddress,
	replicaAddress, replicaName string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
		"serviceAddress": serviceAddress,
		"replicaAddress": replicaAddress,
		"replicaName":    replicaName,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to verify replica rebuild")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return fmt.Errorf("failed to verify replica rebuild: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to verify replica %v rebuild", c.getProxyErrorPrefix(serviceAddress), replicaAddress)
	}()

	req := &rpc.EngineReplicaVerifyRebuildRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:    serviceAddress,
			EngineName: engineName,
			// nolint:all replaced with DataEngine
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			DataEngine:         rpc.DataEngine(driver),
			VolumeName:         volumeName,
		},
		ReplicaAddress: replicaAddress,
		ReplicaName:    replicaName,
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	_, err = c.service.ReplicaVerifyRebuild(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) ReplicaRemove(dataEngine, serviceAddress, engineName, replicaAddress, replicaName string) (err error) {
	input := map[string]string{
		"serviceAddress": serviceAddress,
		"engineName":     engineName,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to remove replica for volume")
	}

	if replicaAddress == "" && replicaName == "" {
		return fmt.Errorf("failed to remove replica for volume: replica address and name are both empty")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return fmt.Errorf("failed to remove replica for volume: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to remove replica %v for volume", c.getProxyErrorPrefix(serviceAddress), replicaAddress)
	}()

	req := &rpc.EngineReplicaRemoveRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:    serviceAddress,
			EngineName: engineName,
			// nolint:all replaced with DataEngine
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			DataEngine:         rpc.DataEngine(driver),
		},
		ReplicaAddress: replicaAddress,
		ReplicaName:    replicaName,
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	_, err = c.service.ReplicaRemove(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) ReplicaModeUpdate(dataEngine, serviceAddress, replicaAddress string, mode string) (err error) {
	input := map[string]string{
		"serviceAddress": serviceAddress,
		"replicaAddress": replicaAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to remove replica for volume")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return fmt.Errorf("failed to remove replica for volume: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to update replica %v mode for volume", c.getProxyErrorPrefix(serviceAddress), replicaAddress)
	}()

	req := &rpc.EngineReplicaModeUpdateRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
			// nolint:all replaced with DataEngine
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			DataEngine:         rpc.DataEngine(driver),
		},
		ReplicaAddress: replicaAddress,
		Mode:           etypes.ReplicaModeToGRPCReplicaMode(etypes.Mode(mode)),
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	_, err = c.service.ReplicaModeUpdate(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) ReplicaRebuildConcurrentSyncLimitSet(dataEngine, engineName, volumeName, serviceAddress string,
	limit int) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to set replica rebuilding concurrent sync limit")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return fmt.Errorf("failed to set replica rebuilding concurrent sync limit: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to set replica rebuilding concurrent sync limit", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineReplicaRebuildConcurrentSyncLimitSetRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:    serviceAddress,
			EngineName: engineName,
			DataEngine: rpc.DataEngine(driver),
			VolumeName: volumeName,
		},
		Limit: int32(limit),
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	if _, err = c.service.ReplicaRebuildConcurrentSyncLimitSet(ctx, req); err != nil {
		return err
	}

	return nil
}

func (c *ProxyClient) ReplicaRebuildConcurrentSyncLimitGet(dataEngine, engineName, volumeName,
	serviceAddress string) (limit int, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return 0, errors.Wrap(err, "failed to get replica rebuilding concurrent sync limit")
	}

	driver, ok := rpc.DataEngine_value[getDataEngine(dataEngine)]
	if !ok {
		return 0, fmt.Errorf("failed to get replica rebuilding concurrent sync limit: invalid data engine %v", dataEngine)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get replica rebuilding concurrent sync limit", c.getProxyErrorPrefix(serviceAddress))
	}()
	req := &rpc.ProxyEngineRequest{
		Address:    serviceAddress,
		EngineName: engineName,
		DataEngine: rpc.DataEngine(driver),
		VolumeName: volumeName,
	}
	ctx, cancel := getContextWithGRPCTimeout(c.ctx)
	defer cancel()
	resp, err := c.service.ReplicaRebuildConcurrentSyncLimitGet(ctx, req)
	if err != nil {
		return 0, err
	}

	limit = int(resp.Limit)
	return limit, nil
}
