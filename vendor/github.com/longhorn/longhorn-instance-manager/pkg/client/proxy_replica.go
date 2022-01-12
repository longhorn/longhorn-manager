package client

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (c *ProxyClient) ReplicaAdd(serviceAddress, replicaAddress string, restore bool) (err error) {
	if serviceAddress == "" || replicaAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to add replica")
	}

	log := logrus.WithFields(logrus.Fields{
		"serviceURL": c.ServiceURL,
		"restore":    restore,
	})
	log.Debugf("Adding replica %v via proxy", replicaAddress)

	req := &rpc.EngineReplicaAddRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		ReplicaAddress: replicaAddress,
		Restore:        restore,
	}
	_, err = c.service.ReplicaAdd(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to add replica %v for volume via proxy %v to %v", replicaAddress, c.ServiceURL, serviceAddress)
	}

	return nil
}

func (c *ProxyClient) ReplicaList(serviceAddress string) (rInfoList []*etypes.ControllerReplicaInfo, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to list replicas")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Listing replicas via proxy")

	req := &rpc.ProxyEngineRequest{
		Address: serviceAddress,
	}
	resp, err := c.service.ReplicaList(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list replicas %v for volume via proxy %v to %v", serviceAddress, c.ServiceURL, serviceAddress)
	}

	for _, cr := range resp.ReplicaList.Replicas {
		rInfoList = append(rInfoList, &etypes.ControllerReplicaInfo{
			Address: cr.Address.Address,
			Mode:    eptypes.GRPCReplicaModeToReplicaMode(cr.Mode),
		})
	}

	return rInfoList, nil
}

func (c *ProxyClient) ReplicaRebuildingStatus(serviceAddress string) (status map[string]*ReplicaRebuildStatus, err error) {
	if serviceAddress == "" {
		return status, errors.Wrapf(ErrParameter, "failed to get replica rebuilding status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting replica rebuilding status via proxy")

	req := &rpc.ProxyEngineRequest{
		Address: serviceAddress,
	}
	recv, err := c.service.ReplicaRebuildingStatus(c.ctx, req)
	if err != nil {
		return status, errors.Wrapf(err, "failed to get replicas rebuilding status via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	status = make(map[string]*ReplicaRebuildStatus)
	for k, v := range recv.Status {
		status[k] = &ReplicaRebuildStatus{
			Error:              v.Error,
			IsRebuilding:       v.IsRebuilding,
			Progress:           int(v.Progress),
			State:              v.State,
			FromReplicaAddress: v.FromReplicaAddress,
		}
	}
	return status, nil
}

func (c *ProxyClient) ReplicaVerifyRebuild(serviceAddress, replicaAddress string) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to verify replica rebuild")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Verifying replica rebuild via proxy")

	req := &rpc.EngineReplicaVerifyRebuildRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		ReplicaAddress: replicaAddress,
	}
	_, err = c.service.ReplicaVerifyRebuild(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to verify replica %v rebuild via proxy %v to %v", replicaAddress, c.ServiceURL, serviceAddress)
	}

	return nil
}

func (c *ProxyClient) ReplicaRemove(serviceAddress, replicaAddress string) (err error) {
	if serviceAddress == "" || replicaAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to remove replica")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Removing replica %v via proxy", replicaAddress)

	req := &rpc.EngineReplicaRemoveRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		ReplicaAddress: replicaAddress,
	}
	_, err = c.service.ReplicaRemove(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to remove replicas %v for volume via proxy %v to %v", replicaAddress, c.ServiceURL, serviceAddress)
	}

	return nil
}
