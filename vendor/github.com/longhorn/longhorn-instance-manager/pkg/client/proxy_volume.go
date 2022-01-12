package client

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (c *ProxyClient) VolumeGet(serviceAddress string) (info *etypes.VolumeInfo, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get volume")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting volume via proxy")

	req := &rpc.ProxyEngineRequest{
		Address: serviceAddress,
	}
	resp, err := c.service.VolumeGet(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get volume via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	info = &etypes.VolumeInfo{
		Name:                  resp.Volume.Name,
		Size:                  resp.Volume.Size,
		ReplicaCount:          int(resp.Volume.ReplicaCount),
		Endpoint:              resp.Volume.Endpoint,
		Frontend:              resp.Volume.Frontend,
		FrontendState:         resp.Volume.FrontendState,
		IsExpanding:           resp.Volume.IsExpanding,
		LastExpansionError:    resp.Volume.LastExpansionError,
		LastExpansionFailedAt: resp.Volume.LastExpansionFailedAt,
	}
	return info, nil
}

func (c *ProxyClient) VolumeExpand(serviceAddress string, size int64) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to expand volume")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Expanding volume via proxy")

	req := &rpc.EngineVolumeExpandRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		Expand: &eptypes.VolumeExpandRequest{
			Size: size,
		},
	}
	_, err = c.service.VolumeExpand(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to expand volume via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	return nil
}

func (c *ProxyClient) VolumeFrontendStart(serviceAddress, frontendName string) (err error) {
	if serviceAddress == "" || frontendName == "" {
		return errors.Wrapf(ErrParameter, "failed to start volume frontend")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Starting volume frontend %v via proxy", frontendName)

	req := &rpc.EngineVolumeFrontendStartRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		FrontendStart: &eptypes.VolumeFrontendStartRequest{
			Frontend: frontendName,
		},
	}
	_, err = c.service.VolumeFrontendStart(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to start volume frontend %v via proxy %v to %v", frontendName, c.ServiceURL, serviceAddress)
	}

	return nil
}

func (c *ProxyClient) VolumeFrontendShutdown(serviceAddress string) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to shutdown volume frontend")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Shutting down volume frontend via proxy")

	req := &rpc.ProxyEngineRequest{
		Address: serviceAddress,
	}
	_, err = c.service.VolumeFrontendShutdown(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to shutdown volume frontend via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	return nil
}
