package engineapi

import (
	"fmt"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) ReplicaAdd(obj interface{}, replicaName, replicaAddress string, restore, fastSync bool, localSync *etypes.FileLocalSync, replicaFileSyncHTTPClientTimeout, grpcTimeoutSeconds int64) (err error) {
	switch v := obj.(type) {
	case *longhorn.Engine:
		return p.grpcClient.ReplicaAdd(string(v.Spec.DataEngine), v.Name, "", v.Spec.VolumeName, p.DirectToURL(v),
			replicaName, replicaAddress, restore, v.Spec.VolumeSize, v.Status.CurrentSize,
			int(replicaFileSyncHTTPClientTimeout), fastSync, localSync, grpcTimeoutSeconds)
	case *longhorn.EngineFrontend:
		// For v2, replica add must be initiated through EngineFrontend. Replica
		// list/remove remain engine-owned operations and continue to use the
		// engine-based ReplicaList/ReplicaRemove APIs below.
		return p.grpcClient.ReplicaAdd(string(v.Spec.DataEngine), v.Spec.EngineName, v.Name, v.Spec.VolumeName, p.DirectToURL(v),
			replicaName, replicaAddress, restore, v.Spec.VolumeSize, 0,
			int(replicaFileSyncHTTPClientTimeout), fastSync, localSync, grpcTimeoutSeconds)
	default:
		return fmt.Errorf("unsupported object type %T for replica add", obj)
	}
}

func (p *Proxy) ReplicaRemove(e *longhorn.Engine, address, replicaName string) (err error) {
	return p.grpcClient.ReplicaRemove(string(e.Spec.DataEngine), p.DirectToURL(e), e.Name, address, replicaName)
}

func (p *Proxy) ReplicaList(e *longhorn.Engine) (replicas map[string]*Replica, err error) {
	resp, err := p.grpcClient.ReplicaList(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	replicas = make(map[string]*Replica)
	for _, r := range resp {
		mode := longhorn.ReplicaMode(r.Mode)
		if mode != longhorn.ReplicaModeRW && mode != longhorn.ReplicaModeWO {
			mode = longhorn.ReplicaModeERR
		}
		replicas[r.Address] = &Replica{
			URL:  r.Address,
			Mode: mode,
		}
	}
	return replicas, nil
}

func (p *Proxy) ReplicaRebuildStatus(e *longhorn.Engine) (status map[string]*longhorn.RebuildStatus, err error) {
	recv, err := p.grpcClient.ReplicaRebuildingStatus(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	status = make(map[string]*longhorn.RebuildStatus)
	for k, v := range recv {
		status[k] = (*longhorn.RebuildStatus)(v)
	}
	return status, nil
}

func (p *Proxy) ReplicaRebuildQosSet(e *longhorn.Engine, qosLimitMbps int64) error {
	return p.grpcClient.ReplicaRebuildingQosSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), qosLimitMbps)
}

func (p *Proxy) ReplicaRebuildVerify(e *longhorn.Engine, replicaName, url string) (err error) {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}
	return p.grpcClient.ReplicaVerifyRebuild(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), url, replicaName)
}

func (p *Proxy) ReplicaModeUpdate(e *longhorn.Engine, url, mode string) (err error) {
	if err := ValidateReplicaURL(url); err != nil {
		return err
	}

	return p.grpcClient.ReplicaModeUpdate(string(e.Spec.DataEngine), p.DirectToURL(e), url, mode)
}

func (p *Proxy) ReplicaRebuildConcurrentSyncLimitSet(e *longhorn.Engine, limit int) (err error) {
	return p.grpcClient.ReplicaRebuildConcurrentSyncLimitSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), limit)
}

func (p *Proxy) ReplicaRebuildConcurrentSyncLimitGet(e *longhorn.Engine) (limit int, err error) {
	return p.grpcClient.ReplicaRebuildConcurrentSyncLimitGet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e))
}
