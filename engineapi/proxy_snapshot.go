package engineapi

import (
	"github.com/cockroachdb/errors"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) SnapshotCreate(obj DataEngineObject, name string, labels map[string]string, freezeFilesystem bool) (string, error) {
	return p.grpcClient.VolumeSnapshot(obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(), p.DirectToURL(obj),
		name, labels, freezeFilesystem)
}

func (p *Proxy) SnapshotList(obj DataEngineObject) (snapshots map[string]*longhorn.SnapshotInfo, err error) {
	recv, err := p.grpcClient.SnapshotList(obj.GetDataEngine(), obj.GetEngineName(), obj.GetVolumeName(), p.DirectToURL(obj))
	if err != nil {
		return nil, err
	}

	snapshots = map[string]*longhorn.SnapshotInfo{}
	for k, v := range recv {
		snapshots[k] = (*longhorn.SnapshotInfo)(v)
	}
	return snapshots, nil
}

func (p *Proxy) SnapshotGet(obj DataEngineObject, name string) (snapshot *longhorn.SnapshotInfo, err error) {
	recv, err := p.SnapshotList(obj)
	if err != nil {
		return nil, err
	}

	return recv[name], nil
}

func (p *Proxy) SnapshotClone(obj DataEngineObject, snapshotName, fromEngineAddress, fromVolumeName, fromEngineName string,
	fileSyncHTTPClientTimeout, grpcTimeoutSeconds int64, cloneMode string) (err error) {
	return p.grpcClient.SnapshotClone(obj.GetDataEngine(), obj.GetEngineName(), obj.GetVolumeName(), p.DirectToURL(obj),
		snapshotName, fromEngineAddress, fromVolumeName, fromEngineName, int(fileSyncHTTPClientTimeout), grpcTimeoutSeconds, cloneMode)
}

func (p *Proxy) SnapshotCloneStatus(obj DataEngineObject) (status map[string]*longhorn.SnapshotCloneStatus, err error) {
	recv, err := p.grpcClient.SnapshotCloneStatus(obj.GetDataEngine(), obj.GetEngineName(), obj.GetVolumeName(),
		p.DirectToURL(obj))
	if err != nil {
		return nil, err
	}

	status = map[string]*longhorn.SnapshotCloneStatus{}
	for k, v := range recv {
		status[k] = (*longhorn.SnapshotCloneStatus)(v)
	}
	return status, nil
}

func (p *Proxy) SnapshotRevert(obj DataEngineObject, snapshotName string) (err error) {
	return p.grpcClient.SnapshotRevert(obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(), p.DirectToURL(obj),
		snapshotName)
}

func (p *Proxy) SnapshotPurge(obj DataEngineObject) (err error) {
	v, err := p.ds.GetVolumeRO(obj.GetVolumeName())
	if err != nil {
		return errors.Wrapf(err, "failed to get volume %v before purging snapshots", obj.GetVolumeName())
	}

	if util.IsVolumeMigrating(v) {
		return errors.Errorf("failed to start snapshot purge for engine %v and volume %v because the volume is migrating", obj.GetEngineName(), obj.GetVolumeName())
	}

	return p.grpcClient.SnapshotPurge(obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(), p.DirectToURL(obj),
		true)
}

func (p *Proxy) SnapshotPurgeStatus(obj DataEngineObject) (status map[string]*longhorn.PurgeStatus, err error) {
	recv, err := p.grpcClient.SnapshotPurgeStatus(obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(),
		p.DirectToURL(obj))
	if err != nil {
		return nil, err
	}

	status = map[string]*longhorn.PurgeStatus{}
	for k, v := range recv {
		status[k] = (*longhorn.PurgeStatus)(v)
	}
	return status, nil
}

func (p *Proxy) SnapshotDelete(obj DataEngineObject, name string) (err error) {
	return p.grpcClient.SnapshotRemove(obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(), p.DirectToURL(obj),
		[]string{name})
}

func (p *Proxy) SnapshotHash(obj DataEngineObject, snapshotName string, rehash bool) error {
	return p.grpcClient.SnapshotHash(obj.GetDataEngine(), obj.GetEngineName(), obj.GetVolumeName(),
		p.DirectToURL(obj), snapshotName, rehash)
}

func (p *Proxy) SnapshotHashStatus(obj DataEngineObject, snapshotName string) (status map[string]*longhorn.HashStatus, err error) {
	recv, err := p.grpcClient.SnapshotHashStatus(obj.GetDataEngine(), obj.GetEngineName(), obj.GetVolumeName(),
		p.DirectToURL(obj), snapshotName)
	if err != nil {
		return nil, err
	}

	status = map[string]*longhorn.HashStatus{}
	for k, v := range recv {
		status[k] = (*longhorn.HashStatus)(v)
	}
	return status, nil
}
