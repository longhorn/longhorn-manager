package engineapi

import (
	"github.com/cockroachdb/errors"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) SnapshotCreate(obj interface{}, name string, labels map[string]string, freezeFilesystem bool) (string, error) {
	dataEngine, engineName, engineFrontendName, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return "", err
	}
	return p.grpcClient.VolumeSnapshot(dataEngine, engineName, engineFrontendName, volumeName, p.DirectToURL(obj),
		name, labels, freezeFilesystem)
}

func (p *Proxy) SnapshotList(obj interface{}) (snapshots map[string]*longhorn.SnapshotInfo, err error) {
	dataEngine, engineName, _, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return nil, err
	}
	recv, err := p.grpcClient.SnapshotList(string(dataEngine), engineName, volumeName, p.DirectToURL(obj))
	if err != nil {
		return nil, err
	}

	snapshots = map[string]*longhorn.SnapshotInfo{}
	for k, v := range recv {
		snapshots[k] = (*longhorn.SnapshotInfo)(v)
	}
	return snapshots, nil
}

func (p *Proxy) SnapshotGet(obj interface{}, name string) (snapshot *longhorn.SnapshotInfo, err error) {
	recv, err := p.SnapshotList(obj)
	if err != nil {
		return nil, err
	}

	return recv[name], nil
}

func (p *Proxy) SnapshotClone(obj interface{}, snapshotName, fromEngineAddress, fromVolumeName, fromEngineName string,
	fileSyncHTTPClientTimeout, grpcTimeoutSeconds int64, cloneMode string) (err error) {
	dataEngine, engineName, _, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return err
	}

	return p.grpcClient.SnapshotClone(string(dataEngine), engineName, volumeName, p.DirectToURL(obj),
		snapshotName, fromEngineAddress, fromVolumeName, fromEngineName, int(fileSyncHTTPClientTimeout), grpcTimeoutSeconds, cloneMode)
}

func (p *Proxy) SnapshotCloneStatus(obj interface{}) (status map[string]*longhorn.SnapshotCloneStatus, err error) {
	dataEngine, engineName, _, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return nil, err
	}

	recv, err := p.grpcClient.SnapshotCloneStatus(string(dataEngine), engineName, volumeName,
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

func (p *Proxy) SnapshotRevert(obj interface{}, snapshotName string) (err error) {
	dataEngine, engineName, engineFrontendName, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return err
	}

	return p.grpcClient.SnapshotRevert(string(dataEngine), engineName, engineFrontendName, volumeName, p.DirectToURL(obj),
		snapshotName)
}

func (p *Proxy) SnapshotPurge(obj interface{}) (err error) {
	dataEngine, engineName, engineFrontendName, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return err
	}

	v, err := p.ds.GetVolumeRO(volumeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume %v before purging snapshots", volumeName)
	}

	if util.IsVolumeMigrating(v) {
		return errors.Errorf("failed to start snapshot purge for engine %v and volume %v because the volume is migrating", engineName, volumeName)
	}

	return p.grpcClient.SnapshotPurge(string(dataEngine), engineName, engineFrontendName, volumeName, p.DirectToURL(obj),
		true)
}

func (p *Proxy) SnapshotPurgeStatus(obj interface{}) (status map[string]*longhorn.PurgeStatus, err error) {
	dataEngine, engineName, engineFrontendName, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return nil, err
	}
	recv, err := p.grpcClient.SnapshotPurgeStatus(string(dataEngine), engineName, engineFrontendName, volumeName,
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

func (p *Proxy) SnapshotDelete(obj interface{}, name string) (err error) {
	dataEngine, engineName, engineFrontendName, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return err
	}
	return p.grpcClient.SnapshotRemove(string(dataEngine), engineName, engineFrontendName, volumeName, p.DirectToURL(obj),
		[]string{name})
}

func (p *Proxy) SnapshotHash(obj interface{}, snapshotName string, rehash bool) error {
	dataEngine, engineName, _, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return err
	}
	return p.grpcClient.SnapshotHash(string(dataEngine), engineName, volumeName,
		p.DirectToURL(obj), snapshotName, rehash)
}

func (p *Proxy) SnapshotHashStatus(obj interface{}, snapshotName string) (status map[string]*longhorn.HashStatus, err error) {
	dataEngine, engineName, _, volumeName, err := p.GetObjInfo(obj)
	if err != nil {
		return nil, err
	}
	recv, err := p.grpcClient.SnapshotHashStatus(string(dataEngine), engineName, volumeName,
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
