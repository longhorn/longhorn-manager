package engineapi

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) SnapshotCreate(e *longhorn.Engine, name string, labels map[string]string) (string, error) {
	return p.grpcClient.VolumeSnapshot(p.DirectToURL(e), name, labels)
}

func (p *Proxy) SnapshotList(e *longhorn.Engine) (snapshots map[string]*longhorn.Snapshot, err error) {
	recv, err := p.grpcClient.SnapshotList(p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	snapshots = map[string]*longhorn.Snapshot{}
	for k, v := range recv {
		snapshots[k] = (*longhorn.Snapshot)(v)
	}
	return snapshots, nil
}
