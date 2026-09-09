package manager

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (m *VolumeManager) GetShardGroup(name string) (*longhorn.ShardGroup, error) {
	return m.ds.GetShardGroup(name)
}

func (m *VolumeManager) ListShardGroups() (map[string]*longhorn.ShardGroup, error) {
	return m.ds.ListShardGroups()
}

func (m *VolumeManager) GetShard(name string) (*longhorn.Shard, error) {
	return m.ds.GetShard(name)
}

func (m *VolumeManager) ListShards() (map[string]*longhorn.Shard, error) {
	return m.ds.ListShards()
}
