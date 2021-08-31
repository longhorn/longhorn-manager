package manager

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
)

var (
	VERSION   = "v0.3.0"
	GitCommit = "HEAD"
)

func (m *VolumeManager) GetLonghornEventList() (*v1.EventList, error) {
	return m.ds.GetLonghornEventList()
}

func FriendlyVersion() string {
	return fmt.Sprintf("%s (%s)", VERSION, GitCommit)
}
