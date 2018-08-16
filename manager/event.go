package manager

import (
	"k8s.io/api/core/v1"
)

func (m *VolumeManager) ListEvent() ([]*v1.Event, error) {
	eventList, err := m.ds.ListEvents()
	if err != nil {
		return nil, err
	}
	return eventList, nil
}
