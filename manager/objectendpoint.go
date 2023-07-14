package manager

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/sirupsen/logrus"
)

func (m *VolumeManager) ListObjectEndpointsSorted() ([]*longhorn.ObjectEndpoint, error) {
	endpointMap, err := m.ds.ListObjectEndpoints()
	if err != nil {
		return []*longhorn.ObjectEndpoint{}, err
	}

	endpoints := make([]*longhorn.ObjectEndpoint, len(endpointMap))
	endpointNames, err := util.SortKeys(endpointMap)
	if err != nil {
		return []*longhorn.ObjectEndpoint{}, err
	}

	for i, name := range endpointNames {
		endpoints[i] = endpointMap[name]
	}
	return endpoints, nil
}

func (m *VolumeManager) CreateObjectEndpoint(endpoint *longhorn.ObjectEndpoint) (*longhorn.ObjectEndpoint, error) {
	endpoint, err := m.ds.CreateObjectEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created Object Endpoint %v", endpoint.ObjectMeta.Name)
	return endpoint, nil
}

func (m *VolumeManager) DeleteObjectEndpoint(name string) error {
	if err := m.ds.DeleteObjectEndpoint(name); err != nil {
		return err
	}
	logrus.Infof("Deleted Object Endpoint %v", name)
	return nil
}
