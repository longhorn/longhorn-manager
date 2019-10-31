package csi

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	longhornclient "github.com/longhorn/longhorn-manager/client"
)

type Manager struct {
	ids *IdentityServer
	ns  *NodeServer
	cs  *ControllerServer
}

func init() {}

func GetCSIManager() *Manager {
	return &Manager{}
}

func (m *Manager) Run(driverName, nodeID, endpoint, csiVersion, identityVersion, managerURL string) error {
	logrus.Infof("CSI Driver: %v csiVersion: %v", driverName, csiVersion)

	// Longhorn API Client
	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize Longhorn API client")
	}

	// Create GRPC servers
	m.ids = NewIdentityServer(driverName, identityVersion)
	m.ns = NewNodeServer(nodeID)
	m.cs = NewControllerServer(apiClient, nodeID)
	s := NewNonBlockingGRPCServer()
	s.Start(endpoint, m.ids, m.cs, m.ns)
	s.Wait()

	return nil
}
