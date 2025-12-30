package csi

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	longhornclient "github.com/longhorn/longhorn-manager/client"
)

type Manager struct {
	ids *IdentityServer
	ns  *NodeServer
	cs  *ControllerServer
	sms *SnapshotMetadataServer
}

// It can take up to 10s for each try. So total retry time would be 180s
const rancherClientInitMaxRetry = 18

func init() {}

func GetCSIManager() *Manager {
	return &Manager{}
}

func (m *Manager) Run(driverName, nodeID, endpoint, identityVersion, managerURL string) error {
	logrus.Infof("CSI Driver: %v version: %v, manager URL %v", driverName, identityVersion, managerURL)

	// Longhorn API Client
	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := initRancherClient(clientOpts)
	if err != nil {
		return err
	}

	// Create GRPC servers
	m.ids = NewIdentityServer(driverName, identityVersion)
	m.ns, err = NewNodeServer(apiClient, nodeID)
	if err != nil {
		return errors.Wrap(err, "Failed to create CSI node server ")
	}

	m.cs, err = NewControllerServer(apiClient, nodeID)
	if err != nil {
		return errors.Wrap(err, "failed to create CSI controller server")
	}

	m.sms, err = NewSnapshotMetadataServer(apiClient, nodeID)
	if err != nil {
		return errors.Wrap(err, "failed to create CSI snapshot metadata server")
	}

	s := NewNonBlockingGRPCServer()
	s.Start(endpoint, m.ids, m.cs, m.ns, m.sms)
	s.Wait()

	return nil
}

func initRancherClient(clientOpts *longhornclient.ClientOpts) (*longhornclient.RancherClient, error) {
	var lastErr error

	for i := 0; i < rancherClientInitMaxRetry; i++ {
		apiClient, err := longhornclient.NewRancherClient(clientOpts)
		if err == nil {
			return apiClient, nil
		}
		logrus.Warnf("Failed to initialize Longhorn API client %v. Retrying", err)
		lastErr = err
		time.Sleep(time.Second)
	}

	return nil, errors.Wrap(lastErr, "Failed to initialize Longhorn API client")
}
