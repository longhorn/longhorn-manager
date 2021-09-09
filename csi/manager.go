package csi

import (
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/rest"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
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

func (m *Manager) Run(driverName, nodeID, endpoint, identityVersion, managerURL string) error {
	logrus.Infof("CSI Driver: %v version: %v, manager URL %v", driverName, identityVersion, managerURL)

	// Longhorn API Client
	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize Longhorn API client")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "Failed to get Longhorn client config")
	}
	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize Longhorn clientset")
	}

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return errors.Errorf("cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	// Create GRPC servers
	m.ids = NewIdentityServer(driverName, identityVersion)
	m.ns = NewNodeServer(apiClient, nodeID)
	m.cs = NewControllerServer(apiClient, lhClient, namespace, nodeID)
	s := NewNonBlockingGRPCServer()
	s.Start(endpoint, m.ids, m.cs, m.ns)
	s.Wait()

	return nil
}
