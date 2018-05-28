package csi

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"

	longhornclient "github.com/rancher/longhorn-manager/client"
)

type Manager struct {
	ids *IdentityServer
	ns  *NodeServer
	cs  *ControllerServer

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

func init() {}

func GetCSIManager() *Manager {
	return &Manager{}
}

func (m *Manager) Run(driverName, nodeID, endpoint, csiVersion, identityVersion, managerURL string) error {
	logrus.Infof("CSI Driver: %v csiVersion: %v", driverName, csiVersion)

	// Initialize CSI driver
	driver := csicommon.NewCSIDriver(driverName, csiVersion, nodeID)
	if driver == nil {
		return errors.New("Failed to initialize CSI Driver")
	}

	// TODO add GET_CAPACITY after capacity based scheduling implemented
	// TODO LIST_VOLUMES ?
	driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	})

	driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	// Longhorn API Client
	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize Longhorn API client")
	}

	// Create GRPC servers
	m.ids = NewIdentityServer(driverName, identityVersion)
	m.ns = NewNodeServer(driver)
	m.cs = NewControllerServer(driver, apiClient)
	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(endpoint, m.ids, m.cs, m.ns)
	s.Wait()

	return nil
}
