package shareapi

import (
	"fmt"

	smclient "github.com/longhorn/longhorn-share-manager/pkg/client"
	smtype "github.com/longhorn/longhorn-share-manager/pkg/types"
	smutil "github.com/longhorn/longhorn-share-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

const (
	ShareManagerAPIVersion  = 0
	ShareManagerDefaultPort = 8500
)

type ShareManagerClient struct {
	ip            string
	apiMinVersion int
	apiVersion    int

	// The gRPC client supports backward compatibility.
	grpcClient *smclient.ShareManagerClient
}

func CheckShareManagerCompatibility(minVersion, version int) error {
	if ShareManagerAPIVersion > version || ShareManagerAPIVersion < minVersion {
		return fmt.Errorf("current ShareManager version %v is not compatible with ShareManagerAPIVersion %v and ShareManagerAPIMinVersion %v",
			ShareManagerAPIVersion, version, minVersion)
	}
	return nil
}

func NewShareManagerClient(sm *longhorn.ShareManager) (*ShareManagerClient, error) {
	// Do not check the major version here. Since cannot get the major version without using this client to call VersionGet().
	if sm.Status.State != types.ShareManagerStateRunning || sm.Status.IP == "" {
		return nil, fmt.Errorf("invalid Share Manager %v, state: %v, IP: %v", sm.Name, sm.Status.State, sm.Status.IP)
	}

	return &ShareManagerClient{
		ip:            sm.Status.IP,
		apiMinVersion: sm.Status.APIMinVersion,
		apiVersion:    sm.Status.APIVersion,
		grpcClient:    smclient.NewShareManagerClient(smutil.GetURL(sm.Status.IP, ShareManagerDefaultPort)),
	}, nil
}

func rpcToShare(obj *smtype.Share) *types.Share {
	if obj == nil {
		return nil
	}

	return &types.Share{
		Volume: obj.Volume,
		State:  types.ShareState(obj.State),
		Error:  obj.Error,
	}
}

func (c *ShareManagerClient) ShareCreate(volume string) (*types.Share, error) {
	if err := CheckShareManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	rsp, err := c.grpcClient.ShareCreate(volume)
	if err != nil {
		return nil, err
	}

	return rpcToShare(rsp), nil
}

func (c *ShareManagerClient) ShareDelete(volume string) (*types.Share, error) {
	if err := CheckShareManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	rsp, err := c.grpcClient.ShareDelete(volume)
	if err != nil {
		return nil, err
	}

	return rpcToShare(rsp), nil
}

func (c *ShareManagerClient) ShareGet(volume string) (*types.Share, error) {
	if err := CheckShareManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	rsp, err := c.grpcClient.ShareGet(volume)
	if err != nil {
		return nil, err
	}

	return rpcToShare(rsp), nil
}

func (c *ShareManagerClient) ShareList() (map[string]*types.Share, error) {
	if err := CheckShareManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}

	rsp, err := c.grpcClient.ShareList()
	if err != nil {
		return nil, err
	}

	shares := map[string]*types.Share{}
	for vol, obj := range rsp {
		shares[vol] = rpcToShare(obj)
	}

	return shares, nil
}

func (c *ShareManagerClient) ShareWatch() (*smclient.ShareStream, error) {
	if err := CheckShareManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	return c.grpcClient.ShareWatch()
}

func (c *ShareManagerClient) LogWatch() (*smclient.LogStream, error) {
	if err := CheckShareManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	return c.grpcClient.LogWatch()
}

type APIVersion struct {
	MinVersion int
	Version    int
}

func (c *ShareManagerClient) VersionGet() (APIVersion, error) {
	output, err := c.grpcClient.VersionGet()
	if err != nil {
		return APIVersion{}, err
	}
	return APIVersion{MinVersion: output.APIMinVersion, Version: output.APIVersion}, nil
}
