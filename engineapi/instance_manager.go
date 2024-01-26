package engineapi

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	CurrentInstanceManagerAPIVersion = 3
	MinInstanceManagerAPIVersion     = 1
	UnknownInstanceManagerAPIVersion = 0

	UnknownInstanceManagerProxyAPIVersion = 0
	// UnsupportedInstanceManagerProxyAPIVersion means the instance manager without the proxy client (Longhorn release before v1.3.0)
	UnsupportedInstanceManagerProxyAPIVersion = 0

	DefaultEnginePortCount  = 1
	DefaultReplicaPortCount = 15

	DefaultPortArg         = "--listen,0.0.0.0:"
	DefaultTerminateSignal = "SIGHUP"

	// IncompatibleInstanceManagerAPIVersion means the instance manager version in v0.7.0
	IncompatibleInstanceManagerAPIVersion = -1
	DeprecatedInstanceManagerBinaryName   = "longhorn-instance-manager"
)

type InstanceManagerClient struct {
	ip            string
	apiMinVersion int
	apiVersion    int

	// The gRPC client supports backward compatibility.
	grpcClient *imclient.ProcessManagerClient
}

func (c *InstanceManagerClient) Close() error {
	if c.grpcClient == nil {
		return nil
	}

	return c.grpcClient.Close()
}

func GetDeprecatedInstanceManagerBinary(image string) string {
	cname := types.GetImageCanonicalName(image)
	return filepath.Join(types.EngineBinaryDirectoryOnHost, cname, DeprecatedInstanceManagerBinaryName)
}

func CheckInstanceManagerCompatibility(imMinVersion, imVersion int) error {
	if MinInstanceManagerAPIVersion > imVersion || CurrentInstanceManagerAPIVersion < imMinVersion {
		return fmt.Errorf("current InstanceManager version %v-%v is not compatible with InstanceManagerAPIVersion %v and InstanceManagerAPIMinVersion %v",
			CurrentInstanceManagerAPIVersion, MinInstanceManagerAPIVersion, imVersion, imMinVersion)
	}
	return nil
}

func CheckInstanceManagerProxySupport(im *longhorn.InstanceManager) error {
	if UnsupportedInstanceManagerProxyAPIVersion == im.Status.ProxyAPIVersion {
		return fmt.Errorf("%v does not support proxy", im.Name)
	}
	return nil
}

<<<<<<< HEAD
func NewInstanceManagerClient(im *longhorn.InstanceManager) (*InstanceManagerClient, error) {
=======
// NewInstanceManagerClient creates a new instance manager client
func NewInstanceManagerClient(ctx context.Context, ctxCancel context.CancelFunc, im *longhorn.InstanceManager) (*InstanceManagerClient, error) {
>>>>>>> 2f891130 (Check connections to instance and process manager services)
	// Do not check the major version here. Since IM cannot get the major version without using this client to call VersionGet().
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning || im.Status.IP == "" {
		return nil, fmt.Errorf("invalid Instance Manager %v, state: %v, IP: %v", im.Name, im.Status.CurrentState, im.Status.IP)
	}
	// HACK: TODO: fix me
	endpoint := "tcp://" + imutil.GetURL(im.Status.IP, InstanceManagerDefaultPort)

	initTLSClient := func() (*imclient.ProcessManagerClient, error) {
		// check for tls cert file presence
		processManagerClient, err := imclient.NewProcessManagerClientWithTLS(ctx, ctxCancel, endpoint,
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCAFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCertFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSKeyFile),
			"longhorn-backend.longhorn-system",
		)
		defer func() {
			if err != nil && processManagerClient != nil {
				processManagerClient.Close()
			}
		}()
		if err != nil {
			return nil, fmt.Errorf("failed to load Instance Manager Client TLS files Error: %w", err)
		}
<<<<<<< HEAD
<<<<<<< HEAD

		if _, err = pmClient.VersionGet(); err != nil {
<<<<<<< HEAD
			return nil, fmt.Errorf("failed to check check version of Instance Manager Client with TLS connection Error: %w", err)
=======
=======
		if _, err = processManagerClient.VersionGet(); err != nil {
>>>>>>> 9a798439 (Fix connection leak during TLS fallback)
			return nil, errors.Wrap(err, "failed to check version of Instance Manager Process Manager Service Client with TLS connection")
>>>>>>> 181c414a (Support proxy connections over TLS)
=======
		if err = processManagerClient.CheckConnection(); err != nil {
			return processManagerClient, errors.Wrapf(err, "failed to check Instance Manager Process Manager Service Client connection for %v ip %v",
				im.Name, im.Status.IP)
		}

		if _, err = processManagerClient.VersionGet(); err != nil {
			return processManagerClient, errors.Wrap(err, "failed to check version of Instance Manager Process Manager Service Client with TLS connection")
>>>>>>> 2f891130 (Check connections to instance and process manager services)
		}

		return processManagerClient, nil
	}

<<<<<<< HEAD
	pmClient, err := initTLSClient()
	if err != nil {
=======
	initInstanceServiceTLSClient := func(endpoint string) (*imclient.InstanceServiceClient, error) {
		// check for tls cert file presence
		instanceServiceClient, err := imclient.NewInstanceServiceClientWithTLS(ctx, ctxCancel, endpoint,
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCAFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSCertFile),
			filepath.Join(types.TLSDirectoryInContainer, types.TLSKeyFile),
			"longhorn-backend.longhorn-system",
		)
		defer func() {
			if err != nil && instanceServiceClient != nil {
				instanceServiceClient.Close()
			}
		}()
		if err != nil {
			return nil, errors.Wrap(err, "failed to load Instance Manager Instance Service Client TLS files")
		}
		if err = instanceServiceClient.CheckConnection(); err != nil {
			return instanceServiceClient, errors.Wrapf(err, "failed to check Instance Manager Instance Service Client connection for %v IP %v",
				im.Name, im.Status.IP)
		}

		if _, err = instanceServiceClient.VersionGet(); err != nil {
			return instanceServiceClient, errors.Wrap(err, "failed to check version of Instance Manager Instance Service Client with TLS connection")
		}

		return instanceServiceClient, nil
	}

	// Create a new process manager client
	// HACK: TODO: fix me
	var err error
	var processManagerClient *imclient.ProcessManagerClient
	endpoint := "tcp://" + imutil.GetURL(im.Status.IP, InstanceManagerProcessManagerServiceDefaultPort)
	if im.Status.APIVersion < 4 {
		processManagerClient, err = initProcessManagerTLSClient(endpoint)
		defer func() {
			if err != nil && processManagerClient != nil {
				processManagerClient.Close()
			}
		}()
		if err != nil {
			logrus.WithError(err).Tracef("Falling back to non-tls client for Instance Manager Process Manager Service Client for %v IP %v",
				im.Name, im.Status.IP)
			// fallback to non tls client, there is no way to differentiate between im versions unless we get the version via the im client
			// TODO: remove this im client fallback mechanism in a future version maybe 2.4 / 2.5 or the next time we update the api version
			processManagerClient, err = imclient.NewProcessManagerClient(ctx, ctxCancel, endpoint, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to initialize Instance Manager Process Manager Service Client for %v IP %v",
					im.Name, im.Status.IP)
			}
			if err = processManagerClient.CheckConnection(); err != nil {
				return nil, errors.Wrapf(err, "failed to check Instance Manager Process Manager Service Client connection for %v IP %v",
					im.Name, im.Status.IP)
			}

			version, err := processManagerClient.VersionGet()
			if err != nil {
				return nil, errors.Wrapf(err, "failed to check version of Instance Manager Process Manager Service Client without TLS for %v IP %v",
					im.Name, im.Status.IP)
			}
			logrus.Tracef("Instance Manager Process Manager Service Client Version: %+v", version)
		}

		return &InstanceManagerClient{
			ip:                       im.Status.IP,
			apiMinVersion:            im.Status.APIMinVersion,
			apiVersion:               im.Status.APIVersion,
			processManagerGrpcClient: processManagerClient,
		}, nil
	}

	// Create a new instance service  client
	endpoint = "tcp://" + imutil.GetURL(im.Status.IP, InstanceManagerInstanceServiceDefaultPort)
	instanceServiceClient, err := initInstanceServiceTLSClient(endpoint)
	defer func() {
		if err != nil && instanceServiceClient != nil {
			instanceServiceClient.Close()
		}
	}()
	if err != nil {
		logrus.WithError(err).Tracef("Falling back to non-tls client for Instance Manager Instance Service Client for %v, IP %v",
			im.Name, im.Status.IP)
>>>>>>> 181c414a (Support proxy connections over TLS)
		// fallback to non tls client, there is no way to differentiate between im versions unless we get the version via the im client
		// TODO: remove this im client fallback mechanism in a future version maybe 2.4 / 2.5 or the next time we update the api version
<<<<<<< HEAD
		pmClient, err = imclient.NewProcessManagerClient(endpoint, nil)
=======
		instanceServiceClient, err = imclient.NewInstanceServiceClient(ctx, ctxCancel, endpoint, nil)
>>>>>>> 2f891130 (Check connections to instance and process manager services)
		if err != nil {
<<<<<<< HEAD
			return nil, fmt.Errorf("failed to initialize Instance Manager Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
		}

		if _, err = pmClient.VersionGet(); err != nil {
			return nil, fmt.Errorf("failed to get Version of Instance Manager Client for %v, state: %v, IP: %v, TLS: %v, Error: %w",
				im.Name, im.Status.CurrentState, im.Status.IP, false, err)
=======
			return nil, errors.Wrapf(err, "failed to initialize Instance Manager Instance Service Client for %v IP %v",
				im.Name, im.Status.IP)
		}
		if err = instanceServiceClient.CheckConnection(); err != nil {
			return nil, errors.Wrapf(err, "failed to check Instance Manager Instance Service Client connection for %v IP %v",
				im.Name, im.Status.IP)
		}

		version, err := instanceServiceClient.VersionGet()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to check version of Instance Manager Instance Service Client without TLS for %v IP %v",
				im.Name, im.Status.IP)
>>>>>>> 181c414a (Support proxy connections over TLS)
		}
	}

	// TODO: consider evaluating im client version since we do the call anyway to validate the connection, i.e. fallback to non tls
	//  This way we don't need the per call compatibility check, ref: `CheckInstanceManagerCompatibility`

	return &InstanceManagerClient{
		ip:            im.Status.IP,
		apiMinVersion: im.Status.APIMinVersion,
		apiVersion:    im.Status.APIVersion,
		grpcClient:    pmClient,
	}, nil
}

func (c *InstanceManagerClient) parseProcess(p *imapi.Process) *longhorn.InstanceProcess {
	if p == nil {
		return nil
	}

	return &longhorn.InstanceProcess{
		Spec: longhorn.InstanceProcessSpec{
			Name: p.Name,
		},
		Status: longhorn.InstanceProcessStatus{
			State:      longhorn.InstanceState(p.ProcessStatus.State),
			ErrorMsg:   p.ProcessStatus.ErrorMsg,
			Conditions: p.ProcessStatus.Conditions,
			PortStart:  p.ProcessStatus.PortStart,
			PortEnd:    p.ProcessStatus.PortEnd,

			// These fields are not used, maybe we can deprecate them later.
			Type:     "",
			Listen:   "",
			Endpoint: "",
		},
	}

}

func (c *InstanceManagerClient) EngineProcessCreate(e *longhorn.Engine, volumeFrontend longhorn.VolumeFrontend,
	engineReplicaTimeout, replicaFileSyncHTTPClientTimeout int64, dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	frontend, err := GetEngineProcessFrontend(volumeFrontend)
	if err != nil {
		return nil, err
	}
	args := []string{"controller", e.Spec.VolumeName,
		"--frontend", frontend,
	}

	if e.Spec.RevisionCounterDisabled {
		args = append(args, "--disableRevCounter")
	}

	if e.Spec.SalvageRequested {
		args = append(args, "--salvageRequested")
	}

	if engineCLIAPIVersion >= 6 {
		args = append(args,
			"--size", strconv.FormatInt(e.Spec.VolumeSize, 10),
			"--current-size", strconv.FormatInt(e.Status.CurrentSize, 10))
	}

	if engineCLIAPIVersion >= 7 {
		args = append(args,
			"--engine-replica-timeout", strconv.FormatInt(engineReplicaTimeout, 10),
			"--file-sync-http-client-timeout", strconv.FormatInt(replicaFileSyncHTTPClientTimeout, 10))

		if dataLocality == longhorn.DataLocalityStrictLocal {
			args = append(args, "--data-server-protocol", "unix")
		}

		if e.Spec.UnmapMarkSnapChainRemovedEnabled {
			args = append(args, "--unmap-mark-snap-chain-removed")
		}
	}

	for _, addr := range e.Status.CurrentReplicaAddressMap {
		args = append(args, "--replica", GetBackendReplicaURL(addr))
	}
	binary := filepath.Join(types.GetEngineBinaryDirectoryForEngineManagerContainer(e.Spec.EngineImage), types.EngineBinaryName)

	engineProcess, err := c.grpcClient.ProcessCreate(
		e.Name, binary, DefaultEnginePortCount, args, []string{DefaultPortArg})
	if err != nil {
		return nil, err
	}
	return c.parseProcess(engineProcess), nil
}

func (c *InstanceManagerClient) ReplicaProcessCreate(replica *longhorn.Replica, dataPath, backingImagePath string, dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	args := []string{
		"replica", types.GetReplicaMountedDataPath(dataPath),
		"--size", strconv.FormatInt(replica.Spec.VolumeSize, 10),
	}
	if backingImagePath != "" {
		args = append(args, "--backing-file", backingImagePath)
	}
	if replica.Spec.RevisionCounterDisabled {
		args = append(args, "--disableRevCounter")
	}

	if engineCLIAPIVersion >= 7 {
		args = append(args, "--volume-name", replica.Spec.VolumeName)

		if dataLocality == longhorn.DataLocalityStrictLocal {
			args = append(args, "--data-server-protocol", "unix")
		}

		if replica.Spec.UnmapMarkDiskChainRemovedEnabled {
			args = append(args, "--unmap-mark-disk-chain-removed")
		}
	}

	binary := filepath.Join(types.GetEngineBinaryDirectoryForReplicaManagerContainer(replica.Spec.EngineImage), types.EngineBinaryName)

	replicaProcess, err := c.grpcClient.ProcessCreate(
		replica.Name, binary, DefaultReplicaPortCount, args, []string{DefaultPortArg})
	if err != nil {
		return nil, err
	}
	return c.parseProcess(replicaProcess), nil
}

func (c *InstanceManagerClient) ProcessDelete(name string) error {
	if _, err := c.grpcClient.ProcessDelete(name); err != nil {
		return err
	}
	return nil
}

func (c *InstanceManagerClient) ProcessGet(name string) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	process, err := c.grpcClient.ProcessGet(name)
	if err != nil {
		return nil, err
	}
	return c.parseProcess(process), nil
}

func (c *InstanceManagerClient) ProcessGetBinary(name string) (string, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return "", err
	}
	process, err := c.grpcClient.ProcessGet(name)
	if err != nil {
		return "", err
	}
	return process.Binary, nil
}

// ProcessLog returns a grpc stream that will be closed when the passed context is cancelled or the underlying grpc client is closed
func (c *InstanceManagerClient) ProcessLog(ctx context.Context, name string) (*imapi.LogStream, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	return c.grpcClient.ProcessLog(ctx, name)
}

// ProcessWatch returns a grpc stream that will be closed when the passed context is cancelled or the underlying grpc client is closed
func (c *InstanceManagerClient) ProcessWatch(ctx context.Context) (*imapi.ProcessStream, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	return c.grpcClient.ProcessWatch(ctx)
}

func (c *InstanceManagerClient) ProcessList() (map[string]longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	processes, err := c.grpcClient.ProcessList()
	if err != nil {
		return nil, err
	}
	result := map[string]longhorn.InstanceProcess{}
	for name, process := range processes {
		result[name] = *c.parseProcess(process)
	}
	return result, nil
}

func (c *InstanceManagerClient) EngineProcessUpgrade(e *longhorn.Engine, volumeFrontend longhorn.VolumeFrontend,
	engineReplicaTimeout, replicaFileSyncHTTPClientTimeout int64, dataLocality longhorn.DataLocality, engineCLIAPIVersion int) (*longhorn.InstanceProcess, error) {
	if err := CheckInstanceManagerCompatibility(c.apiMinVersion, c.apiVersion); err != nil {
		return nil, err
	}
	frontend, err := GetEngineProcessFrontend(volumeFrontend)
	if err != nil {
		return nil, err
	}
	args := []string{"controller", e.Spec.VolumeName, "--frontend", frontend, "--upgrade"}
	for _, addr := range e.Spec.UpgradedReplicaAddressMap {
		args = append(args, "--replica", GetBackendReplicaURL(addr))
	}

	if engineCLIAPIVersion >= 6 {
		args = append(args,
			"--size", strconv.FormatInt(e.Spec.VolumeSize, 10),
			"--current-size", strconv.FormatInt(e.Status.CurrentSize, 10))
	}

	if engineCLIAPIVersion >= 7 {
		args = append(args,
			"--engine-replica-timeout", strconv.FormatInt(engineReplicaTimeout, 10),
			"--file-sync-http-client-timeout", strconv.FormatInt(replicaFileSyncHTTPClientTimeout, 10))

		if dataLocality == longhorn.DataLocalityStrictLocal {
			args = append(args,
				"--data-server-protocol", "unix")
		}

		if e.Spec.UnmapMarkSnapChainRemovedEnabled {
			args = append(args, "--unmap-mark-snap-chain-removed")
		}
	}

	binary := filepath.Join(types.GetEngineBinaryDirectoryForEngineManagerContainer(e.Spec.EngineImage), types.EngineBinaryName)

	engineProcess, err := c.grpcClient.ProcessReplace(
		e.Name, binary, DefaultEnginePortCount, args, []string{DefaultPortArg}, DefaultTerminateSignal)
	if err != nil {
		return nil, err
	}
	return c.parseProcess(engineProcess), nil
}

func (c *InstanceManagerClient) VersionGet() (int, int, int, int, error) {
	output, err := c.grpcClient.VersionGet()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return output.InstanceManagerAPIMinVersion, output.InstanceManagerAPIVersion,
		output.InstanceManagerProxyAPIMinVersion, output.InstanceManagerProxyAPIVersion, nil
}
