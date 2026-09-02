package engineapi

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"
	imclient "github.com/longhorn/longhorn-instance-manager/pkg/client"
	imrpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestGetV1PortArgs(t *testing.T) {
	tests := []struct {
		name               string
		dataEngineIPFamily string
		want               []string
	}{
		{
			name:               "default preserves legacy listener",
			dataEngineIPFamily: types.DataEngineIPFamilyDefault,
			want:               []string{DefaultPortArg},
		},
		{
			name:               "empty transport preserves legacy listener",
			dataEngineIPFamily: "",
			want:               []string{DefaultPortArg},
		},
		{
			name:               "unknown preserves legacy listener",
			dataEngineIPFamily: "ipv3",
			want:               []string{DefaultPortArg},
		},
		{
			name:               "ipv4 binds all IPv4 addresses",
			dataEngineIPFamily: types.DataEngineIPFamilyIPv4,
			want:               []string{"--listen,0.0.0.0:"},
		},
		{
			name:               "ipv6 binds all IPv6 addresses",
			dataEngineIPFamily: types.DataEngineIPFamilyIPv6,
			want:               []string{"--listen,[::]:"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := getV1PortArgs(tc.dataEngineIPFamily)
			require.Equal(t, tc.want, got)
		})
	}
}
func TestGetAppliedIPFamilyDistinguishesUninitializedAndEmpty(t *testing.T) {
	empty := ""
	defaultFamily := types.DataEngineIPFamilyDefault
	ipv6 := types.DataEngineIPFamilyIPv6

	tests := []struct {
		name        string
		im          *longhorn.InstanceManager
		want        string
		initialized bool
	}{
		{name: "nil manager", im: nil, want: "", initialized: false},
		{name: "nil status pointer", im: &longhorn.InstanceManager{}, want: "", initialized: false},
		{name: "preexisting empty status normalizes to default", im: &longhorn.InstanceManager{Status: longhorn.InstanceManagerStatus{IPFamily: &empty}}, want: defaultFamily, initialized: true},
		{name: "applied default", im: &longhorn.InstanceManager{Status: longhorn.InstanceManagerStatus{IPFamily: &defaultFamily}}, want: defaultFamily, initialized: true},
		{name: "applied ipv6", im: &longhorn.InstanceManager{Status: longhorn.InstanceManagerStatus{IPFamily: &ipv6}}, want: ipv6, initialized: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, initialized := GetAppliedIPFamily(tc.im)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.initialized, initialized)
		})
	}
}

func TestV2IPFamilyCapabilityGate(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion int
		family     string
		supported  bool
	}{
		{name: "old manager accepts default compatibility", apiVersion: 7, family: types.DataEngineIPFamilyDefault, supported: true},
		{name: "old manager accepts empty transport compatibility", apiVersion: 7, family: "", supported: true},
		{name: "old manager rejects explicit ipv4", apiVersion: 7, family: types.DataEngineIPFamilyIPv4, supported: false},
		{name: "old manager rejects explicit ipv6", apiVersion: 7, family: types.DataEngineIPFamilyIPv6, supported: false},
		{name: "capable manager accepts ipv4", apiVersion: 8, family: types.DataEngineIPFamilyIPv4, supported: true},
		{name: "capable manager accepts ipv6", apiVersion: 8, family: types.DataEngineIPFamilyIPv6, supported: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.supported, IsV2IPFamilySupported(tc.apiVersion, tc.family))
		})
	}
}

func TestIPFamilyTransportConversion(t *testing.T) {
	tests := []struct {
		name      string
		family    string
		transport string
		status    string
	}{
		{name: "default", family: types.DataEngineIPFamilyDefault, transport: "", status: types.DataEngineIPFamilyDefault},
		{name: "empty transport", family: "", transport: "", status: types.DataEngineIPFamilyDefault},
		{name: "ipv4", family: types.DataEngineIPFamilyIPv4, transport: types.DataEngineIPFamilyIPv4, status: types.DataEngineIPFamilyIPv4},
		{name: "ipv6", family: types.DataEngineIPFamilyIPv6, transport: types.DataEngineIPFamilyIPv6, status: types.DataEngineIPFamilyIPv6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.transport, serializeIPFamily(tc.family))
			require.Equal(t, tc.status, deserializeIPFamily(tc.transport))
		})
	}
}

type ipFamilyCaptureInstanceService struct {
	imrpc.UnimplementedInstanceServiceServer
	requests       []*imrpc.InstanceCreateRequest
	responseFamily string
}

func (s *ipFamilyCaptureInstanceService) InstanceCreate(_ context.Context, req *imrpc.InstanceCreateRequest) (*imrpc.InstanceResponse, error) {
	s.requests = append(s.requests, req)
	return &imrpc.InstanceResponse{
		Spec: &imrpc.InstanceSpec{
			Name:       req.Spec.Name,
			Type:       req.Spec.Type,
			DataEngine: req.Spec.DataEngine,
			IpFamily:   s.responseFamily,
		},
		Status: &imrpc.InstanceStatus{IpFamily: s.responseFamily},
	}, nil
}

func newIPFamilyCaptureClient(t *testing.T, responseFamily string) (*InstanceManagerClient, *ipFamilyCaptureInstanceService, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	capture := &ipFamilyCaptureInstanceService{responseFamily: responseFamily}
	imrpc.RegisterInstanceServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	instanceServiceClient, err := imclient.NewInstanceServiceClient(ctx, cancel, "tcp://"+listener.Addr().String(), nil)
	require.NoError(t, err)

	client := &InstanceManagerClient{
		apiMinVersion:             1,
		apiVersion:                MinInstanceManagerAPIVersionForPerInstanceIPFamily,
		instanceServiceGrpcClient: instanceServiceClient,
	}
	cleanup := func() {
		_ = client.Close()
		server.Stop()
		_ = listener.Close()
	}
	return client, capture, cleanup
}

func TestV2InstanceCreateIPFamilyTransportRoundTrip(t *testing.T) {
	tests := []struct {
		name            string
		family          string
		transportFamily string
	}{
		{name: "default", family: types.DataEngineIPFamilyDefault, transportFamily: ""},
		{name: "ipv4", family: types.DataEngineIPFamilyIPv4, transportFamily: types.DataEngineIPFamilyIPv4},
		{name: "ipv6", family: types.DataEngineIPFamilyIPv6, transportFamily: types.DataEngineIPFamilyIPv6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, capture, cleanup := newIPFamilyCaptureClient(t, tc.transportFamily)
			defer cleanup()

			engine, err := client.EngineInstanceCreate(&EngineInstanceCreateRequest{
				Engine: &longhorn.Engine{
					ObjectMeta: metav1.ObjectMeta{Name: "engine-" + tc.name},
					Spec: longhorn.EngineSpec{
						InstanceSpec: longhorn.InstanceSpec{
							DataEngine: longhorn.DataEngineTypeV2,
							VolumeName: "volume-" + tc.name,
							VolumeSize: 1,
						},
					},
				},
				VolumeFrontend: longhorn.VolumeFrontendBlockDev,
				IPFamily:       tc.family,
			})
			require.NoError(t, err)
			require.Equal(t, tc.family, engine.Status.IPFamily)

			engineFrontend, err := client.EngineFrontendInstanceCreate(&EngineFrontendInstanceCreateRequest{
				EngineFrontend: &longhorn.EngineFrontend{
					ObjectMeta: metav1.ObjectMeta{Name: "engine-frontend-" + tc.name},
					Spec: longhorn.EngineFrontendSpec{
						InstanceSpec: longhorn.InstanceSpec{
							DataEngine: longhorn.DataEngineTypeV2,
							VolumeName: "volume-" + tc.name,
						},
					},
				},
				VolumeFrontend: longhorn.VolumeFrontendBlockDev,
				IPFamily:       tc.family,
			})
			require.NoError(t, err)
			require.Equal(t, tc.family, engineFrontend.Status.IPFamily)

			replica, err := client.ReplicaInstanceCreate(&ReplicaInstanceCreateRequest{
				Replica: &longhorn.Replica{
					ObjectMeta: metav1.ObjectMeta{Name: "replica-" + tc.name},
					Spec: longhorn.ReplicaSpec{
						InstanceSpec: longhorn.InstanceSpec{
							DataEngine: longhorn.DataEngineTypeV2,
							VolumeName: "volume-" + tc.name,
							VolumeSize: 1,
						},
					},
				},
				IPFamily: tc.family,
			})
			require.NoError(t, err)
			require.Equal(t, tc.family, replica.Status.IPFamily)

			require.Len(t, capture.requests, 3)
			for _, request := range capture.requests {
				require.Equal(t, tc.transportFamily, request.Spec.IpFamily)
			}
		})
	}
}

func TestParseInstancePropagatesIPFamily(t *testing.T) {
	instance := &imapi.Instance{
		Name:       "engine-a",
		Type:       string(longhorn.InstanceTypeEngine),
		DataEngine: string(longhorn.DataEngineTypeV2),
		InstanceStatus: imapi.InstanceStatus{
			State:    string(longhorn.InstanceStateRunning),
			IPFamily: types.DataEngineIPFamilyIPv6,
			Endpoint: "2001:db8::10:4420",
		},
	}

	parsed := parseInstance(instance)
	require.NotNil(t, parsed)
	require.Equal(t, types.DataEngineIPFamilyIPv6, parsed.Status.IPFamily)
	require.Equal(t, "2001:db8::10:4420", parsed.Status.Endpoint)
	require.Equal(t, longhorn.DataEngineTypeV2, parsed.Spec.DataEngine)
}

func TestParseInstanceDefaultsEmptyIPFamily(t *testing.T) {
	parsed := parseInstance(&imapi.Instance{
		Name:       "engine-a",
		Type:       string(longhorn.InstanceTypeEngine),
		DataEngine: string(longhorn.DataEngineTypeV2),
	})
	require.NotNil(t, parsed)
	require.Equal(t, types.DataEngineIPFamilyDefault, parsed.Status.IPFamily)
}

func TestParseProcessDefaultsIPFamily(t *testing.T) {
	parsed := parseProcess(&imapi.Process{Name: "engine-a"})
	require.NotNil(t, parsed)
	require.Equal(t, types.DataEngineIPFamilyDefault, parsed.Status.IPFamily)
}

func TestV1PortArgsUseAppliedInstanceManagerFamily(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	im := &longhorn.InstanceManager{Status: longhorn.InstanceManagerStatus{IPFamily: &family}}
	applied, initialized := GetAppliedIPFamily(im)
	require.True(t, initialized)
	require.Equal(t, []string{"--listen,[::]:"}, getV1PortArgs(applied))
}

func TestOldV2InstanceManagerRejectsExplicitFamilyForAllObjects(t *testing.T) {
	client := &InstanceManagerClient{apiMinVersion: 1, apiVersion: 7}

	_, err := client.EngineInstanceCreate(&EngineInstanceCreateRequest{
		Engine: &longhorn.Engine{Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{
			DataEngine: longhorn.DataEngineTypeV2,
		}}},
		IPFamily: types.DataEngineIPFamilyIPv6,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "API version >= 8")

	_, err = client.EngineFrontendInstanceCreate(&EngineFrontendInstanceCreateRequest{
		IPFamily: types.DataEngineIPFamilyIPv6,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "API version >= 8")

	_, err = client.ReplicaInstanceCreate(&ReplicaInstanceCreateRequest{
		Replica: &longhorn.Replica{Spec: longhorn.ReplicaSpec{InstanceSpec: longhorn.InstanceSpec{
			DataEngine: longhorn.DataEngineTypeV2,
		}}},
		IPFamily: types.DataEngineIPFamilyIPv6,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "API version >= 8")
}

func TestParseInstancePropagatesIPFamilyForEachV2Object(t *testing.T) {
	tests := []struct {
		name string
		typ  string
	}{
		{name: "engine", typ: string(longhorn.InstanceTypeEngine)},
		{name: "engine frontend", typ: string(longhorn.InstanceTypeEngineFrontend)},
		{name: "replica", typ: string(longhorn.InstanceTypeReplica)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed := parseInstance(&imapi.Instance{
				Name:       tc.name,
				Type:       tc.typ,
				DataEngine: string(longhorn.DataEngineTypeV2),
				InstanceStatus: imapi.InstanceStatus{
					IPFamily: types.DataEngineIPFamilyIPv4,
				},
			})
			require.NotNil(t, parsed)
			require.Equal(t, types.DataEngineIPFamilyIPv4, parsed.Status.IPFamily)
		})
	}
}

func TestV1ReplicaProcessArgumentsRemainAvailableForInstanceCreate(t *testing.T) {
	replica := &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{Name: "replica-a"},
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				Image:      "engine-image",
				VolumeName: "volume-a",
				VolumeSize: 1 << 20,
				DataEngine: longhorn.DataEngineTypeV1,
			},
		},
	}

	binary, args, err := getBinaryAndArgsForReplicaProcessCreation(
		replica, "/var/lib/longhorn", "", longhorn.DataLocalityDisabled, DefaultReplicaPortCountV1, 9, false)
	require.NoError(t, err)
	require.NotEmpty(t, binary)
	require.NotEmpty(t, args)
	require.Contains(t, args, "replica")
	require.Contains(t, args, "--volume-name")
}
