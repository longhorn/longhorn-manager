package client

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/backing-image-manager/api"
	"github.com/longhorn/backing-image-manager/pkg/meta"
	"github.com/longhorn/backing-image-manager/pkg/rpc"
	"github.com/longhorn/backing-image-manager/pkg/types"
)

type BackingImageManagerClient struct {
	Address string
}

func NewBackingImageManagerClient(address string) *BackingImageManagerClient {
	return &BackingImageManagerClient{
		Address: address,
	}
}

func (cli *BackingImageManagerClient) Pull(name, url, uuid string) (*api.BackingImage, error) {
	if name == "" || url == "" || uuid == "" {
		return nil, fmt.Errorf("failed to pull backing image: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.Pull(ctx, &rpc.PullRequest{
		Spec: &rpc.BackingImageSpec{
			Name: name,
			Url:  url,
			Uuid: uuid,
		},
	})
	if err != nil {
		return nil, err
	}
	return api.RPCToBackingImage(resp), nil
}

func (cli *BackingImageManagerClient) Sync(name, url, uuid, fromHost, toHost string, size int64) (*api.BackingImage, error) {
	if name == "" || uuid == "" || fromHost == "" || toHost == "" || size <= 0 {
		return nil, fmt.Errorf("failed to sync backing image: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.Sync(ctx, &rpc.SyncRequest{
		BackingImageSpec: &rpc.BackingImageSpec{
			Name: name,
			Url:  url,
			Uuid: uuid,
			Size: size,
		},
		FromHost: fromHost,
		ToHost:   toHost,
	})
	if err != nil {
		return nil, err
	}
	return api.RPCToBackingImage(resp), nil
}

func (cli *BackingImageManagerClient) Send(name, toAddress string) error {
	if name == "" || toAddress == "" {
		return fmt.Errorf("failed to send backing image: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err = client.Send(ctx, &rpc.SendRequest{
		Name:      name,
		ToAddress: toAddress,
	})
	return err
}

func (cli *BackingImageManagerClient) Delete(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete backing image: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err = client.Delete(ctx, &rpc.DeleteRequest{
		Name: name,
	})
	return err
}

func (cli *BackingImageManagerClient) Get(name string) (*api.BackingImage, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get backing image: missing required parameter")
	}

	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.Get(ctx, &rpc.GetRequest{
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	return api.RPCToBackingImage(resp), nil
}

func (cli *BackingImageManagerClient) List() (map[string]*api.BackingImage, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.List(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}
	return api.RPCToBackingImageList(resp), nil
}

func (cli *BackingImageManagerClient) OwnershipTransferStart() (map[string]*api.BackingImage, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	transferResp, err := client.OwnershipTransferStart(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}

	resp := map[string]*api.BackingImage{}
	for biName, biSpec := range transferResp.ReadyBackingImages {
		resp[biName] = &api.BackingImage{
			Name: biSpec.Name,
			URL:  biSpec.Url,
			UUID: biSpec.Uuid,
			Size: biSpec.Size,
		}
	}

	return resp, nil
}

func (cli *BackingImageManagerClient) OwnershipTransferConfirm(transferringBackingImages map[string]*api.BackingImage) error {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}
	defer conn.Close()

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	input := map[string]*rpc.BackingImageSpec{}
	for biName, bi := range transferringBackingImages {
		input[biName] = &rpc.BackingImageSpec{
			Name: bi.Name,
			Url:  bi.URL,
			Uuid: bi.UUID,
			Size: bi.Size,
		}
	}

	_, err = client.OwnershipTransferConfirm(ctx, &rpc.OwnershipTransferConfirmRequest{
		ReadyBackingImages: input,
	})
	return err
}

func (cli *BackingImageManagerClient) VersionGet() (*meta.VersionOutput, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}

	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.VersionGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %v", err)
	}
	return &meta.VersionOutput{
		Version:   resp.Version,
		GitCommit: resp.GitCommit,
		BuildDate: resp.BuildDate,

		BackingImageManagerAPIVersion:    int(resp.BackingImageManagerApiVersion),
		BackingImageManagerAPIMinVersion: int(resp.BackingImageManagerApiMinVersion),
	}, nil
}

func (cli *BackingImageManagerClient) Watch() (*api.BackingImageStream, error) {
	conn, err := grpc.Dial(cli.Address, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect backing image manager service to %v: %v", cli.Address, err)
	}

	// Don't cleanup the Client here, we don't know when the user will be done with the Stream. Pass it to the wrapper
	// and allow the user to take care of it.
	client := rpc.NewBackingImageManagerServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Watch(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}
	return api.NewBackingImageStream(conn, cancel, stream), nil
}
