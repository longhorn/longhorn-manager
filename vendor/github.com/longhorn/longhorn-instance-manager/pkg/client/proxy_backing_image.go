package client

import (
	"context"
	"fmt"

	"github.com/longhorn/longhorn-instance-manager/pkg/api"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (c *ProxyClient) SPDKBackingImageCreate(name, backingImageUUID, diskUUID, checksum, fromAddress, srcLvsUUID string, size uint64) (*api.BackingImage, error) {
	if name == "" || backingImageUUID == "" || checksum == "" || diskUUID == "" || size == 0 {
		return nil, fmt.Errorf("failed to create backing image: missing required parameters")
	}

	client := c.service
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.SPDKBackingImageCreate(ctx, &rpc.SPDKBackingImageCreateRequest{
		Name:             name,
		BackingImageUuid: backingImageUUID,
		DiskUuid:         diskUUID,
		Size:             size,
		Checksum:         checksum,
		FromAddress:      fromAddress,
		SrcLvsUuid:       srcLvsUUID,
	})
	if err != nil {
		return nil, err
	}

	return api.RPCToBackingImage(resp), nil
}

func (c *ProxyClient) SPDKBackingImageDelete(name, diskUUID string) error {
	if name == "" || diskUUID == "" {
		return fmt.Errorf("failed to delete backing image: missing required parameters")
	}

	client := c.service
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	_, err := client.SPDKBackingImageDelete(ctx, &rpc.SPDKBackingImageDeleteRequest{
		Name:     name,
		DiskUuid: diskUUID,
	})
	return err
}

func (c *ProxyClient) SPDKBackingImageGet(name, diskUUID string) (*api.BackingImage, error) {
	if name == "" || diskUUID == "" {
		return nil, fmt.Errorf("failed to get backing image: missing required parameters")
	}

	client := c.service
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.SPDKBackingImageGet(ctx, &rpc.SPDKBackingImageGetRequest{
		Name:     name,
		DiskUuid: diskUUID,
	})
	if err != nil {
		return nil, err
	}
	return api.RPCToBackingImage(resp), nil
}

func (c *ProxyClient) SPDKBackingImageList() (map[string]*api.BackingImage, error) {
	client := c.service
	ctx, cancel := context.WithTimeout(context.Background(), types.GRPCServiceTimeout)
	defer cancel()

	resp, err := client.SPDKBackingImageList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list backing images")
	}
	return api.RPCToBackingImageList(resp), nil
}

func (c *ProxyClient) SPDKBackingImageWatch(ctx context.Context) (*api.BackingImageStream, error) {
	client := c.service
	stream, err := client.SPDKBackingImageWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open backing image update stream")
	}

	return api.NewBackingImageStream(stream), nil
}
