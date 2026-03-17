package client

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
)

// BackingImageCreate creates a backing image in the specified lvstore.
func (c *SPDKClient) BackingImageCreate(name, backingImageUUID, lvsUUID string, size uint64, checksum string, fromAddress string, srcLvsUUID string) (*api.BackingImage, error) {
	if name == "" || backingImageUUID == "" || checksum == "" || lvsUUID == "" || size == 0 {
		return nil, fmt.Errorf("failed to start SPDK backing image: missing required parameters")
	}
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageCreate(ctx, &spdkrpc.BackingImageCreateRequest{
		Name:             name,
		BackingImageUuid: backingImageUUID,
		LvsUuid:          lvsUUID,
		Size:             size,
		Checksum:         checksum,
		FromAddress:      fromAddress,
		SrcLvsUuid:       srcLvsUUID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start SPDK backing image")
	}
	return api.ProtoBackingImageToBackingImage(resp), nil
}

// BackingImageDelete deletes a backing image from the specified lvstore.
func (c *SPDKClient) BackingImageDelete(name, lvsUUID string) error {
	if name == "" || lvsUUID == "" {
		return fmt.Errorf("failed to delete SPDK backingImage: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.BackingImageDelete(ctx, &spdkrpc.BackingImageDeleteRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	return errors.Wrapf(err, "failed to delete SPDK backing image %v", name)
}

// BackingImageGet returns the current state of a backing image.
func (c *SPDKClient) BackingImageGet(name, lvsUUID string) (*api.BackingImage, error) {
	if name == "" || lvsUUID == "" {
		return nil, fmt.Errorf("failed to get SPDK BackingImage: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageGet(ctx, &spdkrpc.BackingImageGetRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK backing image %v", name)
	}
	return api.ProtoBackingImageToBackingImage(resp), nil
}

// BackingImageList returns all backing images known to the SPDK service.
func (c *SPDKClient) BackingImageList() (map[string]*api.BackingImage, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageList(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK backing images")
	}

	res := map[string]*api.BackingImage{}
	for name, backingImage := range resp.BackingImages {
		res[name] = api.ProtoBackingImageToBackingImage(backingImage)
	}
	return res, nil
}

// BackingImageWatch opens a watch stream for backing image change events.
func (c *SPDKClient) BackingImageWatch(ctx context.Context) (*api.BackingImageStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.BackingImageWatch(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open backing image watch stream")
	}

	return api.NewBackingImageStream(stream), nil
}

// BackingImageExpose exposes a backing image as a snapshot lvol and returns its address.
func (c *SPDKClient) BackingImageExpose(name, lvsUUID string) (exposedSnapshotLvolAddress string, err error) {
	if name == "" || lvsUUID == "" {
		return "", fmt.Errorf("failed to expose SPDK backing image: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.BackingImageExpose(ctx, &spdkrpc.BackingImageGetRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to expose SPDK backing image %v in lvstore: %v", name, lvsUUID)
	}
	return resp.ExposedSnapshotLvolAddress, nil
}

// BackingImageUnexpose stops exposing a backing image in the specified lvstore.
func (c *SPDKClient) BackingImageUnexpose(name, lvsUUID string) error {
	if name == "" || lvsUUID == "" {
		return fmt.Errorf("failed to unexpose SPDK backing image: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.BackingImageUnexpose(ctx, &spdkrpc.BackingImageGetRequest{
		Name:    name,
		LvsUuid: lvsUUID,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to unexpose SPDK backing image %v in lvstore %v", name, lvsUUID)
	}
	return nil
}
