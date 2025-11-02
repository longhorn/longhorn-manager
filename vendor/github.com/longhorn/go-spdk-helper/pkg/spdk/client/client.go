package client

import (
	"context"
	"net"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/types"
)

type Client struct {
	conn net.Conn

	jsonCli *jsonrpc.Client
}

func NewClient(ctx context.Context) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, types.DefaultJSONServerNetwork, types.DefaultUnixDomainSocketPath)
	if err != nil {
		return nil, errors.Wrap(err, "error opening socket for spdk client")
	}

	return &Client{
		conn:    conn,
		jsonCli: jsonrpc.NewClient(ctx, conn),
	}, nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
