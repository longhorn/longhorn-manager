package kvstore

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	eCli "github.com/coreos/etcd/client"
)

type ETCDBackend struct {
	Servers []string

	kapi eCli.KeysAPI
}

func NewETCDBackend(servers []string) (*ETCDBackend, error) {
	eCfg := eCli.Config{
		Endpoints:               servers,
		Transport:               eCli.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	etcdc, err := eCli.New(eCfg)
	if err != nil {
		return nil, err
	}
	backend := &ETCDBackend{
		Servers: servers,

		kapi: eCli.NewKeysAPI(etcdc),
	}
	return backend, nil
}

func (s *ETCDBackend) Create(key string, obj interface{}) error {
	value, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	if _, err := s.kapi.Create(context.Background(), key, string(value)); err != nil {
		return err
	}
	return nil
}

func (s *ETCDBackend) Set(key string, obj interface{}) error {
	value, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	if _, err := s.kapi.Set(context.Background(), key, string(value), nil); err != nil {
		return err
	}
	return nil
}

func (s *ETCDBackend) IsNotFoundError(err error) bool {
	return eCli.IsKeyNotFound(err)
}

func (s *ETCDBackend) Get(key string, obj interface{}) error {
	resp, err := s.kapi.Get(context.Background(), key, nil)
	if err != nil {
		return err
	}
	node := resp.Node
	if node.Dir {
		return errors.Errorf("invalid node %v is a directory",
			node.Key)
	}

	if err := json.Unmarshal([]byte(node.Value), obj); err != nil {
		return errors.Wrap(err, "fail to unmarshal json")
	}
	return nil
}

func (s *ETCDBackend) Keys(prefix string) ([]string, error) {
	resp, err := s.kapi.Get(context.Background(), prefix, nil)
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !resp.Node.Dir {
		return nil, errors.Errorf("invalid node %v is not a directory",
			resp.Node.Key)
	}

	ret := []string{}
	for _, node := range resp.Node.Nodes {
		ret = append(ret, node.Key)
	}
	return ret, nil
}

func (s *ETCDBackend) Delete(key string) error {
	_, err := s.kapi.Delete(context.Background(), key, &eCli.DeleteOptions{
		Recursive: true,
	})
	if err != nil {
		if eCli.IsKeyNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}
