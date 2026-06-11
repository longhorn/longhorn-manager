package api

import (
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

func (s *Server) ShardGroupList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	sgs, err := s.shardGroupList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(sgs)
	return nil
}

func (s *Server) shardGroupList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListShardGroups()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list shard groups")
	}
	return toShardGroupCollection(list), nil
}

func (s *Server) ShardGroupGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	name := mux.Vars(req)["name"]
	sg, err := s.m.GetShardGroup(name)
	if err != nil {
		return errors.Wrapf(err, "failed to get shard group '%s'", name)
	}
	apiContext.Write(toShardGroupResource(sg))
	return nil
}

func (s *Server) ShardList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	shards, err := s.shardList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(shards)
	return nil
}

func (s *Server) shardList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListShards()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list shards")
	}
	return toShardCollection(list), nil
}

func (s *Server) ShardGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	name := mux.Vars(req)["name"]
	shard, err := s.m.GetShard(name)
	if err != nil {
		return errors.Wrapf(err, "failed to get shard '%s'", name)
	}
	apiContext.Write(toShardResource(shard))
	return nil
}
