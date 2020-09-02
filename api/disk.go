package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

func (s *Server) DiskList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	diskList, err := s.diskList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(diskList)
	return nil
}

func (s *Server) diskList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	diskList, err := s.m.ListDisksSorted()
	if err != nil {
		return nil, errors.Wrap(err, "fail to list disks")
	}
	return toDiskCollection(diskList, apiContext), nil
}

func (s *Server) DiskGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["name"]

	disk, err := s.m.GetDisk(id)
	if err != nil {
		return errors.Wrapf(err, "fail to get disk %v", id)
	}
	apiContext.Write(toDiskResource(disk, apiContext))
	return nil
}

func (s *Server) DiskDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteDisk(id); err != nil {
		return errors.Wrap(err, "unable to delete disk")
	}

	return nil
}

func (s *Server) DiskCreate(rw http.ResponseWriter, req *http.Request) error {
	var d Disk
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&d); err != nil {
		return err
	}

	diskSpec := &types.DiskSpec{
		AllowScheduling:   d.AllowScheduling,
		EvictionRequested: d.EvictionRequested,
		StorageReserved:   d.StorageReserved,
		Tags:              d.Tags,
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.CreateDisk(d.NodeID, d.Path, diskSpec)
	})
	if err != nil {
		return err
	}

	disk, ok := obj.(*longhorn.Disk)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to disk object")
	}
	apiContext.Write(toDiskResource(disk, apiContext))
	return nil
}

func (s *Server) DiskUpdate(rw http.ResponseWriter, req *http.Request) error {
	var d Disk
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&d); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	// Only scheduling disabled disk can be evicted
	// Can not enable scheduling on an evicting disk
	if d.EvictionRequested == true && d.AllowScheduling != false {
		return fmt.Errorf("Need to disable scheduling on this disk for disk eviction, or cancel eviction to enable scheduling on this disk")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		disk, err := s.m.GetDisk(id)
		if err != nil {
			return nil, err
		}
		disk.Spec.AllowScheduling = d.AllowScheduling
		disk.Spec.EvictionRequested = d.EvictionRequested
		disk.Spec.StorageReserved = d.StorageReserved
		disk.Spec.Tags = d.Tags
		return s.m.UpdateDisk(disk)
	})
	if err != nil {
		return err
	}

	disk, ok := obj.(*longhorn.Disk)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to disk %v object", id)
	}
	apiContext.Write(toDiskResource(disk, apiContext))
	return nil
}
