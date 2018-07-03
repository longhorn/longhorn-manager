package api

import (
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

func (s *Server) NodeList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	nodeList, err := s.nodeList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(nodeList)
	return nil
}

func (s *Server) nodeList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	nodeList, err := s.m.ListNodesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "fail to list nodes")
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return nil, errors.Wrap(err, "fail to get node ip")
	}
	return toNodeCollection(nodeList, nodeIPMap, apiContext), nil
}

func (s *Server) NodeGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["name"]

	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrap(err, "fail to get node")
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}
	apiContext.Write(toNodeResource(node, nodeIPMap[node.Name], apiContext))
	return nil
}

func (s *Server) NodeUpdate(rw http.ResponseWriter, req *http.Request) error {
	var n Node
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&n); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]
	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrap(err, "fail to get node")
	}
	node.Spec.AllowScheduling = n.AllowScheduling

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}

	unode, err := s.m.UpdateNode(node)
	if err != nil {
		return err
	}
	apiContext.Write(toNodeResource(unode, nodeIPMap[node.Name], apiContext))
	return nil
}

func (s *Server) DiskUpdate(rw http.ResponseWriter, req *http.Request) error {
	var diskUpdate DiskUpdateInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&diskUpdate); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]
	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrap(err, "fail to get node")
	}

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}

	originDisks := node.Spec.Disks
	diskUpdateMap := map[string]types.DiskSpec{}
	for _, uDisk := range diskUpdate.Disks {
		diskInfo, err := util.GetDiskInfo(uDisk.Path)
		if err != nil {
			return err
		}
		// update disks
		if oDisk, ok := originDisks[diskInfo.Fsid]; ok {
			if oDisk.Path != uDisk.Path {
				// current disk is the same file system with exist disk
				return fmt.Errorf("Add Disk on node %v error: The disk %v is the same file system with %v ", id, uDisk.Path, oDisk.Path)
			} else if oDisk.StorageMaximum != uDisk.StorageMaximum && uDisk.StorageMaximum != diskInfo.StorageMaximum {
				logrus.Warnf("StorageMaximum has been changed for disk %v of node %v. Detected maximum storage %v, current setting %v", diskInfo.Path, id, diskInfo.StorageMaximum, uDisk.StorageMaximum)
			}
		} else {
			// add disks
			if uDisk.StorageMaximum != 0 && uDisk.StorageMaximum != diskInfo.StorageMaximum {
				logrus.Warnf("StorageMaximum has been changed for disk %v of node %v. Detected maximum storage %v, current setting %v", diskInfo.Path, id, diskInfo.StorageMaximum, uDisk.StorageMaximum)
			} else {
				uDisk.StorageMaximum = diskInfo.StorageMaximum
			}
		}
		diskUpdateMap[diskInfo.Fsid] = uDisk
	}

	// delete disks
	for fsid, oDisk := range originDisks {
		if _, ok := diskUpdateMap[fsid]; !ok {
			if oDisk.AllowScheduling || node.Status.DiskStatus[fsid].StorageScheduled != 0 {
				return fmt.Errorf("Delete Disk on node %v error: Please disable the disk %v and remove all replicas first ", id, oDisk.Path)
			}
		}
	}
	node.Spec.Disks = diskUpdateMap

	unode, err := s.m.UpdateNode(node)
	if err != nil {
		return err
	}
	apiContext.Write(toNodeResource(unode, nodeIPMap[node.Name], apiContext))
	return nil
}
