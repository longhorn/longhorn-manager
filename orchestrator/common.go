package orchestrator

import (
	"fmt"

	"github.com/rancher/longhorn-manager/util"
)

func ValidateRequestStartController(request *Request) error {
	if request.Instance == "" {
		return fmt.Errorf("missing required field %+v", request)
	}

	if request.VolumeName == "" ||
		request.ReplicaURLs == nil ||
		request.NodeID == "" {
		return fmt.Errorf("missing required field %+v", request)
	}
	size, err := util.ConvertSize(request.VolumeSize)
	if err != nil {
		return err
	}
	if size == 0 {
		return fmt.Errorf("invalid volume size %v", request.VolumeSize)
	}
	return nil
}

func ValidateRequestStartReplica(request *Request) error {
	if request.Instance == "" || request.VolumeName == "" {
		return fmt.Errorf("missing required field %+v", request)
	}
	size, err := util.ConvertSize(request.VolumeSize)
	if err != nil {
		return err
	}
	if size == 0 {
		return fmt.Errorf("invalid volume size %v", request.VolumeSize)
	}
	return nil
}

func ValidateRequestInstanceOps(request *Request) error {
	if request.Instance == "" || request.VolumeName == "" {
		return fmt.Errorf("missing required field %+v", request)
	}
	return nil
}
