package orchestrator

import (
	"fmt"
)

func ValidateRequestStartController(request *Request) error {
	if request.Instance == "" {
		return fmt.Errorf("missing required field %+v", request)
	}

	if request.VolumeName == "" ||
		request.VolumeSize == 0 ||
		request.ReplicaURLs == nil {
		return fmt.Errorf("missing required field %+v", request)
	}
	return nil
}

func ValidateRequestCreateReplica(request *Request) error {
	if request.Instance == "" || request.VolumeSize == 0 {
		return fmt.Errorf("missing required field %+v", request)
	}
	return nil
}

func ValidateRequestInstanceOps(request *Request) error {
	if request.Instance == "" || request.VolumeName == "" {
		return fmt.Errorf("missing required field %+v", request)
	}
	return nil
}
