package orchestrator

import (
	"fmt"
)

func ValidateRequestCreateController(request *Request) error {
	if request.InstanceName == "" {
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
	if request.InstanceName == "" || request.VolumeSize == 0 {
		return fmt.Errorf("missing required field %+v", request)
	}
	return nil
}

func ValidateRequestInstanceOps(request *Request) error {
	if request.InstanceName == "" || request.InstanceID == "" || request.VolumeName == "" {
		return fmt.Errorf("missing required field %+v", request)
	}
	return nil
}
