package api

import (
	"fmt"
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func (s *Server) VolumeList(rw http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "unable to list")
	}()

	apiContext := api.GetApiContext(req)

	resp := &client.GenericCollection{}

	volumes, err := s.ds.ListVolumes()
	if err != nil {
		return err
	}

	for _, v := range volumes {
		controller, err := s.ds.GetVolumeEngine(v.Name)
		if err != nil {
			return err
		}
		replicas, err := s.ds.GetVolumeReplicas(v.Name)
		if err != nil {
			return err
		}
		resp.Data = append(resp.Data, toVolumeResource(v, controller, replicas, apiContext))
	}
	resp.ResourceType = "volume"
	resp.CreateTypes = map[string]string{
		"volume": apiContext.UrlBuilder.Collection("volume"),
	}
	apiContext.Write(resp)

	return nil
}

func (s *Server) VolumeGet(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	return s.responseWithVolume(rw, req, id)
}

func (s *Server) responseWithVolume(rw http.ResponseWriter, req *http.Request, id string) error {
	apiContext := api.GetApiContext(req)

	v, err := s.ds.GetVolume(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	if v == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}
	controller, err := s.ds.GetVolumeEngine(id)
	if err != nil {
		return err
	}
	replicas, err := s.ds.GetVolumeReplicas(id)
	if err != nil {
		return err
	}

	apiContext.Write(toVolumeResource(v, controller, replicas, apiContext))
	return nil
}

func (s *Server) VolumeCreate(rw http.ResponseWriter, req *http.Request) error {
	var v Volume
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&v); err != nil {
		return err
	}

	if err := s.createVolume(&v); err != nil {
		return errors.Wrap(err, "unable to create volume")
	}
	return s.responseWithVolume(rw, req, v.Name)
}

func (s *Server) VolumeDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.ds.DeleteVolume(id); err != nil {
		return errors.Wrap(err, "unable to delete volume")
	}

	return nil
}

func (s *Server) VolumeAttach(rw http.ResponseWriter, req *http.Request) error {
	var input AttachInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	if err := s.attachVolume(id, input.HostID); err != nil {
		return err
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) VolumeDetach(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.detachVolume(id); err != nil {
		return err
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) VolumeSalvage(rw http.ResponseWriter, req *http.Request) error {
	var input SalvageInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read replicaRemoveInput")
	}

	id := mux.Vars(req)["name"]

	if err := s.salvageVolume(id, input.Names); err != nil {
		return errors.Wrap(err, "unable to remove replica")
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) VolumeRecurringUpdate(rw http.ResponseWriter, req *http.Request) error {
	var input RecurringInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error reading recurringInput")
	}

	for _, job := range input.Jobs {
		if job.Cron == "" || job.Type == "" || job.Name == "" || job.Retain == 0 {
			return fmt.Errorf("invalid job %+v", job)
		}
	}

	if err := s.updateVolumeRecurringJobs(id, input.Jobs); err != nil {
		return errors.Wrapf(err, "unable to update recurring jobs for volume %v", id)
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) ReplicaRemove(rw http.ResponseWriter, req *http.Request) error {
	var input ReplicaRemoveInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read replicaRemoveInput")
	}

	id := mux.Vars(req)["name"]

	if err := s.ds.DeleteReplica(input.Name); err != nil {
		return errors.Wrap(err, "unable to remove replica")
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) createVolume(volume *Volume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to create volume %+v", volume)
	}()

	// make it random node's responsibility
	ownerID, err := s.getRandomOwnerID()
	if err != nil {
		return err
	}

	size := volume.Size
	if volume.FromBackup != "" {
		backup, err := engineapi.GetBackup(volume.FromBackup)
		if err != nil {
			return fmt.Errorf("cannot get backup %v: %v", volume.FromBackup, err)
		}
		size = backup.VolumeSize
	}

	v := &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volume.Name,
		},
		Spec: types.VolumeSpec{
			OwnerID:             ownerID,
			Size:                size,
			FromBackup:          volume.FromBackup,
			NumberOfReplicas:    volume.NumberOfReplicas,
			StaleReplicaTimeout: volume.StaleReplicaTimeout,
			DesireState:         types.VolumeStateDetached,
		},
	}
	if _, err := s.ds.CreateVolume(v); err != nil {
		return err
	}
	logrus.Debugf("Created volume %v", v.Name)
	return nil
}

func (s *Server) getRandomOwnerID() (string, error) {
	var node *longhorn.Node
	nodes, err := s.ds.ListNodes()
	if err != nil {
		return "", err
	}
	// map is random in Go
	for _, n := range nodes {
		node = n
		break
	}

	if node == nil {
		return "", fmt.Errorf("cannot find healthy node")
	}
	return node.ID, nil
}

func (s *Server) attachVolume(volumeName, nodeID string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to attach volume %v to %v", volumeName, nodeID)
	}()

	v, err := s.ds.GetVolume(volumeName)
	if err != nil {
		return err
	}
	if v == nil {
		return fmt.Errorf("cannot find volume %v", volumeName)
	}
	// already desired to be attached
	if v.Spec.DesireState == types.VolumeStateHealthy {
		if v.Spec.NodeID != nodeID {
			return fmt.Errorf("Node to be attached %v is different from previous spec %v", nodeID, v.Spec.NodeID)
		}
		return nil
	}
	if v.Status.State != types.VolumeStateDetached {
		return fmt.Errorf("invalid state to attach %v: %v", volumeName, v.Status.State)
	}
	v.Spec.NodeID = nodeID
	// Must be owned by the manager on the same node
	v.Spec.OwnerID = v.Spec.NodeID
	v.Spec.DesireState = types.VolumeStateHealthy
	if _, err := s.ds.UpdateVolume(v); err != nil {
		return err
	}
	logrus.Debugf("Attaching volume %v to %v", v.Name, v.Spec.NodeID)
	return err
}

func (s *Server) detachVolume(volumeName string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to detach volume %v", volumeName)
	}()

	v, err := s.ds.GetVolume(volumeName)
	if err != nil {
		return err
	}
	if v == nil {
		return fmt.Errorf("cannot find volume %v", volumeName)
	}
	if v.Status.State != types.VolumeStateHealthy && v.Status.State != types.VolumeStateDegraded {
		return fmt.Errorf("invalid state to detach %v: %v", v.Name, v.Status.State)
	}

	v.Spec.DesireState = types.VolumeStateDetached
	v.Spec.NodeID = ""
	if _, err := s.ds.UpdateVolume(v); err != nil {
		return err
	}
	logrus.Debugf("Detaching volume %v from %v", v.Name, v.Spec.NodeID)
	return nil
}

func (s *Server) updateVolumeRecurringJobs(volumeName string, jobs []types.RecurringJob) (err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update volume recurring jobs for %v", volumeName)
	}()

	v, err := s.ds.GetVolume(volumeName)
	if err != nil {
		return err
	}
	if v == nil {
		return fmt.Errorf("cannot find volume %v", volumeName)
	}

	v.Spec.RecurringJobs = jobs
	if _, err := s.ds.UpdateVolume(v); err != nil {
		return err
	}
	logrus.Debugf("Updating volume %v recurring jobs", v.Name)
	return nil
}

func (s *Server) salvageVolume(volumeName string, salvageReplicaNames []string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to salvage volume %v", volumeName)
	}()

	v, err := s.ds.GetVolume(volumeName)
	if err != nil {
		return err
	}
	if v == nil {
		return fmt.Errorf("cannot find volume %v", volumeName)
	}
	if v.Status.State != types.VolumeStateFault {
		return fmt.Errorf("invalid state to salvage: %v", v.Status.State)
	}

	for _, names := range salvageReplicaNames {
		r, err := s.ds.GetReplica(names)
		if err != nil {
			return err
		}
		if r.Spec.VolumeName != v.Name {
			return fmt.Errorf("replica %v doesn't belong to volume %v", r.Name, v.Name)
		}
		if r.Spec.FailedAt == "" {
			return fmt.Errorf("replica %v is not in failed state", r.Name)
		}
		r.Spec.FailedAt = ""
		if _, err := s.ds.UpdateReplica(r); err != nil {
			return err
		}
	}

	v.Spec.DesireState = types.VolumeStateDetached
	if _, err := s.ds.UpdateVolume(v); err != nil {
		return err
	}
	logrus.Debugf("Salvaging replica %+v for volume %v", salvageReplicaNames, v.Name)
	return nil
}
