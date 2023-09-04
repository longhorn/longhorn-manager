package api

import (
	"net/http"

	"github.com/gorilla/mux"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Server) ObjectStoreList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)
	col, err := s.objectStoreList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(col)
	return nil
}

func (s *Server) objectStoreList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListObjectStoresSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list object stores")
	}
	return toObjectStoreCollection(list, apiContext), nil
}

func (s *Server) ObjectStoreGet(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)
	resp := &client.GenericCollection{}
	apiContext.Write(resp)
	return nil
}

func (s *Server) ObjectStoreCreate(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	var input ObjectStoreInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	obj, err := s.m.CreateObjectStore(&longhorn.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: input.Name,
		},
		Spec: longhorn.ObjectStoreSpec{
			Storage: longhorn.ObjectStoreStorageSpec{
				Size:                        resource.MustParse(input.Size),
				NumberOfReplicas:            input.NumberOfReplicas,
				ReplicaSoftAntiAffinity:     input.ReplicaSoftAntiAffinity,
				ReplicaZoneSoftAntiAffinity: input.ReplicaZoneSoftAntiAffinity,
				ReplicaDiskSoftAntiAffinity: input.ReplicaDiskSoftAntiAffinity,
				DiskSelector:                input.DiskSelector,
				NodeSelector:                input.NodeSelector,
				DataLocality:                input.DataLocality,
				FromBackup:                  input.FromBackup,
				StaleReplicaTimeout:         input.StaleReplicaTimeout,
				RecurringJobSelector:        input.RecurringJobSelector,
				ReplicaAutoBalance:          input.ReplicaAutoBalance,
				RevisionCounterDisabled:     input.RevisionCounterDisabled,
				UnmapMarkSnapChainRemoved:   input.UnmapMarkSnapChainRemoved,
				BackendStoreDriver:          input.BackendStoreDriver,
			},
			Credentials: longhorn.ObjectStoreCredentials{
				AccessKey: input.AccessKey,
				SecretKey: input.SecretKey,
			},
			Endpoints:   []longhorn.ObjectStoreEndpointSpec{},
			TargetState: input.TargetState,
			Image:       input.Image,
			UiImage:     input.UIImage,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create object store %v", input.Name)
	}

	resp := toObjectStoreResource(obj)
	apiContext.Write(resp)
	return nil
}

// ObjectStoreUpdate - currently intentionally stubbed out
func (s *Server) ObjectStoreUpdate(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	var input ObjectStoreInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	// TODO: reduce to set of properties that make sense being updated
	obj, err := s.m.UpdateObjectStore(&longhorn.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: input.Name,
		},
		Spec: longhorn.ObjectStoreSpec{
			Storage: longhorn.ObjectStoreStorageSpec{
				Size:                        resource.MustParse(input.Size),
				NumberOfReplicas:            input.NumberOfReplicas,
				ReplicaSoftAntiAffinity:     input.ReplicaSoftAntiAffinity,
				ReplicaZoneSoftAntiAffinity: input.ReplicaZoneSoftAntiAffinity,
				ReplicaDiskSoftAntiAffinity: input.ReplicaDiskSoftAntiAffinity,
				DiskSelector:                input.DiskSelector,
				NodeSelector:                input.NodeSelector,
				DataLocality:                input.DataLocality,
				FromBackup:                  input.FromBackup,
				StaleReplicaTimeout:         input.StaleReplicaTimeout,
				RecurringJobSelector:        input.RecurringJobSelector,
				ReplicaAutoBalance:          input.ReplicaAutoBalance,
				RevisionCounterDisabled:     input.RevisionCounterDisabled,
				UnmapMarkSnapChainRemoved:   input.UnmapMarkSnapChainRemoved,
				BackendStoreDriver:          input.BackendStoreDriver,
			},
			Credentials: longhorn.ObjectStoreCredentials{
				AccessKey: input.AccessKey,
				SecretKey: input.SecretKey,
			},
			Endpoints:   []longhorn.ObjectStoreEndpointSpec{},
			TargetState: input.TargetState,
			Image:       input.Image,
			UiImage:     input.UIImage,
		},
	})
	resp := toObjectStoreResource(obj)
	apiContext.Write(resp)
	return nil
}

func (s *Server) ObjectStoreDelete(rw http.ResponseWriter, req *http.Request) (err error) {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteObjectStore(id); err != nil {
		return errors.Wrapf(err, "failed to delete object store %v", id)
	}
	return nil
}
