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

func (s *Server) ObjectEndpointList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)
	col, err := s.objectEndpointList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(col)
	return nil
}

func (s *Server) objectEndpointList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListObjectEndpointsSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list object endpoints")
	}
	return toObjectEndpointCollection(list, apiContext), nil
}

func (s *Server) ObjectEndpointGet(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)
	resp := &client.GenericCollection{}
	apiContext.Write(resp)
	return nil
}

func (s *Server) ObjectEndpointCreate(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	var input ObjectEndpointInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	obj, err := s.m.CreateObjectEndpoint(&longhorn.ObjectEndpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name: input.Name,
		},
		Spec: longhorn.ObjectEndpointSpec{
			Size:         resource.MustParse(input.Size),
			StorageClass: input.StorageClass,
			Credentials: longhorn.ObjectEndpointCredentials{
				AccessKey: input.AccessKey,
				SecretKey: input.SecretKey,
			},
		},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create object endpoint %v", input.Name)
	}

	resp := toObjectEndpointResource(obj)
	apiContext.Write(resp)
	return nil
}

func (s *Server) ObjectEndpointUpdate(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)
	resp := &client.GenericCollection{}
	apiContext.Write(resp)
	return nil
}

func (s *Server) ObjectEndpointDelete(rw http.ResponseWriter, req *http.Request) (err error) {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteObjectEndpoint(id); err != nil {
		return errors.Wrapf(err, "failed to delete object endpoint %v", id)
	}
	return nil
}
