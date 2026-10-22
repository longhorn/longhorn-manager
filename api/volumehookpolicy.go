package api

import (
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

// VolumeHookPolicyList serves GET /v1/volumehookpolicies[?namespace=<ns>].
// Without the query parameter it lists the policies of every namespace,
// sorted by namespace and name. Commands are not part of the response.
func (s *Server) VolumeHookPolicyList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	collection, err := s.volumeHookPolicyList(req.URL.Query().Get("namespace"))
	if err != nil {
		return err
	}
	apiContext.Write(collection)
	return nil
}

// volumeHookPolicyListAll backs the websocket stream. The stream has no
// access to the request, so it always streams every namespace.
func (s *Server) volumeHookPolicyListAll(*api.ApiContext) (*client.GenericCollection, error) {
	return s.volumeHookPolicyList("")
}

func (s *Server) volumeHookPolicyList(namespace string) (*client.GenericCollection, error) {
	policies, err := s.m.ListVolumeHookPolicies(namespace)
	if err != nil {
		if namespace == "" {
			return nil, errors.Wrap(err, "failed to list volume hook policies")
		}
		return nil, errors.Wrapf(err, "failed to list volume hook policies in namespace %v", namespace)
	}
	return toVolumeHookPolicyCollection(policies), nil
}

// VolumeHookPolicyGet serves GET /v1/volumehookpolicies/{namespace}/{name}.
// A policy that does not exist is a 404, which is also the answer a client
// wants for a group that failed with PolicyNotFound.
func (s *Server) VolumeHookPolicyGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	namespace := mux.Vars(req)["namespace"]
	name := mux.Vars(req)["name"]
	policy, err := s.m.GetVolumeHookPolicy(namespace, name)
	if err != nil {
		return errors.Wrapf(err, "failed to get volume hook policy %v/%v", namespace, name)
	}
	apiContext.Write(toVolumeHookPolicyResource(policy))
	return nil
}
