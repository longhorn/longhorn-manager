package server

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/longhorn/longhorn-manager/api"
)

func NewRouter(s *RecoveryBackendServer) *mux.Router {
	schemas := NewSchema()
	r := mux.NewRouter().StrictSlash(true)
	f := api.HandleError

	r.Methods(http.MethodPost).Path("/v1/recoverybackend").Handler(f(schemas, s.Create))
	r.Methods(http.MethodPut).Path("/v1/recoverybackend/{hostname}").Handler(f(schemas, s.EndGrace))
	r.Methods(http.MethodPut).Path("/v1/recoverybackend/{hostname}/{clientid}").Handler(f(schemas, s.AddClientID))
	r.Methods(http.MethodDelete).Path("/v1/recoverybackend/{hostname}/{clientid}").Handler(f(schemas, s.RemoveClientID))
	r.Methods(http.MethodGet).Path("/v1/recoverybackend/{hostname}").Handler(f(schemas, s.RecoveryBackendReadClientIDs))
	r.Methods(http.MethodPut).Path("/v1/recoverybackend/{hostname}/{clientid}/{revokeFilehandle}").Handler(f(schemas, s.AddRevokeFilehandle))

	return r
}
