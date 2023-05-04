package server

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/recovery_backend/backend"
)

func (r *RecoveryBackendServer) Create(rw http.ResponseWriter, req *http.Request) error {
	var input RecoveryBackendInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	r.rb.Logger.Infof("Creating a recovery backend %v (version %v)", input.Hostname, input.Version)
	return r.rb.CreateConfigMap(input.Hostname, input.Version)
}

func (r *RecoveryBackendServer) EndGrace(rw http.ResponseWriter, req *http.Request) error {
	var input RecoveryBackendInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	hostname := mux.Vars(req)["hostname"]

	r.rb.Logger.Infof("Ending grace for recovery backend %v (version %v)", hostname, input.Version)
	return r.rb.EndGrace(hostname, input.Version)
}

func (r *RecoveryBackendServer) AddClientID(rw http.ResponseWriter, req *http.Request) error {
	var input RecoveryBackendInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	hostname := mux.Vars(req)["hostname"]
	clientID := mux.Vars(req)["clientid"]

	r.rb.Logger.Infof("Adding client '%v' to the recovery backend %v (version %v)", clientID, hostname, input.Version)
	return r.rb.AddClientID(hostname, input.Version, backend.ClientID(clientID))
}

func (r *RecoveryBackendServer) RemoveClientID(rw http.ResponseWriter, req *http.Request) error {
	hostname := mux.Vars(req)["hostname"]
	clientID := mux.Vars(req)["clientid"]

	r.rb.Logger.Infof("Removing client '%v' from the recovery backend %v", clientID, hostname)
	return r.rb.RemoveClientID(hostname, backend.ClientID(clientID))
}

func (r *RecoveryBackendServer) RecoveryBackendReadClientIDs(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	hostname := mux.Vars(req)["hostname"]

	r.rb.Logger.Infof("Reading clients from recovery backend %v", hostname)
	clients, err := r.rb.ReadClientIDs(hostname)
	if err != nil {
		return err
	}

	status := &RecoveryBackendStatus{
		Resource: client.Resource{
			Id:    hostname,
			Type:  "recoveryBackendStatus",
			Links: map[string]string{},
		},
		Hostname: hostname,
		Clients:  clients,
	}

	apiContext.Write(status)
	return nil
}

func (r *RecoveryBackendServer) AddRevokeFilehandle(rw http.ResponseWriter, req *http.Request) error {
	var input RecoveryBackendInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	hostname := mux.Vars(req)["hostname"]
	clientID := mux.Vars(req)["clientid"]
	revokeFilehandle := mux.Vars(req)["revokeFilehandle"]

	r.rb.Logger.Infof("Adding client '%v' revoke filehandle '%v' to the recovery backend %v with version %v",
		clientID, revokeFilehandle, hostname, input.Version)
	return r.rb.AddRevokeFilehandle(hostname, input.Version, backend.ClientID(clientID), backend.RevokeFileHandle(revokeFilehandle))
}
