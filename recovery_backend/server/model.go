package server

import (
	"github.com/rancher/go-rancher/client"
)

type RecoveryBackendInput struct {
	Hostname string `json:"hostname"`
	Version  string `json:"version"`
}

type RecoveryBackendStatus struct {
	client.Resource
	Hostname string   `json:"hostname"`
	Clients  []string `json:"clients"`
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("error", client.ServerApiError{})

	schemas.AddType("recoveryBackendInput", RecoveryBackendInput{})
	schemas.AddType("recoveryBackendStatus", RecoveryBackendStatus{})

	return schemas
}
