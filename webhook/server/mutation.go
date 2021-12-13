package server

import (
	"net/http"

	"github.com/rancher/wrangler/pkg/webhook"

	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/client"
)

func Mutation(client *client.Client) (http.Handler, []admission.Resource, error) {
	resources := []admission.Resource{}
	mutators := []admission.Mutator{}

	router := webhook.NewRouter()
	for _, m := range mutators {
		addHandler(router, admission.AdmissionTypeMutation, m)
		resources = append(resources, m.Resource())
	}

	return router, resources, nil
}
