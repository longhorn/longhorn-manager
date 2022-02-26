package server

import (
	"net/http"

	"github.com/rancher/wrangler/pkg/webhook"

	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/client"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/engineimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/node"
	"github.com/longhorn/longhorn-manager/webhook/resources/recurringjob"
	"github.com/longhorn/longhorn-manager/webhook/resources/setting"
)

func Validation(client *client.Client) (http.Handler, []admission.Resource, error) {
	resources := []admission.Resource{}
	validators := []admission.Validator{
		node.NewValidator(client.Datastore),
		setting.NewValidator(client.Datastore),
		engineimage.NewValidator(client.Datastore),
		recurringjob.NewValidator(client.Datastore),
		backingimage.NewValidator(client.Datastore),
	}

	router := webhook.NewRouter()
	for _, v := range validators {
		addHandler(router, admission.AdmissionTypeValidation, admission.NewValidatorAdapter(v))
		resources = append(resources, v.Resource())
	}

	return router, resources, nil
}
