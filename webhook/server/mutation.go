package server

import (
	"net/http"

	"github.com/rancher/wrangler/pkg/webhook"

	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/client"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimagedatasource"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimagemanager"
	"github.com/longhorn/longhorn-manager/webhook/resources/backup"
	"github.com/longhorn/longhorn-manager/webhook/resources/engine"
	"github.com/longhorn/longhorn-manager/webhook/resources/engineimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/node"
	"github.com/longhorn/longhorn-manager/webhook/resources/orphan"
	"github.com/longhorn/longhorn-manager/webhook/resources/recurringjob"
	"github.com/longhorn/longhorn-manager/webhook/resources/volume"
)

func Mutation(client *client.Client) (http.Handler, []admission.Resource, error) {
	resources := []admission.Resource{}
	mutators := []admission.Mutator{
		backup.NewMutator(client.Datastore),
		backingimage.NewMutator(client.Datastore),
		backingimagemanager.NewMutator(client.Datastore),
		backingimagedatasource.NewMutator(client.Datastore),
		node.NewMutator(client.Datastore),
		volume.NewMutator(client.Datastore),
		engine.NewMutator(client.Datastore),
		recurringjob.NewMutator(client.Datastore),
		engineimage.NewMutator(client.Datastore),
		orphan.NewMutator(client.Datastore),
	}

	router := webhook.NewRouter()
	for _, m := range mutators {
		addHandler(router, admission.AdmissionTypeMutation, m)
		resources = append(resources, m.Resource())
	}

	return router, resources, nil
}
