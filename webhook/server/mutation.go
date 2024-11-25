package server

import (
	"net/http"

	"github.com/rancher/wrangler/v3/pkg/webhook"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimagedatasource"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimagemanager"
	"github.com/longhorn/longhorn-manager/webhook/resources/backup"
	"github.com/longhorn/longhorn-manager/webhook/resources/backupbackingimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/backupvolume"
	"github.com/longhorn/longhorn-manager/webhook/resources/dataengineupgrademanager"
	"github.com/longhorn/longhorn-manager/webhook/resources/engine"
	"github.com/longhorn/longhorn-manager/webhook/resources/engineimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/instancemanager"
	"github.com/longhorn/longhorn-manager/webhook/resources/node"
	"github.com/longhorn/longhorn-manager/webhook/resources/nodedataengineupgrade"
	"github.com/longhorn/longhorn-manager/webhook/resources/orphan"
	"github.com/longhorn/longhorn-manager/webhook/resources/recurringjob"
	"github.com/longhorn/longhorn-manager/webhook/resources/replica"
	"github.com/longhorn/longhorn-manager/webhook/resources/sharemanager"
	"github.com/longhorn/longhorn-manager/webhook/resources/snapshot"
	"github.com/longhorn/longhorn-manager/webhook/resources/supportbundle"
	"github.com/longhorn/longhorn-manager/webhook/resources/systembackup"
	"github.com/longhorn/longhorn-manager/webhook/resources/volume"
	"github.com/longhorn/longhorn-manager/webhook/resources/volumeattachment"
)

func Mutation(ds *datastore.DataStore) (http.Handler, []admission.Resource, error) {
	resources := []admission.Resource{}
	mutators := []admission.Mutator{
		backup.NewMutator(ds),
		backingimage.NewMutator(ds),
		backingimagemanager.NewMutator(ds),
		backingimagedatasource.NewMutator(ds),
		node.NewMutator(ds),
		volume.NewMutator(ds),
		engine.NewMutator(ds),
		recurringjob.NewMutator(ds),
		engineimage.NewMutator(ds),
		orphan.NewMutator(ds),
		sharemanager.NewMutator(ds),
		backupvolume.NewMutator(ds),
		snapshot.NewMutator(ds),
		replica.NewMutator(ds),
		supportbundle.NewMutator(ds),
		systembackup.NewMutator(ds),
		volumeattachment.NewMutator(ds),
		instancemanager.NewMutator(ds),
		backupbackingimage.NewMutator(ds),
		dataengineupgrademanager.NewMutator(ds),
		nodedataengineupgrade.NewMutator(ds),
	}

	router := webhook.NewRouter()
	for _, m := range mutators {
		addHandler(router, admission.AdmissionTypeMutation, m)
		resources = append(resources, m.Resource())
	}

	return router, resources, nil
}
