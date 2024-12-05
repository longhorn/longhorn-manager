package server

import (
	"net/http"

	"github.com/rancher/wrangler/v3/pkg/webhook"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/dataengineupgrademanager"
	"github.com/longhorn/longhorn-manager/webhook/resources/engine"
	"github.com/longhorn/longhorn-manager/webhook/resources/instancemanager"
	"github.com/longhorn/longhorn-manager/webhook/resources/node"
	"github.com/longhorn/longhorn-manager/webhook/resources/nodedataengineupgrade"
	"github.com/longhorn/longhorn-manager/webhook/resources/orphan"
	"github.com/longhorn/longhorn-manager/webhook/resources/recurringjob"
	"github.com/longhorn/longhorn-manager/webhook/resources/replica"
	"github.com/longhorn/longhorn-manager/webhook/resources/setting"
	"github.com/longhorn/longhorn-manager/webhook/resources/snapshot"
	"github.com/longhorn/longhorn-manager/webhook/resources/supportbundle"
	"github.com/longhorn/longhorn-manager/webhook/resources/systembackup"
	"github.com/longhorn/longhorn-manager/webhook/resources/systemrestore"
	"github.com/longhorn/longhorn-manager/webhook/resources/volume"
	"github.com/longhorn/longhorn-manager/webhook/resources/volumeattachment"
)

func Validation(ds *datastore.DataStore) (http.Handler, []admission.Resource, error) {
	currentNodeID, err := util.GetRequiredEnv(types.EnvNodeName)
	if err != nil {
		return nil, nil, err
	}

	resources := []admission.Resource{}
	validators := []admission.Validator{
		node.NewValidator(ds),
		setting.NewValidator(ds),
		recurringjob.NewValidator(ds),
		backingimage.NewValidator(ds),
		volume.NewValidator(ds, currentNodeID),
		orphan.NewValidator(ds),
		snapshot.NewValidator(ds),
		supportbundle.NewValidator(ds),
		systembackup.NewValidator(ds),
		systemrestore.NewValidator(ds),
		volumeattachment.NewValidator(ds),
		engine.NewValidator(ds),
		replica.NewValidator(ds),
		instancemanager.NewValidator(ds),
		dataengineupgrademanager.NewValidator(ds),
		nodedataengineupgrade.NewValidator(ds),
	}

	router := webhook.NewRouter()
	for _, v := range validators {
		addHandler(router, admission.AdmissionTypeValidation, admission.NewValidatorAdapter(v))
		resources = append(resources, v.Resource())
	}

	return router, resources, nil
}
