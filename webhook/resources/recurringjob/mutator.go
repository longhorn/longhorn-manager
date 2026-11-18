package recurringjob

import (
	"fmt"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type recurringJobMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &recurringJobMutator{ds: ds}
}

func (r *recurringJobMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "recurringjobs",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.RecurringJob{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (r *recurringJobMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	recurringjob, ok := newObj.(*longhorn.RecurringJob)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.RecurringJob", newObj), "")
	}

	var patchOps admission.PatchOps

	name := util.AutoCorrectName(recurringjob.Name, datastore.NameMaximumLength)
	if name != recurringjob.Name {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/metadata/name", "value": "%s"}`, name))
	}
	if recurringjob.Spec.Name == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/name", "value": "%s"}`, recurringjob.Name))
	}
	if recurringjob.Spec.Groups == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/groups", "value": []}`)
	}
	if recurringjob.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}
	if recurringjob.Spec.Parameters == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/parameters", "value": {}}`)
	}
	if recurringjob.Spec.Concurrency == 0 {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/concurrency", "value": %d}`, types.DefaultRecurringJobConcurrency))
	}

	log := logrus.WithFields(logrus.Fields{
		"recurringJob": recurringjob.Name,
		"task":         recurringjob.Spec.Task,
	})
	patchOps = mutateRetainCountAndRetainAge(patchOps, recurringjob, log)

	return patchOps, nil
}

func (r *recurringJobMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	newRecurringjob, ok := newObj.(*longhorn.RecurringJob)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.RecurringJob", newObj), "")
	}
	var patchOps admission.PatchOps

	if newRecurringjob.Spec.Name == "" {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/name", "value": "%s"}`, newRecurringjob.Name))
	}
	if newRecurringjob.Spec.Groups == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/groups", "value": []}`)
	}
	if newRecurringjob.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}
	if newRecurringjob.Spec.Parameters == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/parameters", "value": {}}`)
	}
	if newRecurringjob.Spec.Concurrency == 0 {
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/concurrency", "value": %d}`, types.DefaultRecurringJobConcurrency))
	}

	log := logrus.WithFields(logrus.Fields{
		"recurringJob": newRecurringjob.Name,
		"task":         newRecurringjob.Spec.Task,
	})
	patchOps = mutateRetainCountAndRetainAge(patchOps, newRecurringjob, log)

	return patchOps, nil
}

func mutateRetainCountAndRetainAge(patchOps admission.PatchOps, recurringJob *longhorn.RecurringJob, log *logrus.Entry) admission.PatchOps {
	switch recurringJob.Spec.Task {
	case longhorn.RecurringJobTypeSnapshotCleanup, longhorn.RecurringJobTypeFilesystemTrim:
		if recurringJob.Spec.Retain != 0 {
			log.Debugf("Replacing ineffective retain value in RecurringJob: from %v to 0", recurringJob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 0}`)
		}
		if recurringJob.Spec.RetainAge.Duration != 0 {
			log.Debugf("Replacing ineffective retainAge value in RecurringJob: from %v to 0s", recurringJob.Spec.RetainAge)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retainAge", "value": "0s"}`)
		}
	case longhorn.RecurringJobTypeSnapshotDelete:
		if recurringJob.Spec.Retain < 0 {
			log.Debugf("Replacing ineffective retain value in RecurringJob: from %v to 0", recurringJob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 0}`)
		}
		if recurringJob.Spec.RetainAge.Duration != 0 {
			log.Debugf("Replacing ineffective retainAge value in RecurringJob: from %v to 0s", recurringJob.Spec.RetainAge)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retainAge", "value": "0s"}`)
		}
	default:
		if recurringJob.Spec.Retain < 1 {
			log.Debugf("Replacing invalid retain value in RecurringJob: from %v to 1", recurringJob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 1}`)
		}
		if recurringJob.Spec.RetainAge.Duration <= 0 {
			// The default value of RetainAge is 0, which means recurring jobs will not start if retention policy is age-based.
			log.Debugf("Replacing ineffective retainAge value in RecurringJob: from %v to 0s", recurringJob.Spec.RetainAge)
			patchOps = append(patchOps, `{"op": "add", "path": "/spec/retainAge", "value": "0s"}`)
		}
	}

	return patchOps
}
