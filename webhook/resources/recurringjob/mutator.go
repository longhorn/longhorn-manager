package recurringjob

import (
	"fmt"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	werror "github.com/longhorn/longhorn-manager/webhook/error"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
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
	if recurringjob.Spec.Groups == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/groups", "value": []}`)
	}
	if recurringjob.Spec.Labels == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/labels", "value": {}}`)
	}
	if recurringjob.Spec.Parameters == nil {
		patchOps = append(patchOps, `{"op": "replace", "path": "/spec/parameters", "value": {}}`)
	}

	log := logrus.WithFields(logrus.Fields{
		"recurringJob": recurringjob.Name,
		"task":         recurringjob.Spec.Task,
	})
	switch recurringjob.Spec.Task {
	case longhorn.RecurringJobTypeSnapshotCleanup, longhorn.RecurringJobTypeFilesystemTrim:
		if recurringjob.Spec.Retain != 0 {
			log.Debugf("Replacing ineffective retain value in RecurringJob: from %v to 0", recurringjob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 0}`)
		}
	case longhorn.RecurringJobTypeSnapshotDelete:
		if recurringjob.Spec.Retain < 0 {
			log.Debugf("Replacing ineffective retain value in RecurringJob: from %v to 0", recurringjob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 0}`)
		}
	default:
		if recurringjob.Spec.Retain < 1 {
			log.Debugf("Replacing invalid retain value in RecurringJob: from %v to 1", recurringjob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 1}`)
		}
	}

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

	log := logrus.WithFields(logrus.Fields{
		"recurringJob": newRecurringjob.Name,
		"task":         newRecurringjob.Spec.Task,
	})
	switch newRecurringjob.Spec.Task {
	case longhorn.RecurringJobTypeSnapshotCleanup, longhorn.RecurringJobTypeFilesystemTrim:
		if newRecurringjob.Spec.Retain != 0 {
			log.Debugf("Replacing ineffective retain value in RecurringJob: from %v to 0", newRecurringjob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 0}`)
		}
	case longhorn.RecurringJobTypeSnapshotDelete:
		if newRecurringjob.Spec.Retain < 0 {
			log.Debugf("Replacing ineffective retain value in RecurringJob: from %v to 0", newRecurringjob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 0}`)
		}
	default:
		if newRecurringjob.Spec.Retain < 1 {
			log.Debugf("Replacing invalid retain value in RecurringJob: from %v to 1", newRecurringjob.Spec.Retain)
			patchOps = append(patchOps, `{"op": "replace", "path": "/spec/retain", "value": 1}`)
		}
	}

	return patchOps, nil
}
