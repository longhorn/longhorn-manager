package recurringjob

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

const (
	RecurringJobErrRetainValueFmt = "retain value should be less than or equal to %v"
)

type recurringJobValidator struct {
	admission.DefaultValidator
	ds *datastore.DataStore
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &recurringJobValidator{ds: ds}
}

func (r *recurringJobValidator) Resource() admission.Resource {
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

func (r *recurringJobValidator) Create(request *admission.Request, newObj runtime.Object) error {
	recurringJob, ok := newObj.(*longhorn.RecurringJob)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.RecurringJob", newObj), "")
	}

	if !util.ValidateName(recurringJob.Name) {
		return werror.NewInvalidError(fmt.Sprintf("invalid name %v", recurringJob.Name), "")
	}

	maxRecurringJobRetain, err := r.ds.GetSettingAsInt(types.SettingNameRecurringJobMaxRetention)
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if recurringJob.Spec.Retain > int(maxRecurringJobRetain) {
		return werror.NewInvalidError(fmt.Sprintf(RecurringJobErrRetainValueFmt, maxRecurringJobRetain), "")
	}

	jobs := []longhorn.RecurringJobSpec{
		{
			Name:        recurringJob.Spec.Name,
			Groups:      recurringJob.Spec.Groups,
			Task:        recurringJob.Spec.Task,
			Cron:        recurringJob.Spec.Cron,
			Retain:      recurringJob.Spec.Retain,
			Concurrency: recurringJob.Spec.Concurrency,
			Labels:      recurringJob.Spec.Labels,
			Parameters:  recurringJob.Spec.Parameters,
		},
	}
	if err := r.ds.ValidateRecurringJobs(jobs); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil

}

func (r *recurringJobValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	newRecurringJob, ok := newObj.(*longhorn.RecurringJob)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.RecurringJob", newObj), "")
	}

	maxRecurringJobRetain, err := r.ds.GetSettingAsInt(types.SettingNameRecurringJobMaxRetention)
	if err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	if newRecurringJob.Spec.Retain > int(maxRecurringJobRetain) {
		return werror.NewInvalidError(fmt.Sprintf(RecurringJobErrRetainValueFmt, maxRecurringJobRetain), "")
	}

	jobs := []longhorn.RecurringJobSpec{
		{
			Name:        newRecurringJob.Spec.Name,
			Groups:      newRecurringJob.Spec.Groups,
			Task:        newRecurringJob.Spec.Task,
			Cron:        newRecurringJob.Spec.Cron,
			Retain:      newRecurringJob.Spec.Retain,
			Concurrency: newRecurringJob.Spec.Concurrency,
			Labels:      newRecurringJob.Spec.Labels,
			Parameters:  newRecurringJob.Spec.Parameters,
		},
	}
	if err := r.ds.ValidateRecurringJobs(jobs); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}
