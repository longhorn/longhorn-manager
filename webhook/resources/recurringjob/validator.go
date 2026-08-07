package recurringjob

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

const (
	RecurringJobErrRetainValueFmt     = "retain value should be less than or equal to %v"
	RecurringJobErrRetainAgeNegative  = "retainAge should not be negative"
	RecurringJobErrRetentionPolicyFmt = "retentionPolicy should be %v or %v"
	RecurringJobErrRetainAgeRequired  = "retainAge should be positive when retentionPolicy is %v"
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

// validateRetainAge rejects a negative retain age. A negative age makes every
// snapshot/backup count as older than the window, so the next run of the job
// would delete all of them.
func validateRetainAge(retainAge metav1.Duration) error {
	if retainAge.Duration < 0 {
		return werror.NewInvalidError(RecurringJobErrRetainAgeNegative, "")
	}
	return nil
}

// validateRetentionPolicy rejects an unrecognized retention policy, and a
// policy that is not paired with the field it cleans up by.
//
// An empty policy is allowed and means "count-base", both because that is the
// CRD default and because it is what jobs created before the field existed
// carry. An unrecognized policy has to be rejected here rather than silently
// falling back to "count-base", which would delete snapshots/backups the user
// expected an age window to keep.
//
// "age-base" additionally requires a positive retainAge: the job goes by the age
// alone, so without a window it would never clean anything up and grow forever.
func validateRetentionPolicy(policy longhorn.RecurringJobRetentionPolicy, retainAge metav1.Duration) error {
	switch policy {
	case "", longhorn.RecurringJobRetentionPolicyCountBase:
		return nil
	case longhorn.RecurringJobRetentionPolicyAgeBase:
		if retainAge.Duration <= 0 {
			return werror.NewInvalidError(fmt.Sprintf(RecurringJobErrRetainAgeRequired,
				longhorn.RecurringJobRetentionPolicyAgeBase), "")
		}
		return nil
	}
	return werror.NewInvalidError(fmt.Sprintf(RecurringJobErrRetentionPolicyFmt,
		longhorn.RecurringJobRetentionPolicyCountBase, longhorn.RecurringJobRetentionPolicyAgeBase), "")
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

	if err := validateRetainAge(recurringJob.Spec.RetainAge); err != nil {
		return err
	}

	if err := validateRetentionPolicy(recurringJob.Spec.RetentionPolicy, recurringJob.Spec.RetainAge); err != nil {
		return err
	}

	jobs := []longhorn.RecurringJobSpec{
		{
			Name:            recurringJob.Spec.Name,
			Groups:          recurringJob.Spec.Groups,
			Task:            recurringJob.Spec.Task,
			Cron:            recurringJob.Spec.Cron,
			Retain:          recurringJob.Spec.Retain,
			RetainAge:       recurringJob.Spec.RetainAge,
			RetentionPolicy: recurringJob.Spec.RetentionPolicy,
			Concurrency:     recurringJob.Spec.Concurrency,
			Labels:          recurringJob.Spec.Labels,
			Parameters:      recurringJob.Spec.Parameters,
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

	if err := validateRetainAge(newRecurringJob.Spec.RetainAge); err != nil {
		return err
	}

	if err := validateRetentionPolicy(newRecurringJob.Spec.RetentionPolicy, newRecurringJob.Spec.RetainAge); err != nil {
		return err
	}

	jobs := []longhorn.RecurringJobSpec{
		{
			Name:            newRecurringJob.Spec.Name,
			Groups:          newRecurringJob.Spec.Groups,
			Task:            newRecurringJob.Spec.Task,
			Cron:            newRecurringJob.Spec.Cron,
			Retain:          newRecurringJob.Spec.Retain,
			RetainAge:       newRecurringJob.Spec.RetainAge,
			RetentionPolicy: newRecurringJob.Spec.RetentionPolicy,
			Concurrency:     newRecurringJob.Spec.Concurrency,
			Labels:          newRecurringJob.Spec.Labels,
			Parameters:      newRecurringJob.Spec.Parameters,
		},
	}
	if err := r.ds.ValidateRecurringJobs(jobs); err != nil {
		return werror.NewInvalidError(err.Error(), "")
	}

	return nil
}
