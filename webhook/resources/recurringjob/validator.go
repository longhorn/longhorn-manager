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

// validateRetentionPolicy rejects a retention policy that is empty, unrecognized,
// or not paired with the field it cleans up by.
//
// The policy is never empty in practice: new jobs get the CRD default "count-based"
// and jobs predating the field are backfilled to "count-based" during the upgrade.
// An empty or unrecognized policy is therefore rejected rather than silently treated
// as "count-based", which could delete snapshots/backups a user expected an age window to keep.
//
// "age-based" additionally requires a positive retainAge: the job goes by the age alone,
// so without a window it would never clean anything up and grow forever.
func validateRetentionPolicy(policy longhorn.RecurringJobRetentionPolicy, task longhorn.RecurringJobType, retain int, retainAge metav1.Duration) error {
	if retainAge.Duration < 0 {
		return werror.NewInvalidError(RecurringJobErrRetainAgeNegative, "")
	}

	notCleanupTask := (task != longhorn.RecurringJobTypeSnapshotCleanup &&
		task != longhorn.RecurringJobTypeFilesystemTrim &&
		task != longhorn.RecurringJobTypeSnapshotDelete)

	switch policy {
	case longhorn.RecurringJobRetentionPolicyCountBased:
		if notCleanupTask && retain <= 0 {
			return fmt.Errorf("recurring job retain count %v must be greater than 0", retain)
		}
		return nil
	case longhorn.RecurringJobRetentionPolicyAgeBased:
		if !notCleanupTask {
			return fmt.Errorf("recurring job retention policy %v can not be used with task %v", policy, task)
		}
		return nil
	}
	return werror.NewInvalidError(fmt.Sprintf(RecurringJobErrRetentionPolicyFmt,
		longhorn.RecurringJobRetentionPolicyCountBased, longhorn.RecurringJobRetentionPolicyAgeBased), "")
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

	if err := validateRetentionPolicy(recurringJob.Spec.RetentionPolicy, recurringJob.Spec.Task, recurringJob.Spec.Retain, recurringJob.Spec.RetainAge); err != nil {
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

	if err := validateRetentionPolicy(newRecurringJob.Spec.RetentionPolicy, newRecurringJob.Spec.Task, newRecurringJob.Spec.Retain, newRecurringJob.Spec.RetainAge); err != nil {
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
