package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type RecurringJobController struct {
	*baseController

	namespace string

	controllerID   string
	ManagerImage   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewRecurringJobController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount, managerImage string,
) *RecurringJobController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: typedv1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	c := &RecurringJobController{
		baseController: newBaseController("longhorn-recurring-job", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		ManagerImage:   managerImage,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-recurring-job-controller"}),

		ds: ds,
	}

	ds.RecurringJobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueRecurringJob,
		UpdateFunc: func(old, cur interface{}) { c.enqueueRecurringJob(cur) },
		DeleteFunc: c.enqueueRecurringJob,
	})
	c.cacheSyncs = append(c.cacheSyncs, ds.RecurringJobInformer.HasSynced)

	return c
}

func (c *RecurringJobController) enqueueRecurringJob(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

func (c *RecurringJobController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logrus.Infof("Starting Longhorn Recurring Job controller")
	defer logrus.Infof("Shut down Longhorn Recurring Job controller")

	if !cache.WaitForNamedCacheSync("longhorn recurring jobs", stopCh, c.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *RecurringJobController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *RecurringJobController) processNextWorkItem() bool {
	key, quit := c.queue.Get()

	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncRecurringJob(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *RecurringJobController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn recurring job %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn recurring job %v out of the queue: %v", key, err)
	c.queue.Forget(key)
}

func getLoggerForRecurringJob(logger logrus.FieldLogger, recurringJob *longhorn.RecurringJob) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"recurringJob": recurringJob.Name,
		},
	)
}

func (c *RecurringJobController) syncRecurringJob(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync recurring job for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}

	recurringJob, err := c.ds.GetRecurringJob(name)
	if err != nil {
		log := c.logger.WithField("recurringJob", name)
		if !datastore.ErrorIsNotFound(err) {
			log.WithError(err).Error("failed to retrieve recurring job from datastore")
			return err
		}
		log.Debug("Cannot find recurring job, may have been deleted")

		// The detachVolumeAutoAttachedByRecurringJob is a workaround to
		// resolve volume unable to detach via the recurring job. The volume
		// could remain attached via recurringjob auto-attachment when the
		// recurring job pod gets force terminated and unable to complete
		// detachment within the grace period.
		// This should be handled when a separate controller is introduced for
		// attachment and detachment handling.
		// https://github.com/longhorn/longhorn-manager/pull/1223#discussion_r814655791
		volumes, err := c.ds.ListVolumes()
		if err != nil {
			return err
		}
		for _, vol := range volumes {
			if err := c.detachVolumeAutoAttachedByRecurringJob(name, vol); err != nil {
				return err
			}
		}
		return nil
	}

	log := getLoggerForRecurringJob(c.logger, recurringJob)

	if !c.isResponsibleFor(recurringJob) {
		return nil
	}
	if recurringJob.Status.OwnerID != c.controllerID {
		recurringJob.Status.OwnerID = c.controllerID
		recurringJob, err = c.ds.UpdateRecurringJobStatus(recurringJob)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Recurring Job got new owner %v", c.controllerID)
	}

	if recurringJob.DeletionTimestamp != nil {
		return c.cleanupVolumeRecurringJob(recurringJob)
	}

	existingRecurringJob := recurringJob.DeepCopy()
	defer func() {
		if err != nil {
			return
		}
		if reflect.DeepEqual(existingRecurringJob.Status, recurringJob.Status) &&
			reflect.DeepEqual(existingRecurringJob.Spec, recurringJob.Spec) {
			return
		}
		_, err := c.ds.UpdateRecurringJob(recurringJob)
		if err != nil && apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			c.enqueueRecurringJob(recurringJob)
		}
	}()

	err = c.reconcileRecurringJob(recurringJob)
	if err != nil {
		log.WithError(err).Error("failed to reconcile recurring job")
	}

	return nil
}

func (c *RecurringJobController) cleanupVolumeRecurringJob(recurringJob *longhorn.RecurringJob) error {
	// Check if each group of the recurring job contains other recurring jobs.
	// If No, it means the recurring job is the last job of the group then
	// Longhorn will clean up this group labels for all volumes.
	checkRecurringJobs, err := c.ds.ListRecurringJobs()
	if err != nil {
		return err
	}
	rmGroups := []string{}
	for _, group := range recurringJob.Spec.Groups {
		inUse := false
		for _, checkRecurringJob := range checkRecurringJobs {
			if checkRecurringJob.Name == recurringJob.Name {
				continue
			}
			if util.Contains(checkRecurringJob.Spec.Groups, group) {
				inUse = true
				break
			}
		}
		if !inUse {
			rmGroups = append(rmGroups, group)
		}
	}

	// delete volume labels
	volumes, err := c.ds.ListVolumes()
	if err != nil {
		return err
	}
	for _, vol := range volumes {
		jobs := datastore.MarshalLabelToVolumeRecurringJob(vol.Labels)
		for jobName, job := range jobs {
			if job.IsGroup {
				if !util.Contains(rmGroups, jobName) {
					continue
				}
				c.logger.Debugf("Clean up recurring job-group %v for %v", jobName, vol.Name)
				labelKey := types.GetRecurringJobLabelKeyByType(jobName, true)
				vol, err = c.ds.RemoveRecurringJobLabelFromVolume(vol, labelKey)
				if err != nil {
					return err
				}
			} else if jobName == recurringJob.Name {
				c.logger.Debugf("Clean up recurring job %v for %v", jobName, vol.Name)
				labelKey := types.GetRecurringJobLabelKeyByType(jobName, false)
				vol, err = c.ds.RemoveRecurringJobLabelFromVolume(vol, labelKey)
				if err != nil {
					return err
				}
			}
		}

		if _, err := c.ds.UpdateVolume(vol); err != nil {
			return err
		}
	}
	return nil
}

func (c *RecurringJobController) detachVolumeAutoAttachedByRecurringJob(name string, v *longhorn.Volume) error {
	if v.Spec.LastAttachedBy != name {
		return nil
	}
	if v.Status.State == longhorn.VolumeStateAttached {
		c.logger.Infof("requesting auto-attached volume %v to detach from node %v", v.Name, v.Spec.NodeID)
		v.Spec.NodeID = ""
		if _, err := c.ds.UpdateVolume(v); err != nil {
			return err
		}
	}
	return nil
}

func (c *RecurringJobController) isResponsibleFor(recurringJob *longhorn.RecurringJob) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, recurringJob.Name, "", recurringJob.Status.OwnerID)
}

func (c *RecurringJobController) reconcileRecurringJob(recurringJob *longhorn.RecurringJob) (err error) {
	cronJob, err := c.newCronJob(recurringJob)
	if err != nil {
		return errors.Wrap(err, "failed to create new cron job for recurring job")
	}

	appliedCronJob, err := c.ds.GetCronJobROByRecurringJob(recurringJob)
	if err != nil {
		return errors.Wrapf(err, "failed to get cron job by recurring job")
	}
	if appliedCronJob == nil {
		err = c.createCronJob(cronJob, recurringJob)
		if err != nil {
			return errors.Wrap(err, "failed to create cron job")
		}
	} else {
		err = c.checkAndUpdateCronJob(cronJob, appliedCronJob)
		if err != nil {
			return errors.Wrap(err, "failed to update cron job")
		}
	}
	return nil
}

func (c *RecurringJobController) createCronJob(cronJob *batchv1.CronJob, recurringJob *longhorn.RecurringJob) error {
	var err error

	cronJobSpecB, err := json.Marshal(cronJob.Spec)
	if err != nil {
		return err
	}
	err = util.SetAnnotation(cronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix), string(cronJobSpecB))
	if err != nil {
		return err
	}
	_, err = c.ds.CreateCronJob(cronJob)
	if err != nil {
		return errors.Wrapf(err, "failed to create cron job")
	}
	return nil
}

func (c *RecurringJobController) checkAndUpdateCronJob(cronJob, appliedCronJob *batchv1.CronJob) (err error) {
	cronJobSpecB, err := json.Marshal(cronJob.Spec)
	if err != nil {
		return err
	}
	cronJobSpec := string(cronJobSpecB)

	lastAppliedSpec, err := util.GetAnnotation(appliedCronJob, types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix))
	if err != nil {
		return errors.Wrapf(err, "failed to get annotation from cron job")
	}
	if lastAppliedSpec == cronJobSpec {
		return nil
	}
	annotation := types.GetLonghornLabelKey(LastAppliedCronJobSpecAnnotationKeySuffix)
	if err := util.SetAnnotation(cronJob, annotation, cronJobSpec); err != nil {
		return errors.Wrapf(err, "failed to set annotation for cron job")
	}
	if _, err := c.ds.UpdateCronJob(cronJob); err != nil {
		return err
	}
	return nil
}

func (c *RecurringJobController) newCronJob(recurringJob *longhorn.RecurringJob) (*batchv1.CronJob, error) {
	backoffLimit := int32(CronJobBackoffLimit)
	settingSuccessfulJobsHistoryLimit, err := c.ds.GetSettingAsInt(types.SettingNameRecurringSuccessfulJobsHistoryLimit)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get setting of RecurringSuccessfulJobsHistoryLimit")
	}
	successfulJobsHistoryLimit := int32(settingSuccessfulJobsHistoryLimit)
	settingFailedJobsHistoryLimit, err := c.ds.GetSettingAsInt(types.SettingNameRecurringFailedJobsHistoryLimit)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get setting of RecurringFailedJobsHistoryLimit")
	}
	failedJobsHistoryLimit := int32(settingFailedJobsHistoryLimit)

	cmd := []string{
		"longhorn-manager", "-d",
		"recurring-job", recurringJob.Name,
		"--manager-url", types.GetDefaultManagerURL(),
	}

	tolerations, err := c.ds.GetSettingTaintToleration()
	if err != nil {
		return nil, err
	}
	priorityClass, err := c.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return nil, err
	}
	nodeSelector, err := c.ds.GetSettingSystemManagedComponentsNodeSelector()
	if err != nil {
		return nil, err
	}
	registrySecretSetting, err := c.ds.GetSetting(types.SettingNameRegistrySecret)
	if err != nil {
		return nil, err
	}
	registrySecret := registrySecretSetting.Value

	// for mounting inside container
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      recurringJob.Name,
			Namespace: recurringJob.Namespace,
			Labels: types.GetCronJobLabels(&longhorn.RecurringJobSpec{
				Name: recurringJob.Name,
				Task: longhorn.RecurringJobType(recurringJob.Spec.Task),
			}),
			OwnerReferences: datastore.GetOwnerReferencesForRecurringJob(recurringJob),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   recurringJob.Spec.Cron,
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     &failedJobsHistoryLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit: &backoffLimit,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name: recurringJob.Name,
							Labels: types.GetCronJobLabels(&longhorn.RecurringJobSpec{
								Name: recurringJob.Name,
								Task: longhorn.RecurringJobType(recurringJob.Spec.Task),
							}),
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:    recurringJob.Name,
									Image:   c.ManagerImage,
									Command: cmd,
									Env: []corev1.EnvVar{
										{
											Name: "POD_NAMESPACE",
											ValueFrom: &corev1.EnvVarSource{
												FieldRef: &corev1.ObjectFieldSelector{
													FieldPath: "metadata.namespace",
												},
											},
										},
									},
									VolumeMounts: []corev1.VolumeMount{
										{
											Name:      "engine-binaries",
											MountPath: types.EngineBinaryDirectoryOnHost,
										},
									},
								},
							},
							Volumes: []corev1.Volume{
								{
									Name: "engine-binaries",
									VolumeSource: corev1.VolumeSource{
										HostPath: &corev1.HostPathVolumeSource{
											Path: types.EngineBinaryDirectoryOnHost,
										},
									},
								},
							},
							ServiceAccountName: c.serviceAccount,
							RestartPolicy:      corev1.RestartPolicyOnFailure,
							Tolerations:        util.GetDistinctTolerations(tolerations),
							NodeSelector:       nodeSelector,
							PriorityClassName:  priorityClass.Value,
						},
					},
				},
			},
		},
	}

	if registrySecret != "" {
		cronJob.Spec.JobTemplate.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}

	return cronJob, nil
}

func syncRecurringJobLabelsToTargetResource(targetKind string, targetObj, sourceObj runtime.Object, log logrus.FieldLogger) error {
	sourceMeta, err := meta.Accessor(sourceObj)
	if err != nil {
		return errors.Wrap(err, "failed to get source object accessor")
	}
	targetMeta, err := meta.Accessor(targetObj)
	if err != nil {
		return errors.Wrap(err, "failed to get target object accessor")
	}

	sourceLabels := sourceMeta.GetLabels()
	if sourceLabels == nil {
		sourceLabels = map[string]string{}
	}
	targetLabels := targetMeta.GetLabels()
	if targetLabels == nil {
		targetLabels = map[string]string{}
	}

	for key, value := range targetLabels {
		if !types.IsRecurringJobLabel(key) {
			continue
		}

		if sourceLabelValue, exist := sourceLabels[key]; exist && sourceLabelValue == value {
			continue
		}

		log.Debugf("Removing %v %v recurring job label %v", targetKind, targetMeta.GetName(), key)
		delete(targetLabels, key)
	}

	for key, value := range sourceLabels {
		if !types.IsRecurringJobLabel(key) {
			continue
		}

		if targetLabelValue := targetLabels[key]; targetLabelValue == value {
			continue
		}

		log.Debugf("Adding %v %v recurring job label %v: %v", targetKind, targetMeta.GetName(), key, value)
		targetLabels[key] = value
	}

	targetMeta.SetLabels(targetLabels)
	return nil
}
