package recurringjob

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	apputil "github.com/longhorn/longhorn-manager/app/util"
	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

func NewJob(name string, logger *logrus.Logger, managerURL string, recurringJob *longhorn.RecurringJob, lhClient *lhclientset.Clientset) (*Job, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("failed detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	clientOpts := &longhornclient.ClientOpts{
		Url:     managerURL,
		Timeout: HTTPClientTimout,
	}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return nil, errors.Wrap(err, "could not create longhorn-manager api client")
	}

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, errors.Wrap(err, "failed to create scheme")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client config")
	}
	eventBroadcaster, err := apputil.CreateEventBroadcaster(config)
	if err != nil {
		return nil, err
	}

	parameters := map[string]string{}
	if recurringJob.Spec.Parameters != nil {
		parameters = recurringJob.Spec.Parameters
	}

	return &Job{
		api:      apiClient,
		lhClient: lhClient,

		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-recurring-job"}),
		logger:        logger,

		name:           name,
		namespace:      namespace,
		retain:         recurringJob.Spec.Retain,
		task:           recurringJob.Spec.Task,
		parameters:     parameters,
		executionCount: recurringJob.Status.ExecutionCount,
	}, nil
}

func (job *Job) GetVolume(name string) (*longhorn.Volume, error) {
	return job.lhClient.LonghornV1beta2().Volumes(job.namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (job *Job) GetEngineImage(name string) (*longhorn.EngineImage, error) {
	return job.lhClient.LonghornV1beta2().EngineImages(job.namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (job *Job) UpdateVolumeStatus(v *longhorn.Volume) (*longhorn.Volume, error) {
	return job.lhClient.LonghornV1beta2().Volumes(job.namespace).UpdateStatus(context.TODO(), v, metav1.UpdateOptions{})
}

// GetSettingAsBool returns boolean of the setting value searching by name.
func (job *Job) GetSettingAsBool(name types.SettingName) (bool, error) {
	obj, err := job.lhClient.LonghornV1beta2().Settings(job.namespace).Get(context.TODO(), string(name), metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	value, err := strconv.ParseBool(obj.Value)
	if err != nil {
		return false, err
	}

	return value, nil
}

func (job *Job) CreateSystemBackup(systemBackup *longhorn.SystemBackup) (*longhorn.SystemBackup, error) {
	return job.lhClient.LonghornV1beta2().SystemBackups(job.namespace).Create(context.TODO(), systemBackup, metav1.CreateOptions{})
}

func (job *Job) DeleteSystemBackup(name string) error {
	return job.lhClient.LonghornV1beta2().SystemBackups(job.namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

func (job *Job) GetSystemBackup(name string) (*longhorn.SystemBackup, error) {
	return job.lhClient.LonghornV1beta2().SystemBackups(job.namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (job *Job) ListSystemBackup() (*longhorn.SystemBackupList, error) {
	labelKey := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, string(longhorn.RecurringJobTypeSystemBackup))
	label := fmt.Sprintf("%s=%s", labelKey, job.name)

	job.logger.Infof("Getting SystemBackups by label %v", label)
	return job.lhClient.LonghornV1beta2().SystemBackups(job.namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: label,
	})
}
