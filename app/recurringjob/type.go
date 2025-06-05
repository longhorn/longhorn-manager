package recurringjob

import (
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/record"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

// Job is a base job that contains the necessary clients, configuration, and general information.
type Job struct {
	api      *longhornclient.RancherClient // Rancher client used to interact with the Longhorn API.
	lhClient *lhclientset.Clientset        // Kubernetes clientset for Longhorn resources.

	eventRecorder record.EventRecorder // Used to record events related to the job.
	logger        *logrus.Logger       // Log messages related to the job.

	name           string                    // Name for the RecurringJob.
	namespace      string                    // Kubernetes namespace in which the RecurringJob is running.
	retain         int                       // Number of task CRs to retain.
	task           longhorn.RecurringJobType // Type of task to be executed.
	parameters     map[string]string         // Additional parameters for the task.
	executionCount int                       // Number of times the job has been executed.
}

// VolumeJob is a job for volume tasks.
// It embeds the Job struct and includes additional fields specific to volume operations.
type VolumeJob struct {
	*Job // Embedding the base Job struct.

	logger *logrus.Entry // Log messages related to the volume job.

	volumeName   string            // Name of the volume on which the job operates.
	snapshotName string            // Name of the snapshot associated with the job.
	specLabels   map[string]string // A map of labels from the RecurringJob.Spec.
	groups       []string          // A list of groups associated with the volume.
	concurrent   int               // Number of concurrent operations allowed for the job.
}

// SystemBackupJob is a job for system backup tasks.
// It embeds the Job struct and includes additional fields specific to system backup operations.
type SystemBackupJob struct {
	*Job // Embedding the base Job struct.

	logger *logrus.Entry // Log messages related to the volume job.

	systemBackupName   string                                        // Name of the SystemBackup.
	volumeBackupPolicy longhorn.SystemBackupCreateVolumeBackupPolicy // backup policy used for the SystemBackup.Spec.
}

// NameWithTimestamp for resource cleanup.
type NameWithTimestamp struct {
	Name      string
	Timestamp time.Time
}
