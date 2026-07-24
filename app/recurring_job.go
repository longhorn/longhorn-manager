package app

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/longhorn/longhorn-manager/app/recurringjob"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type recurringJobGetter interface {
	Get(context.Context, string, metav1.GetOptions) (*longhorn.RecurringJob, error)
}

func RecurringJobCmd() *cli.Command {
	return &cli.Command{
		Name: "recurring-job",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  FlagManagerURL,
				Usage: "Longhorn manager API URL",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if err := recurringJob(ctx, cmd); err != nil {
				logrus.WithError(err).Fatal("Failed to do a recurring job")
			}
			return nil
		},
	}
}

func recurringJob(ctx context.Context, cmd *cli.Command) (err error) {
	logger := logrus.StandardLogger()

	var managerURL = cmd.String(FlagManagerURL)
	if managerURL == "" {
		return fmt.Errorf("require %v", FlagManagerURL)
	}

	if cmd.NArg() != 1 {
		return errors.New("job name is required")
	}
	jobName := cmd.Args().Get(0)

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return fmt.Errorf("failed detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	lhClient, err := recurringjob.GetLonghornClientset()
	if err != nil {
		return errors.Wrap(err, "failed to get clientset")
	}

	recurringJob, err := getRecurringJob(ctx, lhClient.LonghornV1beta2().RecurringJobs(namespace), jobName)
	if err != nil {
		return err
	}

	recurringJob.Status.ExecutionCount += 1
	if _, err = lhClient.LonghornV1beta2().RecurringJobs(namespace).UpdateStatus(ctx, recurringJob, metav1.UpdateOptions{}); err != nil {
		return errors.Wrap(err, "failed to update job execution count")
	}

	job, err := recurringjob.NewJob(jobName, logger, managerURL, recurringJob, lhClient)
	if err != nil {
		return errors.Wrap(err, "failed to initialize job")
	}

	switch recurringJob.Spec.Task {
	case longhorn.RecurringJobTypeSystemBackup:
		return recurringjob.StartSystemBackupJob(job, recurringJob)
	default:
		return recurringjob.StartVolumeJobs(job, recurringJob)
	}
}

func getRecurringJob(ctx context.Context, client recurringJobGetter, jobName string) (*longhorn.RecurringJob, error) {
	ctx, cancel := context.WithTimeout(ctx, recurringjob.InitClientTimeout)
	defer cancel()

	// Nine attempts produce about 49-59 seconds of delay for immediate failures.
	// The context deadline also bounds the time spent inside slow requests.
	return getRecurringJobWithBackoff(ctx, client, jobName, wait.Backoff{
		Duration: recurringjob.InitClientRetryInterval,
		Factor:   1.5,
		Jitter:   0.2,
		Steps:    9,
	})
}

func getRecurringJobWithBackoff(ctx context.Context, client recurringJobGetter, jobName string, backoff wait.Backoff) (*longhorn.RecurringJob, error) {
	var (
		job      *longhorn.RecurringJob
		lastErr  error
		attempts int
	)

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		attempts++
		job, lastErr = client.Get(ctx, jobName, metav1.GetOptions{})
		if lastErr == nil {
			return true, nil
		}
		if !isRetryableRecurringJobGetError(lastErr) {
			return false, lastErr
		}
		if attempts < backoff.Steps {
			logrus.WithError(lastErr).WithField("attempt", attempts).Warnf("Failed to get recurring job %v, retrying", jobName)
		}
		return false, nil
	})
	if err == nil {
		return job, nil
	}
	if wait.Interrupted(err) && lastErr != nil {
		err = lastErr
	}

	return nil, errors.Wrapf(err, "failed to get recurring job %v", jobName)
}

func isRetryableRecurringJobGetError(err error) bool {
	return apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) ||
		utilnet.IsTimeout(err) ||
		utilnet.IsConnectionRefused(err) ||
		utilnet.IsConnectionReset(err) ||
		utilnet.IsProbableEOF(err) ||
		utilnet.IsHTTP2ConnectionLost(err)
}
