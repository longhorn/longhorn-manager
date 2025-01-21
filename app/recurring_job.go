package app

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/app/recurringjob"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func RecurringJobCmd() cli.Command {
	return cli.Command{
		Name: "recurring-job",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  FlagManagerURL,
				Usage: "Longhorn manager API URL",
			},
		},
		Action: func(c *cli.Context) {
			if err := recurringJob(c); err != nil {
				logrus.WithError(err).Fatal("Failed to do a recurring job")
			}
		},
	}
}

func recurringJob(c *cli.Context) (err error) {
	logger := logrus.StandardLogger()

	var managerURL string = c.String(FlagManagerURL)
	if managerURL == "" {
		return fmt.Errorf("require %v", FlagManagerURL)
	}

	if c.NArg() != 1 {
		return errors.New("job name is required")
	}
	jobName := c.Args()[0]

	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return fmt.Errorf("failed detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	lhClient, err := recurringjob.GetLonghornClientset()
	if err != nil {
		return errors.Wrap(err, "failed to get clientset")
	}

	recurringJob, err := lhClient.LonghornV1beta2().RecurringJobs(namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		logrus.WithError(err).Errorf("Failed to get recurring job %v.", jobName)
		return nil
	}

	recurringJob.Status.ExecutionCount += 1
	if _, err = lhClient.LonghornV1beta2().RecurringJobs(namespace).UpdateStatus(context.TODO(), recurringJob, metav1.UpdateOptions{}); err != nil {
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
