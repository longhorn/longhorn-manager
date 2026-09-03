package controller

import (
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newTestRecurringJobController(t *testing.T) *RecurringJobController {
	t.Helper()

	originalSkipListerCheck := datastore.SkipListerCheck
	datastore.SkipListerCheck = true
	t.Cleanup(func() {
		datastore.SkipListerCheck = originalSkipListerCheck
	})

	kubeClient := fake.NewSimpleClientset()                   // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                   // nolint: staticcheck
	extensionClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)

	c, err := NewRecurringJobController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient,
		TestNamespace, TestOwnerID1, TestServiceAccount, TestManagerImage)
	if err != nil {
		t.Fatalf("failed to create recurring job controller: %v", err)
	}
	return c
}

func newTestRecurringJob(activeDeadlineSeconds int64) *longhorn.RecurringJob {
	return &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-recurring-job",
			Namespace: TestNamespace,
		},
		Spec: longhorn.RecurringJobSpec{
			Name:                  "test-recurring-job",
			Task:                  longhorn.RecurringJobTypeBackup,
			Cron:                  "0 0 * * *",
			Retain:                1,
			Concurrency:           1,
			ActiveDeadlineSeconds: activeDeadlineSeconds,
		},
	}
}

// A recurring job with a runtime bound must pass it to the generated Job, so a
// hung job is terminated by Kubernetes instead of running indefinitely.
func TestNewCronJobPropagatesActiveDeadlineSeconds(t *testing.T) {
	c := newTestRecurringJobController(t)

	cronJob, err := c.newCronJob(newTestRecurringJob(3600))
	if err != nil {
		t.Fatalf("failed to build cron job: %v", err)
	}

	activeDeadlineSeconds := cronJob.Spec.JobTemplate.Spec.ActiveDeadlineSeconds
	if activeDeadlineSeconds == nil {
		t.Fatal("unexpected nil activeDeadlineSeconds: want 3600")
	}
	if *activeDeadlineSeconds != 3600 {
		t.Fatalf("unexpected activeDeadlineSeconds: got %d, want 3600", *activeDeadlineSeconds)
	}
}

// Without a runtime bound the generated Job must stay unbounded, which is the
// behavior of every recurring job created before the field existed.
func TestNewCronJobOmitsUnsetActiveDeadlineSeconds(t *testing.T) {
	c := newTestRecurringJobController(t)

	cronJob, err := c.newCronJob(newTestRecurringJob(0))
	if err != nil {
		t.Fatalf("failed to build cron job: %v", err)
	}

	if activeDeadlineSeconds := cronJob.Spec.JobTemplate.Spec.ActiveDeadlineSeconds; activeDeadlineSeconds != nil {
		t.Fatalf("unexpected activeDeadlineSeconds: got %d, want nil", *activeDeadlineSeconds)
	}
}
