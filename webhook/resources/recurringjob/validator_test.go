package recurringjob

import (
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testNamespace        = "longhorn-system"
	testBackupTargetName = "secondary-backup-target"
	testRecurringJobName = "test-recurring-job"
)

func newTestBackupTarget(name string) *longhorn.BackupTarget {
	return &longhorn.BackupTarget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
	}
}

func newTestRecurringJob(backupTargetName string) *longhorn.RecurringJob {
	return &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testRecurringJobName,
			Namespace: testNamespace,
		},
		Spec: longhorn.RecurringJobSpec{
			Name:         testRecurringJobName,
			Task:         longhorn.RecurringJobTypeBackup,
			Cron:         "0 0 * * *",
			Retain:       1,
			BackupTarget: backupTargetName,
		},
	}
}

func newTestValidator(t *testing.T, stopCh chan struct{}, objects ...runtime.Object) *recurringJobValidator {
	t.Helper()

	lhClient := lhfake.NewSimpleClientset(objects...)          // nolint: staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStore(testNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	informerFactories.Start(stopCh)
	require.True(t, cache.WaitForCacheSync(stopCh,
		ds.BackupTargetInformer.HasSynced,
		ds.SettingInformer.HasSynced,
	))

	return &recurringJobValidator{ds: ds}
}

// TestValidatorBackupTarget pins that Spec.BackupTarget (longhorn/longhorn#11421)
// is carried into the validation call on both Create and Update. The validator
// rebuilds a RecurringJobSpec field by field before handing it to
// ValidateRecurringJobs, so dropping the new field there would silently accept a
// job pointing at a backup target that does not exist -- the job would then fail
// on every run instead of being rejected up front.
func TestValidatorBackupTarget(t *testing.T) {
	tests := map[string]struct {
		existingObjects []runtime.Object
		recurringJob    *longhorn.RecurringJob
		expectError     string
	}{
		"existing backup target is accepted": {
			existingObjects: []runtime.Object{newTestBackupTarget(testBackupTargetName)},
			recurringJob:    newTestRecurringJob(testBackupTargetName),
		},
		"nonexistent backup target is rejected": {
			recurringJob: newTestRecurringJob(testBackupTargetName),
			expectError:  "has invalid backup target " + testBackupTargetName,
		},
		"empty backup target is accepted": {
			recurringJob: newTestRecurringJob(""),
		},
	}

	for name, tc := range tests {
		for _, operation := range []string{"create", "update"} {
			t.Run(operation+" "+name, func(t *testing.T) {
				stopCh := make(chan struct{})
				defer close(stopCh)
				validator := newTestValidator(t, stopCh, tc.existingObjects...)

				var err error
				if operation == "create" {
					err = validator.Create(nil, tc.recurringJob)
				} else {
					err = validator.Update(nil, newTestRecurringJob(""), tc.recurringJob)
				}

				if tc.expectError == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.expectError)
				}
			})
		}
	}
}
