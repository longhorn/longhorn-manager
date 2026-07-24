package monitor

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubecontroller "k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

type testset struct {
	hashStatus       map[string]*longhorn.HashStatus
	existingChecksum string

	expectedChecksum string
	expectedErr      error
}

func TestDetermineChecksumFromHashStatus(t *testing.T) {
	assert := require.New(t)

	snapshotName := "snap-01"

	testsets := []testset{
		// 1 replica
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		// The snapshot is somehow changed
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "cde",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "cde",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since snapshot disk files are silently corrupted", snapshotName),
		},

		// 2 replicas
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since the existing checksum is not found in the checksums from replicas", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since there is no existing checksum", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "abc",
					Error:             "",
					SilentlyCorrupted: false,
				},
				"addr-02": {
					State:             "Completed",
					Checksum:          "def",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "def",
					Error:             "",
					SilentlyCorrupted: false,
				},
				"addr-02": {
					State:             "Completed",
					Checksum:          "ghi",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "def",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:             "Completed",
					Checksum:          "def",
					Error:             "",
					SilentlyCorrupted: true,
				},
				"addr-02": {
					State:             "Completed",
					Checksum:          "ghi",
					Error:             "",
					SilentlyCorrupted: true,
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since snapshot disk files are silently corrupted", snapshotName),
		},

		// 3 replicas
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "fgh",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "abc",
			expectedErr:      nil,
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "fgh",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since there is no existing checksum", snapshotName),
		},

		// 5 replicas
		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-04": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
				"addr-05": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"since there is no existing checksum", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-04": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
				"addr-05": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "abc",
			expectedChecksum: "",
			expectedErr:      fmt.Errorf(prefixChecksumDetermineFailure+"from the existing checksum and the checksums from replicas", snapshotName),
		},

		{
			hashStatus: map[string]*longhorn.HashStatus{
				"addr-01": {
					State:    "Completed",
					Checksum: "abc",
					Error:    "",
				},
				"addr-02": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-03": {
					State:    "Completed",
					Checksum: "cde",
					Error:    "",
				},
				"addr-04": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
				"addr-05": {
					State:    "Completed",
					Checksum: "efg",
					Error:    "",
				},
			},
			existingChecksum: "cde",
			expectedChecksum: "cde",
			expectedErr:      nil,
		},
	}

	log := logrus.StandardLogger().WithField("node", "unknown")

	for _, t := range testsets {
		finalChecksum, err := determineChecksumFromHashStatus(log, snapshotName, t.existingChecksum, t.hashStatus)
		assert.Equal(finalChecksum, t.expectedChecksum)
		if t.expectedErr == nil {
			assert.Nil(err)
		} else {
			assert.Equal(err.Error(), t.expectedErr.Error())
		}
	}
}

const testSnapshotMonitorNamespace = "default"

func newSnapshotDataIntegrityCronJobSetting(v1CronJob, v2CronJob string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(types.SettingNameSnapshotDataIntegrityCronJob),
			Namespace: testSnapshotMonitorNamespace,
		},
		Value: fmt.Sprintf("{%q:%q,%q:%q}", longhorn.DataEngineTypeV1, v1CronJob, longhorn.DataEngineTypeV2, v2CronJob),
	}
}

// newTestSnapshotMonitor sets up a SnapshotMonitor backed by fake clientsets so the
// gocron v2 scheduling logic in UpdateConfiguration/Stop can be exercised without a
// real API server.
func newTestSnapshotMonitor(t *testing.T) (*SnapshotMonitor, *datastore.DataStore, func(*longhorn.Setting)) {
	t.Helper()

	originalSkipListerCheck := datastore.SkipListerCheck
	datastore.SkipListerCheck = true
	t.Cleanup(func() {
		datastore.SkipListerCheck = originalSkipListerCheck
	})

	kubeClient := kubefake.NewSimpleClientset()                // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(testSnapshotMonitorNamespace, kubeClient, lhClient, kubecontroller.NoResyncPeriodFunc())
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

	ds := datastore.NewDataStore(testSnapshotMonitorNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	putSetting := func(setting *longhorn.Setting) {
		created, err := lhClient.LonghornV1beta2().Settings(testSnapshotMonitorNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		if err != nil {
			created, err = lhClient.LonghornV1beta2().Settings(testSnapshotMonitorNamespace).Update(context.TODO(), setting, metav1.UpdateOptions{})
			require.NoError(t, err)
		}
		require.NoError(t, settingIndexer.Update(created))
	}

	eventRecorder := record.NewFakeRecorder(100)
	snapshotChangeEventQueue := workqueue.NewTyped[any]()

	mon, err := NewSnapshotMonitor(logrus.StandardLogger(), ds, "test-node", eventRecorder, snapshotChangeEventQueue, func(key string) {})
	require.NoError(t, err)
	t.Cleanup(mon.Stop)

	return mon, ds, putSetting
}

// TestSnapshotMonitorUpdateConfiguration verifies the gocron v2 based scheduling
// logic: a job is created per data engine, unchanged cron settings don't reschedule
// the job, and changed cron settings replace the previously scheduled job.
func TestSnapshotMonitorUpdateConfiguration(t *testing.T) {
	assert := require.New(t)

	mon, _, putSetting := newTestSnapshotMonitor(t)

	// No setting exists yet, so the (JSON) default cron is used for both data engines.
	assert.NoError(mon.UpdateConfiguration(nil))

	mon.RLock()
	v1Job := mon.scheduledJobs[longhorn.DataEngineTypeV1]
	v2Job := mon.scheduledJobs[longhorn.DataEngineTypeV2]
	assert.NotNil(v1Job)
	assert.NotNil(v2Job)
	assert.NotEqual(v1Job.ID(), v2Job.ID())
	mon.RUnlock()

	// Calling again with the same (default) setting should be a no-op: the same jobs remain scheduled.
	assert.NoError(mon.UpdateConfiguration(nil))
	mon.RLock()
	assert.Equal(v1Job.ID(), mon.scheduledJobs[longhorn.DataEngineTypeV1].ID())
	assert.Equal(v2Job.ID(), mon.scheduledJobs[longhorn.DataEngineTypeV2].ID())
	mon.RUnlock()

	// Changing the cron setting should remove the old job and schedule a new one.
	putSetting(newSnapshotDataIntegrityCronJobSetting("*/5 * * * *", "0 0 */7 * *"))
	assert.NoError(mon.UpdateConfiguration(nil))

	mon.RLock()
	newV1Job := mon.scheduledJobs[longhorn.DataEngineTypeV1]
	assert.NotNil(newV1Job)
	assert.NotEqual(v1Job.ID(), newV1Job.ID())
	assert.Equal("*/5 * * * *", mon.existingDataIntegrityCronJobs[longhorn.DataEngineTypeV1])
	mon.RUnlock()

	nextRun, err := newV1Job.NextRun()
	assert.NoError(err)
	assert.False(nextRun.IsZero())
}
