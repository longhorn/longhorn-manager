package recurringjob

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/record"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testRecurringJob  = "demo-group-snapshot"          // Job.name: the RecurringJob name, used as the owner label value
	testSnapshotGroup = "demo-group-snapshot-1315f068" // the SnapshotGroup object name
)

var (
	testVolumeLabels  = map[string]string{"app-group": "demo"}
	testOwnerLabelKey = types.GetRecurringJobLabelKey(
		types.LonghornLabelRecurringJob, string(longhorn.RecurringJobTypeSnapshotGroup))
)

func TestNewSnapshotGroupJob(t *testing.T) {
	// assigned builds an attached volume carrying the given assignment labels.
	assigned := func(name string, labelSets []map[string]string, mutators ...func(*longhorn.Volume)) *longhorn.Volume {
		labels := map[string]string{}
		for _, set := range labelSets {
			for k, v := range set {
				labels[k] = v
			}
		}
		v := &longhorn.Volume{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Labels: labels},
			Spec:       longhorn.VolumeSpec{SnapshotMaxCount: 254},
			Status:     longhorn.VolumeStatus{State: longhorn.VolumeStateAttached},
		}
		for _, m := range mutators {
			m(v)
		}
		return v
	}

	jobLabel := types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJob, testRecurringJob)
	groupLabel := func(g string) map[string]string {
		return types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJobGroup, g)
	}

	faulted := func(v *longhorn.Volume) { v.Status.Robustness = longhorn.VolumeRobustnessFaulted }
	restoring := func(v *longhorn.Volume) { v.Status.RestoreRequired = true }
	detached := func(v *longhorn.Volume) { v.Status.State = longhorn.VolumeStateDetached }
	snapshotMaxCount := func(count int) func(v *longhorn.Volume) {
		return func(v *longhorn.Volume) {
			v.Spec.SnapshotMaxCount = count
		}
	}
	snapshotMaxSize := func(size int64) func(v *longhorn.Volume) {
		return func(v *longhorn.Volume) {
			v.Spec.SnapshotMaxSize = size
		}
	}
	// engine builds the volume's engine whose status reports the given snapshots plus the volume head.
	engine := func(volumeName string, snapshots ...*longhorn.SnapshotInfo) *longhorn.Engine {
		e := &longhorn.Engine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName + "-e-0",
				Namespace: testNamespace,
				Labels:    types.GetVolumeLabels(volumeName),
			},
			Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{VolumeName: volumeName}},
		}
		e.Status.Snapshots = map[string]*longhorn.SnapshotInfo{"volume-head": {Name: "volume-head", Size: "100"}}
		for _, s := range snapshots {
			e.Status.Snapshots[s.Name] = s
		}
		return e
	}
	snap := func(name, size string) *longhorn.SnapshotInfo { return &longhorn.SnapshotInfo{Name: name, Size: size} }
	removedSnap := func(name, size string) *longhorn.SnapshotInfo {
		return &longhorn.SnapshotInfo{Name: name, Size: size, Removed: true}
	}

	tests := []struct {
		name          string
		labels        map[string]string // RecurringJob.Spec.Labels (snapshot labels)
		groups        []string          // RecurringJob.Spec.Groups
		volumes       []*longhorn.Volume
		engines       []*longhorn.Engine
		retainCount   int
		wantVolumes   []string
		wantErr       string
		allowDetached bool
	}{
		{
			name:        "volume assigned directly",
			volumes:     []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel})},
			wantVolumes: []string{"vol-a"},
		},
		{
			name:        "volume assigned through a job group",
			groups:      []string{"g1"},
			volumes:     []*longhorn.Volume{assigned("vol-a", []map[string]string{groupLabel("g1")})},
			wantVolumes: []string{"vol-a"},
		},
		{
			name:   "direct and group assignments are unioned and deduplicated",
			groups: []string{"g1"},
			volumes: []*longhorn.Volume{
				assigned("vol-c", []map[string]string{groupLabel("g1")}),
				assigned("vol-a", []map[string]string{jobLabel}),
				assigned("vol-b", []map[string]string{jobLabel, groupLabel("g1")}), // both labels: listed once
				assigned("vol-other", []map[string]string{groupLabel("g-not-on-this-job")}),
			},
			wantVolumes: []string{"vol-a", "vol-b", "vol-c"},
		},
		{
			name:        "snapshot labels are preserved and a job without labels is valid",
			labels:      map[string]string{"env": "test"},
			volumes:     []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel})},
			wantVolumes: []string{"vol-a"},
		},
		{
			// Regression: Spec.Labels used to be treated as a volume selector.
			name:    "snapshot labels do not select volumes",
			labels:  map[string]string{"app-group": "demo"},
			volumes: []*longhorn.Volume{assigned("vol-labeled", []map[string]string{{"app-group": "demo"}})},
			wantErr: "no volumes are assigned",
		},
		{
			name:    "no assigned volumes",
			wantErr: "no volumes are assigned",
		},
		{
			name: "one assigned volume is faulted",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}),
				assigned("vol-b", []map[string]string{jobLabel}, faulted),
			},
			wantErr: "skipping snapshotGroup",
		},
		{
			name: "one assigned volume is restoring",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}),
				assigned("vol-b", []map[string]string{jobLabel}, restoring),
			},
			wantErr: "skipping snapshotGroup",
		},
		{
			name: "detached volume while detached jobs are disabled",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}),
				assigned("vol-b", []map[string]string{jobLabel}, detached),
			},
			wantErr: "skipping snapshotGroup",
		},
		{
			name:          "detached volume while detached jobs are allowed",
			allowDetached: true,
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}),
				assigned("vol-b", []map[string]string{jobLabel}, detached),
			},
			wantVolumes: []string{"vol-a", "vol-b"},
		},
		{
			// every assigned volume is ineligible: must report "skipping", not "no volumes are assigned"
			name: "all assigned volumes are ineligible",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}, faulted),
				assigned("vol-b", []map[string]string{jobLabel}, restoring),
			},
			wantErr: "skipping snapshotGroup",
		},
		{
			name:   "volume with both labels does not hide a faulted member",
			groups: []string{"g1"},
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}, faulted),
				assigned("vol-b", []map[string]string{jobLabel, groupLabel("g1")}),
			},
			wantErr: "skipping snapshotGroup",
		},
		{
			name: "retain count of job equals snapshotMaxCount of all volumes",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(2)),
				assigned("vol-b", []map[string]string{jobLabel}, snapshotMaxCount(2)),
			},
			retainCount: 2,
			wantErr:     "must be less than SnapshotMaxCount of every member volume: vol-a(snapshotMaxCount=2), vol-b(snapshotMaxCount=2)",
		},
		{
			name: "retain count of job exceeds snapshotMaxCount of one volume",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(4)),
				assigned("vol-b", []map[string]string{jobLabel}, snapshotMaxCount(1)),
			},
			retainCount: 3,
			wantErr:     "must be less than SnapshotMaxCount of every member volume: vol-b(snapshotMaxCount=1)",
		},
		{
			name: "retain count one below snapshotMaxCount of every volume",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(4)),
				assigned("vol-b", []map[string]string{jobLabel}, snapshotMaxCount(4)),
			},
			retainCount: 3,
			wantVolumes: []string{"vol-a", "vol-b"},
		},
		{
			name:   "volume assigned through a job group exceeds snapshotMaxCount",
			groups: []string{"g1"},
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}),
				assigned("vol-b", []map[string]string{groupLabel("g1")}, snapshotMaxCount(2)),
			},
			retainCount: 2,
			wantErr:     "vol-b(snapshotMaxCount=2)",
		},
		{
			name:    "volume at snapshotMaxCount is rejected",
			volumes: []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(2))},
			engines: []*longhorn.Engine{engine("vol-a", snap("s1", "10"), snap("s2", "10"))},
			wantErr: "member volumes have reached their snapshot limits: vol-a (2/2 snapshots)",
		},
		{
			name:        "volume one below snapshotMaxCount passes",
			volumes:     []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(3))},
			engines:     []*longhorn.Engine{engine("vol-a", snap("s1", "10"), snap("s2", "10"))},
			wantVolumes: []string{"vol-a"},
		},
		{
			name:    "snapshot marked removed but not purged still counts toward snapshotMaxCount",
			volumes: []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(2))},
			engines: []*longhorn.Engine{engine("vol-a", snap("s1", "10"), removedSnap("s2", "10"))},
			wantErr: "vol-a (2/2 snapshots)",
		},
		{
			name:    "volume at snapshotMaxSize is rejected",
			volumes: []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxSize(100))},
			engines: []*longhorn.Engine{engine("vol-a", snap("s1", "60"), snap("s2", "40"))},
			wantErr: "member volumes have reached their snapshot limits: vol-a (100/100 bytes of snapshots)",
		},
		{
			name:        "volume under snapshotMaxSize passes",
			volumes:     []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxSize(101))},
			engines:     []*longhorn.Engine{engine("vol-a", snap("s1", "60"), snap("s2", "40"))},
			wantVolumes: []string{"vol-a"},
		},
		{
			name:        "snapshot marked removed does not count toward snapshotMaxSize",
			volumes:     []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxSize(100))},
			engines:     []*longhorn.Engine{engine("vol-a", snap("s1", "60"), removedSnap("s2", "40"))},
			wantVolumes: []string{"vol-a"},
		},
		{
			name:        "snapshotMaxSize 0 means no size limit",
			volumes:     []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel})},
			engines:     []*longhorn.Engine{engine("vol-a", snap("s1", "999999999999"))},
			wantVolumes: []string{"vol-a"},
		},
		{
			name: "only the member volumes at a limit are named",
			volumes: []*longhorn.Volume{
				assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(3)),
				assigned("vol-b", []map[string]string{jobLabel}, snapshotMaxCount(2)),
			},
			engines: []*longhorn.Engine{
				engine("vol-a", snap("a1", "10")),
				engine("vol-b", snap("b1", "10"), snap("b2", "10")),
			},
			wantErr: "member volumes have reached their snapshot limits: vol-b (2/2 snapshots);",
		},
		{
			name:    "engine of a volume that is not a member is ignored",
			volumes: []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel}, snapshotMaxCount(2))},
			engines: []*longhorn.Engine{
				engine("vol-a", snap("a1", "10")),
				engine("vol-other", snap("o1", "10"), snap("o2", "10"), snap("o3", "10")),
			},
			wantVolumes: []string{"vol-a"},
		},
		{
			name:    "unparsable snapshot size returns an error",
			volumes: []*longhorn.Volume{assigned("vol-a", []map[string]string{jobLabel})},
			engines: []*longhorn.Engine{engine("vol-a", snap("s1", "not-a-number"))},
			wantErr: `failed to parse size "not-a-number" of snapshot s1 of volume vol-a`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{newSetting(strconv.FormatBool(tc.allowDetached))}
			for _, v := range tc.volumes {
				objs = append(objs, v)
			}
			for _, e := range tc.engines {
				objs = append(objs, e)
			}
			env := newSnapshotGroupJobForTest(objs...)
			if tc.retainCount != 0 {
				env.retentionPolicy = longhorn.RecurringJobRetentionPolicyCountBased
				env.retainCount = tc.retainCount
			}

			sgJob, err := newSnapshotGroupJob(env.Job, newRecurringJob(tc.labels, tc.groups...))

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Nil(t, sgJob)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantVolumes, sgJob.volumes)
			assert.Equal(t, tc.labels, sgJob.labels)
			assert.NotEmpty(t, sgJob.snapshotGroup)
		})
	}
}

func TestStartSnapshotGroupJob(t *testing.T) {
	activeSG := func(vols []string, phase longhorn.SnapshotGroupPhase) longhorn.SnapshotGroup {
		return longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-sg-1", Namespace: testNamespace,
				Labels: map[string]string{testOwnerLabelKey: testRecurringJob},
			},
			Spec: longhorn.SnapshotGroupSpec{
				Volumes: vols,
			},
			Status: longhorn.SnapshotGroupStatus{Phase: phase},
		}
	}
	tests := []struct {
		name                string
		labels              map[string]string
		volumes             []string
		existingSGs         []longhorn.SnapshotGroup
		getPhases           []longhorn.SnapshotGroupPhase
		getErrs             []error
		listErr             error
		createErr           error
		wantCreate          bool
		wantErr             bool
		expectedErrStr      string
		expectedEventReason string
	}{
		{
			name:    "skip when previous snapshot group is in progress",
			labels:  testVolumeLabels,
			volumes: []string{"vol-a", "vol-b", "vol-c"},
			existingSGs: []longhorn.SnapshotGroup{
				activeSG([]string{"vol-a", "vol-b", "vol-c"}, longhorn.SnapshotGroupPhaseInProgress)},
			wantErr:             false,
			expectedEventReason: "SkippedSnapshotGroup",
		},
		{
			name:                "skip when previous snapshot group is pending (phase not yet set)",
			labels:              testVolumeLabels,
			volumes:             []string{"vol-a"},
			existingSGs:         []longhorn.SnapshotGroup{activeSG([]string{"vol-a"}, "")},
			expectedEventReason: "SkippedSnapshotGroup",
		},
		{
			name:                "fail when no volumes are assigned",
			wantErr:             true,
			expectedErrStr:      "no volumes are assigned",
			expectedEventReason: "FailedSnapshotGroup",
		},
		{
			name:                "fail when ListSnapshotGroup returns error",
			labels:              testVolumeLabels,
			volumes:             []string{"vol-a", "vol-b"},
			listErr:             errors.New("list boom"),
			wantErr:             true,
			expectedErrStr:      "failed to list SnapshotGroups for recurring job",
			expectedEventReason: "FailedSnapshotGroup",
		},
		{
			name:    "successful run to Ready phase",
			labels:  testVolumeLabels,
			volumes: []string{"vol-a", "vol-b"},
			getPhases: []longhorn.SnapshotGroupPhase{
				longhorn.SnapshotGroupPhaseInProgress, longhorn.SnapshotGroupPhaseReady},
			wantCreate:          true,
			wantErr:             false,
			expectedEventReason: "CompletedSnapshotGroup",
		},
		{
			name:                "job finishes in Failed phase",
			labels:              testVolumeLabels,
			volumes:             []string{"vol-a"},
			getPhases:           []longhorn.SnapshotGroupPhase{longhorn.SnapshotGroupPhaseFailed},
			wantCreate:          true,
			wantErr:             true,
			expectedErrStr:      "ended in phase",
			expectedEventReason: "FailedSnapshotGroup",
		},
		{
			name:                "fail when CreateSnapshotGroup errors out",
			labels:              testVolumeLabels,
			volumes:             []string{"vol-a"},
			createErr:           errors.New("create boom"),
			wantCreate:          true, // create request is sent, then rejected
			wantErr:             true,
			expectedErrStr:      "failed to create SnapshotGroup",
			expectedEventReason: "FailedSnapshotGroup",
		},
		{
			name:                "transient GET error while waiting, then Ready",
			labels:              testVolumeLabels,
			volumes:             []string{"vol-a"},
			getErrs:             []error{errors.New("connection reset")},
			getPhases:           []longhorn.SnapshotGroupPhase{longhorn.SnapshotGroupPhaseInProgress, longhorn.SnapshotGroupPhaseReady},
			wantCreate:          true,
			expectedEventReason: "CompletedSnapshotGroup",
		},
		{
			name:    "group deleted while waiting is terminal",
			labels:  testVolumeLabels,
			volumes: []string{"vol-a"},
			getErrs: []error{apierrors.NewNotFound(
				schema.GroupResource{Group: "longhorn.io", Resource: "snapshotgroups"}, testSnapshotGroup)},
			getPhases:           []longhorn.SnapshotGroupPhase{longhorn.SnapshotGroupPhaseInProgress},
			wantCreate:          true,
			wantErr:             true,
			expectedErrStr:      "not found",
			expectedEventReason: "FailedSnapshotGroup",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recurringJob := newRecurringJob(tc.labels)

			runtimeObjs := make([]runtime.Object, 0, len(tc.existingSGs))
			// newSnapshotGroupJob resolves members from the volumes assigned to the job,
			// which first reads this setting.
			runtimeObjs = append(runtimeObjs, newSetting("false"))

			for _, name := range tc.volumes {
				runtimeObjs = append(runtimeObjs, &longhorn.Volume{
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: testNamespace,
						Labels:    types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJob, testRecurringJob),
					},
					Status: longhorn.VolumeStatus{State: longhorn.VolumeStateAttached},
				})
			}
			for i := range tc.existingSGs {
				runtimeObjs = append(runtimeObjs, &tc.existingSGs[i])
			}

			job := newJob(&longhornclient.RancherClient{}, runtimeObjs...)

			fakeClient, ok := job.lhClient.(*lhfake.Clientset)
			require.True(t, ok, "lhClient must be a *lhfake.Clientset")

			if tc.listErr != nil {
				fakeClient.PrependReactor("list", "snapshotgroups",
					func(k8stesting.Action) (bool, runtime.Object, error) {
						return true, nil, tc.listErr
					})
			}

			if tc.createErr != nil {
				fakeClient.PrependReactor("create", "snapshotgroups",
					func(k8stesting.Action) (bool, runtime.Object, error) {
						return true, nil, tc.createErr
					})
			}

			if len(tc.getPhases) > 0 || len(tc.getErrs) > 0 {
				scriptPhases(fakeClient, tc.getPhases, tc.getErrs...)
			}

			err := StartSnapshotGroupJob(job, recurringJob)

			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrStr)
			} else {
				require.NoError(t, err)
			}

			fakeRecorder := job.eventRecorder.(*record.FakeRecorder)
			select {
			case event := <-fakeRecorder.Events:
				if !strings.Contains(event, tc.expectedEventReason) {
					t.Errorf("Expected event reason %q, got event: %q", tc.expectedEventReason, event)
				}
			default:
				if tc.expectedEventReason != "" {
					t.Errorf("Expected event containing %q, but no event was emitted", tc.expectedEventReason)
				}
			}

			// Inspect what the job actually asked the API to create, not the scripted responses.
			created := createdSnapshotGroups(fakeClient)
			if !tc.wantCreate {
				assert.Empty(t, created, "this run must not send a create request")
				return
			}
			require.Len(t, created, 1)
			sg := created[0]
			assert.Equal(t, tc.volumes, sg.Spec.Volumes, "expected volumes: %v, SnapshotGroup got: %v", tc.volumes, sg.Spec.Volumes)
			assert.Equal(t, tc.labels, sg.Spec.Labels,
				"expected labels: %v, SnapshotGroup got: %v", tc.labels, sg.Spec.Labels)
			assert.Equal(t, map[string]string{testOwnerLabelKey: testRecurringJob}, sg.Labels,
				"SnapshotGroup does not have expected owning-job label: %v", sg.Labels)
			assert.EqualValues(t, defaultDeadlineSeconds, sg.Spec.DeadlineSeconds,
				"expected deadline: %v, SnapshotGroup got: %v", defaultDeadlineSeconds, sg.Spec.DeadlineSeconds)
			assert.Equal(t, testNamespace, sg.Namespace)
			assert.NotEmpty(t, sg.Name)

			// the job must poll the group it created, not some other name
			for _, a := range fakeClient.Actions() {
				if a.Matches("get", "snapshotgroups") {
					assert.Equal(t, sg.Name, a.(k8stesting.GetAction).GetName())
				}
			}
		})
	}
}

func TestSnapshotGroupJobCleanup(t *testing.T) {
	// sg builds a job-owned group created `age` ago.
	sg := func(name string, phase longhorn.SnapshotGroupPhase, age time.Duration,
		mutators ...func(*longhorn.SnapshotGroup)) *longhorn.SnapshotGroup {
		g := &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         testNamespace,
				Labels:            map[string]string{testOwnerLabelKey: testRecurringJob},
				CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
			},
			Status: longhorn.SnapshotGroupStatus{Phase: phase},
		}
		for _, m := range mutators {
			m(g)
		}
		return g
	}

	manual := func(g *longhorn.SnapshotGroup) { g.Labels = nil } // created by hand, no owner label
	otherJob := func(g *longhorn.SnapshotGroup) { g.Labels = map[string]string{testOwnerLabelKey: "other-job"} }

	members := func(snapshots ...string) func(g *longhorn.SnapshotGroup) {
		return func(g *longhorn.SnapshotGroup) {
			for i, s := range snapshots {
				g.Spec.Members = append(g.Spec.Members, longhorn.SnapshotGroupMember{
					VolumeName:   "vol-" + strconv.Itoa(i),
					SnapshotName: s,
				})
			}
		}
	}
	backup := func(name, snapshot string, state longhorn.BackupState) *longhorn.Backup {
		return &longhorn.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec:       longhorn.BackupSpec{SnapshotName: snapshot},
			Status:     longhorn.BackupStatus{State: state},
		}
	}

	h := time.Hour

	tests := []struct {
		name        string
		policy      longhorn.RecurringJobRetentionPolicy
		retainCount int
		retainAge   time.Duration
		groups      []*longhorn.SnapshotGroup // seeded in this order
		backups     []*longhorn.Backup
		wantDeleted []string
	}{
		// ---- count-based, retain 3 ----
		{
			name:        "at the limit deletes nothing",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 3,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 3*h),
				sg("sg-2", longhorn.SnapshotGroupPhaseReady, 2*h),
				sg("sg-3", longhorn.SnapshotGroupPhaseReady, 1*h),
			},
		},
		{
			name:        "over the limit deletes the oldest ready groups",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 3,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 5*h),
				sg("sg-2", longhorn.SnapshotGroupPhaseReady, 4*h),
				sg("sg-3", longhorn.SnapshotGroupPhaseReady, 3*h),
				sg("sg-4", longhorn.SnapshotGroupPhaseReady, 2*h),
				sg("sg-5", longhorn.SnapshotGroupPhaseReady, 1*h),
			},
			wantDeleted: []string{"sg-1", "sg-2"},
		},
		{
			// seeded out of age order on purpose: catches a broken comparator
			name:        "failed group goes before ready groups, regardless of age",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 3,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-5", longhorn.SnapshotGroupPhaseReady, 1*h),
				sg("sg-2", longhorn.SnapshotGroupPhaseReady, 4*h),
				sg("sg-4", longhorn.SnapshotGroupPhaseFailed, 2*h),
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 5*h),
				sg("sg-3", longhorn.SnapshotGroupPhaseReady, 3*h),
			},
			wantDeleted: []string{"sg-4", "sg-1"},
		},
		{
			name:        "every failed group goes before any ready group is touched",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 3,
			groups: []*longhorn.SnapshotGroup{
				sg("r-1", longhorn.SnapshotGroupPhaseReady, 6*h),
				sg("r-2", longhorn.SnapshotGroupPhaseReady, 5*h),
				sg("r-3", longhorn.SnapshotGroupPhaseReady, 4*h),
				sg("f-1", longhorn.SnapshotGroupPhaseFailed, 2*h),
				sg("f-2", longhorn.SnapshotGroupPhaseFailed, 1*h),
			},
			wantDeleted: []string{"f-1", "f-2"},
		},
		{
			name:   "only failed groups over the limit deletes the oldest",
			policy: longhorn.RecurringJobRetentionPolicyCountBased, retainCount: 3,
			groups: []*longhorn.SnapshotGroup{
				sg("f-1", longhorn.SnapshotGroupPhaseFailed, 5*h),
				sg("f-2", longhorn.SnapshotGroupPhaseFailed, 4*h),
				sg("f-3", longhorn.SnapshotGroupPhaseFailed, 3*h),
				sg("f-4", longhorn.SnapshotGroupPhaseFailed, 2*h),
				sg("f-5", longhorn.SnapshotGroupPhaseFailed, 1*h),
			},
			wantDeleted: []string{"f-1", "f-2"},
		},
		{
			name:        "retain 0 deletes every eligible group",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 0,
			groups: []*longhorn.SnapshotGroup{
				sg("r-1", longhorn.SnapshotGroupPhaseReady, 3*h),
				sg("r-2", longhorn.SnapshotGroupPhaseReady, 2*h),
				sg("f-1", longhorn.SnapshotGroupPhaseReady, 1*h),
			},
			wantDeleted: []string{"r-1", "r-2", "f-1"},
		},
		{
			name:        "in-progress and pending groups are neither counted nor deleted",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 3,
			groups: []*longhorn.SnapshotGroup{
				sg("r-1", longhorn.SnapshotGroupPhaseReady, 3*h),
				sg("r-2", longhorn.SnapshotGroupPhaseReady, 2*h),
				sg("r-3", longhorn.SnapshotGroupPhaseReady, 1*h),
				sg("busy", longhorn.SnapshotGroupPhaseInProgress, 10*h),
				sg("pending", "", 10*h),
			},
			// busy & pending never make it to the eligible criteria,
			// hence nothing deleted: only 3 eligible groups exist and retain is 3
		},
		{
			name:        "manual groups and other jobs' groups are untouched",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 3,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 5*h),
				sg("sg-2", longhorn.SnapshotGroupPhaseReady, 4*h),
				sg("sg-3", longhorn.SnapshotGroupPhaseReady, 3*h),
				sg("sg-4", longhorn.SnapshotGroupPhaseReady, 2*h),
				sg("sg-5", longhorn.SnapshotGroupPhaseReady, 1*h),
				sg("manual-sg", longhorn.SnapshotGroupPhaseReady, 9*h, manual),
				sg("other-sg", longhorn.SnapshotGroupPhaseReady, 9*h, otherJob),
			},
			wantDeleted: []string{"sg-1", "sg-2"},
		},

		// ---- age-based, retain 24h ----
		{
			name:      "deletes groups older than retainAge regardless of phase",
			policy:    longhorn.RecurringJobRetentionPolicyAgeBased,
			retainAge: 24 * h,
			groups: []*longhorn.SnapshotGroup{
				sg("old-ready", longhorn.SnapshotGroupPhaseReady, 48*h),
				sg("old-failed", longhorn.SnapshotGroupPhaseFailed, 30*h),
				sg("new-ready", longhorn.SnapshotGroupPhaseReady, 1*h),
				sg("new-failed", longhorn.SnapshotGroupPhaseFailed, 2*h),
				sg("old-busy", longhorn.SnapshotGroupPhaseInProgress, 72*h), // non-terminal: never deleted
			},
			wantDeleted: []string{"old-ready", "old-failed"},
		},
		{
			name:      "retainAge 0 deletes nothing",
			policy:    longhorn.RecurringJobRetentionPolicyAgeBased,
			retainAge: 0,
			groups:    []*longhorn.SnapshotGroup{sg("sg-1", longhorn.SnapshotGroupPhaseReady, 48*h)},
		},

		// ---- safety ----
		{
			name:        "unknown policy deletes nothing",
			policy:      longhorn.RecurringJobRetentionPolicy("bogus"),
			retainCount: 1,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 3*h),
				sg("sg-2", longhorn.SnapshotGroupPhaseReady, 2*h),
				sg("sg-3", longhorn.SnapshotGroupPhaseReady, 1*h),
			},
		},
		// ---- active backups ----
		{
			name:        "count-based: group with a member under an in-progress backup is deferred",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 1,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 3*h, members("snap-1a", "snap-1b")),
				sg("sg-2", longhorn.SnapshotGroupPhaseReady, 2*h, members("snap-2a", "snap-2b")),
				sg("sg-3", longhorn.SnapshotGroupPhaseReady, 1*h, members("snap-3a", "snap-3b")),
			},
			backups: []*longhorn.Backup{
				backup("backup-1", "snap-1b", longhorn.BackupStateInProgress),
			},
			wantDeleted: []string{"sg-2"},
		},
		{
			name:      "age-based: group with a member under an in-progress backup is deferred",
			policy:    longhorn.RecurringJobRetentionPolicyAgeBased,
			retainAge: 24 * h,
			groups: []*longhorn.SnapshotGroup{
				sg("old-1", longhorn.SnapshotGroupPhaseReady, 48*h, members("snap-1")),
				sg("old-2", longhorn.SnapshotGroupPhaseFailed, 30*h, members("snap-2")),
				sg("new-1", longhorn.SnapshotGroupPhaseReady, 1*h, members("snap-3")),
			},
			backups: []*longhorn.Backup{
				backup("backup-1", "snap-2", longhorn.BackupStateInProgress),
			},
			wantDeleted: []string{"old-1"},
		},
		{
			name:        "finished backups do not defer deletion",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 0,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 4*h, members("snap-1")),
				sg("sg-2", longhorn.SnapshotGroupPhaseReady, 3*h, members("snap-2")),
				sg("sg-3", longhorn.SnapshotGroupPhaseReady, 2*h, members("snap-3")),
				sg("sg-4", longhorn.SnapshotGroupPhaseReady, 1*h, members("snap-4")),
			},
			backups: []*longhorn.Backup{
				backup("backup-completed", "snap-1", longhorn.BackupStateCompleted),
				backup("backup-error", "snap-2", longhorn.BackupStateError),
				backup("backup-unknown", "snap-3", longhorn.BackupStateUnknown),
				backup("backup-deleting", "snap-4", longhorn.BackupStateDeleting),
			},
			wantDeleted: []string{"sg-1", "sg-2", "sg-3", "sg-4"},
		},
		{
			name:        "active backups of non-member snapshots do not defer deletion",
			policy:      longhorn.RecurringJobRetentionPolicyCountBased,
			retainCount: 0,
			groups: []*longhorn.SnapshotGroup{
				sg("sg-1", longhorn.SnapshotGroupPhaseReady, 1*h, members("snap-1")),
			},
			backups: []*longhorn.Backup{
				backup("backup-other", "snap-unrelated", longhorn.BackupStateInProgress),
				backup("backup-no-snapshot", "", longhorn.BackupStateInProgress),
			},
			wantDeleted: []string{"sg-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objs := make([]runtime.Object, 0, len(tc.groups)+len(tc.backups))
			for _, g := range tc.groups {
				objs = append(objs, g)
			}
			for _, b := range tc.backups {
				objs = append(objs, b)
			}
			env := newSnapshotGroupJobForTest(objs...)
			env.retentionPolicy = tc.policy
			env.retainCount = tc.retainCount
			env.retainAge = tc.retainAge

			env.cleanup()

			fakeClient := env.lhClient.(*lhfake.Clientset)
			assert.ElementsMatch(t, tc.wantDeleted, deletedSnapshotGroups(fakeClient))
		})
	}
}

func TestStartSnapshotGroupJobRetentionOnSnapshotLimit(t *testing.T) {
	jobLabel := types.GetRecurringJobLabelValueMap(types.LonghornLabelRecurringJob, testRecurringJob)
	// readyGroup builds a Ready group of this job created `age` ago.
	readyGroup := func(name string, age time.Duration) *longhorn.SnapshotGroup {
		return &longhorn.SnapshotGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         testNamespace,
				Labels:            map[string]string{testOwnerLabelKey: testRecurringJob},
				CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
			},
			Status: longhorn.SnapshotGroupStatus{Phase: longhorn.SnapshotGroupPhaseReady},
		}
	}
	// fullEngine builds vol-a's engine reporting two snapshots plus the volume head.
	fullEngine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{Name: "vol-a-e-0", Namespace: testNamespace, Labels: types.GetVolumeLabels("vol-a")},
		Spec:       longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{VolumeName: "vol-a"}},
		Status: longhorn.EngineStatus{Snapshots: map[string]*longhorn.SnapshotInfo{
			"volume-head": {Name: "volume-head"},
			"s1":          {Name: "s1", Size: "10"},
			"s2":          {Name: "s2", Size: "10"},
		}},
	}

	tests := []struct {
		name        string
		volumeLabel map[string]string // labels of vol-a; nil leaves it unassigned
		wantErr     string
		wantDeleted []string
	}{
		{
			name:        "volume at its snapshot limit runs retention for the job's expired groups",
			volumeLabel: jobLabel,
			wantErr:     "member volumes have reached their snapshot limits: vol-a (2/2 snapshots)",
			wantDeleted: []string{"sg-1", "sg-2"},
		},
		{
			name:    "a rejection for another reason does not run retention",
			wantErr: "no volumes are assigned",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			volume := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "vol-a", Namespace: testNamespace, Labels: tc.volumeLabel},
				Spec:       longhorn.VolumeSpec{SnapshotMaxCount: 2},
				Status:     longhorn.VolumeStatus{State: longhorn.VolumeStateAttached},
			}
			job := newJob(&longhornclient.RancherClient{},
				newSetting("false"), volume, fullEngine,
				readyGroup("sg-1", 3*time.Hour), readyGroup("sg-2", 2*time.Hour), readyGroup("sg-3", 1*time.Hour))
			job.retentionPolicy = longhorn.RecurringJobRetentionPolicyCountBased
			job.retainCount = 1

			err := StartSnapshotGroupJob(job, newRecurringJob(nil))

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			fakeClient := job.lhClient.(*lhfake.Clientset)
			assert.Empty(t, createdSnapshotGroups(fakeClient), "no group should be created when the run is rejected")
			assert.ElementsMatch(t, tc.wantDeleted, deletedSnapshotGroups(fakeClient))
		})
	}
}

// newJob builds a Job backed by a fake Longhorn clientset seeded with objects.
func newJob(api *longhornclient.RancherClient, objects ...runtime.Object) *Job {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	return &Job{
		api:           api,
		lhClient:      lhfake.NewSimpleClientset(objects...), // nolint: staticcheck
		logger:        logger,
		eventRecorder: record.NewFakeRecorder(10),
		name:          testRecurringJob,
		namespace:     testNamespace,
		task:          longhorn.RecurringJobTypeSnapshotGroup,
	}
}

// newSnapshotGroupJobForTest wires a SnapshotGroupJob to a fake Longhorn clientset
// seeded with the given SnapshotGroupJobs.
func newSnapshotGroupJobForTest(objs ...runtime.Object) *SnapshotGroupJob {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	return &SnapshotGroupJob{
		Job:             newJob(&longhornclient.RancherClient{}, objs...),
		logger:          logrus.NewEntry(logger),
		snapshotGroup:   testSnapshotGroup,
		volumes:         []string{"vol-a"},
		labels:          testVolumeLabels,
		deadlineSeconds: defaultDeadlineSeconds,
	}
}

func newRecurringJob(labels map[string]string, groups ...string) *longhorn.RecurringJob {
	return &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testRecurringJob,
			Namespace: testNamespace,
		},
		Spec: longhorn.RecurringJobSpec{
			Labels: labels,
			Groups: groups,
		},
	}
}

// createdSnapshotGroups returns the SnapshotGroups the job asked the API to create.
func createdSnapshotGroups(c *lhfake.Clientset) []*longhorn.SnapshotGroup {
	var created []*longhorn.SnapshotGroup
	for _, a := range c.Actions() {
		if a.Matches("create", "snapshotgroups") {
			created = append(created, a.(k8stesting.CreateAction).GetObject().(*longhorn.SnapshotGroup))
		}
	}
	return created
}

func snapshotGroupWithPhase(name string, phase longhorn.SnapshotGroupPhase) *longhorn.SnapshotGroup {
	return &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{testOwnerLabelKey: testRecurringJob},
		},
		Status: longhorn.SnapshotGroupStatus{Phase: phase},
	}
}

// scriptPhases makes the fake clientset report the given phases for the
// SnapshotGroup the job creates, whichever way the production code polls:
//   - "get":   each call returns the next phase (last one repeats)
//   - "list":  the FIRST call (the "is a previous group running?" pre-check)
//     falls through to the tracker; later calls return the next phase
//   - "watch": a buffered fake watcher is pre-loaded with one event per phase
func scriptPhases(fakeClient *lhfake.Clientset, phases []longhorn.SnapshotGroupPhase, getErrs ...error) {
	getCalls := 0
	fakeClient.PrependReactor("get", "snapshotgroups",
		func(a k8stesting.Action) (bool, runtime.Object, error) {
			call := getCalls
			getCalls++
			if call < len(getErrs) {
				return true, nil, getErrs[call]
			}
			p := phases[min(call-len(getErrs), len(phases)-1)]
			return true, snapshotGroupWithPhase(a.(k8stesting.GetAction).GetName(), p), nil
		})

	listCalls := 0
	fakeClient.PrependReactor("list", "snapshotgroups",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			listCalls++
			if listCalls == 1 {
				return false, nil, nil // pre-check: use the tracker (empty list)
			}
			idx := listCalls - 2
			p := phases[min(idx, len(phases)-1)]
			return true, &longhorn.SnapshotGroupList{
				Items: []longhorn.SnapshotGroup{*snapshotGroupWithPhase(testSnapshotGroup, p)},
			}, nil
		})

	w := watch.NewFakeWithChanSize(len(phases), false)
	for _, p := range phases {
		w.Modify(snapshotGroupWithPhase(testSnapshotGroup, p))
	}
	fakeClient.PrependWatchReactor("snapshotgroups", k8stesting.DefaultWatchReactor(w, nil))
}

// deletedSnapshotGroups returns the names of SnapshotGroups the job asked the API to delete.
func deletedSnapshotGroups(c *lhfake.Clientset) []string {
	var deleted []string
	for _, a := range c.Actions() {
		if a.Matches("delete", "snapshotgroups") {
			deleted = append(deleted, a.(k8stesting.DeleteAction).GetName())
		}
	}
	return deleted
}
