package recurringjob

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

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

	tests := []struct {
		name          string
		labels        map[string]string // RecurringJob.Spec.Labels (snapshot labels)
		groups        []string          // RecurringJob.Spec.Groups
		volumes       []*longhorn.Volume
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objs := []runtime.Object{newSetting(strconv.FormatBool(tc.allowDetached))}
			for _, v := range tc.volumes {
				objs = append(objs, v)
			}
			env := newSnapshotGroupJobForTest(objs...)

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
