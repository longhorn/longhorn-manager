package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clienttesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

// TestCreatePreservesSpecFields guards the Create spec rebuild: fields plumbed
// from the API layer must survive into the created Volume CR. This is where a
// newly added spec field silently disappears when it is not copied here.
func TestCreatePreservesSpecFields(t *testing.T) {
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories("default", kubeClient, lhClient, controller.NoResyncPeriodFunc())

	ds := datastore.NewDataStoreForGlobal("default", lhClient, kubeClient, extensionsClient, informerFactories)

	// Start the informers so the datastore's create-then-verify lister sees
	// objects created through the fake clientset.
	stop := make(chan struct{})
	defer close(stop)
	informerFactories.LhInformerFactory.Start(stop)
	informerFactories.KubeInformerFactory.Start(stop)
	informerFactories.LhInformerFactory.WaitForCacheSync(stop)
	informerFactories.KubeInformerFactory.WaitForCacheSync(stop)

	m := NewVolumeManager("test-node", ds, util.NewAtomicCounter(), nil)

	spec := &longhorn.VolumeSpec{
		Size:             1073741824,
		NumberOfReplicas: 1,
		NodeSelector:     []string{"remote-storage"},
		TopologyRequirement: []longhorn.VolumeTopologyTerm{
			{Zone: "zone-a", Region: "region-1"},
		},
	}

	v, err := m.Create("test-volume", spec, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(v.Spec.TopologyRequirement) != 1 ||
		v.Spec.TopologyRequirement[0].Zone != "zone-a" ||
		v.Spec.TopologyRequirement[0].Region != "region-1" {
		t.Errorf("TopologyRequirement not preserved through Create: %+v", v.Spec.TopologyRequirement)
	}
	if len(v.Spec.NodeSelector) != 1 || v.Spec.NodeSelector[0] != "remote-storage" {
		t.Errorf("NodeSelector not preserved through Create: %+v", v.Spec.NodeSelector)
	}
}

func TestExpandDefersAffectedV2VolumeDuringLiveUpgrade(t *testing.T) {
	for _, state := range []longhorn.VolumeState{longhorn.VolumeStateAttached, longhorn.VolumeStateDetached} {
		t.Run(string(state), func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories("default", kubeClient, lhClient, controller.NoResyncPeriodFunc())

			volume := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "volume", Namespace: "default"},
				Spec: longhorn.VolumeSpec{
					Size:       2 * 1024 * 1024 * 1024,
					DataEngine: longhorn.DataEngineTypeV2,
					NodeID:     "node-a",
				},
				Status: longhorn.VolumeStatus{
					State:         state,
					CurrentNodeID: "node-a",
					Conditions: []longhorn.Condition{
						{
							Type:   string(longhorn.VolumeConditionTypeScheduled),
							Status: longhorn.ConditionStatusTrue,
						},
					},
				},
			}
			if _, err := lhClient.LonghornV1beta2().Volumes("default").Create(context.TODO(), volume, metav1.CreateOptions{}); err != nil {
				t.Fatalf("failed to create volume: %v", err)
			}
			upgrade := &longhorn.InstanceManagerUpgrade{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade", Namespace: "default"},
				Spec:       longhorn.InstanceManagerUpgradeSpec{NodeID: "node-a"},
				Status:     longhorn.InstanceManagerUpgradeStatus{State: longhorn.InstanceManagerUpgradeStateRelocatingEngines},
			}
			if _, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades("default").Create(context.TODO(), upgrade, metav1.CreateOptions{}); err != nil {
				t.Fatalf("failed to create instance manager upgrade: %v", err)
			}

			ds := datastore.NewDataStoreForGlobal("default", lhClient, kubeClient, extensionsClient, informerFactories)
			stop := make(chan struct{})
			defer close(stop)
			informerFactories.LhInformerFactory.Start(stop)
			informerFactories.KubeInformerFactory.Start(stop)
			informerFactories.LhInformerFactory.WaitForCacheSync(stop)
			informerFactories.KubeInformerFactory.WaitForCacheSync(stop)

			m := NewVolumeManager("test-node", ds, util.NewAtomicCounter(), nil)
			if _, err := m.Expand(volume.Name, 3*1024*1024*1024); err == nil {
				t.Fatal("expected expansion to be deferred during the live upgrade")
			}

			updated, err := lhClient.LonghornV1beta2().Volumes("default").Get(context.TODO(), volume.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get volume: %v", err)
			}
			if updated.Spec.Size != volume.Spec.Size {
				t.Fatalf("volume size changed to %v while expansion was deferred, want %v", updated.Spec.Size, volume.Spec.Size)
			}
		})
	}
}

func TestV2VolumeExpansionIgnoresTerminalLiveUpgrade(t *testing.T) {
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories("default", kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volume := &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "volume", Namespace: "default"},
		Spec: longhorn.VolumeSpec{
			DataEngine: longhorn.DataEngineTypeV2,
		},
		Status: longhorn.VolumeStatus{
			State:         longhorn.VolumeStateAttached,
			CurrentNodeID: "node-a",
		},
	}
	if _, err := lhClient.LonghornV1beta2().Volumes("default").Create(context.TODO(), volume, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create volume: %v", err)
	}
	for _, state := range []longhorn.InstanceManagerUpgradeState{
		longhorn.InstanceManagerUpgradeStateCompleted,
		longhorn.InstanceManagerUpgradeStateFailed,
	} {
		upgrade := &longhorn.InstanceManagerUpgrade{
			ObjectMeta: metav1.ObjectMeta{Name: "upgrade-" + string(state), Namespace: "default"},
			Spec:       longhorn.InstanceManagerUpgradeSpec{NodeID: "node-a"},
			Status: longhorn.InstanceManagerUpgradeStatus{
				State:     state,
				StartedAt: "2026-09-16T00:00:00Z",
			},
		}
		if _, err := lhClient.LonghornV1beta2().InstanceManagerUpgrades("default").Create(context.TODO(), upgrade, metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to create %v instance manager upgrade: %v", state, err)
		}
	}

	ds := datastore.NewDataStoreForGlobal("default", lhClient, kubeClient, extensionsClient, informerFactories)
	stop := make(chan struct{})
	defer close(stop)
	informerFactories.LhInformerFactory.Start(stop)
	informerFactories.KubeInformerFactory.Start(stop)
	informerFactories.LhInformerFactory.WaitForCacheSync(stop)
	informerFactories.KubeInformerFactory.WaitForCacheSync(stop)

	m := NewVolumeManager("test-node", ds, util.NewAtomicCounter(), nil)
	blocked, err := m.isV2VolumeExpansionBlockedByLiveUpgrade(volume)
	if err != nil {
		t.Fatalf("unexpected error checking live upgrade: %v", err)
	}
	if blocked {
		t.Fatal("terminal instance manager upgrades must not block expansion")
	}
}

func TestCheckExpansionCancelable(t *testing.T) {
	const (
		originalSize  = int64(1024 * 1024 * 1024)
		requestedSize = 2 * originalSize
	)

	for name, tc := range map[string]struct {
		prepare     func(v *longhorn.Volume, e *longhorn.Engine)
		expectedErr string
	}{
		"failed v1 expansion": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {},
		},
		"expansion not started": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				v.Status.ExpansionRequired = false
			},
			expectedErr: "not started",
		},
		"standby volume": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				v.Status.IsStandby = true
			},
			expectedErr: "standby",
		},
		"v2 volume": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				v.Spec.DataEngine = longhorn.DataEngineTypeV2
			},
			expectedErr: "only supported for v1",
		},
		"encrypted volume": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				v.Spec.Encrypted = true
			},
			expectedErr: "encrypted",
		},
		// A replica was marked ERR, e.g. a rollback failed or only some
		// replicas failed.
		"degraded volume": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				v.Status.Robustness = longhorn.VolumeRobustnessDegraded
			},
			expectedErr: "robustness",
		},
		// A larger size was requested after the failure and may be
		// propagating to the engine.
		"larger size requested after the failure": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				v.Spec.Size = 3 * originalSize
			},
			expectedErr: "does not match the requested size",
		},
		"engine already reports the requested size": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				e.Status.CurrentSize = requestedSize
			},
			expectedErr: "engine current size",
		},
		"engine never ran": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				e.Status.CurrentSize = 0
			},
			expectedErr: "engine current size",
		},
		// The engine monitor clears the failure before it retries.
		"no recorded expansion failure": {
			prepare: func(v *longhorn.Volume, e *longhorn.Engine) {
				e.Status.LastExpansionError = ""
			},
			expectedErr: "only a failed expansion",
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := &longhorn.Volume{
				Spec: longhorn.VolumeSpec{Size: requestedSize, DataEngine: longhorn.DataEngineTypeV1},
				Status: longhorn.VolumeStatus{
					ExpansionRequired: true,
					Robustness:        longhorn.VolumeRobustnessHealthy,
				},
			}
			e := &longhorn.Engine{
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{VolumeSize: requestedSize}},
				Status: longhorn.EngineStatus{
					CurrentSize:        originalSize,
					LastExpansionError: "the expansion failed since all replica expansion failed",
				},
			}
			tc.prepare(v, e)

			err := checkExpansionCancelable(v, e)
			if tc.expectedErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.expectedErr) {
				t.Fatalf("expected error containing %q, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestCheckEngineExpansionFailed(t *testing.T) {
	const (
		originalSize  = int64(1024 * 1024 * 1024)
		requestedSize = 2 * originalSize
	)

	rwReplicas := func() map[string]*engineapi.Replica {
		return map[string]*engineapi.Replica{
			"tcp://10.0.0.1:10000": {Mode: longhorn.ReplicaModeRW},
			"tcp://10.0.0.2:10000": {Mode: longhorn.ReplicaModeRW},
		}
	}

	for name, tc := range map[string]struct {
		volumeInfo  engineapi.Volume
		replicas    map[string]*engineapi.Replica
		expectedErr string
	}{
		"all replicas failed and rolled back": {
			volumeInfo: engineapi.Volume{Size: originalSize, LastExpansionError: "the expansion failed since all replica expansion failed"},
			replicas:   rwReplicas(),
		},
		// The engine CR may still show the old failure while a retry runs.
		"retry in progress": {
			volumeInfo:  engineapi.Volume{Size: originalSize, IsExpanding: true},
			replicas:    rwReplicas(),
			expectedErr: "in progress",
		},
		// longhorn/longhorn#9466: the expansion completed but the engine CR
		// still shows the old size.
		"expansion succeeded": {
			volumeInfo:  engineapi.Volume{Size: requestedSize},
			replicas:    rwReplicas(),
			expectedErr: "only a failed expansion",
		},
		"some replicas failed but the engine expanded": {
			volumeInfo:  engineapi.Volume{Size: requestedSize, LastExpansionError: "the expansion succeeded, but some replica expansion failed"},
			replicas:    rwReplicas(),
			expectedErr: "already been expanded",
		},
		"a replica failed to roll back": {
			volumeInfo: engineapi.Volume{Size: originalSize, LastExpansionError: "the expansion failed since all replica expansion failed"},
			replicas: map[string]*engineapi.Replica{
				"tcp://10.0.0.1:10000": {Mode: longhorn.ReplicaModeRW},
				"tcp://10.0.0.2:10000": {Mode: longhorn.ReplicaModeERR},
			},
			expectedErr: "mode ERR",
		},
		"no replicas": {
			volumeInfo:  engineapi.Volume{Size: originalSize, LastExpansionError: "the expansion failed since all replica expansion failed"},
			replicas:    map[string]*engineapi.Replica{},
			expectedErr: "no replicas",
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := &longhorn.Volume{Spec: longhorn.VolumeSpec{Size: requestedSize}}
			err := checkEngineExpansionFailed(v, &tc.volumeInfo, tc.replicas)
			if tc.expectedErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.expectedErr) {
				t.Fatalf("expected error containing %q, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestCancelExpansionRejectsBeforeQueryingEngine(t *testing.T) {
	const (
		originalSize  = 1024 * 1024 * 1024
		requestedSize = 2 * originalSize
	)

	for _, tc := range []struct {
		name              string
		expansionRequired bool
		dataEngine        longhorn.DataEngineType
		expectedErr       string
	}{
		{
			name:        "expansion not started",
			dataEngine:  longhorn.DataEngineTypeV1,
			expectedErr: "not started",
		},
		{
			name:              "v2 volume",
			expansionRequired: true,
			dataEngine:        longhorn.DataEngineTypeV2,
			expectedErr:       "only supported for v1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories("default", kubeClient, lhClient, controller.NoResyncPeriodFunc())

			volume := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "volume", Namespace: "default"},
				Spec:       longhorn.VolumeSpec{Size: requestedSize, DataEngine: tc.dataEngine},
				Status: longhorn.VolumeStatus{
					ExpansionRequired: tc.expansionRequired,
					Robustness:        longhorn.VolumeRobustnessHealthy,
				},
			}
			engine := &longhorn.Engine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engine",
					Namespace: "default",
					Labels:    types.GetVolumeLabels(volume.Name),
				},
				Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{
					VolumeName: volume.Name,
					VolumeSize: requestedSize,
				}},
				Status: longhorn.EngineStatus{CurrentSize: originalSize},
			}
			if _, err := lhClient.LonghornV1beta2().Volumes("default").Create(context.TODO(), volume, metav1.CreateOptions{}); err != nil {
				t.Fatalf("failed to create volume: %v", err)
			}
			if _, err := lhClient.LonghornV1beta2().Engines("default").Create(context.TODO(), engine, metav1.CreateOptions{}); err != nil {
				t.Fatalf("failed to create engine: %v", err)
			}

			ds := datastore.NewDataStoreForGlobal("default", lhClient, kubeClient, extensionsClient, informerFactories)
			stop := make(chan struct{})
			defer close(stop)
			informerFactories.LhInformerFactory.Start(stop)
			informerFactories.KubeInformerFactory.Start(stop)
			informerFactories.LhInformerFactory.WaitForCacheSync(stop)
			informerFactories.KubeInformerFactory.WaitForCacheSync(stop)

			m := NewVolumeManager("test-node", ds, util.NewAtomicCounter(), nil)
			if _, err := m.CancelExpansion(volume.Name); err == nil || !strings.Contains(err.Error(), tc.expectedErr) {
				t.Fatalf("expected error containing %q, got %v", tc.expectedErr, err)
			}

			updatedVolume, err := lhClient.LonghornV1beta2().Volumes("default").Get(context.TODO(), volume.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get volume: %v", err)
			}
			if updatedVolume.Spec.Size != requestedSize {
				t.Fatalf("expected volume size %v, got %v", requestedSize, updatedVolume.Spec.Size)
			}
		})
	}
}

func TestRollBackVolumeSize(t *testing.T) {
	const (
		originalSize  = int64(1024 * 1024 * 1024)
		requestedSize = 2 * originalSize
		generation    = int64(3)
	)

	conflictErr := apierrors.NewConflict(longhorn.Resource("volumes"), "volume", errors.New("the object has been modified"))
	timeoutErr := apierrors.NewTimeoutError("request timed out", 1)

	for name, tc := range map[string]struct {
		storedGeneration int64
		updateErrs       []error
		expectedErr      string
		expectedSize     int64
		expectedUpdates  int
	}{
		"rolled back": {
			storedGeneration: generation,
			expectedSize:     originalSize,
			expectedUpdates:  1,
		},
		// A conflict means the update was not applied, e.g. the volume status
		// was updated after the volume was read.
		"retried after a status update": {
			storedGeneration: generation,
			updateErrs:       []error{conflictErr},
			expectedSize:     originalSize,
			expectedUpdates:  2,
		},
		// Another cancellation or a new request changed the volume spec.
		"volume spec changed": {
			storedGeneration: generation + 1,
			updateErrs:       []error{conflictErr},
			expectedErr:      "the volume spec changed",
			expectedSize:     requestedSize,
			expectedUpdates:  1,
		},
		// A timeout may still be applied, so it is not retried.
		"ambiguous error": {
			storedGeneration: generation,
			updateErrs:       []error{timeoutErr},
			expectedErr:      "request timed out",
			expectedSize:     requestedSize,
			expectedUpdates:  1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories("default", kubeClient, lhClient, controller.NoResyncPeriodFunc())
			ds := datastore.NewDataStoreForGlobal("default", lhClient, kubeClient, extensionsClient, informerFactories)

			stored := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "volume", Namespace: "default", Generation: tc.storedGeneration},
				Spec:       longhorn.VolumeSpec{Size: requestedSize},
			}
			if _, err := lhClient.LonghornV1beta2().Volumes("default").Create(context.TODO(), stored, metav1.CreateOptions{}); err != nil {
				t.Fatalf("failed to create volume: %v", err)
			}

			updates := 0
			lhClient.PrependReactor("update", "volumes", func(action clienttesting.Action) (bool, runtime.Object, error) {
				updates++
				if updates <= len(tc.updateErrs) {
					return true, nil, tc.updateErrs[updates-1]
				}
				return false, nil, nil
			})

			v := stored.DeepCopy()
			v.Generation = generation
			m := NewVolumeManager("test-node", ds, util.NewAtomicCounter(), nil)
			_, err := m.rollBackVolumeSize(v, originalSize)
			if tc.expectedErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.expectedErr) {
				t.Fatalf("expected error containing %q, got %v", tc.expectedErr, err)
			}
			if updates != tc.expectedUpdates {
				t.Fatalf("expected %v updates, got %v", tc.expectedUpdates, updates)
			}

			updatedVolume, err := lhClient.LonghornV1beta2().Volumes("default").Get(context.TODO(), "volume", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get volume: %v", err)
			}
			if updatedVolume.Spec.Size != tc.expectedSize {
				t.Fatalf("expected volume size %v, got %v", tc.expectedSize, updatedVolume.Spec.Size)
			}
		})
	}
}
