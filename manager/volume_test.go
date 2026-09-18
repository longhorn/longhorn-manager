package manager

import (
	"context"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
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
