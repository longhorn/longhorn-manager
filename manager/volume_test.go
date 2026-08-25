package manager

import (
	"testing"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"

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

	ds := datastore.NewDataStore("default", lhClient, kubeClient, extensionsClient, informerFactories)

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
