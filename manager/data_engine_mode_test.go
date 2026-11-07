package manager

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestCreateResolvesCapabilityBeforeBackingImageOrStorageWrites(t *testing.T) {
	for _, mode := range []types.DataEngineMode{types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
			lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
			factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
			store := datastore.NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, mode)
			manager := &VolumeManager{ds: store}
			lh.PrependReactor("create", "volumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
				volume := action.(k8stesting.CreateAction).GetObject().(*longhorn.Volume).DeepCopy()
				volume.Namespace, volume.UID, volume.ResourceVersion = "longhorn-system", "created-volume", "1"
				if err := factories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer().Add(volume); err != nil {
					t.Fatal(err)
				}
				return true, volume, nil
			})
			spec := &longhorn.VolumeSpec{DataEngine: engine, Size: 4 << 20}
			if mode == types.DataEngineModeV2Only && engine == longhorn.DataEngineTypeV1 {
				spec.BackingImage = "must-not-be-restored"
			}
			created, err := manager.Create("new-volume", spec, nil)
			if mode == types.DataEngineModeV2Only && engine == longhorn.DataEngineTypeV1 {
				if err == nil || !strings.Contains(err.Error(), "capability") || len(lh.Actions()) != 0 {
					t.Fatalf("V1 creation reached backing-image or storage processing: err=%v actions=%v", err, lh.Actions())
				}
				continue
			}
			want := engine
			if mode == types.DataEngineModeV2Only {
				want = longhorn.DataEngineTypeV2
			}
			if err != nil || created.Spec.DataEngine != want || spec.DataEngine != engine {
				t.Fatalf("mode=%s input=%q: returned=%v inputChanged=%t err=%v", mode, engine, created, spec.DataEngine != engine, err)
			}
		}
	}
}
