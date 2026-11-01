package volume

import (
	"encoding/json"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestCreateSelectsEngineBeforeEngineSpecificDefaults(t *testing.T) {
	for _, mode := range []types.DataEngineMode{types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
		factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
		store := datastore.NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, mode)
		for name, value := range map[types.SettingName]string{
			types.SettingNameDefaultReplicaCount:         `{"v1":"1","v2":"2"}`,
			types.SettingNameDefaultEngineImage:          "engine-v1",
			types.SettingNameDefaultInstanceManagerImage: "im-v2",
		} {
			if err := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: value}); err != nil {
				t.Fatal(err)
			}
		}
		input := &longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: "new-volume", Namespace: "longhorn-system", Labels: map[string]string{"fixture": "preserve"}, Finalizers: []string{"foreign"}}, Spec: longhorn.VolumeSpec{Size: 4 << 20, Encrypted: true}}
		wantEngine, wantImage, wantReplicas := longhorn.DataEngineTypeV2, "im-v2", 2
		if mode == types.DataEngineModeV1Capable {
			input.Spec.DataEngine = longhorn.DataEngineTypeV1
			wantEngine, wantImage, wantReplicas = longhorn.DataEngineTypeV1, "engine-v1", 1
		}
		before, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		patches, err := NewMutator(store).Create(nil, input.DeepCopy())
		if err != nil {
			t.Fatalf("mode=%s creation failed: %v", mode, err)
		}
		patch, err := jsonpatch.DecodePatch([]byte("[" + strings.Join(patches, ",") + "]"))
		if err != nil {
			t.Fatal(err)
		}
		after, err := patch.Apply(before)
		if err != nil {
			t.Fatal(err)
		}
		var applied longhorn.Volume
		if err := json.Unmarshal(after, &applied); err != nil {
			t.Fatal(err)
		}
		if applied.Spec.DataEngine != wantEngine || applied.Spec.NumberOfReplicas != wantReplicas || applied.Spec.Image != wantImage {
			t.Fatalf("mode=%s selected defaults before resolving the engine: engine=%s replicas=%d image=%s", mode, applied.Spec.DataEngine, applied.Spec.NumberOfReplicas, applied.Spec.Image)
		}
		if mode == types.DataEngineModeV2Only && (applied.Labels[types.LonghornLabelV2EncryptedVolumeWithLuksHeader] != longhorn.TrueValue || applied.Spec.UnmapMarkSnapChainRemoved != longhorn.UnmapMarkSnapChainRemovedDisabled) {
			t.Fatalf("V2 encryption or shared defaults were lost: labels=%v unmap=%s", applied.Labels, applied.Spec.UnmapMarkSnapChainRemoved)
		}
	}
}
