package datastore

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestConstructedDataStoreCapabilityOverridesStaleSettings(t *testing.T) {
	for _, nodeLocal := range []bool{false, true} {
		for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
			lh, kube := modeStateClients()
			factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
			var store *DataStore
			if nodeLocal {
				store = NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, mode)
			} else {
				store = NewDataStoreForGlobal("longhorn-system", lh, kube, nil, factories, mode)
			}
			// The cache still says both engines are enabled. The process's
			// immutable ceiling must prevail even before the informer catches up.
			indexer := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
			for _, name := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
				if err := indexer.Add(&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: "true"}); err != nil {
					t.Fatal(err)
				}
			}
			for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll} {
				want := mode != types.DataEngineModeV2Only || engine == longhorn.DataEngineTypeV2
				got, err := store.IsDataEngineEnabled(engine)
				if err != nil || got != want {
					t.Fatalf("node-local=%t mode=%q engine=%q ignored capability: got=%t err=%v", nodeLocal, mode, engine, got, err)
				}
			}
			engines := store.GetDataEngines()
			_, hasV1 := engines[longhorn.DataEngineTypeV1]
			_, hasV2 := engines[longhorn.DataEngineTypeV2]
			if hasV1 != (mode != types.DataEngineModeV2Only) || !hasV2 {
				t.Fatal("mutable engine discovery widened the process capability")
			}
			setting := &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(types.SettingNameV2DataEngine), Namespace: "longhorn-system"}, Value: "false"}
			if err := indexer.Update(setting); err != nil {
				t.Fatal(err)
			}
			if enabled, err := store.IsDataEngineEnabled(longhorn.DataEngineTypeV2); err != nil || enabled {
				t.Fatal("capability was mistaken for mutable engine enablement")
			}
			if _, enabled := store.GetDataEngines()[longhorn.DataEngineTypeV2]; enabled {
				t.Fatal("engine discovery enabled a disabled engine")
			}
			setting.Value = "invalid"
			if err := indexer.Update(setting); err != nil {
				t.Fatal(err)
			}
			if enabled, err := store.IsDataEngineEnabled(longhorn.DataEngineTypeV2); err == nil || enabled != (mode != types.DataEngineModeV2Only) {
				t.Fatal("setting read failure lost its error or changed the default error contract")
			}
		}
	}
}
