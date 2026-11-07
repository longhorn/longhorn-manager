package datastore

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestV2OnlyStartupBindsRawConfigMapAcrossBootstrapInventory(t *testing.T) {
	for _, replace := range []bool{false, true} {
		lh, kube := bootstrapClients()
		factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
		store := NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, types.DataEngineModeV2Only)
		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: types.DefaultDefaultSettingConfigMapName, Namespace: "longhorn-system", UID: "config-uid", ResourceVersion: "1"},
			Data:       map[string]string{types.DefaultSettingYAMLFileName: "v1-data-engine: 'false'\nv2-data-engine: 'true'"},
		}
		if err := kube.Tracker().Add(configMap); err != nil {
			t.Fatal(err)
		}
		if replace {
			kube.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				changed := configMap.DeepCopy()
				changed.ResourceVersion = "2"
				if err := kube.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace); err != nil {
					t.Fatal(err)
				}
				return false, nil, nil
			})
		}
		err := store.ValidateDataEngineModeStartup(t.Context())
		if (err == nil) == replace {
			t.Fatalf("startup intent continuity: replaced=%t err=%v", replace, err)
		}
		for _, action := range lh.Actions() {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatal("startup preflight mutated engine state")
			}
		}
	}
}

func TestV2OnlyRawDefaultsRejectAmbiguityWithoutLoggingPayload(t *testing.T) {
	previous := logrus.StandardLogger().Out
	var output bytes.Buffer
	logrus.SetOutput(&output)
	defer logrus.SetOutput(previous)
	store := &DataStore{namespace: "longhorn-system", dataEngineMode: types.DataEngineModeV2Only}
	for _, raw := range []string{"v1-data-engine: [sentinel-secret", "v1-data-engine: 'false'\nv1-data-engine: 'true'", "v1-data-engine: 'false'\n---\nv1-data-engine: 'true'\n"} {
		configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: types.DefaultDefaultSettingConfigMapName, Namespace: "longhorn-system", UID: "uid", ResourceVersion: "1"}, Data: map[string]string{types.DefaultSettingYAMLFileName: raw}}
		err := store.validateDataEngineModeCustomizedSettings(configMap)
		if err == nil || strings.Contains(err.Error(), "sentinel-secret") || strings.Contains(output.String(), "sentinel-secret") {
			t.Fatal("ambiguous raw engine defaults cleared validation or leaked the payload")
		}
	}
}

func TestDefaultStartupPreservesNoAdditionalAPIChecks(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable} {
		store := &DataStore{dataEngineMode: mode}
		if err := store.ValidateDataEngineModeStartup(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestV2OnlySettingValidationRejectsUnchangedContradictions(t *testing.T) {
	lh, kube := modeStateClients()
	factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
	store := NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, types.DataEngineModeV2Only)
	for name, value := range map[types.SettingName]string{types.SettingNameV1DataEngine: "true", types.SettingNameV2DataEngine: "false"} {
		setting := &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: value}
		if err := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(setting); err != nil {
			t.Fatal(err)
		}
		if err := store.ValidateSetting(string(name), value); err == nil || types.ErrorIsNotSupport(err) {
			t.Fatalf("unchanged contradictory Setting bypassed the capability ceiling: name=%s err=%v", name, err)
		}
	}
}

func TestV2OnlyGenericSettingSyncCannotCreateOrUpsertEngineTuple(t *testing.T) {
	lh, kube := modeStateClients()
	factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
	store := NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, types.DataEngineModeV2Only)
	indexer := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	for _, name := range types.SettingNameList {
		if name == types.SettingNameV1DataEngine || name == types.SettingNameV2DataEngine {
			continue
		}
		if err := indexer.Add(&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: "present"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.createNonExistingSettingCRsWithDefaultSetting("config-version"); err != nil {
		t.Fatal(err)
	}
	customized := map[string]string{string(types.SettingNameV1DataEngine): "false", string(types.SettingNameV2DataEngine): "true"}
	if err := store.syncSettingCRsWithCustomizedDefaultSettings(customized, "config-version"); err != nil {
		t.Fatal(err)
	}
	if result := store.filterCustomizedDefaultSettings(customized, "config-version"); len(result) != 0 {
		t.Fatal("generic customized Settings changed engine definitions")
	}
	if len(lh.Actions()) != 0 {
		t.Fatal("generic Settings sync touched the create-only engine tuple")
	}
}

func TestDefaultSettingValidationPreservesMutableEngineSettings(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable} {
		lh, kube := modeStateClients()
		factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
		store := NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, mode)
		for name, value := range map[types.SettingName]string{types.SettingNameV1DataEngine: "true", types.SettingNameV2DataEngine: "false"} {
			setting := &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: value}
			if err := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(setting); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateSetting(string(name), value); err != nil {
				t.Fatalf("default mode lost mutable engine Settings: name=%s err=%v", name, err)
			}
		}
	}
}

func TestV2OnlySettingValidationAllowsCompatibleBootstrapSingleton(t *testing.T) {
	for name, value := range map[types.SettingName]string{types.SettingNameV1DataEngine: "false", types.SettingNameV2DataEngine: "true"} {
		lh, kube := bootstrapClients()
		factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
		store := NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, types.DataEngineModeV2Only)
		if err := store.ValidateSetting(string(name), value); err != nil {
			t.Fatalf("compatible singleton blocked on absent sibling: name=%s err=%v", name, err)
		}
	}
}

func TestV2OnlyCustomizedEngineIntentFailsBeforeAnyWrite(t *testing.T) {
	for _, raw := range []string{"v1-data-engine: 'true'", "v2-data-engine: 'false'", "v1-data-engine: invalid-bool"} {
		lh, kube := modeStateClients()
		factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
		store := NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, types.DataEngineModeV2Only)
		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: types.DefaultDefaultSettingConfigMapName, Namespace: "longhorn-system", UID: "config-uid", ResourceVersion: "1"},
			Data:       map[string]string{types.DefaultSettingYAMLFileName: raw},
		}
		if err := factories.KubeNamespaceFilteredInformerFactory.Core().V1().ConfigMaps().Informer().GetIndexer().Add(configMap); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateCustomizedSettings(map[types.SettingName]string{types.SettingNameDefaultEngineImage: "must-not-write"}); err == nil {
			t.Fatalf("invalid raw engine intent cleared customized settings: %q", raw)
		}
		for _, action := range lh.Actions() {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatalf("invalid raw engine intent permitted mutation before validation: %q %s", raw, action.GetVerb())
			}
		}
	}
}
