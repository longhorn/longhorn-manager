package webhook_test

import (
	"net/http"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	enginewebhook "github.com/longhorn/longhorn-manager/webhook/resources/engine"
	imwebhook "github.com/longhorn/longhorn-manager/webhook/resources/instancemanager"
	replicawebhook "github.com/longhorn/longhorn-manager/webhook/resources/replica"
	volumewebhook "github.com/longhorn/longhorn-manager/webhook/resources/volume"
)

type capabilityWebhook struct {
	name      string
	mutator   func(*datastore.DataStore) admission.Mutator
	validator func(*datastore.DataStore) admission.Validator
	object    func(longhorn.DataEngineType) runtime.Object
}

func capabilityWebhooks() []capabilityWebhook {
	metadata := func() metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: "retained", Namespace: "longhorn-system", UID: "source-uid", ResourceVersion: "10", Finalizers: []string{"foreign", "longhorn.io"}}
	}
	return []capabilityWebhook{
		{"volume", volumewebhook.NewMutator, func(ds *datastore.DataStore) admission.Validator { return volumewebhook.NewValidator(ds, "node") }, func(e longhorn.DataEngineType) runtime.Object {
			return &longhorn.Volume{ObjectMeta: metadata(), Spec: longhorn.VolumeSpec{DataEngine: e}}
		}},
		{"engine", enginewebhook.NewMutator, enginewebhook.NewValidator, func(e longhorn.DataEngineType) runtime.Object {
			return &longhorn.Engine{ObjectMeta: metadata(), Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: e}}}
		}},
		{"replica", replicawebhook.NewMutator, replicawebhook.NewValidator, func(e longhorn.DataEngineType) runtime.Object {
			return &longhorn.Replica{ObjectMeta: metadata(), Spec: longhorn.ReplicaSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: e}}}
		}},
		{"instance-manager", imwebhook.NewMutator, imwebhook.NewValidator, func(e longhorn.DataEngineType) runtime.Object {
			return &longhorn.InstanceManager{ObjectMeta: metadata(), Spec: longhorn.InstanceManagerSpec{DataEngine: e}}
		}},
	}
}

func capabilityWebhookStore(t *testing.T) *datastore.DataStore {
	t.Helper()
	lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
	factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
	store := datastore.NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, types.DataEngineModeV2Only)
	for _, name := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
		if err := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: "true"}); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func requireCapabilityDenial(t *testing.T, err error) {
	t.Helper()
	status, ok := err.(interface{ AsResult() *metav1.Status })
	if !ok || status.AsResult().Code != http.StatusForbidden {
		t.Fatalf("request reached native processing instead of the capability boundary: %v", err)
	}
}

func TestV2OnlyNativeWebhookCreationBoundaries(t *testing.T) {
	for _, webhook := range capabilityWebhooks() {
		t.Run(webhook.name, func(t *testing.T) {
			store := capabilityWebhookStore(t)
			for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unknown"} {
				if webhook.name != "volume" || engine != "" {
					_, err := webhook.mutator(store).Create(nil, webhook.object(engine))
					requireCapabilityDenial(t, err)
				}
				requireCapabilityDenial(t, webhook.validator(store).Create(nil, webhook.object(engine)))
			}
		})
	}
}

func TestV2OnlyNativeWebhooksRejectLegacyRelabeling(t *testing.T) {
	for _, webhook := range capabilityWebhooks() {
		t.Run(webhook.name, func(t *testing.T) {
			store := capabilityWebhookStore(t)
			for _, oldEngine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll} {
				oldObj, newObj := webhook.object(oldEngine), webhook.object(longhorn.DataEngineTypeV2)
				_, err := webhook.mutator(store).Update(nil, oldObj, newObj)
				requireCapabilityDenial(t, err)
				requireCapabilityDenial(t, webhook.validator(store).Update(nil, oldObj, newObj))
			}
		})
	}
}

func TestV2OnlyNativeWebhookCleanupHasZeroPatches(t *testing.T) {
	for _, webhook := range capabilityWebhooks() {
		t.Run(webhook.name, func(t *testing.T) {
			store := capabilityWebhookStore(t)
			for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
				oldObj := webhook.object(engine)
				oldMeta, _ := meta.Accessor(oldObj)
				deleted := metav1.Now()
				oldMeta.SetDeletionTimestamp(&deleted)
				newObj := oldObj.DeepCopyObject()
				newMeta, _ := meta.Accessor(newObj)
				newMeta.SetFinalizers([]string{"foreign"})
				patches, err := webhook.mutator(store).Update(nil, oldObj, newObj)
				if err != nil || len(patches) != 0 {
					t.Fatalf("cleanup changed the object or required native reads: engine=%q patches=%v err=%v", engine, patches, err)
				}
				if err := webhook.validator(store).Update(nil, oldObj, newObj); err != nil {
					t.Fatalf("cleanup blocked: engine=%q err=%v", engine, err)
				}
			}
		})
	}
}
