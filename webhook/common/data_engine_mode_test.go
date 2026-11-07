package common

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func capabilityTestStore(t *testing.T, mode types.DataEngineMode) *datastore.DataStore {
	return capabilityTestStoreSettings(t, mode, "true", "true")
}

func capabilityTestStoreSettings(t *testing.T, mode types.DataEngineMode, v1, v2 string) *datastore.DataStore {
	t.Helper()
	lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
	factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
	store := datastore.NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, mode)
	for name, value := range map[types.SettingName]string{types.SettingNameV1DataEngine: v1, types.SettingNameV2DataEngine: v2} {
		if err := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func capabilityObject(kind int, engine longhorn.DataEngineType) runtime.Object {
	metadata := metav1.ObjectMeta{Name: "retained", Namespace: "longhorn-system", UID: "retained-uid", ResourceVersion: "10", Finalizers: []string{"foreign", "longhorn.io"}}
	switch kind {
	case 0:
		return &longhorn.Volume{ObjectMeta: metadata, Spec: longhorn.VolumeSpec{DataEngine: engine}}
	case 1:
		return &longhorn.Engine{ObjectMeta: metadata, Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engine}}}
	case 2:
		return &longhorn.Replica{ObjectMeta: metadata, Spec: longhorn.ReplicaSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engine}}}
	default:
		return &longhorn.InstanceManager{ObjectMeta: metadata, Spec: longhorn.InstanceManagerSpec{DataEngine: engine}}
	}
}

func TestV2OnlyCommonSettingGateCannotWidenCapability(t *testing.T) {
	store := capabilityTestStore(t, types.DataEngineModeV2Only)
	for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unknown"} {
		if err := ValidateRequiredDataEngineEnabled(store, engine); err == nil {
			t.Fatalf("mutable V1=true admitted unsupported engine %q", engine)
		}
	}
	if err := ValidateRequiredDataEngineEnabled(store, longhorn.DataEngineTypeV2); err != nil {
		t.Fatal(err)
	}
}

func TestV2OnlyUpdateRejectsLegacyRelabeling(t *testing.T) {
	store := capabilityTestStore(t, types.DataEngineModeV2Only)
	for kind := range 4 {
		for _, oldEngine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll} {
			for _, newEngine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll} {
				cleanup, err := ValidateDataEngineCapabilityUpdate(store, capabilityObject(kind, oldEngine), capabilityObject(kind, newEngine))
				allowed := oldEngine == longhorn.DataEngineTypeV2 && newEngine == longhorn.DataEngineTypeV2
				if cleanup || (err == nil) != allowed {
					t.Fatalf("kind=%d old=%q new=%q: cleanup=%t err=%v", kind, oldEngine, newEngine, cleanup, err)
				}
			}
		}
	}
}

func TestV2OnlyStrictFinalizerCleanupPreservesTheWholeObject(t *testing.T) {
	store := capabilityTestStore(t, types.DataEngineModeV2Only)
	for kind := range 4 {
		for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
			oldObj := capabilityObject(kind, engine)
			oldMeta, _ := meta.Accessor(oldObj)
			deleted := metav1.Now()
			oldMeta.SetDeletionTimestamp(&deleted)
			newObj := oldObj.DeepCopyObject()
			newMeta, _ := meta.Accessor(newObj)
			newMeta.SetFinalizers([]string{"foreign"})
			oldBefore, newBefore := oldObj.DeepCopyObject(), newObj.DeepCopyObject()
			if cleanup, err := ValidateDataEngineCapabilityUpdate(store, oldObj, newObj); err != nil || !cleanup {
				t.Fatalf("kind=%d engine=%q blocked exact cleanup: cleanup=%t err=%v", kind, engine, cleanup, err)
			}
			if !reflect.DeepEqual(oldObj, oldBefore) || !reflect.DeepEqual(newObj, newBefore) {
				t.Fatal("cleanup validation mutated an admission object")
			}
			for _, mutate := range []func(metav1.Object){
				func(m metav1.Object) { m.SetName("replacement") },
				func(m metav1.Object) { m.SetNamespace("different") },
				func(m metav1.Object) { m.SetUID("different") },
				func(m metav1.Object) { m.SetResourceVersion("11") },
				func(m metav1.Object) { m.SetGeneration(2) },
				func(m metav1.Object) { m.SetLabels(map[string]string{"activation": "changed"}) },
				func(m metav1.Object) { m.SetAnnotations(map[string]string{"activation": "changed"}) },
				func(m metav1.Object) {
					m.SetOwnerReferences([]metav1.OwnerReference{{Name: "replacement", UID: "different"}})
				},
				func(m metav1.Object) { m.SetFinalizers([]string{"substituted"}) },
				func(m metav1.Object) {
					changed := metav1.NewTime(deleted.Add(time.Second))
					m.SetDeletionTimestamp(&changed)
				},
			} {
				changed := newObj.DeepCopyObject()
				metadata, _ := meta.Accessor(changed)
				mutate(metadata)
				if cleanup, err := ValidateDataEngineCapabilityUpdate(store, oldObj, changed); err == nil || cleanup {
					t.Fatalf("kind=%d engine=%q cleanup hid another metadata change", kind, engine)
				}
			}
			for _, mutate := range []func(runtime.Object){
				func(object runtime.Object) {
					switch object := object.(type) {
					case *longhorn.Volume:
						object.Spec.NodeID = "activate"
					case *longhorn.Engine:
						object.Spec.DesireState = longhorn.InstanceStateRunning
					case *longhorn.Replica:
						object.Spec.DesireState = longhorn.InstanceStateRunning
					case *longhorn.InstanceManager:
						object.Spec.NodeID = "activate"
					}
				},
				func(object runtime.Object) {
					switch object := object.(type) {
					case *longhorn.Volume:
						object.Status.CurrentNodeID = "activate"
					case *longhorn.Engine:
						object.Status.CurrentState = longhorn.InstanceStateRunning
					case *longhorn.Replica:
						object.Status.CurrentState = longhorn.InstanceStateRunning
					case *longhorn.InstanceManager:
						object.Status.CurrentState = longhorn.InstanceManagerStateRunning
					}
				},
			} {
				changed := newObj.DeepCopyObject()
				mutate(changed)
				if cleanup, err := ValidateDataEngineCapabilityUpdate(store, oldObj, changed); err == nil || cleanup {
					t.Fatalf("kind=%d engine=%q cleanup hid a spec or status change", kind, engine)
				}
			}
			oldMeta.SetFinalizers([]string{"foreign", "longhorn.io", "longhorn.io"})
			if cleanup, err := ValidateDataEngineCapabilityUpdate(store, oldObj, newObj); err == nil || cleanup {
				t.Fatal("duplicate Longhorn finalizers cleared cleanup")
			}
		}
	}
}

func TestCapabilityGatePrecedesSettingsAndPreservesEnablement(t *testing.T) {
	store := capabilityTestStoreSettings(t, types.DataEngineModeV2Only, "invalid", "true")
	err := ValidateRequiredDataEngineEnabled(store, longhorn.DataEngineTypeV1)
	status, ok := err.(interface{ AsResult() *metav1.Status })
	if !ok || status.AsResult().Code != http.StatusForbidden {
		t.Fatalf("V1 capability denial read the invalid mutable Setting first: %v", err)
	}
	for _, value := range []string{"false", "invalid"} {
		store = capabilityTestStoreSettings(t, types.DataEngineModeV2Only, "false", value)
		err = ValidateRequiredDataEngineEnabled(store, longhorn.DataEngineTypeV2)
		if err == nil {
			t.Fatalf("capability enabled V2 despite mutable Setting %q", value)
		}
	}
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable} {
		store = capabilityTestStore(t, mode)
		for kind := range 4 {
			for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll} {
				if cleanup, err := ValidateDataEngineCapabilityUpdate(store, capabilityObject(kind, engine), capabilityObject(kind, engine)); err != nil || cleanup {
					t.Fatalf("default mode update behavior changed: %v", err)
				}
			}
		}
	}
}

func TestFinalizerCleanupRejectsInvalidObjectsWithoutPanic(t *testing.T) {
	for _, invalid := range []runtime.Object{nil, (*longhorn.Volume)(nil), &metav1.Status{}} {
		if cleanup, err := IsLonghornFinalizerOnlyRemoval(invalid, capabilityObject(0, longhorn.DataEngineTypeV1)); err == nil || cleanup {
			t.Fatal("invalid cleanup object was accepted")
		}
	}
}

func TestStrictCleanupAllowsOnlyAPIBookkeepingAndSoleFinalizerRemoval(t *testing.T) {
	store := capabilityTestStore(t, types.DataEngineModeV2Only)
	for kind := range 4 {
		oldObj := capabilityObject(kind, longhorn.DataEngineTypeV1)
		oldMeta, _ := meta.Accessor(oldObj)
		deleted := metav1.Now()
		oldMeta.SetDeletionTimestamp(&deleted)
		oldMeta.SetFinalizers([]string{"longhorn.io"})
		newObj := oldObj.DeepCopyObject()
		newMeta, _ := meta.Accessor(newObj)
		newMeta.SetFinalizers(nil)
		newMeta.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "garbage-collector", Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: longhorn.SchemeGroupVersion.String()}})
		if cleanup, err := ValidateDataEngineCapabilityUpdate(store, oldObj, newObj); err != nil || !cleanup {
			t.Fatalf("kind=%d rejected sole-finalizer cleanup or API bookkeeping: cleanup=%t err=%v", kind, cleanup, err)
		}
	}
}
