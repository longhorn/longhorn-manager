package datastore

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestV2OnlyBootstrapRequiresEmptyStorageWhenSettingsAreMissing(t *testing.T) {
	for _, mask := range []int{0, 1, 2, 3} {
		for _, engine := range []longhorn.DataEngineType{"none", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
			objects := []runtime.Object{}
			for index, name := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
				if mask&(1<<index) == 0 {
					continue
				}
				value := "false"
				if index == 1 {
					value = "true"
				}
				objects = append(objects, &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system", UID: "setting-uid", ResourceVersion: "1"}, Value: value})
			}
			if engine != "none" {
				objects = append(objects, &longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: "retained", Namespace: "longhorn-system"}, Spec: longhorn.VolumeSpec{DataEngine: engine}})
			}
			lh, kube := lhfake.NewClientset(objects...), kubefake.NewClientset()
			err := ValidateDataEngineModeBootstrapState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube)
			wantAllowed := engine == "none" || (mask == 3 && engine == longhorn.DataEngineTypeV2)
			if (err == nil) != wantAllowed {
				t.Fatalf("settings-mask=%d engine=%q: allowed=%t want=%t err=%v", mask, engine, err == nil, wantAllowed, err)
			}
			for _, action := range lh.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatal("bootstrap preflight mutated state")
				}
			}
		}
	}
}

func bootstrapClients(objects ...runtime.Object) (*lhfake.Clientset, *kubefake.Clientset) {
	lh, kube := lhfake.NewClientset(objects...), kubefake.NewClientset()
	lh.PrependReactor("create", "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
		setting := action.(k8stesting.CreateAction).GetObject().(*longhorn.Setting)
		resource := longhorn.SchemeGroupVersion.WithResource("settings")
		if _, err := lh.Tracker().Get(resource, setting.Namespace, setting.Name); err == nil {
			return true, nil, apierrors.NewAlreadyExists(resource.GroupResource(), setting.Name)
		} else if !apierrors.IsNotFound(err) {
			return true, nil, err
		}
		setting.UID, setting.ResourceVersion = k8stypes.UID("new-"+setting.Name), "1"
		if err := lh.Tracker().Add(setting.DeepCopy()); err != nil {
			return true, nil, err
		}
		return true, setting.DeepCopy(), nil
	})
	return lh, kube
}

func TestV2OnlyBootstrapCreatesOnlyMissingSettingsAndResumesInterruption(t *testing.T) {
	lh, kube := bootstrapClients()
	interrupt := true
	lh.PrependReactor("create", "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if interrupt && action.(k8stesting.CreateAction).GetObject().(*longhorn.Setting).Name == string(types.SettingNameV2DataEngine) {
			return true, nil, errors.New("interrupted before second member")
		}
		return false, nil, nil
	})
	if err := InitializeDataEngineModeSettings(t.Context(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
		t.Fatal("incomplete initialization reported success")
	}
	first, err := lh.LonghornV1beta2().Settings("longhorn-system").Get(t.Context(), string(types.SettingNameV1DataEngine), metav1.GetOptions{})
	if err != nil || first.Value != "false" || first.UID == "" {
		t.Fatalf("compatible first member was not retained: %v", err)
	}
	interrupt = false
	if err := InitializeDataEngineModeSettings(t.Context(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDataEngineModeState(t.Context(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err != nil {
		t.Fatal(err)
	}
	after, err := lh.LonghornV1beta2().Settings("longhorn-system").Get(t.Context(), first.Name, metav1.GetOptions{})
	if err != nil || after.UID != first.UID || after.ResourceVersion != first.ResourceVersion {
		t.Fatal("resuming initialization rewrote the first member")
	}
	for _, action := range lh.Actions() {
		if action.GetVerb() == "update" || action.GetVerb() == "patch" || action.GetVerb() == "delete" {
			t.Fatal("initialization mutated an existing Setting")
		}
	}
}

func TestV2OnlyBootstrapRechecksAlreadyExistsBeforeNextCreate(t *testing.T) {
	for _, compatible := range []bool{false, true} {
		lh, kube := bootstrapClients()
		creates := 0
		lh.PrependReactor("create", "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
			creates++
			setting := action.(k8stesting.CreateAction).GetObject().(*longhorn.Setting).DeepCopy()
			if creates != 1 {
				return false, nil, nil
			}
			setting.UID, setting.ResourceVersion = "concurrent-creator", "1"
			if !compatible {
				setting.Value = "true"
			}
			if err := lh.Tracker().Add(setting); err != nil {
				t.Fatal(err)
			}
			return true, nil, apierrors.NewAlreadyExists(longhorn.SchemeGroupVersion.WithResource("settings").GroupResource(), setting.Name)
		})
		err := InitializeDataEngineModeSettings(t.Context(), types.DataEngineModeV2Only, "longhorn-system", lh, kube)
		if compatible && err != nil {
			t.Fatal(err)
		}
		if !compatible && (err == nil || creates != 1) {
			t.Fatalf("conflicting AlreadyExists permitted another create: creates=%d err=%v", creates, err)
		}
	}
}

func TestV2OnlyBootstrapDoesNotWriteAcrossUnresolvedInventory(t *testing.T) {
	for _, missingSettings := range []bool{false, true} {
		lh, kube := modeStateClients(&longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: "retained", Namespace: "longhorn-system"}})
		if missingSettings {
			if err := lh.Tracker().Delete(longhorn.SchemeGroupVersion.WithResource("settings"), "longhorn-system", string(types.SettingNameV1DataEngine)); err != nil {
				t.Fatal(err)
			}
		}
		if err := InitializeDataEngineModeSettings(t.Context(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
			t.Fatal("legacy retained storage cleared bootstrap")
		}
		for _, action := range lh.Actions() {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatal("unresolved storage permitted a Setting mutation")
			}
		}
	}
}

func TestV2OnlyBootstrapRejectsSettingReplacementDuringInventory(t *testing.T) {
	for _, changeUID := range []bool{false, true} {
		setting := &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(types.SettingNameV1DataEngine), Namespace: "longhorn-system", UID: "original", ResourceVersion: "1"}, Value: "false"}
		lh, kube := bootstrapClients(setting)
		kube.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			replacement := setting.DeepCopy()
			if changeUID {
				replacement.UID = "replacement"
			} else {
				replacement.ResourceVersion = "2"
			}
			resource := longhorn.SchemeGroupVersion.WithResource("settings")
			if err := lh.Tracker().Delete(resource, setting.Namespace, setting.Name); err != nil {
				t.Fatal(err)
			}
			if err := lh.Tracker().Add(replacement); err != nil {
				t.Fatal(err)
			}
			return false, nil, nil
		})
		if err := InitializeDataEngineModeSettings(t.Context(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
			t.Fatal("changed Setting identity cleared initialization")
		}
		for _, action := range lh.Actions() {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatal("changed Setting identity permitted mutation")
			}
		}
	}
}

func TestV2OnlyBootstrapBindsCreatedSettingResponseToReadback(t *testing.T) {
	lh, kube := bootstrapClients()
	creates := 0
	lh.PrependReactor("create", "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
		creates++
		setting := action.(k8stesting.CreateAction).GetObject().(*longhorn.Setting).DeepCopy()
		setting.UID, setting.ResourceVersion = "persisted-uid", "1"
		if err := lh.Tracker().Add(setting); err != nil {
			t.Fatal(err)
		}
		response := setting.DeepCopy()
		response.UID = "different-response-uid"
		return true, response, nil
	})
	if err := InitializeDataEngineModeSettings(t.Context(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil || creates != 1 {
		t.Fatalf("unbound create response permitted another mutation: creates=%d err=%v", creates, err)
	}
}

func TestDefaultBootstrapPreservesNoAPIChecks(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable} {
		if err := InitializeDataEngineModeSettings(nil, mode, "", nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := ValidateDataEngineModeBootstrapState(nil, mode, "", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := InitializeDataEngineModeSettings(nil, "unknown", "", nil, nil); err == nil {
		t.Fatal("unknown installation mode cleared initialization")
	}
}
