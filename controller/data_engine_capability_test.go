package controller

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/record"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newRuntimeCapabilityTestStore(mode types.DataEngineMode) *datastore.DataStore {
	lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
	factories := util.NewInformerFactories(TestNamespace, kube, lh, 0)
	return datastore.NewDataStoreForNodeLocal(TestNamespace, lh, kube, nil, factories, mode)
}

func TestV2OnlyRuntimeCreationCeilingPreservesCleanup(t *testing.T) {
	for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll, "unknown"} {
		stub := &stubInstanceManagerHandler{getErr: fmt.Errorf("cannot find process")}
		handler := &InstanceHandler{ds: newRuntimeCapabilityTestStore(types.DataEngineModeV2Only), instanceManagerHandler: stub, eventRecorder: record.NewFakeRecorder(10)}
		object := &longhorn.Engine{ObjectMeta: metav1.ObjectMeta{Name: "guarded-instance", Namespace: TestNamespace}, Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engine}}}
		err := handler.createInstance(object.Name, engine, object)
		allowed := engine == longhorn.DataEngineTypeV2
		if (err == nil) != allowed || stub.createCalled != allowed {
			t.Fatalf("runtime ceiling: engine=%q create=%t err=%v", engine, stub.createCalled, err)
		}
		if err := handler.deleteInstance(object.Name, object); err != nil || !stub.deleteCalled {
			t.Fatalf("runtime ceiling blocked cleanup: engine=%q err=%v", engine, err)
		}
	}
	stub := &stubInstanceManagerHandler{getInstance: &longhorn.InstanceProcess{Status: longhorn.InstanceProcessStatus{State: longhorn.InstanceStateStopped}}}
	handler := &InstanceHandler{ds: newRuntimeCapabilityTestStore(types.DataEngineModeV2Only), instanceManagerHandler: stub, eventRecorder: record.NewFakeRecorder(10)}
	if err := handler.createInstance("stale-v1", longhorn.DataEngineTypeV1, &longhorn.Engine{}); err != errStaleInstanceReaped || !stub.deleteCalled || stub.createCalled {
		t.Fatalf("stale V1 cleanup was blocked or reactivated: err=%v delete=%t create=%t", err, stub.deleteCalled, stub.createCalled)
	}
}

func TestV2OnlyNodeSelectionOverridesStaleV1Setting(t *testing.T) {
	for _, mode := range []types.DataEngineMode{types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
		factories := util.NewInformerFactories(TestNamespace, kube, lh, 0)
		store := datastore.NewDataStoreForNodeLocal(TestNamespace, lh, kube, nil, factories, mode)
		for name, value := range map[types.SettingName]string{types.SettingNameV1DataEngine: "true", types.SettingNameV2DataEngine: "false"} {
			if err := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: TestNamespace}, Value: value}); err != nil {
				t.Fatal(err)
			}
		}
		controller := &NodeController{ds: store, baseController: &baseController{logger: logrus.NewEntry(logrus.New())}}
		node := &longhorn.Node{ObjectMeta: metav1.ObjectMeta{Name: "storage-node"}, Spec: longhorn.NodeSpec{Disks: map[string]longhorn.DiskSpec{"disk": {}}}}
		engines := controller.getImTypeDataEngines(node)
		for kind, selected := range engines {
			want := 1
			if mode == types.DataEngineModeV2Only {
				want = 0
			}
			if len(selected) != want {
				t.Fatalf("mode=%s IM-kind=%s selected unsupported engines: %v", mode, kind, selected)
			}
		}
	}
}

func TestV2OnlyInstanceManagerCreationBoundaries(t *testing.T) {
	for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unknown"} {
		store := newRuntimeCapabilityTestStore(types.DataEngineModeV2Only)
		nodeController := &NodeController{ds: store}
		_, err := nodeController.createInstanceManager(&longhorn.Node{ObjectMeta: metav1.ObjectMeta{Name: "storage-node"}}, "old-im", "image", longhorn.InstanceManagerTypeAllInOne, engine)
		if err == nil || !strings.Contains(err.Error(), "capability") {
			t.Fatalf("unsupported IM creation did not fail at the capability boundary: engine=%q err=%v", engine, err)
		}
		imController := &InstanceManagerController{ds: store, baseController: &baseController{logger: logrus.NewEntry(logrus.New())}}
		err = imController.createInstanceManagerPod(&longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: "old-im"}, Spec: longhorn.InstanceManagerSpec{DataEngine: engine}})
		if err == nil || !strings.Contains(err.Error(), "capability") {
			t.Fatalf("unsupported IM Pod recreation did not fail at the capability boundary: engine=%q err=%v", engine, err)
		}
	}
}

func TestV2OnlyEngineUpgradeCannotLaunchLegacyReplacement(t *testing.T) {
	for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unknown"} {
		controller := &EngineController{ds: newRuntimeCapabilityTestStore(types.DataEngineModeV2Only)}
		object := &longhorn.Engine{Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engine}}}
		err := controller.UpgradeEngineInstance(object, logrus.NewEntry(logrus.New()))
		if err == nil || !strings.Contains(err.Error(), "capability") {
			t.Fatalf("legacy engine replacement reached client setup: engine=%q err=%v", engine, err)
		}
	}
}
