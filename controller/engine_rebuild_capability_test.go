package controller

import (
	"testing"

	"github.com/sirupsen/logrus"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	imrpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type rebuildCapabilityRecorder struct {
	engineapi.EngineClientProxy
	adds    int
	restore bool
}

func (client *rebuildCapabilityRecorder) ReplicaAdd(_ engineapi.DataEngineObject, _, _ string, restore, _ bool, _ *etypes.FileLocalSync, _, _ int64, _ *imrpc.LinkedCloneSource) error {
	client.adds++
	client.restore = restore
	return nil
}

func TestV2OnlyRebuildCannotReactivateRetainedLegacyEngine(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		for _, engineType := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unknown", longhorn.DataEngineTypeV2} {
			for _, restore := range []bool{false, true} {
				engine := &longhorn.Engine{Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engineType}}}
				client := &rebuildCapabilityRecorder{}
				controller := &EngineController{ds: newRuntimeCapabilityTestStore(mode)}
				context := &rebuildContext{engine: engine, currentEngine: engine.DeepCopy(), rebuildObj: engine, rebuildProxy: client}
				err := controller.addReplicaForRebuild(context, restore, nil, 30, 60)
				blocked := mode == types.DataEngineModeV2Only && engineType != longhorn.DataEngineTypeV2
				if blocked {
					if err == nil || client.adds != 0 {
						t.Fatalf("legacy rebuild activation escaped: mode=%q engine=%q restore=%t adds=%d err=%v", mode, engineType, restore, client.adds, err)
					}
				} else if err != nil || client.adds != 1 || client.restore != restore {
					t.Fatalf("supported rebuild changed: mode=%q engine=%q restore=%t adds=%d err=%v", mode, engineType, restore, client.adds, err)
				}
			}
		}
	}
}

func TestV2OnlyRebuildRebindsCurrentEngineAndRPCTarget(t *testing.T) {
	for _, scenario := range []string{"compatible", "legacy-current", "legacy-target", "missing-current", "missing-target", "typed-nil-target"} {
		t.Run(scenario, func(t *testing.T) {
			engine := &longhorn.Engine{Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: longhorn.DataEngineTypeV2}}}
			client := &rebuildCapabilityRecorder{}
			controller := &EngineController{ds: newRuntimeCapabilityTestStore(types.DataEngineModeV2Only)}
			context := &rebuildContext{engine: engine, currentEngine: engine.DeepCopy(), rebuildObj: &longhorn.EngineFrontend{Spec: longhorn.EngineFrontendSpec{DataEngine: longhorn.DataEngineTypeV2}}, rebuildProxy: client}
			switch scenario {
			case "legacy-current":
				context.currentEngine.Spec.DataEngine = longhorn.DataEngineTypeV1
			case "legacy-target":
				context.rebuildObj = &longhorn.Engine{Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: longhorn.DataEngineTypeV1}}}
			case "missing-current":
				context.currentEngine = nil
			case "missing-target":
				context.rebuildObj = nil
			case "typed-nil-target":
				context.rebuildObj = (*longhorn.EngineFrontend)(nil)
			}
			err := controller.addReplicaForRebuild(context, false, nil, 30, 60)
			if scenario == "compatible" {
				if err != nil || client.adds != 1 {
					t.Fatalf("compatible V2 rebuild changed: %v", err)
				}
			} else if err == nil || client.adds != 0 {
				t.Fatal("unresolved rebuild binding reached ReplicaAdd")
			}
		})
	}
}

func TestV2OnlyRebuildFailsBeforeClaimOrClientCreation(t *testing.T) {
	for _, engineType := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unknown"} {
		controller := &EngineController{ds: newRuntimeCapabilityTestStore(types.DataEngineModeV2Only), baseController: &baseController{logger: logrus.NewEntry(logrus.New())}}
		engine := &longhorn.Engine{Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engineType}}}
		if err := controller.startRebuilding(engine, "replica", "address"); err == nil || len(controller.rebuildingEngines) != 0 {
			t.Fatal("legacy rebuild reached claim mutation or client creation")
		}
	}
}
