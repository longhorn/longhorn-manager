package controller

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type frontendCapabilityRecorder struct {
	engineapi.EngineClientProxy
	starts int
}

func (client *frontendCapabilityRecorder) VolumeFrontendStart(*longhorn.Engine) error {
	client.starts++
	return nil
}

func TestV2OnlyRetainedEngineCannotRestartLegacyFrontend(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		for _, engineType := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unknown", longhorn.DataEngineTypeV2} {
			t.Run(string(mode)+"/"+string(engineType), func(t *testing.T) {
				logger := logrus.New()
				logger.Out = io.Discard
				monitor := &EngineMonitor{ds: newRuntimeCapabilityTestStore(mode), logger: logger}
				engine := &longhorn.Engine{Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engineType}, Frontend: longhorn.VolumeFrontendBlockDev}}
				engine.Status.CurrentState = longhorn.InstanceStateRunning
				client := &frontendCapabilityRecorder{}
				err := monitor.startFrontend(engine, client)
				legacy := engineType != longhorn.DataEngineTypeV2
				blocked := mode == types.DataEngineModeV2Only && legacy
				expectedStarts := 0
				if legacy && !blocked {
					expectedStarts = 1
				}
				if (err != nil) != blocked || client.starts != expectedStarts {
					t.Fatalf("frontend capability: mode=%q engine=%q starts=%d err=%v", mode, engineType, client.starts, err)
				}
				engine.Status.Endpoint = "existing"
				client.starts = 0
				if err := monitor.startFrontend(engine, client); err != nil || client.starts != 0 {
					t.Fatal("non-activating observation was blocked or restarted the frontend")
				}
				engine.Status.Endpoint, engine.Spec.DisableFrontend = "", true
				if err := monitor.startFrontend(engine, client); err != nil || client.starts != 0 {
					t.Fatal("disabled frontend was activated or its observation was blocked")
				}
				engine.Spec.DisableFrontend, engine.Spec.Frontend = false, longhorn.VolumeFrontendEmpty
				if err := monitor.startFrontend(engine, client); err != nil || client.starts != 0 {
					t.Fatal("empty frontend was activated or its observation was blocked")
				}
			})
		}
	}
}
