package controller

import (
	"github.com/cockroachdb/errors"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (m *EngineMonitor) startFrontend(engine *longhorn.Engine, client engineapi.EngineClientProxy) error {
	// V2 frontend lifecycle belongs to EngineFrontendController.
	if !types.IsDataEngineV1(engine.Spec.DataEngine) || engine.Status.Endpoint != "" || engine.Spec.DisableFrontend || engine.Spec.Frontend == longhorn.VolumeFrontendEmpty {
		return nil
	}
	if err := validateEngineActivationCapability(m.ds.DataEngineMode(), engine.Spec.DataEngine); err != nil {
		return err
	}
	m.logger.Infof("Starting frontend %v", engine.Spec.Frontend)
	return errors.Wrapf(client.VolumeFrontendStart(engine), "failed to start frontend %v", engine.Spec.Frontend)
}

func validateEngineActivationCapability(mode types.DataEngineMode, engine longhorn.DataEngineType) error {
	parsed, err := types.ParseDataEngineMode(string(mode))
	if err != nil {
		return err
	}
	if parsed == types.DataEngineModeV2Only && engine != longhorn.DataEngineTypeV2 {
		return errors.New("v2-only capability cannot activate a legacy engine")
	}
	return nil
}
