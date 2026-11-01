package controller

import (
	"fmt"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (ec *EngineController) addReplicaForRebuild(rc *rebuildContext, restore bool, localSync *etypes.FileLocalSync, fileSyncTimeout, grpcTimeout int64) error {
	if err := ec.validateRebuildCapability(rc); err != nil {
		return err
	}
	return rc.rebuildProxy.ReplicaAdd(rc.rebuildObj, rc.replicaName, rc.replicaURL, restore, rc.fastReplicaRebuild, localSync, fileSyncTimeout, grpcTimeout, rc.linkedCloneSource)
}

func (ec *EngineController) validateRebuildCapability(rc *rebuildContext) error {
	mode, err := types.ParseDataEngineMode(string(ec.ds.DataEngineMode()))
	if err != nil || mode != types.DataEngineModeV2Only {
		return err
	}
	if rc == nil || rc.engine == nil || rc.currentEngine == nil || rc.engine.Spec.DataEngine != longhorn.DataEngineTypeV2 || rc.currentEngine.Spec.DataEngine != longhorn.DataEngineTypeV2 {
		return fmt.Errorf("v2-only rebuild requires explicitly V2 original and current engines")
	}
	switch object := rc.rebuildObj.(type) {
	case *longhorn.Engine:
		if object != nil && object.Spec.DataEngine == longhorn.DataEngineTypeV2 {
			return nil
		}
	case *longhorn.EngineFrontend:
		if object != nil && object.Spec.DataEngine == longhorn.DataEngineTypeV2 {
			return nil
		}
	}
	return fmt.Errorf("v2-only rebuild requires an explicitly V2 RPC target")
}
