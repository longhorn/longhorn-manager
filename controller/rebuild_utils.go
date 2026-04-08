package controller

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func getReplicaRebuildCandidate(e *longhorn.Engine, logger logrus.FieldLogger) (replicaName, addr string, needRebuild bool) {
	replicaExists := make(map[string]bool)
	replicaRebuildingInProgress := make(map[string]bool)
	for replica, mode := range e.Status.ReplicaModeMap {
		replicaExists[replica] = true
		if mode == longhorn.ReplicaModeWO {
			replicaRebuildingInProgress[replica] = true
		}
	}

	if len(replicaRebuildingInProgress) > 0 {
		logger.WithField("volume", e.Spec.VolumeName).Infof("Skipped rebuilding of replica because there is another rebuild in progress: %v, since we only rebuild one replica at a time", replicaRebuildingInProgress)
		return "", "", false
	}

	for replica, address := range e.Status.CurrentReplicaAddressMap {
		if !replicaExists[replica] {
			return replica, address, true
		}
	}

	return "", "", false
}

func updateReplicaRebuildFailedCondition(ds *datastore.DataStore, replica *longhorn.Replica, errMsg string) (*longhorn.Replica, error) {
	replicaRebuildFailedReason, conditionStatus, err := getReplicaRebuildFailedReason(ds, replica.Spec.NodeID, errMsg)
	if err != nil {
		return nil, err
	}

	replica.Status.Conditions = types.SetCondition(
		replica.Status.Conditions,
		longhorn.ReplicaConditionTypeRebuildFailed,
		conditionStatus,
		replicaRebuildFailedReason,
		errMsg)

	return ds.UpdateReplicaStatus(replica)
}

func getReplicaRebuildFailedReason(ds *datastore.DataStore, replicaNodeID, errMsg string) (failedReason string, conditionStatus longhorn.ConditionStatus, err error) {
	failedReason, conditionStatus, isRebuildingFailedByNetwork := getReplicaRebuildFailedReasonFromError(errMsg)
	if isRebuildingFailedByNetwork {
		replicaNode, err := ds.GetNodeRO(replicaNodeID)
		if err != nil {
			return "", "", err
		}

		replicaRebuildFailedCondition := types.GetCondition(replicaNode.Status.Conditions, longhorn.NodeConditionTypeReady)
		switch replicaRebuildFailedCondition.Reason {
		case longhorn.NodeConditionReasonManagerPodDown, longhorn.NodeConditionReasonKubernetesNodeGone, longhorn.NodeConditionReasonKubernetesNodeNotReady:
			failedReason = replicaRebuildFailedCondition.Reason
		}
	}

	return failedReason, conditionStatus, nil
}

func waitForV2EngineRebuild(ds *datastore.DataStore, engine *longhorn.Engine, replicaName string, timeout int64) error {
	if !types.IsDataEngineV2(engine.Spec.DataEngine) {
		return nil
	}

	ticker := time.NewTicker(EnginePollInterval)
	defer ticker.Stop()
	timer := time.NewTimer(time.Duration(timeout) * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			e, err := ds.GetEngineRO(engine.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return errors.Wrapf(err, "engine %v not found during v2 replica %s rebuild wait", engine.Name, replicaName)
				}
				continue
			}

			r, err := ds.GetReplicaRO(replicaName)
			if err != nil {
				return err
			}
			if e.Spec.ReplicaAddressMap[replicaName] == "" {
				return fmt.Errorf("unknown replica %v for engine", replicaName)
			}
			if r.Status.CurrentState != longhorn.InstanceStateRunning {
				return fmt.Errorf("replica %v is state %s, which is invalid for rebuilding", replicaName, r.Status.CurrentState)
			}

			rebuildingStatus := e.Status.RebuildStatus[engineapi.GetBackendReplicaURL(e.Status.CurrentReplicaAddressMap[replicaName])]
			if e.Status.ReplicaModeMap[replicaName] == longhorn.ReplicaModeRW {
				return nil
			}
			if e.Status.ReplicaModeMap[replicaName] == longhorn.ReplicaModeERR {
				if rebuildingStatus != nil && (rebuildingStatus.State == engineapi.ProcessStateError || rebuildingStatus.Error != "") {
					return fmt.Errorf("failed to wait for v2 replica %s rebuild, rebuilding state %s, error: %v", replicaName, rebuildingStatus.State, rebuildingStatus.Error)
				}
				return fmt.Errorf("replica %v is in ERR mode, which is invalid for rebuilding", replicaName)
			}
			if e.Status.ReplicaModeMap[replicaName] == "" {
				continue
			}
			if rebuildingStatus == nil {
				continue
			}
			if rebuildingStatus.State == engineapi.ProcessStateError || rebuildingStatus.Error != "" {
				return fmt.Errorf("failed to wait for v2 replica %s rebuild, rebuilding state %s, error: %v", replicaName, rebuildingStatus.State, rebuildingStatus.Error)
			}
			if rebuildingStatus.State == engineapi.ProcessStateComplete {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for replica %v to be rebuilt", replicaName)
		}
	}
}

func getReplicaRebuildFailedReasonFromError(errMsg string) (string, longhorn.ConditionStatus, bool) {
	switch {
	case strings.Contains(errMsg, longhorn.ReplicaRebuildFailedCanceledErrorMSG):
		fallthrough
	case strings.Contains(errMsg, longhorn.ReplicaRebuildFailedDeadlineExceededErrorMSG):
		fallthrough
	case strings.Contains(errMsg, longhorn.ReplicaRebuildFailedUnavailableErrorMSG):
		return longhorn.ReplicaConditionReasonRebuildFailedDisconnection, longhorn.ConditionStatusTrue, true
	case errMsg == "":
		return "", longhorn.ConditionStatusFalse, false
	default:
		return longhorn.ReplicaConditionReasonRebuildFailedGeneral, longhorn.ConditionStatusTrue, false
	}
}

func isV2ReplicaAddAlreadyInProgressError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "replica add is already in progress")
}

func isV2ReplicaAddRestoreInProgressError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "restore is in progress")
}
