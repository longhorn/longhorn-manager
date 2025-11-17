package controller

import (
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type snapshotHeavyTask string

var (
	snapshotPurgeTaskType snapshotHeavyTask = "purge"
	snapshotCloneTaskType snapshotHeavyTask = "clone"
)

type SnapshotConcurrentLimiter struct {
	logger logrus.FieldLogger
}

// NewSnapshotConcurrentLimiter creates a new SnapshotConcurrentLimiter instance
// to manage concurrent snapshot heavy I/O tasks like purge and clone.
func NewSnapshotConcurrentLimiter() *SnapshotConcurrentLimiter {
	return &SnapshotConcurrentLimiter{
		logger: logrus.WithField("component", "snapshot-concurrent-limiter"),
	}
}

// CanStartSnapshotPurge ensures purge operations do not exceed the concurrent I/O limit.
func (sc *SnapshotConcurrentLimiter) CanStartSnapshotPurge(engineClientProxy engineapi.EngineClientProxy, engine *longhorn.Engine, ds *datastore.DataStore) (bool, error) {
	if types.IsDataEngineV2(engine.Spec.DataEngine) {
		return true, nil
	}

	return sc.canStartHeavyIOTask(engineClientProxy, engine, ds, snapshotPurgeTaskType)
}

// CanStartSnapshotClone ensures clone operations do not exceed the concurrent I/O limit.
func (sc *SnapshotConcurrentLimiter) CanStartSnapshotClone(engineClientProxy engineapi.EngineClientProxy, engine *longhorn.Engine, ds *datastore.DataStore) (bool, error) {
	return sc.canStartHeavyIOTask(engineClientProxy, engine, ds, snapshotCloneTaskType)
}

func (sc *SnapshotConcurrentLimiter) canStartHeavyIOTask(engineClientProxy engineapi.EngineClientProxy, engine *longhorn.Engine, ds *datastore.DataStore, taskType snapshotHeavyTask) (bool, error) {
	// This limiter is *not* strict. The check is race-prone because multiple engines
	// across different nodes may perform the same load calculation concurrently.
	//
	// Example race:
	//
	//   - Engine A on Node A and Engine B on Node B call canStartHeavyIOTask()
	//     at the same time.
	//   - Both see the same existing nodeLoad (before either starts).
	//   - Both decide “limit not exceeded” and proceed.
	//   - Some nodes hosting replicas of both A and B end up exceeding the limit.
	//
	// This is expected and acceptable because the limiter is intentionally designed
	// as a *best-effort* mechanism, not as a strict distributed lock. It reduces
	// worst-case overload but does not guarantee perfect enforcement.
	//
	// Node-level overload conditions will still be safe because engine operations
	// are idempotent, and the system tolerates temporary oversubscription.

	switch taskType {
	case snapshotPurgeTaskType, snapshotCloneTaskType:
	default:
		return false, errors.Errorf("unknown snapshot heavy I/O task type: %v", taskType)
	}

	concurrentLimit, err := ds.GetSettingAsInt(types.SettingNameSnapshotHeavyTaskConcurrentLimit)
	if err != nil {
		sc.logger.WithError(err).Warnf("Failed to get snapshot purge concurrent limit setting for engine %v, allowing by default", engine.Name)
		return true, nil
	}

	// Unlimited
	if concurrentLimit < 1 {
		return true, nil
	}

	engines, err := ds.ListEngines()
	if err != nil {
		return false, err
	}

	replicas, err := ds.ListReplicas()
	if err != nil {
		return false, err
	}

	// Compute how many heavy tasks are running on each node
	nodeLoad := make(map[string]int)
	for _, e := range engines {
		if sc.checkEngineBusy(e, engineClientProxy, ds) {
			sc.countReplicasPerNode(e, replicas, nodeLoad)
		}
	}

	// Determine the nodes that *would be involved* if we start this engine’s task
	involvedNodes := map[string]int{}
	sc.countReplicasPerNode(engine, replicas, involvedNodes)

	for nodeID := range involvedNodes {
		if nodeLoad[nodeID] >= int(concurrentLimit) {
			sc.logger.Debugf(
				"Denied snapshot %s for engine %s: node %s load (%d) >= limit (%d)",
				taskType, engine.Name, nodeID, nodeLoad[nodeID], concurrentLimit,
			)
			return false, nil
		}

		// just for recording the load in logs
		nodeLoad[nodeID]++
	}

	sc.logger.WithFields(logrus.Fields{
		"taskType": taskType,
		"engine":   engine.Name,
		"nodeLoad": nodeLoad,
		"limit":    concurrentLimit,
	}).Debug("Snapshot heavy I/O task started")

	return true, nil
}

func (sc *SnapshotConcurrentLimiter) checkEngineBusy(engine *longhorn.Engine, engineClientProxy engineapi.EngineClientProxy, ds *datastore.DataStore) bool {
	isPurging := sc.checkEnginePurging(engine, engineClientProxy)
	if isPurging {
		return true
	}

	return sc.checkEngineCloning(engine, engineClientProxy, ds)
}

func (sc *SnapshotConcurrentLimiter) checkEnginePurging(engine *longhorn.Engine, engineClientProxy engineapi.EngineClientProxy) bool {
	purgeStatus, err := engineClientProxy.SnapshotPurgeStatus(engine)
	if err != nil {
		sc.logger.WithError(err).Warnf("Failed to get purge status for engine %v", engine.Name)
		return false
	}

	isPurging := false
	for _, status := range purgeStatus {
		if status.IsPurging || status.State == engineapi.ProcessStateInProgress {
			isPurging = true
			break
		}
	}
	return isPurging
}

func (sc *SnapshotConcurrentLimiter) checkEngineCloning(engine *longhorn.Engine, engineClientProxy engineapi.EngineClientProxy, ds *datastore.DataStore,
) bool {
	cliAPIVersion, err := ds.GetDataEngineImageCLIAPIVersion(engine.Status.CurrentImage, engine.Spec.DataEngine)
	if err != nil {
		sc.logger.WithError(err).Warnf("Failed to get CLI API version for engine %v", engine.Name)
		return false
	}

	if types.IsDataEngineV2(engine.Spec.DataEngine) || cliAPIVersion >= engineapi.CLIVersionFive {
		cloneStatus, err := engineClientProxy.SnapshotCloneStatus(engine)
		if err != nil {
			sc.logger.WithError(err).Warnf("Failed to get clone status for engine %v", engine.Name)
			return false
		}

		isCloning := false
		for _, status := range cloneStatus {
			if status.IsCloning || status.State == engineapi.ProcessStateInProgress {
				isCloning = true
				break
			}
		}

		return isCloning
	}

	return false
}

// countReplicasPerNode applies a callback for each replica node of the given engine.
func (sc *SnapshotConcurrentLimiter) countReplicasPerNode(engine *longhorn.Engine, replicas map[string]*longhorn.Replica, nodeCounter map[string]int) {
	for replicaName := range engine.Spec.ReplicaAddressMap {
		replica, ok := replicas[replicaName]
		if !ok || replica == nil {
			sc.logger.Warnf("Replica %v for engine %v not found", replicaName, engine.Name)
			continue
		}
		nodeCounter[replica.Spec.NodeID]++
	}
}
