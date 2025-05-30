package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	FailedReplicaMaxRetryCount = 5
)

type ReplicaScheduler struct {
	ds *datastore.DataStore

	// Required for unit testing.
	nowHandler func() time.Time
}

type Disk struct {
	longhorn.DiskSpec
	*longhorn.DiskStatus
	NodeID string
}

type DiskSchedulingInfo struct {
	DiskUUID                   string
	StorageAvailable           int64
	StorageMaximum             int64
	StorageReserved            int64
	StorageScheduled           int64
	OverProvisioningPercentage int64
	MinimalAvailablePercentage int64
}

func NewReplicaScheduler(ds *datastore.DataStore) *ReplicaScheduler {
	rcScheduler := &ReplicaScheduler{
		ds: ds,

		// Required for unit testing.
		nowHandler: time.Now,
	}
	return rcScheduler
}

// ScheduleReplica will return (nil, multiError, nil) for unschedulable replica
// The multiError will contain detailed reasons for scheduling failure.
func (rcs *ReplicaScheduler) ScheduleReplica(replica *longhorn.Replica, replicas map[string]*longhorn.Replica, volume *longhorn.Volume) (*longhorn.Replica, util.MultiError, error) {
	// only called when replica is starting for the first time
	if replica.Spec.NodeID != "" {
		return nil, nil, fmt.Errorf("BUG: Replica %v has been scheduled to node %v", replica.Name, replica.Spec.NodeID)
	}

	// not to schedule a replica failed and unused before.
	if replica.Spec.HealthyAt == "" && replica.Spec.FailedAt != "" {
		logrus.WithFields(logrus.Fields{"volume": volume.Name, "replica": replica.Name}).Warn("Attempt to schedule an already failed and unused replica; not scheduling")
		return nil, nil, nil
	}

	diskCandidates, multiError, err := rcs.FindDiskCandidates(replica, replicas, volume)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "fatal error in FindDiskCandidates for replica %s of volume %s", replica.Name, volume.Name)
	}

	if len(diskCandidates) == 0 {
		if multiError == nil || len(multiError) == 0 {
			multiError = util.NewMultiError()
			errMsg := fmt.Sprintf("scheduler internal error: no disk candidates found for replica %v of volume %v, and no specific reasons provided by FindDiskCandidates", replica.Name, volume.Name)
			multiError[errMsg] = struct{}{}
			logrus.WithFields(logrus.Fields{"volume": volume.Name, "replica": replica.Name}).Error(multiError.Join())
		}
		logrus.WithFields(logrus.Fields{"volume": volume.Name, "replica": replica.Name}).Warnf("No available disk for replica, size %v. Reasons: %s", replica.Spec.VolumeSize, multiError.Join())
		return nil, multiError, nil
	}

	rcs.scheduleReplicaToDisk(replica, diskCandidates, volume.Name)
	return replica, nil, nil
}

// FindDiskCandidates identifies suitable disks on eligible nodes for the replica.
//
// Parameters:
// - replica: The replica for which to find disk candidates.
// - replicas: The map of existing replicas.
// - volume: The volume associated with the replica.
//
// Returns:
// - Map of disk candidates (disk UUID to Disk).
// - MultiError for non-fatal errors encountered.
// - Error for any fatal errors encountered.
func (rcs *ReplicaScheduler) FindDiskCandidates(replica *longhorn.Replica,
	allVolumeReplicas map[string]*longhorn.Replica, volume *longhorn.Volume) (map[string]*Disk, util.MultiError, error) {
	overallMultiError := util.NewMultiError()
	logForReplica := logrus.WithFields(logrus.Fields{"volume": volume.Name, "replica": replica.Name})

	nodesInfo, err := rcs.getNodeInfo()
	if err != nil {
		return nil, nil, errors.Wrap(err, "FindDiskCandidates: failed to get node info")
	}

	nodeCandidatesFromFilter, nodeMultiError := rcs.getNodeCandidates(nodesInfo, replica)
	if nodeMultiError != nil && len(nodeMultiError) > 0 {
		overallMultiError.Append(nodeMultiError)
	}

	if len(nodeCandidatesFromFilter) == 0 {
		if len(overallMultiError) == 0 {
			errMsg := fmt.Sprintf("no suitable nodes found for replica %s (hard affinity: '%s', initial node count: %d) after node candidacy checks", replica.Name, replica.Spec.HardNodeAffinity, len(nodesInfo))
			overallMultiError[errMsg] = struct{}{}
			logForReplica.Debug(errMsg)
		} else {
			logForReplica.Debugf("No suitable nodes found. Reasons from getNodeCandidates: %s", overallMultiError.Join())
		}
		return nil, overallMultiError, nil
	}

	nodeDisksMap := map[string]map[string]struct{}{}
	for nodeNameKey, nodeDetails := range nodeCandidatesFromFilter {
		disks := map[string]struct{}{}
		for diskNameInMapKey, diskStatus := range nodeDetails.Status.DiskStatus {
			diskSpec, existsInSpec := nodeDetails.Spec.Disks[diskNameInMapKey]
			if !existsInSpec || !diskSpec.AllowScheduling || diskSpec.EvictionRequested ||
				types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeSchedulable).Status != longhorn.ConditionStatusTrue {
				continue
			}
			disks[diskStatus.DiskUUID] = struct{}{}
		}
		nodeDisksMap[nodeNameKey] = disks
	}

	diskCandidates, diskFilterMultiError := rcs.getDiskCandidates(
		nodeCandidatesFromFilter,
		nodeDisksMap,
		allVolumeReplicas,
		volume,
		replica,
		true,
		false)

	if diskFilterMultiError != nil && len(diskFilterMultiError) > 0 {
		overallMultiError.Append(diskFilterMultiError)
	}

	if len(diskCandidates) == 0 {
		if len(overallMultiError) == 0 {
			errMsg := fmt.Sprintf("no suitable disks found on %d available candidate nodes for replica %s", len(nodeCandidatesFromFilter), replica.Name)
			overallMultiError[errMsg] = struct{}{}
			logForReplica.Debug(errMsg)
		} else if len(diskFilterMultiError) > 0 {
			logForReplica.Debugf("No suitable disks found. Reasons from disk filtering: %s", diskFilterMultiError.Join())
		}
		return nil, overallMultiError, nil
	}

	return diskCandidates, overallMultiError, nil
}

func (rcs *ReplicaScheduler) getNodeCandidates(nodesInfo map[string]*longhorn.Node, schedulingReplica *longhorn.Replica) (map[string]*longhorn.Node, util.MultiError) {
	nodeCandidates := map[string]*longhorn.Node{}
	multiErr := util.NewMultiError()

	if schedulingReplica.Spec.HardNodeAffinity != "" {
		node, exist := nodesInfo[schedulingReplica.Spec.HardNodeAffinity]
		if !exist {
			errMsg := fmt.Sprintf("node %s (hard affinity for replica %s of volume %s): %s", schedulingReplica.Spec.HardNodeAffinity, schedulingReplica.Name, schedulingReplica.Spec.VolumeName, longhorn.ErrorReplicaScheduleHardNodeAffinityNotSatisfied)
			multiErr[errMsg] = struct{}{}
			return nil, multiErr
		}
		nodesInfo = map[string]*longhorn.Node{
			schedulingReplica.Spec.HardNodeAffinity: node,
		}
	}

	if len(nodesInfo) == 0 {
		if len(multiErr) == 0 {
			errMsg := fmt.Sprintf("for replica %s of volume %s: %s", schedulingReplica.Name, schedulingReplica.Spec.VolumeName, longhorn.ErrorReplicaScheduleNodeUnavailable)
			multiErr[errMsg] = struct{}{}
		}
		return nil, multiErr
	}

	for _, node := range nodesInfo {
		log := logrus.WithFields(logrus.Fields{"node": node.Name, "replica": schedulingReplica.Name, "volume": schedulingReplica.Spec.VolumeName})

		if types.IsDataEngineV2(schedulingReplica.Spec.DataEngine) {
			disabled, err := rcs.ds.IsV2DataEngineDisabledForNode(node.Name)
			if err != nil {
				errMsg := fmt.Sprintf("node %s: failed to check v2 data engine status: %v", node.Name, err)
				log.WithError(err).Debug(errMsg)
				multiErr[errMsg] = struct{}{}
				continue
			}
			if disabled {
				errMsg := fmt.Sprintf("node %s: v2 data engine is disabled", node.Name)
				log.Debug(errMsg)
				multiErr[errMsg] = struct{}{}
				continue
			}
		}

		// After a node reboot, it might be listed in the nodeInfo but its InstanceManager
		// is not ready. To prevent scheduling replicas on such nodes, verify the
		// InstanceManager's readiness before including it in the candidate list.
		if isReady, err := rcs.ds.CheckInstanceManagersReadiness(schedulingReplica.Spec.DataEngine, node.Name); !isReady {
			errMsgPrefix := fmt.Sprintf("node %s", node.Name)
			var specificReason string
			if err != nil {
				specificReason = fmt.Sprintf("instance manager readiness check error: %v", err)
				log.WithError(err).Debugf("%s: %s", errMsgPrefix, specificReason)
			} else {
				specificReason = "instance manager not ready"
				log.Debugf("%s: %s", errMsgPrefix, specificReason)
			}
			multiErr[fmt.Sprintf("%s: %s", errMsgPrefix, specificReason)] = struct{}{}
			continue
		}

		if isReady, err := rcs.ds.CheckDataEngineImageReadiness(schedulingReplica.Spec.Image, schedulingReplica.Spec.DataEngine, node.Name); !isReady {
			errMsgPrefix := fmt.Sprintf("node %s", node.Name)
			var specificReason string
			if err != nil {
				specificReason = fmt.Sprintf("data engine image %s readiness check error: %v", schedulingReplica.Spec.Image, err)
				log.WithError(err).Debugf("%s: %s", errMsgPrefix, specificReason)
			} else {
				specificReason = fmt.Sprintf("data engine image %s not ready", schedulingReplica.Spec.Image)
				log.Debugf("%s: %s", errMsgPrefix, specificReason)
			}
			multiErr[fmt.Sprintf("%s: %s", errMsgPrefix, specificReason)] = struct{}{}
			continue
		}
		nodeCandidates[node.Name] = node
	}

	if len(nodeCandidates) == 0 && len(multiErr) == 0 {
		errMsg := fmt.Sprintf("no nodes passed all criteria for replica %s of volume %s (initial node count for candidacy: %d)", schedulingReplica.Name, schedulingReplica.Spec.VolumeName, len(nodesInfo))
		multiErr[errMsg] = struct{}{}
	}
	return nodeCandidates, multiErr
}

// getDiskCandidates returns a map of the most appropriate disks a replica can be scheduled to (assuming it can be
// scheduled at all). For example, consider a case in which there are two disks on nodes without a replica for a volume
// and two disks on nodes with a replica for the same volume. getDiskCandidates only returns the disks without a
// replica, even if the replica can legally be scheduled on all four disks.
// Some callers (e.g. CheckAndReuseFailedReplicas) do not consider a node or zone to be used if it contains a failed
// replica. ignoreFailedReplicas == true supports this use case.
func (rcs *ReplicaScheduler) getDiskCandidates(nodeInfo map[string]*longhorn.Node,
	nodeDisksMap map[string]map[string]struct{},
	allVolumeReplicas map[string]*longhorn.Replica,
	volume *longhorn.Volume,
	replicaBeingScheduled *longhorn.Replica,
	requireSchedulingCheck, ignoreFailedReplicas bool) (map[string]*Disk, util.MultiError) {

	accumulatedMultiError := util.NewMultiError()
	logForSchedReplica := logrus.WithFields(logrus.Fields{"volume": volume.Name, "replica": replicaBeingScheduled.Name})

	biNodeSelector := []string{}
	biDiskSelector := []string{}
	if volume.Spec.BackingImage != "" {
		bi, errBI := rcs.ds.GetBackingImageRO(volume.Spec.BackingImage)
		if errBI != nil {
			errMsg := fmt.Sprintf("failed to get backing image %s for volume %s: %v", volume.Spec.BackingImage, volume.Name, errBI)
			accumulatedMultiError[errMsg] = struct{}{}
			logForSchedReplica.WithError(errBI).Warnf("Failed to get backing image %s", volume.Spec.BackingImage)
		} else if bi != nil {
			biNodeSelector = bi.Spec.NodeSelector
			biDiskSelector = bi.Spec.DiskSelector
		}
	}

	nodeSoftAntiAffinity, err := rcs.ds.GetSettingAsBool(types.SettingNameReplicaSoftAntiAffinity)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get setting %s: %v", types.SettingNameReplicaSoftAntiAffinity, err)
		accumulatedMultiError[errMsg] = struct{}{}
		nodeSoftAntiAffinity = false
		logForSchedReplica.WithError(err).Warnf("Failed to get setting %s, defaulting to %t", types.SettingNameReplicaSoftAntiAffinity, nodeSoftAntiAffinity)
	} else if volume.Spec.ReplicaSoftAntiAffinity != longhorn.ReplicaSoftAntiAffinityDefault && volume.Spec.ReplicaSoftAntiAffinity != "" {
		nodeSoftAntiAffinity = (volume.Spec.ReplicaSoftAntiAffinity == longhorn.ReplicaSoftAntiAffinityEnabled)
	}

	zoneSoftAntiAffinity, err := rcs.ds.GetSettingAsBool(types.SettingNameReplicaZoneSoftAntiAffinity)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get setting %s: %v", types.SettingNameReplicaZoneSoftAntiAffinity, err)
		accumulatedMultiError[errMsg] = struct{}{}
		zoneSoftAntiAffinity = false
		logForSchedReplica.WithError(err).Warnf("Failed to get setting %s, defaulting to %t", types.SettingNameReplicaZoneSoftAntiAffinity, zoneSoftAntiAffinity)
	} else if volume.Spec.ReplicaZoneSoftAntiAffinity != longhorn.ReplicaZoneSoftAntiAffinityDefault && volume.Spec.ReplicaZoneSoftAntiAffinity != "" {
		zoneSoftAntiAffinity = (volume.Spec.ReplicaZoneSoftAntiAffinity == longhorn.ReplicaZoneSoftAntiAffinityEnabled)
	}

	diskSoftAntiAffinity, err := rcs.ds.GetSettingAsBool(types.SettingNameReplicaDiskSoftAntiAffinity)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get setting %s: %v", types.SettingNameReplicaDiskSoftAntiAffinity, err)
		accumulatedMultiError[errMsg] = struct{}{}
		diskSoftAntiAffinity = false
		logForSchedReplica.WithError(err).Warnf("Failed to get setting %s, defaulting to %t", types.SettingNameReplicaDiskSoftAntiAffinity, diskSoftAntiAffinity)
	} else if volume.Spec.ReplicaDiskSoftAntiAffinity != longhorn.ReplicaDiskSoftAntiAffinityDefault && volume.Spec.ReplicaDiskSoftAntiAffinity != "" {
		diskSoftAntiAffinity = (volume.Spec.ReplicaDiskSoftAntiAffinity == longhorn.ReplicaDiskSoftAntiAffinityEnabled)
	}

	creatingNewReplicasForReplenishment := false
	if volume.Status.Robustness == longhorn.VolumeRobustnessDegraded {
		timeToReplacementReplica, _, errCalcTime := rcs.timeToReplacementReplica(volume)
		if errCalcTime != nil {
			errMsg := fmt.Sprintf("failed to get time until replica replacement for volume %s: %v", volume.Name, errCalcTime)
			accumulatedMultiError[errMsg] = struct{}{}
			logForSchedReplica.WithError(errCalcTime).Warn("Failed to calculate time for replica replenishment")
		} else {
			creatingNewReplicasForReplenishment = timeToReplacementReplica == 0
		}
	}

	_getDiskCandidatesFromNodes := func(nodesToConsider map[string]*longhorn.Node, iterationName string) (map[string]*Disk, util.MultiError) {
		log := logrus.WithFields(logrus.Fields{
			"volume":    volume.Name,
			"replica":   replicaBeingScheduled.Name,
			"iteration": iterationName,
		})
		log.Debugf("Attempting to find disk candidates from %d nodes for %s", len(nodesToConsider), iterationName)

		currentIterationDiskCandidates := map[string]*Disk{}
		currentIterationMultiError := util.NewMultiError()

		for _, node := range nodesToConsider {
			disksOnNode, ok := nodeDisksMap[node.Name]
			if !ok {
				log.Warnf("Node %s present in nodesToConsider but not in nodeDisksMap for %s iteration", node.Name, iterationName)
				continue
			}

			diskCandidatesFromNode, errorsFromFilter := rcs.filterNodeDisksForReplica(node, disksOnNode, allVolumeReplicas,
				volume, replicaBeingScheduled, requireSchedulingCheck, biDiskSelector)

			for k, v := range diskCandidatesFromNode {
				currentIterationDiskCandidates[k] = v
			}
			if errorsFromFilter != nil && len(errorsFromFilter) > 0 {
				currentIterationMultiError.Append(errorsFromFilter)
			}
		}

		filteredByDiskAffinity := filterDisksWithMatchingReplicas(currentIterationDiskCandidates, allVolumeReplicas, diskSoftAntiAffinity, ignoreFailedReplicas)

		if len(currentIterationDiskCandidates) > 0 && len(filteredByDiskAffinity) == 0 && !diskSoftAntiAffinity {
			errMsg := fmt.Sprintf("%s for replica %s: all suitable disks on considered nodes are already in use by other replicas and disk soft anti-affinity is disabled", iterationName, replicaBeingScheduled.Name)
			currentIterationMultiError[errMsg] = struct{}{}
		}
		log.Debugf("Found %d disk candidates after filtering for %s (disk soft-anti-affinity applied: %t)", len(filteredByDiskAffinity), iterationName, diskSoftAntiAffinity)
		return filteredByDiskAffinity, currentIterationMultiError
	}

	usedNodes, usedZones, onlyEvictingNodes, onlyEvictingZones := getCurrentNodesAndZones(allVolumeReplicas, nodeInfo,
		ignoreFailedReplicas, creatingNewReplicasForReplenishment)

	allowEmptyNodeSelectorVolume, err := rcs.ds.GetSettingAsBool(types.SettingNameAllowEmptyNodeSelectorVolume)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get setting %s: %v", types.SettingNameAllowEmptyNodeSelectorVolume, err)
		accumulatedMultiError[errMsg] = struct{}{}
		logForSchedReplica.WithError(err).Warnf("Failed to get setting %s, defaulting to behavior of true", types.SettingNameAllowEmptyNodeSelectorVolume)
		allowEmptyNodeSelectorVolume = true
	}

	replicaAutoBalance := rcs.ds.GetAutoBalancedReplicasSetting(volume, logForSchedReplica)

	unusedNodes := map[string]*longhorn.Node{}
	unusedNodesInUnusedZones := map[string]*longhorn.Node{}

	// Per https://github.com/longhorn/longhorn/issues/3076, if a replica is being evicted from one disk on a node, the
	// scheduler must be given the opportunity to schedule it to a different disk on the same node (if it meets other
	// requirements). Track nodes that are evicting all their replicas in case we can reuse one.
	unusedNodesAfterEviction := map[string]*longhorn.Node{}
	unusedNodesInUnusedZonesAfterEviction := map[string]*longhorn.Node{}

	filteredNodeInfoForDiskSearch := map[string]*longhorn.Node{}
	for nodeNameKey, node := range nodeInfo {
		// Filter Nodes. If the Nodes don't match the tags, don't bother marking them as candidates.

		logContextForNodeFilter := logrus.WithFields(logrus.Fields{"volume": volume.Name, "replica": replicaBeingScheduled.Name, "node": node.Name})
		if !types.IsSelectorsInTags(node.Spec.Tags, volume.Spec.NodeSelector, allowEmptyNodeSelectorVolume) {
			errMsg := fmt.Sprintf("node %s: volume node selector '%s' not fulfilled by node tags %v", node.Name, strings.Join(volume.Spec.NodeSelector, ","), node.Spec.Tags)
			accumulatedMultiError[errMsg] = struct{}{}
			logContextForNodeFilter.Debugf("Excluding node from disk candidacy in getDiskCandidates: %s", errMsg)
			continue
		}
		// If the Nodes don't match the tags of the backing image of this volume,
		// don't schedule the replica on it because it will hang there
		if volume.Spec.BackingImage != "" && len(biNodeSelector) > 0 {
			if !types.IsSelectorsInTags(node.Spec.Tags, biNodeSelector, allowEmptyNodeSelectorVolume) {
				errMsg := fmt.Sprintf("node %s: backing image node selector '%s' not fulfilled by node tags %v", node.Name, strings.Join(biNodeSelector, ","), node.Spec.Tags)
				accumulatedMultiError[errMsg] = struct{}{}
				logContextForNodeFilter.Debugf("Excluding node from disk candidacy in getDiskCandidates: %s", errMsg)
				continue
			}
		}
		// Removed comment "Node passed selector checks in getDiskCandidates initial filter"
		filteredNodeInfoForDiskSearch[nodeNameKey] = node
	}

	for nodeNameValue, node := range filteredNodeInfoForDiskSearch {
		_, nodeIsUsed := usedNodes[nodeNameValue]
		_, zoneIsUsed := usedZones[node.Status.Zone]

		if !nodeIsUsed {
			unusedNodes[nodeNameValue] = node
			if !zoneIsUsed {
				unusedNodesInUnusedZones[nodeNameValue] = node
			}
		} else if replicaAutoBalance == longhorn.ReplicaAutoBalanceBestEffort {
			unusedNodes[nodeNameValue] = node
		}

		if onlyEvictingNodes[nodeNameValue] {
			unusedNodesAfterEviction[nodeNameValue] = node
			if !zoneIsUsed || onlyEvictingZones[node.Status.Zone] {
				unusedNodesInUnusedZonesAfterEviction[nodeNameValue] = node
			} else if replicaAutoBalance == longhorn.ReplicaAutoBalanceBestEffort {
				unusedNodesInUnusedZonesAfterEviction[nodeNameValue] = node
			}
		}
	}

	// In all cases, we should try to use a disk on an unused node in an unused zone first. Don't bother considering
	// zoneSoftAntiAffinity and nodeSoftAntiAffinity settings if such disks are available.
	var diskCandidates map[string]*Disk
	var errorsFromAttempt util.MultiError

	diskCandidates, errorsFromAttempt = _getDiskCandidatesFromNodes(unusedNodesInUnusedZones, "unused nodes in unused zones")
	if errorsFromAttempt != nil {
		accumulatedMultiError.Append(errorsFromAttempt)
	}
	if len(diskCandidates) > 0 {
		return diskCandidates, accumulatedMultiError
	}

	switch {
	case !zoneSoftAntiAffinity && !nodeSoftAntiAffinity:
		fallthrough
	// Same as the above. If we cannot schedule two replicas in the same zone, we cannot schedule them on the same node.
	case !zoneSoftAntiAffinity && nodeSoftAntiAffinity:
		diskCandidates, errorsFromAttempt = _getDiskCandidatesFromNodes(unusedNodesInUnusedZonesAfterEviction, "evicting-only nodes in unused/evicting-only zones")
		if errorsFromAttempt != nil {
			accumulatedMultiError.Append(errorsFromAttempt)
		}
		if len(diskCandidates) > 0 {
			return diskCandidates, accumulatedMultiError
		}
	case zoneSoftAntiAffinity && !nodeSoftAntiAffinity:
		diskCandidates, errorsFromAttempt = _getDiskCandidatesFromNodes(unusedNodes, "unused nodes (zone soft, node strict)")
		if errorsFromAttempt != nil {
			accumulatedMultiError.Append(errorsFromAttempt)
		}
		if len(diskCandidates) > 0 {
			return diskCandidates, accumulatedMultiError
		}
		diskCandidates, errorsFromAttempt = _getDiskCandidatesFromNodes(unusedNodesAfterEviction, "evicting-only nodes (zone soft, node strict)")
		if errorsFromAttempt != nil {
			accumulatedMultiError.Append(errorsFromAttempt)
		}
		if len(diskCandidates) > 0 {
			return diskCandidates, accumulatedMultiError
		}
	case zoneSoftAntiAffinity && nodeSoftAntiAffinity:
		diskCandidates, errorsFromAttempt = _getDiskCandidatesFromNodes(unusedNodes, "unused nodes (all soft anti-affinity)")
		if errorsFromAttempt != nil {
			accumulatedMultiError.Append(errorsFromAttempt)
		}
		if len(diskCandidates) > 0 {
			return diskCandidates, accumulatedMultiError
		}

		usedNodesStillCandidates := make(map[string]*longhorn.Node)
		for name, node := range filteredNodeInfoForDiskSearch {
			if _, isUnused := unusedNodes[name]; !isUnused {
				usedNodesStillCandidates[name] = node
			}
		}
		diskCandidates, errorsFromAttempt = _getDiskCandidatesFromNodes(usedNodesStillCandidates, "used nodes (all soft anti-affinity enabled)")
		if errorsFromAttempt != nil {
			accumulatedMultiError.Append(errorsFromAttempt)
		}
		if len(diskCandidates) > 0 {
			return diskCandidates, accumulatedMultiError
		}
	}

	if len(diskCandidates) == 0 && len(accumulatedMultiError) == 0 {
		accumulatedMultiError[fmt.Sprintf("no schedulable disks found for replica %s of volume %s after considering all anti-affinity preferences and node/disk states", replicaBeingScheduled.Name, volume.Name)] = struct{}{}
	}
	return diskCandidates, accumulatedMultiError
}

func (rcs *ReplicaScheduler) filterNodeDisksForReplica(node *longhorn.Node, disks map[string]struct{}, allVolumeReplicas map[string]*longhorn.Replica, volume *longhorn.Volume, replicaBeingScheduled *longhorn.Replica, requireSchedulingCheck bool, biDiskSelector []string) (map[string]*Disk, util.MultiError) {
	multiError := util.NewMultiError()
	preferredDisks := map[string]*Disk{}

	logContext := logrus.WithFields(logrus.Fields{
		"volume":  volume.Name,
		"replica": replicaBeingScheduled.Name,
		"node":    node.Name,
	})

	allowEmptyDiskSelectorVolume, err := rcs.ds.GetSettingAsBool(types.SettingNameAllowEmptyDiskSelectorVolume)
	if err != nil {
		err = errors.Wrapf(err, "failed to get %v setting", types.SettingNameAllowEmptyDiskSelectorVolume)
		multiError[err.Error()] = struct{}{}
		return preferredDisks, multiError
	}

	if len(disks) == 0 {
		multiError[fmt.Sprintf("node %s: no disks passed initial filtering for map population (e.g. not schedulable, eviction requested, or wrong type)", node.Name)] = struct{}{}
		return preferredDisks, multiError
	}

	// find disk that fit for current replica
	for diskUUID := range disks {
		var diskName string
		var diskSpecFound longhorn.DiskSpec
		var diskStatusFound *longhorn.DiskStatus
		diskSpecExists := false
		foundInNodeStatus := false

		for dn, ds := range node.Status.DiskStatus {
			if ds.DiskUUID == diskUUID {
				diskStatusFound = ds
				diskName = dn
				if spec, exists := node.Spec.Disks[dn]; exists {
					diskSpecFound = spec
					diskSpecExists = true
				}
				foundInNodeStatus = true
				break
			}
		}

		if !foundInNodeStatus {
			multiError[fmt.Sprintf("disk UUID %s on node %s: status not found in node object (internal inconsistency)", diskUUID, node.Name)] = struct{}{}
			continue
		}
		if !diskSpecExists {
			multiError[fmt.Sprintf("disk %s (UUID %s) on node %s: spec not found in node object (internal inconsistency)", diskName, diskUUID, node.Name)] = struct{}{}
			continue
		}

		currentDiskLogCtx := logContext.WithFields(logrus.Fields{"diskName": diskName, "diskUUID": diskUUID, "diskPath": diskSpecFound.Path})

		if requireSchedulingCheck {
			condition := types.GetCondition(diskStatusFound.Conditions, longhorn.DiskConditionTypeSchedulable)
			if condition.Status != longhorn.ConditionStatusTrue {
				errMsg := fmt.Sprintf("disk %s on node %s: not schedulable, reason: %s, message: %s", diskName, node.Name, condition.Reason, condition.Message)
				multiError[errMsg] = struct{}{}
				currentDiskLogCtx.Debug(errMsg)
				continue
			}
		}

		isV1EngineFilesystemDisk := types.IsDataEngineV1(volume.Spec.DataEngine) && diskSpecFound.Type == longhorn.DiskTypeFilesystem
		isV2EngineBlockDisk := types.IsDataEngineV2(volume.Spec.DataEngine) && diskSpecFound.Type == longhorn.DiskTypeBlock
		if !isV1EngineFilesystemDisk && !isV2EngineBlockDisk {
			errMsg := fmt.Sprintf("disk %s on node %s: incompatible disk type '%s' for volume data engine '%s'", diskName, node.Name, diskSpecFound.Type, volume.Spec.DataEngine)
			multiError[errMsg] = struct{}{}
			currentDiskLogCtx.Debug(errMsg)
			continue
		}

		if !datastore.IsSupportedVolumeSize(volume.Spec.DataEngine, diskStatusFound.FSType, volume.Spec.Size) {
			errMsg := fmt.Sprintf("disk %s on node %s: volume size %v not compatible with filesystem '%s'", diskName, node.Name, volume.Spec.Size, diskStatusFound.FSType)
			multiError[errMsg] = struct{}{}
			currentDiskLogCtx.Debug(errMsg)
			continue
		}

		if requireSchedulingCheck {
			info, errGetInfo := rcs.GetDiskSchedulingInfo(diskSpecFound, diskStatusFound)
			if errGetInfo != nil {
				errMsg := fmt.Sprintf("disk %s on node %s: failed to get disk scheduling settings: %v", diskName, node.Name, errGetInfo)
				multiError[errMsg] = struct{}{}
				currentDiskLogCtx.WithError(errGetInfo).Debug("Failed to get disk scheduling info")
				continue
			}

			var storageScheduledByOtherReplicasOnNode int64
			for rNameLoop, r := range allVolumeReplicas { // Renamed rName to rNameLoop for loop variable
				if r.Spec.NodeID == node.Name && r.Spec.DiskID == diskUUID && rNameLoop != replicaBeingScheduled.Name {
					if _, ok := diskStatusFound.ScheduledReplica[rNameLoop]; !ok {
						storageScheduledByOtherReplicasOnNode += r.Spec.VolumeSize
					}
				}
			}
			effectiveStorageScheduled := info.StorageScheduled + storageScheduledByOtherReplicasOnNode

			if !rcs.IsSchedulableToDisk(volume.Spec.Size, volume.Status.ActualSize,
				&DiskSchedulingInfo{
					DiskUUID:                   info.DiskUUID,
					StorageAvailable:           info.StorageAvailable,
					StorageMaximum:             info.StorageMaximum,
					StorageReserved:            info.StorageReserved,
					StorageScheduled:           effectiveStorageScheduled,
					OverProvisioningPercentage: info.OverProvisioningPercentage,
					MinimalAvailablePercentage: info.MinimalAvailablePercentage,
				}) {
				errMsg := fmt.Sprintf("disk %s on node %s: %s (available: %d, requested for this replica: %d, current scheduled on disk: %d + others on node for this disk: %d, disk capacity: %d, disk reserved: %d)",
					diskName, node.Name, longhorn.ErrorReplicaScheduleInsufficientStorage,
					info.StorageAvailable, volume.Spec.Size, info.StorageScheduled, storageScheduledByOtherReplicasOnNode, info.StorageMaximum, info.StorageReserved)
				multiError[errMsg] = struct{}{}
				currentDiskLogCtx.Debug(errMsg)
				continue
			}
		}

		// Check if the Disk's Tags are valid.
		if !types.IsSelectorsInTags(diskSpecFound.Tags, volume.Spec.DiskSelector, allowEmptyDiskSelectorVolume) {
			errMsg := fmt.Sprintf("disk %s on node %s: volume disk selector '%s' not fulfilled by disk tags %v", diskName, node.Name, strings.Join(volume.Spec.DiskSelector, ","), diskSpecFound.Tags)
			multiError[errMsg] = struct{}{}
			currentDiskLogCtx.Debug(errMsg)
			continue
		}

		if volume.Spec.BackingImage != "" && len(biDiskSelector) > 0 {
			if !types.IsSelectorsInTags(diskSpecFound.Tags, biDiskSelector, allowEmptyDiskSelectorVolume) {
				errMsg := fmt.Sprintf("disk %s on node %s: backing image disk selector '%s' not fulfilled by disk tags %v", diskName, node.Name, strings.Join(biDiskSelector, ","), diskSpecFound.Tags)
				multiError[errMsg] = struct{}{}
				currentDiskLogCtx.Debug(errMsg)
				continue
			}
		}

		preferredDisks[diskUUID] = &Disk{
			DiskSpec:   diskSpecFound,
			DiskStatus: diskStatusFound,
			NodeID:     node.Name,
		}
	}
	return preferredDisks, multiError
}

// filterDiskWithMatchingReplicas returns disk that have no matching replicas when diskSoftAntiAffinity is false.
// Otherwise, it returns the input disks map.
func filterDisksWithMatchingReplicas(disks map[string]*Disk, replicas map[string]*longhorn.Replica,
	diskSoftAntiAffinity, ignoreFailedReplicas bool) map[string]*Disk {
	replicasCountPerDisk := map[string]int{}
	for _, r := range replicas {
		if r.Spec.FailedAt != "" {
			if ignoreFailedReplicas {
				continue
			}
			if !IsPotentiallyReusableReplica(r) {
				continue // This replica can never be used again, so it does not count in scheduling decisions.
			}
		}
		replicasCountPerDisk[r.Spec.DiskID]++
	}

	disksByReplicaCount := map[int]map[string]*Disk{}
	for diskUUID, disk := range disks {
		count := replicasCountPerDisk[diskUUID]
		if disksByReplicaCount[count] == nil {
			disksByReplicaCount[count] = map[string]*Disk{}
		}
		disksByReplicaCount[count][diskUUID] = disk
	}

	if len(disksByReplicaCount[0]) > 0 || !diskSoftAntiAffinity {
		return disksByReplicaCount[0]
	}

	return disks
}

func (rcs *ReplicaScheduler) getNodeInfo() (map[string]*longhorn.Node, error) {
	nodeInfo, err := rcs.ds.ListNodes()
	if err != nil {
		return nil, err
	}

	scheduledNode := map[string]*longhorn.Node{}

	for _, node := range nodeInfo {
		if node == nil || node.DeletionTimestamp != nil {
			continue
		}

		nodeReadyCondition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
		nodeSchedulableCondition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeSchedulable)

		if nodeReadyCondition.Status != longhorn.ConditionStatusTrue {
			continue
		}
		if nodeSchedulableCondition.Status != longhorn.ConditionStatusTrue {
			continue
		}
		if !node.Spec.AllowScheduling {
			continue
		}
		scheduledNode[node.Name] = node
	}

	return scheduledNode, nil
}

func (rcs *ReplicaScheduler) scheduleReplicaToDisk(replica *longhorn.Replica, diskCandidates map[string]*Disk, volumeName string) {
	disk := rcs.getDiskWithMostUsableStorage(diskCandidates)
	replica.Spec.NodeID = disk.NodeID
	replica.Spec.DiskID = disk.DiskUUID
	replica.Spec.DiskPath = disk.Path
	replica.Spec.DataDirectoryName = replica.Spec.VolumeName + "-" + util.RandomID()

	logrus.WithFields(logrus.Fields{
		"volume":            volumeName,
		"replica":           replica.Name,
		"node":              replica.Spec.NodeID,
		"diskUUID":          replica.Spec.DiskID,
		"diskPath":          replica.Spec.DiskPath,
		"dataDirectoryName": replica.Spec.DataDirectoryName,
	}).Infof("Replica %s for volume %s successfully scheduled to disk %s (%s) on node %s",
		replica.Name, volumeName, replica.Spec.DiskID, replica.Spec.DiskPath, replica.Spec.NodeID)
}

// Investigate
func (rcs *ReplicaScheduler) getDiskWithMostUsableStorage(disks map[string]*Disk) *Disk {
	var diskWithMostUsableStorage *Disk // Initialize to nil
	// Initialize with the first disk encountered to handle the case of a single candidate
	for _, disk := range disks {
		if diskWithMostUsableStorage == nil {
			diskWithMostUsableStorage = disk
		}
		// Calculate usable storage for the current diskWithMostUsableStorage
		// Ensure diskWithMostUsableStorage is not nil before accessing its fields
		currentMaxUsableStorage := diskWithMostUsableStorage.StorageAvailable - diskWithMostUsableStorage.StorageScheduled - diskWithMostUsableStorage.StorageReserved

		// Calculate usable storage for the current disk in the loop
		candidateDiskUsableStorage := disk.StorageAvailable - disk.StorageScheduled - disk.StorageReserved

		if candidateDiskUsableStorage > currentMaxUsableStorage {
			diskWithMostUsableStorage = disk
		}
	}
	return diskWithMostUsableStorage
}

func filterActiveReplicas(replicas map[string]*longhorn.Replica) map[string]*longhorn.Replica {
	result := map[string]*longhorn.Replica{}
	for _, r := range replicas {
		if r.Spec.Active {
			result[r.Name] = r
		}
	}
	return result
}

func (rcs *ReplicaScheduler) CheckAndReuseFailedReplica(replicas map[string]*longhorn.Replica, volume *longhorn.Volume, hardNodeAffinity string) (*longhorn.Replica, error) {
	logVol := logrus.WithFields(logrus.Fields{"volume": volume.Name, "hardNodeAffinity": hardNodeAffinity})

	if types.IsDataEngineV2(volume.Spec.DataEngine) {
		v2DataEngineFastReplicaRebuilding, err := rcs.ds.GetSettingAsBool(types.SettingNameV2DataEngineFastReplicaRebuilding)
		if err != nil {
			logVol.WithError(err).Warnf("Failed to get the setting %v, will consider it as false", types.SettingDefinitionV2DataEngineFastReplicaRebuilding)
		}
		if !v2DataEngineFastReplicaRebuilding {
			logVol.Infof("Skip checking and reusing replicas since setting %v is not enabled", types.SettingNameV2DataEngineFastReplicaRebuilding)
			return nil, nil
		}
	}

	activeReplicas := filterActiveReplicas(replicas)

	allNodesInfo, err := rcs.getNodeInfo()
	if err != nil {
		return nil, errors.Wrap(err, "CheckAndReuseFailedReplica: failed to get node info")
	}

	var potentiallyReusableReplicas []*longhorn.Replica
	for _, r := range activeReplicas {
		isReusable, errIsReusable := rcs.isFailedReplicaReusable(r, volume, allNodesInfo, hardNodeAffinity)
		if errIsReusable != nil {
			return nil, errors.Wrapf(errIsReusable, "CheckAndReuseFailedReplica: error checking if replica %s is reusable", r.Name)
		}
		if isReusable {
			potentiallyReusableReplicas = append(potentiallyReusableReplicas, r)
		}
	}

	if len(potentiallyReusableReplicas) == 0 {
		logVol.Debugf("CheckAndReuseFailedReplica: No potentially reusable failed replicas found")
		return nil, nil
	}

	var bestOverallReusableReplica *longhorn.Replica = nil

	for _, failedReplicaCandidate := range potentiallyReusableReplicas {
		logRep := logVol.WithField("replicaCandidate", failedReplicaCandidate.Name)
		nodeForCandidate, nodeExists := allNodesInfo[failedReplicaCandidate.Spec.NodeID]
		if !nodeExists {
			logRep.Warnf("Node %s for reusable replica not found in allNodesInfo, skipping", failedReplicaCandidate.Spec.NodeID)
			continue
		}

		nodeSpecificNodeInfo := map[string]*longhorn.Node{
			failedReplicaCandidate.Spec.NodeID: nodeForCandidate,
		}
		nodeSpecificDisksMap := map[string]map[string]struct{}{}
		disksOnThisNode := map[string]struct{}{}
		for diskNameInMapKey, diskStatus := range nodeForCandidate.Status.DiskStatus {
			diskSpec, existsInSpec := nodeForCandidate.Spec.Disks[diskNameInMapKey]
			if !existsInSpec || !diskSpec.AllowScheduling || diskSpec.EvictionRequested ||
				types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeSchedulable).Status != longhorn.ConditionStatusTrue {
				continue
			}
			disksOnThisNode[diskStatus.DiskUUID] = struct{}{}
		}
		nodeSpecificDisksMap[failedReplicaCandidate.Spec.NodeID] = disksOnThisNode

		diskCandidates, mErr := rcs.getDiskCandidates(
			nodeSpecificNodeInfo,
			nodeSpecificDisksMap,
			activeReplicas,
			volume,
			failedReplicaCandidate,
			false,
			true)

		if mErr != nil && len(mErr) > 0 {
			logRep.Warnf("Received non-fatal errors/warnings from getDiskCandidates: %s", mErr.Join())
		}

		if targetDisk, diskIsStillCandidate := diskCandidates[failedReplicaCandidate.Spec.DiskID]; diskIsStillCandidate {
			if targetDisk.DiskUUID == failedReplicaCandidate.Spec.DiskID && targetDisk.NodeID == failedReplicaCandidate.Spec.NodeID {
				if bestOverallReusableReplica == nil {
					bestOverallReusableReplica = failedReplicaCandidate
				} else {
					bestOverallReusableReplica = GetLatestFailedReplica(bestOverallReusableReplica, failedReplicaCandidate)
				}
			} else {
				logRep.Debugf("Disk %s on node %s for failed replica is no longer a valid candidate after getDiskCandidates evaluation", failedReplicaCandidate.Spec.DiskID, failedReplicaCandidate.Spec.NodeID)
			}
		} else {
			logRep.Debugf("Disk %s for failed replica is not among candidates returned by getDiskCandidates", failedReplicaCandidate.Spec.DiskID)
		}
	}

	if bestOverallReusableReplica == nil {
		logVol.Infof("CheckAndReuseFailedReplica: Cannot find a viable disk for any reusable failed replica")
		return nil, nil
	}

	logVol.Infof("CheckAndReuseFailedReplica: Selected replica %s for reuse", bestOverallReusableReplica.Name)
	return bestOverallReusableReplica, nil
}

// RequireNewReplica is used to check if creating new replica immediately is necessary **after a reusable failed replica is not found**.
// If creating new replica immediately is necessary, returns 0.
// Otherwise, returns the duration that the caller should recheck.
// A new replica needs to be created when:
//  1. the volume is a new volume (volume.Status.Robustness is Empty)
//  2. data locality is required (hardNodeAffinity is not Empty and volume.Status.Robustness is Healthy)
//  3. replica eviction happens (volume.Status.Robustness is Healthy)
//  4. there is no potential reusable replica
//  5. there is potential reusable replica but the replica replenishment wait interval is passed.
func (rcs *ReplicaScheduler) RequireNewReplica(replicas map[string]*longhorn.Replica, volume *longhorn.Volume, hardNodeAffinity string) time.Duration {
	if volume.Status.Robustness != longhorn.VolumeRobustnessDegraded {
		return 0
	}
	if hardNodeAffinity != "" {
		return 0
	}

	hasPotentiallyReusableReplica := false
	for _, r := range replicas {
		if IsPotentiallyReusableReplica(r) {
			hasPotentiallyReusableReplica = true
			break
		}
	}
	if !hasPotentiallyReusableReplica {
		return 0
	}

	if types.IsDataEngineV2(volume.Spec.DataEngine) {
		V2DataEngineFastReplicaRebuilding, err := rcs.ds.GetSettingAsBool(types.SettingNameV2DataEngineFastReplicaRebuilding)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to get the setting %v, will consider it as false", types.SettingDefinitionV2DataEngineFastReplicaRebuilding)
			V2DataEngineFastReplicaRebuilding = false
		}
		if !V2DataEngineFastReplicaRebuilding {
			logrus.Infof("Skip checking potentially reusable replicas for volume %v since setting %v is not enabled", volume.Name, types.SettingNameV2DataEngineFastReplicaRebuilding)
			return 0
		}
	}

	timeUntilNext, timeOfNext, err := rcs.timeToReplacementReplica(volume)
	if err != nil {
		msg := "Failed to get time until replica replacement, will directly replenish a new replica"
		logrus.WithError(err).Errorf("%s", msg)
	}
	if timeUntilNext > 0 {
		// Adding another second to the checkBackDuration to avoid clock skew.
		timeUntilNext = timeUntilNext + time.Second
		logrus.Infof("Replica replenishment is delayed until %v", timeOfNext.Add(time.Second))
	}
	return timeUntilNext
}

func (rcs *ReplicaScheduler) isFailedReplicaReusable(r *longhorn.Replica, v *longhorn.Volume, nodeInfo map[string]*longhorn.Node, hardNodeAffinity string) (bool, error) {
	// All failedReusableReplicas are also potentiallyFailedReusableReplicas.
	if !IsPotentiallyReusableReplica(r) {
		return false, nil
	}

	if hardNodeAffinity != "" && r.Spec.NodeID != hardNodeAffinity {
		return false, nil
	}

	if isReady, _ := rcs.ds.CheckDataEngineImageReadiness(r.Spec.Image, r.Spec.DataEngine, r.Spec.NodeID); !isReady {
		return false, nil
	}

	allowEmptyDiskSelectorVolume, err := rcs.ds.GetSettingAsBool(types.SettingNameAllowEmptyDiskSelectorVolume)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get %v setting", types.SettingNameAllowEmptyDiskSelectorVolume)
	}

	node, exists := nodeInfo[r.Spec.NodeID]
	if !exists {
		return false, nil
	}
	diskFound := false
	var replicaDiskSpec longhorn.DiskSpec
	for diskName, diskStatus := range node.Status.DiskStatus {
		currentDiskSpec, ok := node.Spec.Disks[diskName]
		if !ok {
			continue
		}

		if types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeReady).Status != longhorn.ConditionStatusTrue {
			continue
		}
		if types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeSchedulable).Status != longhorn.ConditionStatusTrue {
			// We want to reuse replica on the disk that is unschedulable due to allocated space being bigger than max allocable space but the disk is not full yet
			if types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeSchedulable).Reason != string(longhorn.DiskConditionReasonDiskPressure) {
				continue
			}
			schedulingInfo, err := rcs.GetDiskSchedulingInfo(currentDiskSpec, diskStatus)
			if err != nil {
				logrus.Warnf("failed to GetDiskSchedulingInfo of disk %v on node %v when checking replica %v is reusable: %v", diskName, node.Name, r.Name, err)
			}
			if !rcs.isDiskNotFull(schedulingInfo) {
				continue
			}
		}
		if diskStatus.DiskUUID == r.Spec.DiskID {
			diskFound = true
			replicaDiskSpec, exists = node.Spec.Disks[diskName]
			if !exists {
				return false, nil
			}
			if !replicaDiskSpec.AllowScheduling || replicaDiskSpec.EvictionRequested {
				return false, nil
			}
			if !types.IsSelectorsInTags(replicaDiskSpec.Tags, v.Spec.DiskSelector, allowEmptyDiskSelectorVolume) {
				return false, nil
			}
			// Found the disk and it matches all criteria for reuse of replica r
			break // Exit the disk loop as we found the target disk for replica r
		}
	}
	if !diskFound {
		return false, nil
	}

	im, err := rcs.ds.GetInstanceManagerByInstanceRO(r)
	if err != nil {
		logrus.Errorf("Failed to get instance manager when checking replica %v is reusable: %v", r.Name, err)
		return false, nil
	}
	if im.DeletionTimestamp != nil || im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return false, nil
	}

	return true, nil
}

// IsPotentiallyReusableReplica checks if a failed replica is potentially reusable. A potentially reusable replica means
// this failed replica may be able to reuse it later but it’s not valid now due to node/disk down issue.
func IsPotentiallyReusableReplica(r *longhorn.Replica) bool {
	if r.Spec.FailedAt == "" {
		return false
	}
	if r.Spec.NodeID == "" || r.Spec.DiskID == "" {
		return false
	}
	if r.Spec.RebuildRetryCount >= FailedReplicaMaxRetryCount {
		return false
	}
	if r.Spec.EvictionRequested {
		return false
	}
	return true
}

func GetLatestFailedReplica(rs ...*longhorn.Replica) (res *longhorn.Replica) {
	if rs == nil {
		return nil
	}

	var latestFailedAt time.Time
	for _, r := range rs {
		failedAt, err := util.ParseTime(r.Spec.FailedAt)
		if err != nil {
			logrus.Errorf("Failed to check replica %v failure timestamp %v: %v", r.Name, r.Spec.FailedAt, err)
			continue
		}
		if res == nil || failedAt.After(latestFailedAt) {
			res = r
			latestFailedAt = failedAt
		}
	}
	return res
}

func (rcs *ReplicaScheduler) IsSchedulableToDisk(size int64, requiredStorage int64, info *DiskSchedulingInfo) bool {
	// StorageReserved = the space is already used by 3rd party + the space will be used by 3rd party.
	// StorageAvailable = the space can be used by 3rd party or Longhorn system.
	// There is no (direct) relationship between StorageReserved and StorageAvailable.
	return info.StorageMaximum > 0 && info.StorageAvailable > 0 &&
		info.StorageAvailable-requiredStorage > int64(float64(info.StorageMaximum)*float64(info.MinimalAvailablePercentage)/100) &&
		(size+info.StorageScheduled) <= int64(float64(info.StorageMaximum-info.StorageReserved)*float64(info.OverProvisioningPercentage)/100)
}

func (rcs *ReplicaScheduler) IsSchedulableToDiskConsiderDiskPressure(diskPressurePercentage, size, requiredStorage int64, info *DiskSchedulingInfo) bool {
	log := logrus.WithFields(logrus.Fields{
		"diskUUID":               info.DiskUUID,
		"diskPressurePercentage": diskPressurePercentage,
		"requiredStorage":        requiredStorage,
		"storageScheduled":       info.StorageScheduled,
		"storageReserved":        info.StorageReserved,
		"storageMaximum":         info.StorageMaximum,
	})

	if info.StorageMaximum <= 0 {
		log.Warnf("StorageMaximum is %v, skip evaluating new disk usage", info.StorageMaximum)
		return false
	}

	newDiskUsagePercentage := (requiredStorage + info.StorageScheduled + info.StorageReserved) * 100 / info.StorageMaximum
	log.Debugf("Evaluated new disk usage percentage after scheduling replica: %v%%", newDiskUsagePercentage)

	return rcs.IsSchedulableToDisk(size, requiredStorage, info) &&
		newDiskUsagePercentage < int64(diskPressurePercentage)
}

// IsDiskUnderPressure checks if the disk is under pressure based the provided
// threshold percentage.
func (rcs *ReplicaScheduler) IsDiskUnderPressure(diskPressurePercentage int64, info *DiskSchedulingInfo) bool {
	storageUnusedPercentage := int64(0)
	storageUnused := info.StorageAvailable - info.StorageReserved
	if storageUnused > 0 && info.StorageMaximum > 0 {
		storageUnusedPercentage = storageUnused * 100 / info.StorageMaximum
	}
	return storageUnusedPercentage < 100-int64(diskPressurePercentage)
}

// FilterNodesSchedulableForVolume filters nodes that are schedulable for a given volume based on the disk space.
func (rcs *ReplicaScheduler) FilterNodesSchedulableForVolume(nodes map[string]*longhorn.Node, volume *longhorn.Volume) map[string]*longhorn.Node {
	filteredNodes := map[string]*longhorn.Node{}
	for _, node := range nodes {
		isSchedulable := false

		for diskName, diskStatus := range node.Status.DiskStatus {
			diskSpec, exists := node.Spec.Disks[diskName]
			if !exists {
				continue
			}

			diskInfo, err := rcs.GetDiskSchedulingInfo(diskSpec, diskStatus)
			if err != nil {
				logrus.WithError(err).Debugf("Failed to get disk scheduling info for disk %v on node %v", diskName, node.Name)
				continue
			}

			if rcs.IsSchedulableToDisk(volume.Spec.Size, volume.Status.ActualSize, diskInfo) {
				isSchedulable = true
				break
			}
		}

		if isSchedulable {
			logrus.Tracef("Found node %v schedulable for volume %v", node.Name, volume.Name)
			filteredNodes[node.Name] = node
		}
	}

	if len(filteredNodes) == 0 {
		logrus.Debugf("Found no nodes schedulable for volume %v", volume.Name)
	}
	return filteredNodes
}

func (rcs *ReplicaScheduler) isDiskNotFull(info *DiskSchedulingInfo) bool {
	// StorageAvailable = the space can be used by 3rd party or Longhorn system.
	return info.StorageMaximum > 0 && info.StorageAvailable > 0 &&
		info.StorageAvailable > int64(float64(info.StorageMaximum)*float64(info.MinimalAvailablePercentage)/100)
}

func (rcs *ReplicaScheduler) GetDiskSchedulingInfo(disk longhorn.DiskSpec, diskStatus *longhorn.DiskStatus) (*DiskSchedulingInfo, error) {
	// get StorageOverProvisioningPercentage and StorageMinimalAvailablePercentage settings
	overProvisioningPercentage, err := rcs.ds.GetSettingAsInt(types.SettingNameStorageOverProvisioningPercentage)
	if err != nil {
		return nil, err
	}
	minimalAvailablePercentage, err := rcs.ds.GetSettingAsInt(types.SettingNameStorageMinimalAvailablePercentage)
	if err != nil {
		return nil, err
	}
	info := &DiskSchedulingInfo{
		DiskUUID:                   diskStatus.DiskUUID,
		StorageAvailable:           diskStatus.StorageAvailable,
		StorageScheduled:           diskStatus.StorageScheduled,
		StorageReserved:            disk.StorageReserved,
		StorageMaximum:             diskStatus.StorageMaximum,
		OverProvisioningPercentage: overProvisioningPercentage,
		MinimalAvailablePercentage: minimalAvailablePercentage,
	}
	return info, nil
}

func (rcs *ReplicaScheduler) CheckReplicasSizeExpansion(v *longhorn.Volume, oldSize, newSize int64) (diskScheduleMultiError util.MultiError, err error) {
	defer func() {
		err = errors.Wrapf(err, "error while CheckReplicasSizeExpansion for volume %v", v.Name)
	}()

	replicas, err := rcs.ds.ListVolumeReplicas(v.Name)
	if err != nil {
		return nil, err
	}
	diskIDToReplicaCount := map[string]int64{}
	diskIDToDiskInfo := map[string]*DiskSchedulingInfo{}
	for _, r := range replicas {
		if r.Spec.NodeID == "" {
			continue
		}
		node, err := rcs.ds.GetNode(r.Spec.NodeID)
		if err != nil {
			return nil, err
		}
		diskSpec, diskStatus, ok := findDiskSpecAndDiskStatusInNode(r.Spec.DiskID, node)
		if !ok {
			multiErr := util.NewMultiError()
			multiErr[longhorn.ErrorReplicaScheduleDiskNotFound] = struct{}{}
			return multiErr, fmt.Errorf("cannot find the disk %v in node %v", r.Spec.DiskID, node.Name)
		}
		diskInfo, err := rcs.GetDiskSchedulingInfo(diskSpec, &diskStatus)
		if err != nil {
			multiErr := util.NewMultiError()
			multiErr[longhorn.ErrorReplicaScheduleDiskUnavailable] = struct{}{}
			return multiErr, fmt.Errorf("failed to GetDiskSchedulingInfo %v", err)
		}
		diskIDToDiskInfo[r.Spec.DiskID] = diskInfo
		diskIDToReplicaCount[r.Spec.DiskID] = diskIDToReplicaCount[r.Spec.DiskID] + 1
	}

	expandingSize := newSize - oldSize
	for diskID, diskInfo := range diskIDToDiskInfo {
		requestingSizeExpansionOnDisk := expandingSize * diskIDToReplicaCount[diskID]
		if !rcs.IsSchedulableToDisk(requestingSizeExpansionOnDisk, 0, diskInfo) {
			errMsg := fmt.Sprintf("cannot schedule %v more bytes to disk %v with %+v", requestingSizeExpansionOnDisk, diskID, diskInfo)
			logrus.Error(errMsg) // Keep structured logging for errors
			multiErr := util.NewMultiError()
			multiErr[longhorn.ErrorReplicaScheduleInsufficientStorage] = struct{}{}
			return multiErr, errors.New(errMsg)
		}
	}
	return nil, nil
}

func findDiskSpecAndDiskStatusInNode(diskUUID string, node *longhorn.Node) (longhorn.DiskSpec, longhorn.DiskStatus, bool) {
	for diskName, diskStatus := range node.Status.DiskStatus {
		if diskStatus.DiskUUID == diskUUID {
			diskSpec, exists := node.Spec.Disks[diskName]
			if !exists { // Should not happen if data is consistent
				return longhorn.DiskSpec{}, longhorn.DiskStatus{}, false
			}
			return diskSpec, *diskStatus, true
		}
	}
	return longhorn.DiskSpec{}, longhorn.DiskStatus{}, false
}

// getCurrentNodesAndZones returns the nodes and zones a replica is already scheduled to.
//   - Some callers do not consider a node or zone to be used if it contains a failed replica.
//     ignoreFailedReplicas == true supports this use case.
//   - Otherwise, getCurrentNodesAndZones does not consider a node or zone to be occupied by a failed replica that can
//     no longer be used or is likely actively being replaced. This makes nodes and zones with useless replicas
//     available for scheduling.
func getCurrentNodesAndZones(replicas map[string]*longhorn.Replica, nodeInfo map[string]*longhorn.Node,
	ignoreFailedReplicas, creatingNewReplicasForReplenishment bool) (map[string]*longhorn.Node,
	map[string]bool, map[string]bool, map[string]bool) {
	usedNodes := map[string]*longhorn.Node{}
	usedZones := map[string]bool{}
	onlyEvictingNodes := map[string]bool{}
	onlyEvictingZones := map[string]bool{}

	for _, r := range replicas {
		if r.Spec.NodeID == "" {
			continue
		}
		if r.DeletionTimestamp != nil {
			continue
		}
		if r.Spec.FailedAt != "" {
			if ignoreFailedReplicas {
				continue
			}
			if !IsPotentiallyReusableReplica(r) {
				continue // This replica can never be used again, so it does not count in scheduling decisions.
			}
			if creatingNewReplicasForReplenishment {
				continue // Maybe this replica can be used again, but it is being actively replaced anyway.
			}
		}

		if node, ok := nodeInfo[r.Spec.NodeID]; ok {
			if r.Spec.EvictionRequested {
				if _, ok := usedNodes[r.Spec.NodeID]; !ok {
					// This is an evicting replica on a thus far unused node. We won't change this again unless we
					// find a non-evicting replica on this node.
					onlyEvictingNodes[node.Name] = true
				}
				if used := usedZones[node.Status.Zone]; !used {
					// This is an evicting replica in a thus far unused zone. We won't change this again unless we
					// find a non-evicting replica in this zone.
					onlyEvictingZones[node.Status.Zone] = true
				}
			} else {
				// There is now at least one replica on this node and in this zone that is not evicting.
				onlyEvictingNodes[node.Name] = false
				onlyEvictingZones[node.Status.Zone] = false
			}

			usedNodes[node.Name] = node
			// For empty zone label, we treat them as one zone.
			usedZones[node.Status.Zone] = true
		}
	}

	return usedNodes, usedZones, onlyEvictingNodes, onlyEvictingZones
}

// timeToReplacementReplica returns the amount of time until Longhorn should create a new replica for a degraded volume,
// even if there are potentially reusable failed replicas. It returns 0 if replica-replenishment-wait-interval has
// elapsed and a new replica is needed right now.
func (rcs *ReplicaScheduler) timeToReplacementReplica(volume *longhorn.Volume) (time.Duration, time.Time, error) {
	settingValue, err := rcs.ds.GetSettingAsInt(types.SettingNameReplicaReplenishmentWaitInterval)
	if err != nil {
		err = errors.Wrapf(err, "failed to get setting ReplicaReplenishmentWaitInterval")
		return 0, time.Time{}, err
	}
	waitInterval := time.Duration(settingValue) * time.Second

	lastDegradedAt, err := util.ParseTime(volume.Status.LastDegradedAt)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse last degraded timestamp %v", volume.Status.LastDegradedAt)
		return 0, time.Time{}, err
	}

	now := rcs.nowHandler()
	timeOfNext := lastDegradedAt.Add(waitInterval)
	if now.After(timeOfNext) {
		// A replacement replica is needed now.
		return 0, time.Time{}, nil
	}

	return timeOfNext.Sub(now), timeOfNext, nil
}
