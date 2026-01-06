package pod

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type podMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &podMutator{ds: ds}
}

func (p *podMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "pods",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   corev1.SchemeGroupVersion.Group,
		APIVersion: corev1.SchemeGroupVersion.Version,
		ObjectType: &corev1.Pod{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
		},
	}
}

// Create injects node affinity into pods to prefer nodes with existing volume replicas
func (p *podMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	pod, ok := newObj.(*corev1.Pod)
	if !ok {
		return nil, nil
	}

	// Check if storage-aware pod scheduling is enabled
	enabled, err := p.ds.GetSettingAsBool(types.SettingNameStorageAwarePodScheduling)
	if err != nil {
		logrus.WithError(err).Warn("Failed to get storage-aware pod scheduling setting, skipping mutation")
		return nil, nil
	}
	if !enabled {
		return nil, nil
	}

	// Get Longhorn volumes with best-effort data locality used by this pod
	longhornVolumes, err := p.getLonghornVolumesForPod(pod, request.Namespace)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to get Longhorn volumes for pod %s/%s, skipping mutation", request.Namespace, pod.Name)
		return nil, nil
	}
	if len(longhornVolumes) == 0 {
		return nil, nil
	}

	// Collect nodes with replicas and nodes with capacity
	volumeCountToNodes, err := p.collectSchedulableNodes(longhornVolumes)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to collect schedulable nodes for pod %s/%s, skipping mutation", request.Namespace, pod.Name)
		return nil, nil
	}

	if len(volumeCountToNodes) == 0 {
		return nil, nil
	}

	// Generate patch operations to inject node affinity
	patchOps, err := p.generateAffinityPatch(pod, volumeCountToNodes)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to generate affinity patch for pod %s/%s, skipping mutation", request.Namespace, pod.Name)
		return nil, nil
	}
	logrus.Infof("Injecting storage-aware node affinity into pod %s/%s (nodes by volume count: %v)",
		request.Namespace, pod.Name, volumeCountToNodes)

	return patchOps, nil
}

// getLonghornVolumesForPod returns Longhorn volumes with best-effort data locality referenced by the pod's PVCs
func (p *podMutator) getLonghornVolumesForPod(pod *corev1.Pod, namespace string) ([]*longhorn.Volume, error) {
	var longhornVolumes []*longhorn.Volume

	for _, podVolume := range pod.Spec.Volumes {
		if podVolume.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := podVolume.PersistentVolumeClaim.ClaimName
		pvc, err := p.ds.GetPersistentVolumeClaimRO(namespace, pvcName)
		if err != nil {
			logrus.WithError(err).Debugf("Failed to get PVC %s/%s", namespace, pvcName)
			continue
		}

		// Skip unbound PVCs
		if pvc.Spec.VolumeName == "" {
			continue
		}

		pv, err := p.ds.GetPersistentVolumeRO(pvc.Spec.VolumeName)
		if err != nil {
			logrus.WithError(err).Debugf("Failed to get PV %s", pvc.Spec.VolumeName)
			continue
		}

		// Skip non-Longhorn volumes
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != types.LonghornDriverName {
			continue
		}

		volume, err := p.ds.GetVolumeRO(pv.Spec.CSI.VolumeHandle)
		if err != nil {
			logrus.WithError(err).Debugf("Failed to get Longhorn volume %s", pv.Spec.CSI.VolumeHandle)
			continue
		}

		// Skip volumes without best-effort data locality
		if volume.Spec.DataLocality != longhorn.DataLocalityBestEffort {
			continue
		}

		longhornVolumes = append(longhornVolumes, volume)
	}

	return longhornVolumes, nil
}

// collectSchedulableNodes collects nodes grouped by how many volumes have replicas on them.
// Returns a map where the key is the volume count and value is the list of nodes with that many volumes.
func (p *podMutator) collectSchedulableNodes(volumes []*longhorn.Volume) (map[int][]string, error) {
	volumeCountToNodes := make(map[int][]string)

	// Build map of which volumes have replicas on which nodes
	nodeVolumeMap := make(map[string]map[string]bool)
	for _, vol := range volumes {
		replicas, err := p.ds.ListVolumeReplicasRO(vol.Name)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to list replicas for volume %s", vol.Name)
			continue
		}

		// Track nodes that have a healthy replica for this specific volume
		for _, replica := range replicas {
			if replica.Spec.NodeID != "" && replica.Spec.HealthyAt != "" {
				nodeId := replica.Spec.NodeID
				if nodeVolumeMap[nodeId] == nil {
					nodeVolumeMap[nodeId] = make(map[string]bool)
				}
				nodeVolumeMap[nodeId][vol.Name] = true
			}
		}
	}

	// Check each schedulable node to see if it can accommodate the volumes
	nodes, err := p.ds.ListNodesRO()
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if !node.Spec.AllowScheduling || node.Spec.EvictionRequested {
			continue
		}
		if types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady).Status != longhorn.ConditionStatusTrue {
			continue
		}
		if types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeSchedulable).Status != longhorn.ConditionStatusTrue {
			continue
		}

		if nodeVolumeMap[node.Name] == nil {
			nodeVolumeMap[node.Name] = make(map[string]bool)
		}
		// Determine which volumes would need to be moved to this node
		var volumesToMove []*longhorn.Volume
		for _, vol := range volumes {
			if !nodeVolumeMap[node.Name][vol.Name] {
				volumesToMove = append(volumesToMove, vol)
			}
		}
		// Only include node if it can fit the volumes that need to be moved
		if p.canFitVolumes(node, volumesToMove) {
			volumeCount := len(nodeVolumeMap[node.Name])
			volumeCountToNodes[volumeCount] = append(volumeCountToNodes[volumeCount], node.Name)
		}
	}

	return volumeCountToNodes, nil
}

// canFitVolumes checks if the given volumes can be scheduled on the node.
func (p *podMutator) canFitVolumes(node *longhorn.Node, volumes []*longhorn.Volume) bool {
	if len(volumes) == 0 {
		return true
	}

	// Check node selector constraints
	allowEmptyNodeSelectorVolume, err := p.ds.GetSettingAsBool(types.SettingNameAllowEmptyNodeSelectorVolume)
	if err != nil {
		logrus.WithError(err).Debugf("Failed to get setting %s", types.SettingNameAllowEmptyNodeSelectorVolume)
		return false
	}
	for _, vol := range volumes {
		if !types.IsSelectorsInTags(node.Spec.Tags, vol.Spec.NodeSelector, allowEmptyNodeSelectorVolume) {
			return false
		}
	}

	// Get disk scheduling settings
	allowEmptyDiskSelectorVolume, err := p.ds.GetSettingAsBool(types.SettingNameAllowEmptyDiskSelectorVolume)
	if err != nil {
		logrus.WithError(err).Debugf("Failed to get setting %s", types.SettingNameAllowEmptyDiskSelectorVolume)
		return false
	}
	overProvisioningPercentage, err := p.ds.GetSettingAsInt(types.SettingNameStorageOverProvisioningPercentage)
	if err != nil {
		logrus.WithError(err).Debugf("Failed to get setting %s", types.SettingNameStorageOverProvisioningPercentage)
		return false
	}

	// Sort volumes by size (largest first) for better backtracking pruning
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Spec.Size > volumes[j].Spec.Size
	})

	// Solve bin packing: can we assign each volume to a disk without exceeding capacity?
	diskScheduled := make(map[string]int64)
	var backtrack func(int) bool
	backtrack = func(index int) bool {
		if index == len(volumes) {
			return true
		}

		// Try placing current volume on each compatible disk
		for diskName, diskStatus := range node.Status.DiskStatus {
			diskSpec, ok := node.Spec.Disks[diskName]
			if !ok {
				continue
			}
			if !types.IsSelectorsInTags(diskSpec.Tags, volumes[index].Spec.DiskSelector, allowEmptyDiskSelectorVolume) {
				continue
			}

			overProvisionLimit := ((diskStatus.StorageMaximum - diskSpec.StorageReserved) * overProvisioningPercentage) / 100
			storageSchedulable := overProvisionLimit - diskStatus.StorageScheduled

			if diskScheduled[diskName]+volumes[index].Spec.Size <= storageSchedulable {
				// Try placing this volume on this disk
				diskScheduled[diskName] += volumes[index].Spec.Size
				if backtrack(index + 1) {
					return true
				}
				// Backtrack: undo placement and try next disk
				diskScheduled[diskName] -= volumes[index].Spec.Size
			}
		}

		return false
	}

	return backtrack(0)
}

// generateAffinityPatch generates JSON patch operations to inject node affinity
func (p *podMutator) generateAffinityPatch(pod *corev1.Pod, volumeCountToNodes map[int][]string) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	if len(volumeCountToNodes) == 0 {
		return nil, nil
	}

	// Build preferred scheduling terms (higher volume count = higher weight)
	var preferredTerms []corev1.PreferredSchedulingTerm
	for volumeCount, nodes := range volumeCountToNodes {
		weight := int32((volumeCount + 1) * 10)
		if weight > 100 {
			weight = 100
		}
		preferredTerms = append(preferredTerms, corev1.PreferredSchedulingTerm{
			Weight: weight,
			Preference: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{
					{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   nodes,
					},
				},
			},
		})
	}

	// Build the affinity object
	if pod.Spec.Affinity == nil {
		// No existing affinity, create new one
		affinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: preferredTerms,
			},
		}
		affinityBytes, err := json.Marshal(affinity)
		if err != nil {
			return nil, err
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/affinity", "value": %s}`, string(affinityBytes)))
	} else if pod.Spec.Affinity.NodeAffinity == nil {
		// Has affinity but no node affinity
		nodeAffinity := &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: preferredTerms,
		}
		nodeAffinityBytes, err := json.Marshal(nodeAffinity)
		if err != nil {
			return nil, err
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/affinity/nodeAffinity", "value": %s}`, string(nodeAffinityBytes)))
	} else {
		// Has node affinity, append to existing preferred terms
		existingTerms := pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		newTerms := append(existingTerms, preferredTerms...)
		newTermsBytes, err := json.Marshal(newTerms)
		if err != nil {
			return nil, err
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/affinity/nodeAffinity/preferredDuringSchedulingIgnoredDuringExecution", "value": %s}`, string(newTermsBytes)))
	}

	return patchOps, nil
}
