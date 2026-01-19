package pod

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/longhorn/longhorn-manager/util"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const AnnSelectedNode = "volume.kubernetes.io/selected-node"

type podMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &podMutator{ds: ds}
}

func resource() admission.Resource {
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

func (p *podMutator) Resource() admission.Resource {
	return resource()
}

// Create injects node affinity into pods to prefer nodes with existing volume replicas
func (p *podMutator) Create(request *admission.Request, newObj runtime.Object) (patchOps admission.PatchOps, err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("Panic in pod scheduling mutator: %v, skipping mutation", r)
			patchOps = nil
			err = nil
		}
	}()

	pod, ok := newObj.(*corev1.Pod)
	if !ok {
		return nil, nil
	}

	// Get Longhorn volumes with best-effort data locality used by this pod
	longhornVolumes, selectedNode, err := p.getLonghornVolumesForPod(pod, request.Namespace)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to get Longhorn volumes for pod %s/%s, skipping mutation", request.Namespace, pod.Name)
		return nil, nil
	}
	if len(longhornVolumes) == 0 {
		return nil, nil
	}

	// Find the best node for scheduling this pod based on volume replica locations
	zone := p.getNodeZone(selectedNode)
	targetNode, err := p.findNodeForVolumes(longhornVolumes, zone)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to find best node for pod %s/%s, skipping mutation", request.Namespace, pod.Name)
		return nil, nil
	}
	if targetNode == "" {
		return nil, nil
	}

	// Generate patch operations to inject required node affinity
	patchOps, err = p.generateAffinityPatch(pod, targetNode)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to generate affinity patch for pod %s/%s, skipping mutation", request.Namespace, pod.Name)
		return nil, nil
	}
	logrus.Infof("Injecting required node affinity for pod %s/%s to node %s",
		request.Namespace, pod.Name, targetNode)

	return patchOps, nil
}

// getLonghornVolumesForPod returns Longhorn volumes with best-effort data locality referenced by the pod's PVCs,
// along with the node name from the PVC's selected-node annotation (used for zone-aware scheduling).
func (p *podMutator) getLonghornVolumesForPod(pod *corev1.Pod, namespace string) ([]*longhorn.Volume, string, error) {
	var longhornVolumes []*longhorn.Volume
	var selectedNodeName string

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

		// Get node where pod was originally scheduled
		pvcSelectedNode, ok := pvc.Annotations[AnnSelectedNode]
		if ok {
			selectedNodeName = pvcSelectedNode
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

	return longhornVolumes, selectedNodeName, nil
}

// getNodeZone returns the zone label of the given node, or empty string if not found.
func (p *podMutator) getNodeZone(nodeName string) string {
	if nodeName == "" {
		return ""
	}
	kubeNode, err := p.ds.GetKubernetesNodeRO(nodeName)
	if err != nil {
		logrus.WithError(err).Debugf("Failed to get Kubernetes node %s for zone lookup", nodeName)
		return ""
	}
	return kubeNode.Labels[corev1.LabelTopologyZone]
}

// findNodeForVolumes finds a suitable node for scheduling a pod with the given volumes.
// It prioritizes nodes that already have replicas for the volumes (sorted by replica count descending),
// then falls back to any schedulable node in the same zone.
// Returns the node name or empty string if no suitable node is found.
func (p *podMutator) findNodeForVolumes(volumes []*longhorn.Volume, zone string) (string, error) {
	allowEmptyNodeSelectorVolume, err := p.ds.GetSettingAsBool(types.SettingNameAllowEmptyNodeSelectorVolume)
	if err != nil {
		return "", err
	}
	allowEmptyDiskSelectorVolume, err := p.ds.GetSettingAsBool(types.SettingNameAllowEmptyDiskSelectorVolume)
	if err != nil {
		return "", err
	}
	overProvisioningPercentage, err := p.ds.GetSettingAsInt(types.SettingNameStorageOverProvisioningPercentage)
	if err != nil {
		return "", err
	}

	// Build map of which volumes have replicas on which nodes
	nodeVolumeMap := make(map[string]map[string]bool)
	for _, volume := range volumes {
		replicas, err := p.ds.ListVolumeReplicasRO(volume.Name)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to list replicas for volume %s", volume.Name)
			continue
		}

		// Track nodes that have a healthy replica for this specific volume
		for _, replica := range replicas {
			if replica.Spec.NodeID != "" && replica.Spec.HealthyAt != "" {
				nodeId := replica.Spec.NodeID
				if nodeVolumeMap[nodeId] == nil {
					nodeVolumeMap[nodeId] = make(map[string]bool)
				}
				nodeVolumeMap[nodeId][volume.Name] = true
			}
		}
	}
	nodesSortedByVolumeCount, err := util.SortKeysByValueLen(nodeVolumeMap, false)
	if err != nil {
		return "", err
	}
	for _, nodeName := range nodesSortedByVolumeCount {
		var volumesToMove []*longhorn.Volume
		for _, volume := range volumes {
			if !nodeVolumeMap[nodeName][volume.Name] {
				volumesToMove = append(volumesToMove, volume)
			}
		}
		node, err := p.ds.GetNodeRO(nodeName)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to get node %s", nodeName)
			continue
		}
		if p.canScheduleVolumesOnNode(node, volumesToMove, zone, allowEmptyNodeSelectorVolume, allowEmptyDiskSelectorVolume, overProvisioningPercentage) {
			return nodeName, nil
		}
	}

	// Fall back to any schedulable node in this zone
	nodes, err := p.ds.ListNodesRO()
	if err != nil {
		return "", err
	}
	for _, node := range nodes {
		if _, ok := nodeVolumeMap[node.Name]; ok {
			continue
		}
		if p.canScheduleVolumesOnNode(node, volumes, zone, allowEmptyNodeSelectorVolume, allowEmptyDiskSelectorVolume, overProvisioningPercentage) {
			return node.Name, nil
		}
	}

	return "", nil
}

// canScheduleVolumesOnNode checks if the given volumes can be scheduled on the node.
func (p *podMutator) canScheduleVolumesOnNode(node *longhorn.Node, volumes []*longhorn.Volume, zone string, allowEmptyNodeSelectorVolume, allowEmptyDiskSelectorVolume bool, overProvisioningPercentage int64) bool {
	if !node.Spec.AllowScheduling || node.Spec.EvictionRequested {
		return false
	}
	if types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady).Status != longhorn.ConditionStatusTrue {
		return false
	}
	if types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeSchedulable).Status != longhorn.ConditionStatusTrue {
		return false
	}
	if node.Status.Zone != zone {
		return false
	}

	// Check node selector constraints
	for _, volume := range volumes {
		if !types.IsSelectorsInTags(node.Spec.Tags, volume.Spec.NodeSelector, allowEmptyNodeSelectorVolume) {
			return false
		}
	}

	// Sort volumes by size (largest first) for better backtracking pruning
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Spec.Size > volumes[j].Spec.Size
	})

	// Solve bin packing: can we assign each volume to a disk without exceeding max?
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

// generateAffinityPatch generates JSON patch operations to inject required node affinity
// that pins the pod to the specified target node.
func (p *podMutator) generateAffinityPatch(pod *corev1.Pod, targetNode string) (admission.PatchOps, error) {
	var patchOps admission.PatchOps

	if targetNode == "" {
		return nil, nil
	}

	// Build required node selector term for the target node
	requiredTerm := corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{
			{
				Key:      corev1.LabelHostname,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{targetNode},
			},
		},
	}

	if pod.Spec.Affinity == nil {
		// No existing affinity, create new one with required node affinity
		affinity := &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{requiredTerm},
				},
			},
		}
		affinityBytes, err := json.Marshal(affinity)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal affinity: %v", err)
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/affinity", "value": %s}`, string(affinityBytes)))
	} else if pod.Spec.Affinity.NodeAffinity == nil {
		// Has affinity but no node affinity
		nodeAffinity := &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{requiredTerm},
			},
		}
		nodeAffinityBytes, err := json.Marshal(nodeAffinity)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal node affinity: %v", err)
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/affinity/nodeAffinity", "value": %s}`, string(nodeAffinityBytes)))
	} else if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		// Has node affinity but no required terms
		nodeSelector := &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{requiredTerm},
		}
		nodeSelectorBytes, err := json.Marshal(nodeSelector)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal node selector: %v", err)
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "add", "path": "/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution", "value": %s}`, string(nodeSelectorBytes)))
	} else {
		// Has required terms, append our term
		existingTerms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		newTerms := append(existingTerms, requiredTerm)
		newTermsBytes, err := json.Marshal(newTerms)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal node selector terms: %v", err)
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms", "value": %s}`, string(newTermsBytes)))
	}

	return patchOps, nil
}
