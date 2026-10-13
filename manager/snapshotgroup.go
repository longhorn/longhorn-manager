package manager

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (m *VolumeManager) GetSnapshotGroup(name string) (*longhorn.SnapshotGroup, error) {
	return m.ds.GetSnapshotGroup(name)
}

func (m *VolumeManager) ListSnapshotGroupsSorted() ([]*longhorn.SnapshotGroup, error) {
	snapshotGroupMap, err := m.ds.ListSnapshotGroups()
	if err != nil {
		return []*longhorn.SnapshotGroup{}, err
	}

	snapshotGroups := make([]*longhorn.SnapshotGroup, len(snapshotGroupMap))
	snapshotGroupNames, err := util.SortKeys(snapshotGroupMap)
	if err != nil {
		return []*longhorn.SnapshotGroup{}, err
	}
	for i, name := range snapshotGroupNames {
		snapshotGroups[i] = snapshotGroupMap[name]
	}
	return snapshotGroups, nil
}

func (m *VolumeManager) CreateSnapshotGroup(name string, spec *longhorn.SnapshotGroupSpec) (*longhorn.SnapshotGroup, error) {
	// No name auto-correction: the name is identity (member snapshot names
	// embed it) and the webhook is the single validator.
	snapshotGroup := &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: *spec,
	}

	snapshotGroup, err := m.ds.CreateSnapshotGroup(snapshotGroup)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created snapshot group %v", name)
	return snapshotGroup, nil
}

// ResolveSnapshotGroupMemberCandidates previews the member set a snapshot
// group spec would select.
func (m *VolumeManager) ResolveSnapshotGroupMemberCandidates(spec *longhorn.SnapshotGroupSpec) ([]datastore.SnapshotGroupMemberCandidate, error) {
	return m.ds.ResolveSnapshotGroupMemberCandidates(spec)
}

// SnapshotGroupMemberHookPreview is what admission would decide for one
// member of a hooked group: either the member's hook setup, or the message
// and reason admission would reject it with, plus the warnings for an
// accepted member.
type SnapshotGroupMemberHookPreview struct {
	PVCRef        *longhorn.PVCRef
	HookPolicyRef *longhorn.VolumeHookPolicyRef
	// HookPolicy is the resolved policy, set only when it has a hook for
	// the group's operation. The caller uses it to show which hook and
	// container the run would pick for each pod.
	HookPolicy              *longhorn.VolumeHookPolicy
	WorkloadPods            []string
	ValidationFailure       string
	ValidationFailureReason snapshothook.AdmissionFailureReason
	Warnings                []string
}

// PreviewSnapshotGroupMemberHook runs the hook checks admission runs for
// one member volume and reports the first failing check in the preview.
func (m *VolumeManager) PreviewSnapshotGroupMemberHook(volumeName string, operation longhorn.HookOperation) (*SnapshotGroupMemberHookPreview, error) {
	volume, err := m.ds.GetVolumeRO(volumeName)
	if err != nil {
		return nil, err
	}

	preview := &SnapshotGroupMemberHookPreview{
		WorkloadPods: snapshothook.RunningWorkloadPods(volume),
	}
	// Policy resolution only recognizes a bound PVC.
	if namespace, pvcName, bound := snapshothook.BoundPVC(volume); bound {
		preview.PVCRef = &longhorn.PVCRef{Namespace: namespace, Name: pvcName}
	}

	policy, err := m.ds.ResolveVolumeHookPolicyForVolume(volume)
	if err != nil {
		var resolution *snapshothook.PolicyResolutionError
		if !errors.As(err, &resolution) {
			return nil, err
		}
		preview.ValidationFailure = resolution.Message
		preview.ValidationFailureReason = resolution.Reason
		return preview, nil
	}
	preview.HookPolicyRef = snapshothook.PolicyRef(policy)

	if !snapshothook.PolicyCoversOperation(policy, operation) {
		preview.ValidationFailure = snapshothook.NoHookForOperationMessage(policy, operation, volumeName)
		preview.ValidationFailureReason = snapshothook.AdmissionFailureNoHookForOperation
		return preview, nil
	}
	preview.HookPolicy = policy

	groups, err := m.ds.ListSnapshotGroupsRO()
	if err != nil {
		return nil, err
	}
	failure, warnings := snapshothook.CheckMemberClusterState(volume, snapshothook.PolicyNamespacedName(policy), groups, m.ds.Namespace())
	if failure != nil {
		preview.ValidationFailure = failure.Message
		preview.ValidationFailureReason = failure.Reason
		return preview, nil
	}
	preview.Warnings = warnings
	return preview, nil
}

// VerifyHookExecutorAccess runs admission's access checks for a member
// namespace and returns any denial.
func (m *VolumeManager) VerifyHookExecutorAccess(namespace string) (*hookexecutor.AccessDenial, error) {
	return m.ds.VerifyHookExecutorAccess(namespace)
}

// ListOutdatedManagerPods lists the manager pods not yet replaced by an
// ongoing rollout.
func (m *VolumeManager) ListOutdatedManagerPods() ([]string, error) {
	return m.ds.ListOutdatedManagerPods()
}

func (m *VolumeManager) GetWorkloadPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	return m.ds.GetWorkloadPod(ctx, namespace, name)
}

func (m *VolumeManager) DeleteSnapshotGroup(name string) error {
	if err := m.ds.DeleteSnapshotGroup(name); err != nil {
		return err
	}
	logrus.Infof("Deleted snapshot group %v", name)
	return nil
}
