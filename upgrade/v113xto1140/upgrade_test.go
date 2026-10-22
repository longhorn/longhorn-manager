package v113xto1140

import (
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const bimUpgradeNamespace = "longhorn-system"
const bimDiskReadyAt = "2026-10-15T08:00:00Z"

func newBIMPodUpgradeFixture() (*longhorn.BackingImageManager, *longhorn.Node, *corev1.Pod) {
	bim := &longhorn.BackingImageManager{
		Name: "bim-upgrade", Namespace: bimUpgradeNamespace,
		Spec: longhorn.BackingImageManagerSpec{NodeID: "node-1", DiskUUID: "disk-uuid", DiskPath: "/mnt/disk"},
	}
	node := &longhorn.Node{
		Name: bim.Spec.NodeID, Namespace: bimUpgradeNamespace,
		Spec: longhorn.NodeSpec{
			Disks: map[string]longhorn.DiskSpec{"disk-1": {Path: bim.Spec.DiskPath}},
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{
				"disk-1": {
					DiskUUID: bim.Spec.DiskUUID,
					Conditions: []longhorn.Condition{
						{Type: longhorn.DiskConditionTypeReady, Status: longhorn.ConditionStatusTrue, LastTransitionTime: bimDiskReadyAt},
					},
				},
				"nil-disk": nil,
			},
		},
	}
	pod := &corev1.Pod{
		Name: bim.Name, Namespace: bimUpgradeNamespace, UID: "legacy-bim-pod",
		Annotations: map[string]string{"unrelated": "keep"},
		Labels:      types.GetBackingImageManagerLabels(bim.Spec.NodeID, bim.Spec.DiskUUID),
		Spec:        corev1.PodSpec{NodeName: bim.Spec.NodeID},
		Status:      corev1.PodStatus{Phase: corev1.PodRunning},
	}
	return bim, node, pod
}

func TestUpgradeResourcesAnnotatesBackingImageManagerPods(t *testing.T) {
	annotationKey := types.GetLonghornLabelKey(types.BackingImageManagerStartingAtAnnotationKeySuffix)
	for _, tc := range []struct {
		name           string
		configure      func(*longhorn.Node, *corev1.Pod)
		missingPod     bool
		wantAnnotation *string
		wantUpdates    int
	}{
		{name: "legacy running pod", wantAnnotation: new(bimDiskReadyAt), wantUpdates: 1},
		{name: "nil annotations", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations = nil
		}, wantAnnotation: new(bimDiskReadyAt), wantUpdates: 1},
		{name: "already annotated", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations[annotationKey] = bimDiskReadyAt
		}, wantAnnotation: new(bimDiskReadyAt)},
		{name: "stale annotation preserved", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations[annotationKey] = "2026-10-14T08:00:00Z"
		}, wantAnnotation: new("2026-10-14T08:00:00Z")},
		{name: "explicitly empty annotation preserved", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations[annotationKey] = ""
		}, wantAnnotation: new("")},
		{name: "pending pod", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Status.Phase = corev1.PodPending
		}},
		{name: "failed pod", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Status.Phase = corev1.PodFailed
		}},
		{name: "deleting pod", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.DeletionTimestamp = new(metav1.Now())
		}},
		{name: "wrong pod node", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Spec.NodeName = "another-node"
		}},
		{name: "missing node", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Name = "another-node"
		}},
		{name: "missing disk", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			delete(node.Status.DiskStatus, "disk-1")
		}},
		{name: "disk path migrated", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Spec.Disks["disk-1"] = longhorn.DiskSpec{Path: "/mnt/another-path"}
		}},
		{name: "missing pod", missingPod: true},
		{name: "unavailable disk", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Status.DiskStatus["disk-1"].Conditions[0].Status = longhorn.ConditionStatusFalse
		}, wantAnnotation: new(bimDiskReadyAt), wantUpdates: 1},
		{name: "missing transition timestamp", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Status.DiskStatus["disk-1"].Conditions[0].LastTransitionTime = ""
		}, wantAnnotation: new(""), wantUpdates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bim, node, pod := newBIMPodUpgradeFixture()
			if tc.configure != nil {
				tc.configure(node, pod)
			}
			unrelated := &corev1.Pod{
				Name: "unrelated-pod", Namespace: bimUpgradeNamespace,
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			kube := kubefake.NewClientset(unrelated)
			if !tc.missingPod {
				require.NoError(t, kube.Tracker().Add(pod.DeepCopy()))
			}
			resourceMaps := map[string]any{
				types.LonghornKindNode:                map[string]*longhorn.Node{node.Name: node},
				types.LonghornKindBackingImageManager: map[string]*longhorn.BackingImageManager{bim.Name: bim},
			}
			require.NoError(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			checkPod := func() {
				t.Helper()
				actual, err := kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
				if tc.missingPod {
					require.True(t, apierrors.IsNotFound(err))
					return
				}
				require.NoError(t, err)
				value, exists := actual.Annotations[annotationKey]
				if tc.wantAnnotation == nil {
					require.False(t, exists)
				} else {
					require.True(t, exists)
					require.Equal(t, *tc.wantAnnotation, value)
				}
				require.Equal(t, pod.Spec, actual.Spec)
				require.Equal(t, pod.Status, actual.Status)
				require.Equal(t, pod.UID, actual.UID)
				require.Equal(t, pod.Labels, actual.Labels)
				require.Equal(t, pod.Annotations["unrelated"], actual.Annotations["unrelated"])
			}
			checkPod()

			// Upgrade retries must not overwrite an annotation after another disk transition.
			if disk := node.Status.DiskStatus["disk-1"]; disk != nil {
				disk.Conditions[0].Status = longhorn.ConditionStatusTrue
				disk.Conditions[0].LastTransitionTime = "2026-10-16T08:00:00Z"
			}
			require.NoError(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			checkPod()
			updates := 0
			for _, action := range kube.Actions() {
				if action.GetVerb() == "update" {
					updates++
				}
			}
			require.Equal(t, tc.wantUpdates, updates)
			actual, err := kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), unrelated.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Empty(t, actual.Annotations)
		})
	}
}

func TestUpgradeResourcesRetriesBackingImageManagerPodErrors(t *testing.T) {
	for _, verb := range []string{"get", "update"} {
		t.Run(verb, func(t *testing.T) {
			bim, node, pod := newBIMPodUpgradeFixture()
			kube := kubefake.NewClientset(pod)
			fail := true
			kube.PrependReactor(verb, "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				if fail {
					fail = false
					return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name, nil)
				}
				return false, nil, nil
			})
			resourceMaps := map[string]any{
				types.LonghornKindNode:                map[string]*longhorn.Node{node.Name: node},
				types.LonghornKindBackingImageManager: map[string]*longhorn.BackingImageManager{bim.Name: bim},
			}
			require.Error(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			actual, err := kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotContains(t, actual.Annotations, types.GetLonghornLabelKey(types.BackingImageManagerStartingAtAnnotationKeySuffix))
			require.NoError(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			actual, err = kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, bimDiskReadyAt, actual.Annotations[types.GetLonghornLabelKey(types.BackingImageManagerStartingAtAnnotationKeySuffix)])
		})
	}
}

func TestUpgradeResourcesRejectsNilCache(t *testing.T) {
	require.ErrorContains(t, UpgradeResources(bimUpgradeNamespace, nil, nil, nil), "resourceMaps cannot be nil")
}
