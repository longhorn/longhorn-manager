package snapshothook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/types"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func newRunningPod(name, uid, pvcName string, containerIDs ...string) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID(uid)},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, id := range containerIDs {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{ContainerID: id})
	}
	return pod
}

func TestConsumerPodsUsingPVC(t *testing.T) {
	running := newRunningPod("running", "1", "pvc", "c")
	otherPVC := newRunningPod("other", "2", "other-pvc", "c")
	starting := newRunningPod("starting", "3", "pvc", "c")
	starting.Status.Phase = corev1.PodPending
	scheduled := newRunningPod("scheduled", "6", "pvc")
	scheduled.Status.Phase = corev1.PodPending
	terminating := newRunningPod("terminating", "4", "pvc", "c")
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	terminatingSucceeded := newRunningPod("terminating-succeeded", "5", "pvc", "c")
	terminatingSucceeded.Status.Phase = corev1.PodSucceeded
	terminatingSucceeded.DeletionTimestamp = &now
	pods := []corev1.Pod{running, otherPVC, starting, scheduled, terminating, terminatingSucceeded}

	matched := ConsumerPodsUsingPVC(pods, "pvc")
	assert.Len(t, matched, 2)
	assert.Equal(t, "running", matched[0].Name)
	assert.Equal(t, "starting", matched[1].Name)

	shuttingDown := TerminatingPodsUsingPVC(pods, "pvc")
	assert.Len(t, shuttingDown, 1)
	assert.Equal(t, "terminating", shuttingDown[0].Name)
}

func TestPodIdentityChange(t *testing.T) {
	recorded := []longhorn.HookPodStatus{
		{Namespace: "app", Name: "p1", UID: "u1", ContainerIDs: []string{"c1", "c2"}},
	}
	same := newRunningPod("p1", "u1", "pvc", "c2", "c1")
	replaced := newRunningPod("p1", "u2", "pvc", "c1", "c2")
	restarted := newRunningPod("p1", "u1", "pvc", "c1", "c3")
	extra := newRunningPod("p2", "u9", "pvc", "c9")

	cases := []struct {
		name     string
		live     []*corev1.Pod
		expected string // substring of the reported change; empty means no change
	}{
		{"identical pods", []*corev1.Pod{&same}, ""},
		{"recorded pod gone", nil, "no longer running"},
		{"pod replaced with new uid", []*corev1.Pod{&replaced}, "was replaced"},
		{"container restarted", []*corev1.Pod{&restarted}, "restarted a container"},
		{"new pod appeared", []*corev1.Pod{&same, &extra}, "started during the hook window"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			change := PodIdentityChange(recorded, tc.live)
			if tc.expected == "" {
				assert.Empty(t, change)
			} else {
				assert.Contains(t, change, tc.expected)
			}
		})
	}
}
