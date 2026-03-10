package util

import (
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const longhornNamespace = "longhorn-system"

func newTestPod(namespace string, volumes []corev1.Volume) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pod",
			Namespace:   namespace,
			Labels:      map[string]string{"app": "test"},
			Annotations: map[string]string{"note": "test"},
			OwnerReferences: []metav1.OwnerReference{
				{Name: "owner"},
			},
			Finalizers: []string{"finalizer.test"},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kubectl"},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Volumes:  volumes,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

func TestPodTransform(t *testing.T) {
	assert := require.New(t)

	pvcVolume := corev1.Volume{
		Name: "pvc-vol",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "my-pvc",
			},
		},
	}
	ephemeralVolume := corev1.Volume{
		Name: "ephemeral-vol",
		VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{},
		},
	}
	configMapVolume := corev1.Volume{
		Name: "cm-vol",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{},
		},
	}

	tests := []struct {
		name                   string
		pod                    *corev1.Pod
		wantSpecAndStatusEmpty bool
		wantMetaStripped       bool // Labels/Annotations/OwnerRefs/Finalizers cleared
		wantManagedFieldsNil   bool
	}{
		{
			name:                   "longhorn namespace pod is untouched",
			pod:                    newTestPod(longhornNamespace, nil),
			wantSpecAndStatusEmpty: false,
			wantMetaStripped:       false,
			wantManagedFieldsNil:   false,
		},
		{
			name:                   "external pod with PVC volume: only ManagedFields stripped",
			pod:                    newTestPod("other-ns", []corev1.Volume{pvcVolume}),
			wantSpecAndStatusEmpty: false,
			wantMetaStripped:       false,
			wantManagedFieldsNil:   true,
		},
		{
			name:                   "external pod with Ephemeral volume: only ManagedFields stripped",
			pod:                    newTestPod("other-ns", []corev1.Volume{ephemeralVolume}),
			wantSpecAndStatusEmpty: false,
			wantMetaStripped:       false,
			wantManagedFieldsNil:   true,
		},
		{
			name:                   "external pod with PVC and non-PVC volumes: Spec preserved",
			pod:                    newTestPod("other-ns", []corev1.Volume{pvcVolume, configMapVolume}),
			wantSpecAndStatusEmpty: false,
			wantMetaStripped:       false,
			wantManagedFieldsNil:   true,
		},
		{
			name:                   "external pod with non-PVC volume only: Spec/Status/meta cleared",
			pod:                    newTestPod("other-ns", []corev1.Volume{configMapVolume}),
			wantSpecAndStatusEmpty: true,
			wantMetaStripped:       true,
			wantManagedFieldsNil:   true,
		},
		{
			name:                   "external pod with no volumes: Spec/Status/meta cleared",
			pod:                    newTestPod("other-ns", nil),
			wantSpecAndStatusEmpty: true,
			wantMetaStripped:       true,
			wantManagedFieldsNil:   true,
		},
	}

	for _, tc := range tests {
		result := podTransform(tc.pod, longhornNamespace)

		if tc.wantManagedFieldsNil {
			assert.Nil(result.ManagedFields, tc.name+": ManagedFields")
		} else {
			assert.NotNil(result.ManagedFields, tc.name+": ManagedFields")
		}

		if tc.wantSpecAndStatusEmpty {
			assert.Equal(corev1.PodSpec{}, result.Spec, tc.name+": Spec")
			assert.Equal(corev1.PodStatus{}, result.Status, tc.name+": Status")
		} else {
			assert.NotEmpty(result.Spec.NodeName, tc.name+": Spec.NodeName")
			assert.Equal(corev1.PodRunning, result.Status.Phase, tc.name+": Status.Phase")
		}

		if tc.wantMetaStripped {
			assert.Nil(result.Labels, tc.name+": Labels")
			assert.Nil(result.Annotations, tc.name+": Annotations")
			assert.Nil(result.OwnerReferences, tc.name+": OwnerReferences")
			assert.Nil(result.Finalizers, tc.name+": Finalizers")
		} else {
			assert.NotNil(result.Labels, tc.name+": Labels")
			assert.NotNil(result.Annotations, tc.name+": Annotations")
			assert.NotNil(result.OwnerReferences, tc.name+": OwnerReferences")
			assert.NotNil(result.Finalizers, tc.name+": Finalizers")
		}
	}
}

func TestKubeResourceTransform(t *testing.T) {
	assert := require.New(t)

	transform := kubeResourceTransform(longhornNamespace)

	// Pod object is transformed
	pod := newTestPod("other-ns", nil)
	result, err := transform(pod)
	assert.Nil(err)
	resultPod, ok := result.(*corev1.Pod)
	assert.True(ok)
	assert.Equal(corev1.PodSpec{}, resultPod.Spec, "non-Longhorn pod without volumes: Spec cleared")

	// Non-Pod object is passed through unchanged
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
	}
	result, err = transform(node)
	assert.Nil(err)
	assert.Equal(node, result, "non-Pod object passed through unchanged")
}
