package csi

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	longhornmeta "github.com/longhorn/longhorn-manager/meta"
)

func TestNeedToUpdatePodAntiAffinity(t *testing.T) {

	for _, test := range []struct {
		testName string
		existing *appsv1.Deployment
		new      *appsv1.Deployment
		expected bool
	}{
		{
			testName: "Should not update pod anti-affinity when either deployments are nil",
			existing: nil,
			new:      sampleSoftAntiAffinityDeployment(),
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when either deployments are nil",
			existing: sampleSoftAntiAffinityDeployment(),
			new:      nil,
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when existing deployment has no annotations and new deployment's preset is soft (default behavior)",
			existing: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
			new:      sampleSoftAntiAffinityDeployment(),
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when existing deployment has nil annotations and new deployment's preset is soft (default behavior)",
			existing: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: nil,
				},
			},
			new:      sampleSoftAntiAffinityDeployment(),
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when existing deployment has no pod anti-affinity annotation and new deployment's preset is soft (default behavior)",
			existing: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"some-other-annotation": "value",
					},
				},
			},
			new:      sampleSoftAntiAffinityDeployment(),
			expected: false,
		},
		{
			testName: "Should update pod anti-affinity when existing deployment has no annotations and new deployment's preset is hard",
			existing: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
			new:      sampleHardAntiAffinityDeployment(),
			expected: true,
		},
		{
			testName: "Should update pod anti-affinity when existing deployment has nil annotations and new deployment's preset is hard",
			existing: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: nil,
				},
			},
			new:      sampleHardAntiAffinityDeployment(),
			expected: true,
		},
		{
			testName: "Should update pod anti-affinity when existing deployment has no pod anti-affinity annotation and new deployment's preset is hard",
			existing: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"some-other-annotation": "value",
					},
				},
			},
			new:      sampleHardAntiAffinityDeployment(),
			expected: true,
		},
		{
			testName: "Should not update pod anti-affinity when new deployment has no annotations and existing deployment has a preset",
			existing: sampleSoftAntiAffinityDeployment(),
			new: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when new deployment has nil annotations and existing deployment has a preset",
			existing: sampleSoftAntiAffinityDeployment(),
			new: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: nil,
				},
			},
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when new deployment has no pod anti-affinity annotation and existing deployment has a preset",
			existing: sampleSoftAntiAffinityDeployment(),
			new: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"some-other-annotation": "value",
					},
				},
			},
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when both deployments have the same anti-affinity (soft)",
			existing: sampleSoftAntiAffinityDeployment(),
			new:      sampleSoftAntiAffinityDeployment(),
			expected: false,
		},
		{
			testName: "Should not update pod anti-affinity when both deployments have the same anti-affinity (hard)",
			existing: sampleHardAntiAffinityDeployment(),
			new:      sampleHardAntiAffinityDeployment(),
			expected: false,
		},
		{
			testName: "Should update pod anti-affinity when existing deployment has soft anti-affinity and new deployment has hard anti-affinity",
			existing: sampleSoftAntiAffinityDeployment(),
			new:      sampleHardAntiAffinityDeployment(),
			expected: true,
		},
		{
			testName: "Should update pod anti-affinity when existing deployment has hard anti-affinity and new deployment has soft anti-affinity",
			existing: sampleHardAntiAffinityDeployment(),
			new:      sampleSoftAntiAffinityDeployment(),
			expected: true,
		},
	} {
		t.Run(test.testName, func(t *testing.T) {

			res := needToUpdatePodAntiAffinity(test.existing, test.new)
			if res != test.expected {
				t.Errorf("expected result: %v, but got: %v", test.expected, res)
			}

		})
	}
}

func sampleSoftAntiAffinityDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationCSIPodAntiAffinityPreset: CSIPodAntiAffinityPresetSoft,
			},
		},
	}
}

func sampleHardAntiAffinityDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationCSIPodAntiAffinityPreset: CSIPodAntiAffinityPresetHard,
			},
		},
	}
}

// TestDeployUsesUpdateFuncInsteadOfDeleteRecreate verifies that deploy() calls
// updateFunc rather than deleteFunc when updateFunc
// is provided and an existing object is found with a different image.
// The existing object's annotations match longhornmeta.GitCommit/Version so
// that the update is triggered only by the image change (needToUpdateImage),
// not by an annotation mismatch.
func TestDeployUsesUpdateFuncInsteadOfDeleteRecreate(t *testing.T) {
	const existingResourceVersion = "rv-42"

	// existing object returned by getFunc — same annotations as the compiled binary
	// but different image, so the update path is triggered by needToUpdateImage
	existing := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "csi-attacher",
			ResourceVersion: existingResourceVersion,
			Annotations: map[string]string{
				AnnotationCSIGitCommit: longhornmeta.GitCommit,
				AnnotationCSIVersion:   longhornmeta.Version,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "csi-attacher", Image: "old-image:v1"},
					},
				},
			},
		},
	}

	// new object we want to deploy — different image; annotations will be overwritten
	// by deploy() with longhornmeta.GitCommit/Version before comparison
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "csi-attacher",
			Annotations: map[string]string{},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "csi-attacher", Image: "new-image:v2"},
					},
				},
			},
		},
	}

	createCalled := false
	deleteCalled := false
	updateCalled := false

	fakeCreate := func(_ *clientset.Clientset, _ runtime.Object) error {
		createCalled = true
		return nil
	}
	fakeDelete := func(_ *clientset.Clientset, _, _ string) error {
		deleteCalled = true
		return nil
	}
	fakeGet := func(_ *clientset.Clientset, _, _ string) (runtime.Object, error) {
		return existing, nil
	}
	fakeUpdate := func(_ *clientset.Clientset, obj runtime.Object) error {
		updateCalled = true
		return nil
	}

	err := deploy(nil, desired, "deployment", fakeCreate, fakeDelete, fakeGet, fakeUpdate)
	if err != nil {
		t.Fatalf("deploy() returned unexpected error: %v", err)
	}
	if !updateCalled {
		t.Error("expected updateFunc to be called, but it was not")
	}
	if deleteCalled {
		t.Error("expected deleteFunc NOT to be called, but it was")
	}
	if createCalled {
		t.Error("expected createFunc NOT to be called, but it was")
	}
}

// TestDeployDeletesAndRecreatesOnAntiAffinityPresetChange verifies that when the
// only change is the pod anti-affinity preset (soft -> hard), deploy() falls back
// to delete + recreate instead of an in-place rolling update. A rolling update
// cannot converge for this change on a constrained cluster (longhorn/longhorn#13546).
func TestDeployDeletesAndRecreatesOnAntiAffinityPresetChange(t *testing.T) {
	// existing deployment: same git commit/version and image, but soft preset
	existing := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "csi-attacher",
			Annotations: map[string]string{
				AnnotationCSIGitCommit:             longhornmeta.GitCommit,
				AnnotationCSIVersion:               longhornmeta.Version,
				AnnotationCSIPodAntiAffinityPreset: CSIPodAntiAffinityPresetSoft,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "csi-attacher", Image: "same-image:v1"},
					},
				},
			},
		},
	}

	// desired deployment: identical except the preset is now hard
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "csi-attacher",
			Annotations: map[string]string{
				AnnotationCSIPodAntiAffinityPreset: CSIPodAntiAffinityPresetHard,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "csi-attacher", Image: "same-image:v1"},
					},
				},
			},
		},
	}

	createCalled := false
	deleteCalled := false
	updateCalled := false

	fakeCreate := func(_ *clientset.Clientset, _ runtime.Object) error {
		createCalled = true
		return nil
	}
	fakeDelete := func(_ *clientset.Clientset, _, _ string) error {
		deleteCalled = true
		return nil
	}
	// Return the existing object until it is deleted, then report NotFound so
	// cleanup()'s WaitForResourceDeletion completes immediately.
	fakeGet := func(_ *clientset.Clientset, name, _ string) (runtime.Object, error) {
		if deleteCalled {
			return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, name)
		}
		return existing, nil
	}
	fakeUpdate := func(_ *clientset.Clientset, _ runtime.Object) error {
		updateCalled = true
		return nil
	}

	err := deploy(nil, desired, "deployment", fakeCreate, fakeDelete, fakeGet, fakeUpdate)
	if err != nil {
		t.Fatalf("deploy() returned unexpected error: %v", err)
	}
	if updateCalled {
		t.Error("expected updateFunc NOT to be called on preset change, but it was")
	}
	if !deleteCalled {
		t.Error("expected deleteFunc to be called on preset change, but it was not")
	}
	if !createCalled {
		t.Error("expected createFunc to be called on preset change, but it was not")
	}
}

func TestNeedToUpdateReplicas(t *testing.T) {
	deploymentWithReplicas := func(r *int32) *appsv1.Deployment {
		return &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: r}}
	}
	two := int32(2)
	three := int32(3)

	for _, test := range []struct {
		testName string
		existing runtime.Object
		new      runtime.Object
		expected bool
	}{
		{
			testName: "Should update when existing has more replicas than desired",
			existing: deploymentWithReplicas(&three),
			new:      deploymentWithReplicas(&two),
			expected: true,
		},
		{
			testName: "Should not update when replica counts match",
			existing: deploymentWithReplicas(&three),
			new:      deploymentWithReplicas(&three),
			expected: false,
		},
		{
			testName: "Should not update when desired replica count is nil",
			existing: deploymentWithReplicas(&three),
			new:      deploymentWithReplicas(nil),
			expected: false,
		},
		{
			testName: "Should update when existing replica count is nil but desired is set",
			existing: deploymentWithReplicas(nil),
			new:      deploymentWithReplicas(&two),
			expected: true,
		},
		{
			testName: "Should not update for non-Deployment objects",
			existing: &appsv1.DaemonSet{},
			new:      &appsv1.DaemonSet{},
			expected: false,
		},
	} {
		t.Run(test.testName, func(t *testing.T) {
			res := needToUpdateReplicas(test.existing, test.new)
			if res != test.expected {
				t.Errorf("expected result: %v, but got: %v", test.expected, res)
			}
		})
	}
}

func TestDeployUpdatesOnReplicaCountChange(t *testing.T) {
	two := int32(2)
	three := int32(3)

	existing := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "csi-attacher",
			Annotations: map[string]string{
				AnnotationCSIGitCommit: longhornmeta.GitCommit,
				AnnotationCSIVersion:   longhornmeta.Version,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &three,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "csi-attacher", Image: "same-image:v1"},
					},
				},
			},
		},
	}

	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "csi-attacher",
			Annotations: map[string]string{},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &two,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "csi-attacher", Image: "same-image:v1"},
					},
				},
			},
		},
	}

	createCalled := false
	deleteCalled := false
	updateCalled := false

	fakeCreate := func(_ *clientset.Clientset, _ runtime.Object) error {
		createCalled = true
		return nil
	}
	fakeDelete := func(_ *clientset.Clientset, _, _ string) error {
		deleteCalled = true
		return nil
	}
	fakeGet := func(_ *clientset.Clientset, _, _ string) (runtime.Object, error) {
		return existing, nil
	}
	fakeUpdate := func(_ *clientset.Clientset, _ runtime.Object) error {
		updateCalled = true
		return nil
	}

	err := deploy(nil, desired, "deployment", fakeCreate, fakeDelete, fakeGet, fakeUpdate)
	if err != nil {
		t.Fatalf("deploy() returned unexpected error: %v", err)
	}
	if !updateCalled {
		t.Error("expected updateFunc to be called on replica count change, but it was not")
	}
	if deleteCalled {
		t.Error("expected deleteFunc NOT to be called, but it was")
	}
	if createCalled {
		t.Error("expected createFunc NOT to be called, but it was")
	}
}
