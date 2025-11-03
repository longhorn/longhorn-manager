package csi

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
