package pod

import (
	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
)

const MutatorWebhookName = "pod.mutator.longhorn.io"

// MutatorWebhook constructs the pod mutator webhook configuration.
func MutatorWebhook(secret *corev1.Secret, namespace string) admissionregv1.MutatingWebhook {
	mutationPath := "/v1/webhook/mutation"
	port := int32(types.DefaultAdmissionWebhookPort)
	failPolicyIgnore := admissionregv1.Ignore
	matchPolicyExact := admissionregv1.Exact
	sideEffectClassNone := admissionregv1.SideEffectClassNone
	resource := resource()
	scopeNamespaced := resource.Scope

	return admissionregv1.MutatingWebhook{
		Name: MutatorWebhookName,
		ClientConfig: admissionregv1.WebhookClientConfig{
			Service: &admissionregv1.ServiceReference{
				Namespace: namespace,
				Name:      types.AdmissionWebhookServiceName,
				Path:      &mutationPath,
				Port:      &port,
			},
			CABundle: secret.Data[corev1.TLSCertKey],
		},
		Rules: []admissionregv1.RuleWithOperations{
			{
				Operations: resource.OperationTypes,
				Rule: admissionregv1.Rule{
					APIGroups:   []string{resource.APIGroup},
					APIVersions: []string{resource.APIVersion},
					Resources:   []string{resource.Name},
					Scope:       &scopeNamespaced,
				},
			},
		},
		FailurePolicy:           &failPolicyIgnore,
		MatchPolicy:             &matchPolicyExact,
		SideEffects:             &sideEffectClassNone,
		AdmissionReviewVersions: []string{"v1"},
		ObjectSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      types.GetStorageAwarePodLabelKey(),
					Operator: metav1.LabelSelectorOpExists,
				},
			},
		},
	}
}
