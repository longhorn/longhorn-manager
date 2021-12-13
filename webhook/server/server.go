package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rancher/dynamiclistener"
	"github.com/rancher/dynamiclistener/server"
	"github.com/sirupsen/logrus"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"
	"github.com/longhorn/longhorn-manager/webhook/client"
)

const (
	WebhookServiceName = "longhorn-webhook"

	caName   = "longhorn-webhook-ca"
	certName = "longhorn-webhook-tls"
)

var (
	validationPath = "/v1/webhook/" + admission.AdmissionTypeValidation
	mutationPath   = "/v1/webhook/" + admission.AdmissionTypeMutation

	failPolicyFail   = admissionregv1.Fail
	failPolicyIgnore = admissionregv1.Ignore

	sideEffectClassNone = admissionregv1.SideEffectClassNone
)

type WebhookServer struct {
	context   context.Context
	cfg       *rest.Config
	namespace string
}

func New(ctx context.Context, cfg *rest.Config, namespace string) *WebhookServer {
	return &WebhookServer{
		context:   ctx,
		cfg:       cfg,
		namespace: namespace,
	}
}

func (s *WebhookServer) ListenAndServe() error {
	client, err := client.New(s.context, s.cfg, s.namespace)
	if err != nil {
		return err
	}

	validationHandler, validationResources, err := Validation(client)
	if err != nil {
		return err
	}
	mutationHandler, mutationResources, err := Mutation(client)
	if err != nil {
		return err
	}

	router := mux.NewRouter()
	router.Handle(validationPath, validationHandler)
	router.Handle(mutationPath, mutationHandler)
	if err := s.listenAndServe(client, router, validationResources, mutationResources); err != nil {
		return err
	}

	if err := client.Start(s.context); err != nil {
		return err
	}
	return nil
}

func (s *WebhookServer) listenAndServe(client *client.Client, handler http.Handler, validationResources []admission.Resource, mutationResources []admission.Resource) error {
	apply := client.Apply.WithDynamicLookup()
	client.Core.Secret().OnChange(s.context, "secrets", func(key string, secret *corev1.Secret) (*corev1.Secret, error) {
		if secret == nil || secret.Name != caName || secret.Namespace != s.namespace || len(secret.Data[corev1.TLSCertKey]) == 0 {
			return nil, nil
		}
		logrus.Info("Sleeping for 15 seconds then applying webhook config")
		// Sleep here to make sure server is listening and all caches are primed
		time.Sleep(15 * time.Second)

		logrus.Debugf("Building validation rules...")
		validationRules := s.buildRules(validationResources)
		logrus.Debugf("Building mutation rules...")
		mutationRules := s.buildRules(mutationResources)

		port := int32(types.DefaultWebhookServerPort)
		validatingWebhookConfiguration := &admissionregv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: types.ValidatingWebhookName,
			},
			Webhooks: []admissionregv1.ValidatingWebhook{
				{
					Name: "validator.longhorn.io",
					ClientConfig: admissionregv1.WebhookClientConfig{
						Service: &admissionregv1.ServiceReference{
							Namespace: s.namespace,
							Name:      WebhookServiceName,
							Path:      &validationPath,
							Port:      &port,
						},
						CABundle: secret.Data[corev1.TLSCertKey],
					},
					Rules:                   validationRules,
					FailurePolicy:           &failPolicyFail,
					SideEffects:             &sideEffectClassNone,
					AdmissionReviewVersions: []string{"v1", "v1beta1"},
				},
			},
		}

		mutatingWebhookConfiguration := &admissionregv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: types.MutatingWebhookName,
			},
			Webhooks: []admissionregv1.MutatingWebhook{
				{
					Name: "mutator.longhorn.io",
					ClientConfig: admissionregv1.WebhookClientConfig{
						Service: &admissionregv1.ServiceReference{
							Namespace: s.namespace,
							Name:      WebhookServiceName,
							Path:      &mutationPath,
							Port:      &port,
						},
						CABundle: secret.Data[corev1.TLSCertKey],
					},
					Rules:                   mutationRules,
					FailurePolicy:           &failPolicyIgnore,
					SideEffects:             &sideEffectClassNone,
					AdmissionReviewVersions: []string{"v1", "v1beta1"},
				},
			},
		}

		return secret, apply.WithOwner(secret).ApplyObjects(validatingWebhookConfiguration, mutatingWebhookConfiguration)
	})

	tlsName := fmt.Sprintf("%s.%s.svc", WebhookServiceName, s.namespace)

	return server.ListenAndServe(s.context, types.DefaultWebhookServerPort, 0, handler, &server.ListenOpts{
		Secrets:       client.Core.Secret(),
		CertNamespace: s.namespace,
		CertName:      certName,
		CAName:        caName,
		TLSListenerConfig: dynamiclistener.Config{
			SANs: []string{
				tlsName,
			},
			FilterCN: dynamiclistener.OnlyAllow(tlsName),
		},
	})
}

func (s *WebhookServer) buildRules(resources []admission.Resource) []admissionregv1.RuleWithOperations {
	rules := []admissionregv1.RuleWithOperations{}
	for _, rsc := range resources {
		logrus.Debugf("Add rule for %+v", rsc)
		scope := rsc.Scope
		rules = append(rules, admissionregv1.RuleWithOperations{
			Operations: rsc.OperationTypes,
			Rule: admissionregv1.Rule{
				APIGroups:   []string{rsc.APIGroup},
				APIVersions: []string{rsc.APIVersion},
				Resources:   []string{rsc.Name},
				Scope:       &scope,
			},
		})
	}

	return rules
}
