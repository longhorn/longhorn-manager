package server

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/jrhouston/k8slock"
	"github.com/rancher/dynamiclistener"
	"github.com/rancher/dynamiclistener/server"
	"github.com/sirupsen/logrus"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util/client"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	webhooktypes "github.com/longhorn/longhorn-manager/webhook/types"
)

var (
	validationPath = "/v1/webhook/" + admission.AdmissionTypeValidation
	mutationPath   = "/v1/webhook/" + admission.AdmissionTypeMutation
	conversionPath = "/v1/webhook/conversion"

	failPolicyFail = admissionregv1.Fail

	matchPolicyExact = admissionregv1.Exact // nolint: unused

	sideEffectClassNone = admissionregv1.SideEffectClassNone
)

type WebhookServer struct {
	context     context.Context
	namespace   string
	webhookType string
	clients     *client.Clients
	lockers     map[string]*k8slock.Locker
}

func New(ctx context.Context, namespace, nodeName, webhookType string, clients *client.Clients) (*WebhookServer, error) {
	lockerNames := []string{
		webhooktypes.PascalToKebab(types.LonghornKindDataEngineUpgradeManager),
		webhooktypes.PascalToKebab(types.LonghornKindNodeDataEngineUpgrade),
	}
	lockers := map[string]*k8slock.Locker{}

	for _, name := range lockerNames {
		clientID := nodeName + "_" + uuid.New().String()

		logrus.Infof("Creating locker for resource %v with clientID %v", name, clientID)
		locker, err := k8slock.NewLocker(
			name,
			k8slock.Namespace(namespace),
			k8slock.ClientID(clientID),
			k8slock.InClusterConfig(),
			k8slock.TTL(180*time.Second),
		)

		if err != nil {
			return nil, err
		}
		lockers[name] = locker
	}

	return &WebhookServer{
		context:     ctx,
		namespace:   namespace,
		webhookType: webhookType,
		clients:     clients,
		lockers:     lockers,
	}, nil
}

func (s *WebhookServer) admissionWebhookListenAndServe() error {
	validationHandler, validationResources, err := Validation(s.clients.Datastore, s.lockers)
	if err != nil {
		return err
	}
	mutationHandler, mutationResources, err := Mutation(s.clients.Datastore)
	if err != nil {
		return err
	}

	router := mux.NewRouter()

	router.Handle("/v1/healthz", newhealthzHandler())
	router.Handle(validationPath, validationHandler)
	router.Handle(mutationPath, mutationHandler)
	if err := s.runAdmissionWebhookListenAndServe(router, validationResources, mutationResources); err != nil {
		return err
	}

	return s.clients.Start(s.context)
}

func (s *WebhookServer) conversionWebhookListenAndServe() error {
	conversionHandler, conversionResources, err := Conversion()
	if err != nil {
		return err
	}

	router := mux.NewRouter()

	router.Handle("/v1/healthz", newhealthzHandler())
	router.Handle(conversionPath, conversionHandler)
	if err := s.runConversionWebhookListenAndServe(router, conversionResources); err != nil {
		return err
	}

	return s.clients.Start(s.context)
}

func (s *WebhookServer) ListenAndServe() error {
	switch webhookType := s.webhookType; webhookType {
	case "admission":
		return s.admissionWebhookListenAndServe()
	case "conversion":
		return s.conversionWebhookListenAndServe()
	default:
		return fmt.Errorf("unexpected webhook server type %v", webhookType)
	}
}

func (s *WebhookServer) runAdmissionWebhookListenAndServe(handler http.Handler, validationResources []admission.Resource, mutationResources []admission.Resource) error {
	apply := s.clients.Apply.WithDynamicLookup()
	s.clients.Core.Secret().OnChange(s.context, "secrets", func(key string, secret *corev1.Secret) (*corev1.Secret, error) {
		if secret == nil || secret.Name != types.CaName || secret.Namespace != s.namespace || len(secret.Data[corev1.TLSCertKey]) == 0 {
			return nil, nil
		}

		port := int32(types.DefaultAdmissionWebhookPort)

		logrus.Info("Building validation rules...")
		validationRules := s.buildRules(validationResources)
		logrus.Info("Building mutation rules...")
		mutationRules := s.buildRules(mutationResources)

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
							Name:      types.AdmissionWebhookServiceName,
							Path:      &validationPath,
							Port:      &port,
						},
						CABundle: secret.Data[corev1.TLSCertKey],
					},
					Rules:                   validationRules,
					FailurePolicy:           &failPolicyFail,
					MatchPolicy:             &matchPolicyExact,
					SideEffects:             &sideEffectClassNone,
					AdmissionReviewVersions: []string{"v1"},
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
							Name:      types.AdmissionWebhookServiceName,
							Path:      &mutationPath,
							Port:      &port,
						},
						CABundle: secret.Data[corev1.TLSCertKey],
					},
					Rules:                   mutationRules,
					FailurePolicy:           &failPolicyFail,
					MatchPolicy:             &matchPolicyExact,
					SideEffects:             &sideEffectClassNone,
					AdmissionReviewVersions: []string{"v1"},
				},
			},
		}

		return secret, apply.WithOwner(secret).ApplyObjects(validatingWebhookConfiguration, mutatingWebhookConfiguration)
	})

	tlsName := fmt.Sprintf("%s.%s.svc", types.AdmissionWebhookServiceName, s.namespace)

	return server.ListenAndServe(s.context, types.DefaultAdmissionWebhookPort, 0, handler, &server.ListenOpts{
		Secrets:       s.clients.Core.Secret(),
		CertNamespace: s.namespace,
		CertName:      types.CertName,
		CAName:        types.CaName,
		TLSListenerConfig: dynamiclistener.Config{
			SANs: []string{
				tlsName,
			},
			FilterCN: dynamiclistener.OnlyAllow(tlsName),
		},
	})
}

func (s *WebhookServer) runConversionWebhookListenAndServe(handler http.Handler, conversionResources []string) error {
	s.clients.Core.Secret().OnChange(s.context, "secrets", func(key string, secret *corev1.Secret) (*corev1.Secret, error) {
		if secret == nil || secret.Name != types.CaName || secret.Namespace != s.namespace || len(secret.Data[corev1.TLSCertKey]) == 0 {
			return nil, nil
		}

		port := int32(types.DefaultConversionWebhookPort)

		logrus.Infof("Building conversion rules...")
		for _, name := range conversionResources {
			crd, err := s.clients.CRD.CustomResourceDefinition().Get(name, metav1.GetOptions{})
			if err != nil {
				return secret, err
			}

			existingCRD := crd.DeepCopy()
			crd.Spec.Conversion = &apiextv1.CustomResourceConversion{
				Strategy: apiextv1.WebhookConverter,
				Webhook: &apiextv1.WebhookConversion{
					ClientConfig: &apiextv1.WebhookClientConfig{
						Service: &apiextv1.ServiceReference{
							Namespace: s.namespace,
							Name:      types.ConversionWebhookServiceName,
							Path:      &conversionPath,
							Port:      &port,
						},
						CABundle: secret.Data[corev1.TLSCertKey],
					},
					ConversionReviewVersions: []string{"v1beta2", "v1beta1"},
				},
			}

			if !reflect.DeepEqual(existingCRD, crd) {
				logrus.Infof("Update CRD for %+v", name)
				if _, err = s.clients.CRD.CustomResourceDefinition().Update(crd); err != nil {
					return secret, err
				}
			}
		}

		return secret, nil
	})

	tlsName := fmt.Sprintf("%s.%s.svc", types.ConversionWebhookServiceName, s.namespace)

	return server.ListenAndServe(s.context, types.DefaultConversionWebhookPort, 0, handler, &server.ListenOpts{
		Secrets:       s.clients.Core.Secret(),
		CertNamespace: s.namespace,
		CertName:      types.CertName,
		CAName:        types.CaName,
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
