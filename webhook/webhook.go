package webhook

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/util/client"
	"github.com/longhorn/longhorn-manager/webhook/server"
)

var (
	defaultStartTimeout = 60 * time.Second
)

func StartWebhook(ctx context.Context, webhookType string, clients *client.Clients) error {
	logrus.Infof("Starting longhorn %s webhook server", webhookType)

	var webhookLocalEndpoint string
	switch webhookType {
	case types.WebhookTypeAdmission:
		webhookLocalEndpoint = fmt.Sprintf("https://localhost:%d/v1/healthz", types.DefaultAdmissionWebhookPort)
	case types.WebhookTypeConversion:
		webhookLocalEndpoint = fmt.Sprintf("https://localhost:%d/v1/healthz", types.DefaultConversionWebhookPort)
	default:
		return fmt.Errorf("unexpected webhook server type %v", webhookType)
	}

	s := server.New(ctx, clients.Namespace, webhookType, clients)
	go func() {
		if err := s.ListenAndServe(); err != nil {
			logrus.Fatalf("Error %v webhook server failed: %v", webhookType, err)
		}
	}()

	logrus.Infof("Waiting for %v webhook to become ready", webhookType)
	available, err := isServiceAvailable(webhookType, webhookLocalEndpoint, defaultStartTimeout, clients)
	if !available {
		return errors.Wrapf(err, "%v webhook is not ready on localhost after %v sec", webhookType, defaultStartTimeout)
	}
	logrus.Infof("Started longhorn %s webhook server on localhost", webhookType)
	return nil
}

// CheckWebhookServiceAvailability check if the service is available.
// The server on the host is ready does not mean the service is accessible.
func CheckWebhookServiceAvailability(webhookType string, clients *client.Clients) error {
	webhookServiceEndpoint, err := getWebhookServiceEndpoint(webhookType)
	if err != nil {
		return err
	}

	available, err := isServiceAvailable(webhookType, webhookServiceEndpoint, defaultStartTimeout, clients)
	if !available {
		return errors.Wrapf(err, "%v webhook service is not accessible on cluster after %v sec", webhookType, defaultStartTimeout)

	}
	logrus.Infof("%s webhook service is now accessible", webhookType)
	return nil
}

func isAdmissionConfigurationReady(clients *client.Clients) bool {
	// Check if the webhook configurations are applied and not empty
	validatingWebhookConfig, err := clients.K8s.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), types.ValidatingWebhookName, metav1.GetOptions{})
	if err != nil || len(validatingWebhookConfig.Webhooks) == 0 {
		logrus.WithError(err).Warnf("Validating webhook configuration %s is not ready", types.ValidatingWebhookName)
		return false
	}

	mutatingWebhookConfig, err := clients.K8s.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.Background(), types.MutatingWebhookName, metav1.GetOptions{})
	if err != nil || len(mutatingWebhookConfig.Webhooks) == 0 {
		logrus.WithError(err).Warnf("Mutating webhook configuration %s is not ready", types.MutatingWebhookName)
		return false
	}

	return true
}

func isServiceAvailable(webhookType string, endpoint string, timeout time.Duration, clients *client.Clients) (bool, error) {
	cli := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	running := false
	for start := time.Now(); time.Since(start) < timeout; {
		if webhookType == types.WebhookTypeAdmission {
			if !isAdmissionConfigurationReady(clients) {
				time.Sleep(2 * time.Second)
				continue
			}
		}

		resp, err := cli.Get(endpoint)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to check endpoint %v", endpoint)
		} else if resp.StatusCode == 200 {
			running = true
			break
		} else {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				logrus.Warnf("Endpoint return %d not 200: cannot read the body", resp.StatusCode)
			}
			bodyString := string(bodyBytes)
			logrus.Warnf("Endpoint return %d not 200: %v", resp.StatusCode, bodyString)
		}
		time.Sleep(2 * time.Second)
	}

	if !running {
		return false, fmt.Errorf("timed out waiting for endpoint %v to be available", endpoint)
	}

	return true, nil
}

func getWebhookServiceEndpoint(webhookType string) (string, error) {
	switch webhookType {
	case types.WebhookTypeAdmission:
		return fmt.Sprintf("https://%v.%v.svc:%d/v1/healthz", types.AdmissionWebhookServiceName, util.GetNamespace(types.EnvPodNamespace), types.DefaultAdmissionWebhookPort), nil
	case types.WebhookTypeConversion:
		return fmt.Sprintf("https://%v.%v.svc:%d/v1/healthz", types.ConversionWebhookServiceName, util.GetNamespace(types.EnvPodNamespace), types.DefaultConversionWebhookPort), nil
	default:
		return "", fmt.Errorf("unexpected webhook server type %v", webhookType)
	}
}
