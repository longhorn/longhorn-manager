package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/webhook/server"
)

var (
	defaultStartTimeout = 60 * time.Second
)

func startWebhook(ctx context.Context, serviceAccount, kubeconfigPath, webhookType string) error {
	logrus.Infof("Starting longhorn %s webhook server", webhookType)

	var webhookPort int
	switch webhookType {
	case types.WebhookTypeAdmission:
		webhookPort = types.DefaultAdmissionWebhookPort
	case types.WebhookTypeConversion:
		webhookPort = types.DefaultConversionWebhookPort
	default:
		return fmt.Errorf("unexpected webhook server type %v", webhookType)
	}

	namespace := util.GetNamespace(types.EnvPodNamespace)

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("unable to get client config: %v", err)
	}

	s := server.New(ctx, cfg, namespace, webhookType)
	go func() {
		if err := s.ListenAndServe(); err != nil {
			logrus.Fatalf("Error %v webhook server failed: %v", webhookType, err)
		}
	}()

	logrus.Infof("Waiting for %v webhook to become ready", webhookType)
	cli := http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	webhookHealthEndpoint := fmt.Sprintf("https://localhost:%d/v1/healthz", webhookPort)
	running := false
	for start := time.Now(); time.Since(start) < defaultStartTimeout; {
		resp, err := cli.Get(webhookHealthEndpoint)
		if err != nil {
			logrus.WithError(err).Warnf("Error getting webhook health endpoint %v", webhookHealthEndpoint)
		} else if resp.StatusCode == 200 {
			logrus.Infof("Webhook %v is ready", webhookType)
			running = true
			break
		} else {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				logrus.Warnf("Webhook health endpoint return %d not 200: cannot read the body", resp.StatusCode)
			}
			bodyString := string(bodyBytes)
			logrus.Warnf("Webhook health endpoint return %d not 200: %v", resp.StatusCode, bodyString)
		}

		time.Sleep(2 * time.Second)
	}
	if !running {
		return fmt.Errorf("%v webhook is not ready after %v sec", webhookType, defaultStartTimeout)
	}

	logrus.Warnf("Started longhorn %s webhook server", webhookType)
	return nil
}
