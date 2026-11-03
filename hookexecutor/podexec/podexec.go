// Package podexec runs hook commands inside pods through the API server's
// pods/exec. Which command runs in which pod, and when, is decided by the
// hook kind, such as snapshothook for SnapshotGroups.
package podexec

import (
	"context"
	"errors"
	"io"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Executor runs one command in one container and returns when the process
// exits or the context ends. It returns the stream error unchanged so
// callers can classify whether the command ran. Output is discarded.
type Executor interface {
	Exec(ctx context.Context, namespace, pod, container string, command []string) error
}

// remoteCommandExecutor streams pods/exec the way kubectl exec does:
// WebSocket first, falling back to SPDY when the server does not accept
// the WebSocket connection. API servers older than v1.29 only accept SPDY;
// see https://kubernetes.io/blog/2024/08/20/websockets-transition/.
type remoteCommandExecutor struct {
	config     *rest.Config
	kubeClient kubernetes.Interface
}

// NewRemoteCommandExecutor returns the production Executor.
func NewRemoteCommandExecutor(config *rest.Config, kubeClient kubernetes.Interface) Executor {
	return &remoteCommandExecutor{config: config, kubeClient: kubeClient}
}

func (e *remoteCommandExecutor) Exec(ctx context.Context, namespace, pod, container string, command []string) error {
	request := e.kubeClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	websocketExecutor, err := remotecommand.NewWebSocketExecutor(e.config, "GET", request.URL().String())
	if err != nil {
		return err
	}
	spdyExecutor, err := remotecommand.NewSPDYExecutor(e.config, "POST", request.URL())
	if err != nil {
		return err
	}
	executor, err := remotecommand.NewFallbackExecutor(websocketExecutor, spdyExecutor, shouldFallbackToSPDY)
	if err != nil {
		return err
	}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: io.Discard,
		Stderr: io.Discard,
	})
	if statusErr := websocketRejectionStatus(err); statusErr != nil {
		// Return the API status itself, because errors.As cannot find it
		// inside the rejection error. Callers match on Forbidden and NotFound.
		return statusErr
	}
	return err
}

// shouldFallbackToSPDY follows the kubectl fallback rule with one
// exception: when the server rejects the WebSocket connection with an API
// status, such as Forbidden or NotFound, there is no fallback. SPDY would
// get the same answer, only as a plain string that callers can no longer
// match on.
func shouldFallbackToSPDY(err error) bool {
	if websocketRejectionStatus(err) != nil {
		return false
	}
	return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
}

// websocketRejectionStatus returns the API status carried by a rejected
// WebSocket connection, or nil when err is not one or carries none.
func websocketRejectionStatus(err error) *apierrors.StatusError {
	var upgradeErr *httpstream.UpgradeFailureError
	if !errors.As(err, &upgradeErr) {
		return nil
	}
	var statusErr *apierrors.StatusError
	if !errors.As(upgradeErr.Cause, &statusErr) {
		return nil
	}
	return statusErr
}
